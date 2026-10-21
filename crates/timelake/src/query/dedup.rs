//! Dedup on read (spec 12): files that may hold the same `(series…, time)` are merged in key order
//! and only the newest write of each key is kept, so queries never see duplicates that compaction
//! hasn't removed yet.
//!
//! Every file is sorted by the key and free of duplicates on its own (a write keeps the last of
//! equal keys), so only files of one partition whose key bounds overlap can share a key. Such files
//! are read with their commit rank as a constant column, split by a hash of the series (every copy
//! of a key lands in the same slice), each slice merged by `(series…, time, rank)`, and [`DedupExec`]
//! keeps the last row of each key in each slice.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::kernels::cmp::not_distinct;
use arrow::compute::{and, concat_batches, filter_record_batch};
use arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{ChildFilterDescription, FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use iceberg::spec::{Datum, PrimitiveLiteral};

use super::stats::field_id;
use crate::table::LiveEntry;
use crate::write::file_partition;

/// The column that carries each file's commit rank in a deduplicated scan (dropped before output).
pub(super) const RANK: &str = "__timelake_rank";

/// Groups of two or more `files` (indices) that may hold rows with the same key: files of one
/// partition whose bounds overlap on every key column. A file in no group shares no key.
///
/// `keys` are the sort columns, `(series…, time)`, as indices into `schema`.
pub(super) fn overlapping_groups(schema: &SchemaRef, keys: &[usize], files: &[LiveEntry]) -> Vec<Vec<usize>> {
    let Some((&time, _)) = keys.split_last() else { return vec![] };
    let ids: Vec<Option<i32>> = keys.iter().map(|&k| field_id(schema, k)).collect();
    let time_id = field_id(schema, time);
    let mut by_partition: BTreeMap<Option<i32>, Vec<usize>> = BTreeMap::new();
    for (i, f) in files.iter().enumerate() {
        by_partition.entry(file_partition(&f.data_file)).or_default().push(i);
    }
    let mut parent: Vec<usize> = (0..files.len()).collect();
    for mut members in by_partition.into_values() {
        // Sweep in order of the earliest time: a file can only overlap the files still open.
        let span = |i: usize| time_span(&files[i], time_id);
        members.sort_by_key(|&i| span(i));
        let mut open: Vec<usize> = vec![];
        for &i in &members {
            let (start, _) = span(i);
            open.retain(|&j| span(j).1 >= start);
            for &j in &open {
                if ids.iter().all(|&id| bounds_overlap(&files[i], &files[j], id)) {
                    union(&mut parent, i, j);
                }
            }
            open.push(i);
        }
    }
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for i in 0..files.len() {
        groups.entry(find(&mut parent, i)).or_default().push(i);
    }
    groups.into_values().filter(|g| g.len() > 1).collect()
}

/// The file's time bounds in the column's unit (unbounded where the manifest has none).
fn time_span(f: &LiveEntry, id: Option<i32>) -> (i64, i64) {
    let bound = |b: Option<&Datum>| match b.map(Datum::literal) {
        Some(PrimitiveLiteral::Long(v)) => Some(*v),
        _ => None,
    };
    let Some(id) = id else { return (i64::MIN, i64::MAX) };
    let df = &f.data_file;
    (bound(df.lower_bounds().get(&id)).unwrap_or(i64::MIN), bound(df.upper_bounds().get(&id)).unwrap_or(i64::MAX))
}

/// Whether two files' bounds on a column overlap (or can't be compared).
fn bounds_overlap(a: &LiveEntry, b: &LiveEntry, id: Option<i32>) -> bool {
    let Some(id) = id else { return true };
    let before =
        |x: &LiveEntry, y: &LiveEntry| match (x.data_file.upper_bounds().get(&id), y.data_file.lower_bounds().get(&id))
        {
            (Some(hi), Some(lo)) => hi < lo,
            _ => false,
        };
    !before(a, b) && !before(b, a)
}

fn find(parent: &mut [usize], mut i: usize) -> usize {
    while parent[i] != i {
        parent[i] = parent[parent[i]];
        i = parent[i];
    }
    i
}

fn union(parent: &mut [usize], a: usize, b: usize) {
    let (ra, rb) = (find(parent, a), find(parent, b));
    parent[ra.max(rb)] = ra.min(rb);
}

/// The fewest values `filters` (all must hold) allow for one of the `series` columns: `col = literal` or
/// `col IN (literals…)`. `None` if no filter pins a series column.
pub(super) fn series_values(filters: &[Expr], series: &[String]) -> Option<usize> {
    let pinned = |e: &Expr| match e {
        Expr::BinaryExpr(b) if b.op == Operator::Eq => match (b.left.as_ref(), b.right.as_ref()) {
            (Expr::Column(c), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(c)) => {
                series.contains(&c.name).then_some(1)
            }
            _ => None,
        },
        Expr::InList(l) if !l.negated && l.list.iter().all(|v| matches!(v, Expr::Literal(..))) => {
            matches!(l.expr.as_ref(), Expr::Column(c) if series.contains(&c.name)).then_some(l.list.len())
        }
        _ => None,
    };
    filters.iter().flat_map(split_conjunction).filter_map(pinned).min()
}

/// Which rows [`DedupExec`] returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Keep {
    /// The newest write of each key: the deduplicated rows.
    Latest,
    /// The older copies of each key (strategy keys-first: what a later scan must leave out).
    Older,
    /// Every row: only the series order (for the signal aggregates without dedup).
    All,
}

/// Keeps the newest write of each key. Its input is sorted by the key and the commit rank (each
/// partition on its own); it splits the rows into `slices` by a hash of the series (of the key,
/// without a series), so every copy of a key lands in the same slice, merges each slice in order
/// and keeps the last row of each key. `keys` are `(series…, time)`.
///
/// The split is built here, not left to the optimizer: DataFusion would replace an order-preserving
/// repartition with a plain one and a full sort of each slice unless `prefer_existing_sort` is on.
#[derive(Debug)]
pub(super) struct DedupExec {
    input: Arc<dyn ExecutionPlan>,
    /// The order-preserving hash split of `input`, shared by the output partitions.
    split: Arc<RepartitionExec>,
    /// Key column indices in the input schema.
    keys: Vec<usize>,
    ordering: LexOrdering,
    by_series: bool,
    keep: Keep,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DedupExec {
    /// `ordering` is the input's sort order, the key columns `keys` first; `slices` the number of
    /// output partitions, split by a hash of the series (`by_series`) or of the whole key.
    pub(super) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        keys: Vec<usize>,
        ordering: LexOrdering,
        slices: usize,
        by_series: bool,
        keep: Keep,
    ) -> DfResult<Self> {
        let schema = input.schema();
        // Copies share the series, so splitting by it alone is enough, and leaves each series whole
        // in one slice, sorted by time: what windows and groups by series need downstream. When a
        // query reads only a few series, splitting by the whole key keeps every core busy.
        let by = if by_series && keys.len() > 1 { &keys[..keys.len() - 1] } else { &keys[..] };
        let hash = by.iter().map(|&k| Arc::new(Column::new(schema.field(k).name(), k)) as Arc<dyn PhysicalExpr>);
        let partitioning = Partitioning::Hash(hash.collect(), slices.max(1));
        let split = Arc::new(RepartitionExec::try_new(input.clone(), partitioning.clone())?.with_preserve_order());
        let eq = EquivalenceProperties::new_with_orderings(schema, [ordering.clone()]);
        let properties =
            Arc::new(PlanProperties::new(eq, partitioning, EmissionType::Incremental, Boundedness::Bounded));
        Ok(Self { input, split, keys, ordering, by_series, keep, properties, metrics: ExecutionPlanMetricsSet::new() })
    }
}

impl DisplayAs for DedupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let schema = self.input.schema();
        let keys: Vec<&str> = self.keys.iter().map(|&k| schema.field(k).name().as_str()).collect();
        let slices = self.properties.output_partitioning().partition_count();
        let keep = match self.keep {
            Keep::Latest => "last",
            Keep::Older => "older copies",
            Keep::All => "every row",
        };
        write!(f, "DedupExec: keep {keep} of [{}], {slices} slices by hash", keys.join(", "))
    }
}

impl ExecutionPlan for DedupExec {
    fn name(&self) -> &'static str {
        "DedupExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.ordering.clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let slices = self.properties.output_partitioning().partition_count();
        let (keys, ordering, by_series, keep) = (self.keys.clone(), self.ordering.clone(), self.by_series, self.keep);
        Ok(Arc::new(Self::try_new(children.swap_remove(0), keys, ordering, slices, by_series, keep)?))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
        let schema = self.input.schema();
        let mut state = match self.keep {
            Keep::Latest => State::Latest(LastOfKey { keys: self.keys.clone(), held: None }),
            Keep::Older => State::Older(OlderCopies { keys: self.keys.clone(), last: None }),
            Keep::All => State::All,
        };
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let input = self.split.execute(partition, context)?.map(Some).chain(futures::stream::once(async { None }));
        let output = input.filter_map(move |item| {
            let timer = baseline.elapsed_compute().timer();
            let out = match item {
                Some(Ok(batch)) => state.push(&batch).transpose(),
                Some(Err(e)) => Some(Err(e)),
                None => state.finish().map(Ok),
            };
            timer.done();
            if let Some(Ok(batch)) = &out {
                baseline.record_output(batch.num_rows());
            }
            futures::future::ready(out)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DfResult<FilterDescription> {
        // A filter on the key alone keeps or drops every copy of a row together, so it can run
        // before dedup (and prune row groups); any other filter must see only the newest copy.
        let keys: HashSet<usize> = self.keys.iter().copied().collect();
        let child = ChildFilterDescription::from_child_with_allowed_indices(&parent_filters, keys, &self.input)?;
        Ok(FilterDescription::new().with_child(child))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DfResult<Arc<Statistics>> {
        // Fewer rows than the input: the counts are an upper bound.
        Ok(Arc::new(Arc::unwrap_or_clone(self.input.partition_statistics(partition)?).to_inexact()))
    }
}

/// The streaming state of a [`DedupExec`] partition.
enum State {
    Latest(LastOfKey),
    Older(OlderCopies),
    All,
}

impl State {
    fn push(&mut self, batch: &RecordBatch) -> DfResult<Option<RecordBatch>> {
        match self {
            Self::Latest(s) => s.push(batch),
            Self::Older(s) => s.push(batch),
            Self::All => Ok(Some(batch.clone())),
        }
    }

    fn finish(&mut self) -> Option<RecordBatch> {
        match self {
            Self::Latest(s) => s.finish(),
            // The last row seen has no newer copy.
            Self::Older(_) | Self::All => None,
        }
    }
}

/// The older copies of each key: every row followed by a row with the same key.
struct OlderCopies {
    keys: Vec<usize>,
    /// The previous batch's last row, until the next batch shows whether it has a newer copy.
    last: Option<RecordBatch>,
}

impl OlderCopies {
    fn push(&mut self, batch: &RecordBatch) -> DfResult<Option<RecordBatch>> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(None);
        }
        let mut out = vec![];
        if let Some(last) = self.last.replace(batch.slice(n - 1, 1))
            && same_keys(&self.keys, &last, &batch.slice(0, 1))?.value(0)
        {
            out.push(last);
        }
        if n > 1 {
            let same = same_keys(&self.keys, &batch.slice(0, n - 1), &batch.slice(1, n - 1))?;
            if same.true_count() > 0 {
                out.push(filter_record_batch(&batch.slice(0, n - 1), &same)?);
            }
        }
        match out.len() {
            0 => Ok(None),
            1 => Ok(out.pop()),
            _ => Ok(Some(concat_batches(&batch.schema(), &out)?)),
        }
    }
}

/// The streaming state of [`DedupExec`]. Each batch keeps the last of equal keys within it; the
/// previous batch is held back until the next one shows whether its last row has a newer copy.
/// Without copies (the common case) batches pass through unchanged.
struct LastOfKey {
    keys: Vec<usize>,
    held: Option<RecordBatch>,
}

impl LastOfKey {
    /// The held-back batch, now that `batch` follows it (minus its last row if `batch` starts with
    /// a newer copy of that row's key).
    fn push(&mut self, batch: &RecordBatch) -> DfResult<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let current = self.last_of_each_key(batch)?;
        let out = match self.held.replace(current) {
            None => None,
            Some(held) => {
                let n = held.num_rows();
                let next = self.held.as_ref().map(|b| b.slice(0, 1));
                let superseded = next.map(|next| same_keys(&self.keys, &held.slice(n - 1, 1), &next)).transpose()?;
                let held = if superseded.is_some_and(|s| s.value(0)) { held.slice(0, n - 1) } else { held };
                (held.num_rows() > 0).then_some(held)
            }
        };
        Ok(out)
    }

    /// The last batch, at the end of the input.
    fn finish(&mut self) -> Option<RecordBatch> {
        self.held.take()
    }

    /// `batch` without rows whose next row has the same key (the batch itself if there are none).
    fn last_of_each_key(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        let n = batch.num_rows();
        if n < 2 {
            return Ok(batch.clone());
        }
        let same = same_keys(&self.keys, &batch.slice(0, n - 1), &batch.slice(1, n - 1))?;
        if same.true_count() == 0 {
            return Ok(batch.clone());
        }
        let keep: BooleanArray = same.iter().map(|s| Some(s != Some(true))).chain([Some(true)]).collect();
        Ok(filter_record_batch(batch, &keep)?)
    }
}

/// Row by row, whether `a` and `b` (as long) have the same `keys`; nulls equal nulls.
fn same_keys(keys: &[usize], a: &RecordBatch, b: &RecordBatch) -> DfResult<BooleanArray> {
    let mut same: Option<BooleanArray> = None;
    for &k in keys {
        let column = not_distinct(a.column(k), b.column(k))?;
        same = Some(match same {
            Some(s) => and(&s, &column)?,
            None => column,
        });
    }
    Ok(same.unwrap_or_else(|| BooleanArray::from(vec![true; a.num_rows()])))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(keys: &[i32], tags: &[&str]) -> RecordBatch {
        let schema =
            Schema::new(vec![Field::new("k", DataType::Int32, false), Field::new("tag", DataType::Utf8, false)]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(keys.to_vec())), Arc::new(StringArray::from(tags.to_vec()))],
        )
        .unwrap()
    }

    fn tags(batches: &[RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let t = b.column(1).as_any().downcast_ref::<StringArray>().unwrap().clone();
                (0..t.len()).map(move |i| t.value(i).to_string())
            })
            .collect()
    }

    /// A file another engine wrote, unsorted and repeating a key, is deduplicated on its own; a
    /// Timelake file next to it is read as it is.
    #[tokio::test]
    async fn an_unsorted_file_is_deduplicated_on_its_own() {
        use crate::testing::{T0, append_unsorted, batch as rows, faulty_lake, opts, schema};

        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let day =
            timelake_format::rows::sort_and_split(&t.schema(), t.contract(), &[rows(&[(T0, 1, 0.0)])], 0).unwrap()[0].0;
        let t = append_unsorted(&t, day, rows(&[(T0 + 5, 2, 1.0), (T0, 1, 2.0), (T0 + 5, 2, 3.0)])).await;
        let later = T0 + 2 * 86_400_000_000;
        t.append(&[rows(&[(later, 1, 4.0)])], crate::append::AppendOptions::default()).await.unwrap();
        let session = crate::Session::new(lake.clone());
        let sql = "SELECT device, value FROM iot.r ORDER BY time, device";
        let out = session.sql(sql).await.unwrap().collect().await.unwrap();
        let values: Vec<f64> = out
            .iter()
            .flat_map(|b| b.column(1).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().values().to_vec())
            .collect();
        assert_eq!(values, [2.0, 3.0, 4.0]);
    }

    /// The older-copies mode returns exactly the rows the default mode drops.
    #[test]
    fn lists_the_older_copies_across_batches() {
        let mut state = State::Older(OlderCopies { keys: vec![0], last: None });
        let mut out = vec![];
        for b in [batch(&[1, 1, 2], &["a", "b", "c"]), batch(&[2, 3], &["d", "e"]), batch(&[3, 3], &["f", "g"])] {
            out.extend(state.push(&b).unwrap());
        }
        out.extend(state.finish());
        assert_eq!(tags(&out), ["a", "c", "e", "f"]);
    }

    /// Runs of equal keys that span batches keep their last row, wherever it is.
    #[test]
    fn keeps_the_last_row_of_each_key_across_batches() {
        let mut state = LastOfKey { keys: vec![0], held: None };
        let mut out = vec![];
        for b in
            [batch(&[1, 1, 2], &["a", "b", "c"]), batch(&[2, 3], &["d", "e"]), batch(&[3], &["f"]), batch(&[], &[])]
        {
            out.extend(state.push(&b).unwrap());
        }
        out.extend(state.finish());
        assert_eq!(tags(&out), ["b", "d", "f"]);
        // Nulls equal nulls, and a run can span a whole batch.
        let mut state = LastOfKey { keys: vec![0], held: None };
        let nullable = |keys: &[Option<i32>], tags: &[&str]| {
            let schema =
                Schema::new(vec![Field::new("k", DataType::Int32, true), Field::new("tag", DataType::Utf8, false)]);
            RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(Int32Array::from(keys.to_vec())), Arc::new(StringArray::from(tags.to_vec()))],
            )
            .unwrap()
        };
        let mut out = vec![];
        for b in
            [nullable(&[None, None], &["a", "b"]), nullable(&[None], &["c"]), nullable(&[None, Some(1)], &["d", "e"])]
        {
            out.extend(state.push(&b).unwrap());
        }
        out.extend(state.finish());
        assert_eq!(tags(&out), ["d", "e"]);
    }
}
