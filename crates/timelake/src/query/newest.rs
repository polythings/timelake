//! Newest-first scans (spec 25): `ORDER BY time DESC LIMIT k`, or the newest `k` buckets of a
//! `GROUP BY time_bucket(…)`, reads time partitions newest first and stops once older ones can't
//! hold any of the `k` newest rows or buckets. This is threshold-style top-k (Fagin, Lotem and
//! Naor, PODS 2001), a partition's newest time its bound.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{SchemaRef, TimestampMicrosecondType};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, Sort};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::conjunction_opt;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties, SendableRecordBatchStream,
    execute_stream,
};
use futures::StreamExt;

use super::provider::{Kind, TimelakeProvider, scan_under};
use super::rollup::{bucket_width, unalias};
use super::settings::NEWEST_FIRST;

/// The optimizer rule: a sort by the time column, descending, with a limit, over a scan of a
/// time-partitioned Timelake table (through filters and plain projections) makes the scan
/// newest-first; so does a sort by a `time_bucket` of the time column grouped by.
#[derive(Debug)]
pub(super) struct NewestFirst;

impl OptimizerRule for NewestFirst {
    fn name(&self) -> &'static str {
        "timelake_newest_first"
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        let options = config.options();
        plan.transform_down(|node| {
            let LogicalPlan::Sort(sort) = &node else { return Ok(Transformed::no(node)) };
            match newest_first(sort, &options)? {
                Some(input) => Ok(Transformed::yes(LogicalPlan::Sort(Sort { input: Arc::new(input), ..sort.clone() }))),
                None => Ok(Transformed::no(node)),
            }
        })
    }
}

/// The sort's input with its scan made newest-first, if the sort qualifies.
/// The setting `newest-first` (the session's, else the table's) can turn it off.
fn newest_first(sort: &Sort, options: &ConfigOptions) -> DfResult<Option<LogicalPlan>> {
    let (Some(k), [by]) = (sort.fetch, sort.expr.as_slice()) else { return Ok(None) };
    let Expr::Column(column) = &by.expr else { return Ok(None) };
    if by.asc {
        return Ok(None);
    }
    // Down to the scan through what keeps every row's time (filters, column projections,
    // aliases), following the sort column, and through one aggregate grouping by a bucket of it.
    let mut name = column.name.clone();
    let mut bucket: Option<&Expr> = None;
    let mut filtered = false;
    let scan = scan_under(&sort.input, |node| match node {
        LogicalPlan::Filter(f) => {
            filtered |= bucket.is_none();
            Some(&f.input)
        }
        LogicalPlan::Projection(p) if bucket.is_none() => {
            let (e, _) = p.expr.iter().zip(p.schema.fields().iter()).find(|(_, f)| *f.name() == name)?;
            let Expr::Column(c) = unalias(e) else { return None };
            name.clone_from(&c.name);
            Some(&p.input)
        }
        // A filter above the aggregate (`HAVING`) drops buckets: more than k could be needed.
        LogicalPlan::Aggregate(a) if bucket.is_none() && !filtered => {
            let (e, _) = a.group_expr.iter().zip(a.schema.fields().iter()).find(|(_, f)| *f.name() == name)?;
            bucket = Some(e);
            Some(&a.input)
        }
        _ => None,
    });
    let Some(scan) = scan else { return Ok(None) };
    let Some(p) = TimelakeProvider::of_scan(scan) else { return Ok(None) };
    let contract = p.table().contract();
    let partitioned = contract.chunk.is_some();
    if !NEWEST_FIRST.of(options, contract) {
        return Ok(None);
    }
    let Some(time) = contract.time_column.as_deref() else { return Ok(None) };
    let width = match bucket {
        Some(e) => match bucket_width(e, time) {
            Some(w) => Some(w),
            None => return Ok(None),
        },
        None if name == time => None,
        None => return Ok(None),
    };
    if p.kind() != Kind::Data || p.is_routed() || !partitioned {
        return Ok(None);
    }
    let newest = provider_as_source(Arc::new(TimelakeProvider::newest(p.table().clone(), Newest { k, width })));
    let rewritten = sort.input.as_ref().clone().transform_up(|node| match node {
        LogicalPlan::TableScan(mut s) if s.table_name == scan.table_name => {
            s.source = Arc::clone(&newest);
            Ok(Transformed::yes(LogicalPlan::TableScan(s)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(Some(rewritten.data))
}

/// What a newest-first scan stops after: the `k` newest rows, or the rows of the `k` newest
/// buckets of `width` µs (aligned to the Unix epoch).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Newest {
    pub k: usize,
    pub width: Option<i64>,
}

/// The partitions' scans, newest first, read one after another until the rest can't matter.
#[derive(Debug)]
pub(super) struct NewestFirstExec {
    /// Each partition's scan and the newest time (µs) it can hold, newest first.
    waves: Vec<(Arc<dyn ExecutionPlan>, i64)>,
    newest: Newest,
    /// The time column in the scans' output, if it was read (else every partition is read).
    time: Option<usize>,
    /// What all output partitions have seen, shared: each moves on to an older time partition
    /// only while what they've seen together leaves it a chance (a plan runs once).
    seen: Arc<Mutex<Seen>>,
    properties: Arc<PlanProperties>,
}

impl NewestFirstExec {
    pub(super) fn try_new(waves: Vec<(Arc<dyn ExecutionPlan>, i64)>, newest: Newest, time: &str) -> DfResult<Self> {
        let first = waves
            .first()
            .ok_or_else(|| DataFusionError::Internal("a newest-first scan needs at least one partition".to_string()))?;
        let time = first.0.schema().index_of(time).ok();
        Ok(Self::with_waves(waves, newest, time))
    }

    fn with_waves(waves: Vec<(Arc<dyn ExecutionPlan>, i64)>, newest: Newest, time: Option<usize>) -> Self {
        // For buckets, output partition i reads partition i of each time partition's scan in turn,
        // so the aggregate above runs in parallel; rows go to one top-k (several measured 8% slower).
        let partitions = match newest.width {
            Some(_) => waves.iter().map(|(p, _)| p.output_partitioning().partition_count()).max().unwrap_or(1),
            None => 1,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(waves[0].0.schema()),
            Partitioning::UnknownPartitioning(partitions.max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let seen = Arc::new(Mutex::new(Seen::new(newest)));
        Self { waves, newest, time, seen, properties: Arc::new(properties) }
    }

    fn schema(&self) -> SchemaRef {
        self.waves[0].0.schema()
    }
}

impl DisplayAs for NewestFirstExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (n, k) = (self.waves.len(), self.newest.k);
        match self.newest.width {
            None => write!(f, "NewestFirstExec: {n} partitions, newest first, stop at {k} rows"),
            Some(w) => write!(f, "NewestFirstExec: {n} partitions, newest first, stop at {k} buckets of {w} µs"),
        }
    }
}

impl ExecutionPlan for NewestFirstExec {
    fn name(&self) -> &'static str {
        "NewestFirstExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.waves.iter().map(|(p, _)| p).collect()
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let waves = children.into_iter().zip(self.waves.iter().map(|(_, newest)| *newest)).collect();
        Ok(Arc::new(Self::with_waves(waves, self.newest, self.time)))
    }

    // Filters above (the query's, and the sort's runtime filter) go into every partition's scan.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DfResult<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    // A scan that doesn't filter its rows (filter pushdown off) gets the filters it didn't take
    // right above it, below this node: the rows counted toward k must be the ones the query keeps.
    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DfResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let filters = &child_pushdown_result.parent_filters;
        let mut waves = self.waves.clone();
        let mut changed = false;
        for (i, (scan, _)) in waves.iter_mut().enumerate() {
            let missed = filters.iter().filter(|f| matches!(f.child_results.get(i), Some(PushedDown::No)));
            if let Some(predicate) = conjunction_opt(missed.map(|f| Arc::clone(&f.filter))) {
                *scan = Arc::new(FilterExec::try_new(predicate, Arc::clone(scan))?);
                changed = true;
            }
        }
        let propagation = FilterPushdownPropagation::with_parent_pushdown_result(vec![PushedDown::Yes; filters.len()]);
        Ok(if changed {
            propagation.with_updated_node(Arc::new(Self::with_waves(waves, self.newest, self.time)))
        } else {
            propagation
        })
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
        let state = NewestFirstRead {
            waves: self.waves.clone(),
            partition: (self.properties.output_partitioning().partition_count() > 1).then_some(partition),
            next: 0,
            current: None,
            seen: Arc::clone(&self.seen),
            time: self.time,
            context,
        };
        let stream = futures::stream::try_unfold(state, NewestFirstRead::next_batch);
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream)))
    }
}

/// The state of a newest-first read.
struct NewestFirstRead {
    waves: Vec<(Arc<dyn ExecutionPlan>, i64)>,
    /// The output partition: partition `partition` of each wave, or all of each (`None`).
    partition: Option<usize>,
    next: usize,
    current: Option<SendableRecordBatchStream>,
    seen: Arc<Mutex<Seen>>,
    time: Option<usize>,
    context: Arc<TaskContext>,
}

/// The newest of what a read has seen, up to `k`.
#[derive(Debug)]
enum Seen {
    /// Row times (a min-heap: its top is the k-th newest).
    Rows { newest: BinaryHeap<Reverse<i64>>, k: usize },
    /// Distinct starts of buckets of `width` µs.
    Buckets { newest: BTreeSet<i64>, k: usize, width: i64 },
}

impl Seen {
    fn new(newest: Newest) -> Self {
        match newest.width {
            None => Self::Rows { newest: BinaryHeap::new(), k: newest.k },
            Some(width) => Self::Buckets { newest: BTreeSet::new(), k: newest.k, width },
        }
    }

    /// What it keeps.
    fn limit(&self) -> Newest {
        match self {
            Self::Rows { k, .. } => Newest { k: *k, width: None },
            Self::Buckets { k, width, .. } => Newest { k: *k, width: Some(*width) },
        }
    }

    /// `self` with what `other` has seen too.
    fn absorb(&mut self, other: Seen) {
        match other {
            Self::Rows { newest, .. } => newest.into_iter().for_each(|t| self.add(t.0)),
            Self::Buckets { newest, .. } => newest.into_iter().for_each(|t| self.add(t)),
        }
    }

    /// The k-th newest, once there are `k`.
    fn kth(&self) -> Option<i64> {
        match self {
            Self::Rows { newest, k } => (newest.len() >= *k).then(|| newest.peek().map(|t| t.0)).flatten(),
            Self::Buckets { newest, k, .. } => (newest.len() >= *k).then(|| newest.first().copied()).flatten(),
        }
    }

    fn add(&mut self, t: i64) {
        match self {
            Self::Rows { newest, k } => {
                if newest.len() < *k {
                    newest.push(Reverse(t));
                } else if newest.peek().is_some_and(|m| t > m.0) {
                    newest.pop();
                    newest.push(Reverse(t));
                }
            }
            Self::Buckets { newest, k, width } => {
                let start = t - t.rem_euclid(*width);
                // Most rows are in a bucket already kept, or older than all kept.
                if newest.len() >= *k && newest.first().is_some_and(|&kth| start <= kth) || newest.contains(&start) {
                    return;
                }
                newest.insert(start);
                if newest.len() > *k {
                    newest.pop_first();
                }
            }
        }
    }
}

impl NewestFirstRead {
    async fn next_batch(mut self) -> DfResult<Option<(RecordBatch, Self)>> {
        loop {
            if let Some(stream) = &mut self.current {
                match stream.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        self.observe(&batch);
                        return Ok(Some((batch, self)));
                    }
                    None => self.current = None,
                }
            }
            let Some((plan, newest)) = self.waves.get(self.next) else { return Ok(None) };
            // The partitions left are no newer than this one: none can hold one of the k newest
            // rows, or a row of the k newest buckets (each starts after this partition's newest).
            // What's seen only grows, so a partition that stops here was right to.
            let kth = self.seen.lock().unwrap_or_else(PoisonError::into_inner).kth();
            if self.time.is_some() && kth.is_some_and(|t| t > *newest) {
                return Ok(None);
            }
            self.next += 1;
            self.current = match self.partition {
                None => Some(execute_stream(Arc::clone(plan), Arc::clone(&self.context))?),
                Some(i) if i < plan.output_partitioning().partition_count() => {
                    Some(plan.execute(i, Arc::clone(&self.context))?)
                }
                Some(_) => None,
            };
        }
    }

    fn observe(&mut self, batch: &RecordBatch) {
        let Some(times) = self.time.map(|i| batch.column(i)) else { return };
        let Some(times) = times.as_primitive_opt::<TimestampMicrosecondType>() else { return };
        // The batch's own newest first, then one lock to add them; rows no newer than the k-th seen
        // so far (most, once there are k) are passed over.
        let (limit, kth) = {
            let seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
            (seen.limit(), seen.kth().unwrap_or(i64::MIN))
        };
        let mut own = Seen::new(limit);
        for t in times.iter().flatten().filter(|&t| t > kth) {
            own.add(t);
        }
        self.seen.lock().unwrap_or_else(PoisonError::into_inner).absorb(own);
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{T0, answer, batch, faulty_lake, opts};
    use crate::{AppendOptions, Session};

    const MINUTE: i64 = 60_000_000;

    /// With filter pushdown off, the scan returns rows the filter above drops: newest-first must
    /// count only the rows that pass it, or it stops before reaching the older partitions that hold
    /// them.
    #[tokio::test]
    async fn newest_first_counts_filtered_rows_without_pushdown() {
        let (lake, _) = faulty_lake();
        let t = lake.create_table("iot.r", &crate::testing::schema(), opts()).await.unwrap();
        // Three days, one reading a minute; values grow with time, so `value % 10000 < 100` (which file
        // statistics can't prune with) is in the first day.
        let rows: Vec<(i64, i32, f64)> = (0..3 * 24 * 60).map(|i| (T0 + i * MINUTE, 1, i as f64)).collect();
        t.append(&[batch(&rows)], AppendOptions { maintain: Some(false), ..Default::default() }).await.unwrap();
        for sql in [
            "SELECT time, value FROM iot.r WHERE value % 10000 < 100 ORDER BY time DESC LIMIT 10",
            "SELECT time_bucket(INTERVAL '10 minutes', time) AS b, count(*) AS n FROM iot.r WHERE value % 10000 < 100
             GROUP BY 1 ORDER BY 1 DESC LIMIT 3",
        ] {
            let mut answers = vec![];
            for on in ["true", "false"] {
                let s = Session::new(lake.clone());
                s.sql("SET timelake.filter_pushdown = false").await.unwrap();
                s.sql(&format!("SET timelake.newest_first = {on}")).await.unwrap();
                let plan = answer(&s, &format!("EXPLAIN {sql}")).await.unwrap();
                assert_eq!(plan.contains("NewestFirstExec"), on == "true", "{sql}\n{plan}");
                answers.push(answer(&s, sql).await.unwrap());
            }
            assert_eq!(answers[0], answers[1], "{sql}");
            assert!(answers[0].contains("| 99.0  |") || answers[0].contains("| 10 |"), "{sql}\n{}", answers[0]);
        }
    }

    /// Planning a newest-first scan reads the partitions' footers at once, not one after another.
    #[tokio::test(start_paused = true)]
    async fn partitions_are_planned_concurrently() {
        const RTT: std::time::Duration = std::time::Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let mut o = opts();
        // No head, so the footers aren't in it.
        o.properties.insert(crate::property::HEAD_FORMAT.into(), "none".into());
        let t = lake.create_table("iot.r", &crate::testing::schema(), o).await.unwrap();
        let rows: Vec<(i64, i32, f64)> = (0..6).map(|d| (T0 + d * 24 * 60 * MINUTE, 1, 1.0)).collect();
        t.append(&[batch(&rows)], AppendOptions { maintain: Some(false), ..Default::default() }).await.unwrap();
        let s = Session::new(crate::testing::reopen(&store));
        // Loads the table, but no footer.
        s.sql("DESCRIBE iot.r").await.unwrap().collect().await.unwrap();
        store.set_latency(RTT);
        let started = tokio::time::Instant::now();
        let df = s.sql("SELECT * FROM iot.r ORDER BY time DESC LIMIT 3").await.unwrap();
        df.create_physical_plan().await.unwrap();
        let round_trips = started.elapsed().as_millis() / RTT.as_millis();
        assert!(round_trips <= 2, "{round_trips} round trips for 6 partitions");
    }

    /// Newest-first gives the rows a full scan gives, across partitions, with filters, and
    /// shows in the plan.
    #[tokio::test]
    async fn newest_first_gives_the_rows_a_full_scan_gives() {
        let (lake, _) = faulty_lake();
        let t = lake.create_table("iot.r", &crate::testing::schema(), opts()).await.unwrap();
        // Five days, one reading a minute, each from one of 7 devices: every time is distinct.
        let rows: Vec<(i64, i32, f64)> =
            (0..5 * 24 * 60).map(|i| (T0 + i * MINUTE, i32::try_from(i % 7).unwrap(), i as f64)).collect();
        t.append(&[batch(&rows)], AppendOptions { maintain: Some(false), ..Default::default() }).await.unwrap();
        let queries = [
            "SELECT time, device, value FROM iot.r ORDER BY time DESC LIMIT 3",
            // More rows than the newest day holds.
            "SELECT time, value FROM iot.r ORDER BY time DESC LIMIT 2000",
            "SELECT * FROM iot.r WHERE time <= TIMESTAMP '2026-01-03 12:00:00' ORDER BY time DESC LIMIT 5",
            "SELECT time FROM iot.r WHERE device = 3 AND value < 4000 ORDER BY time DESC LIMIT 50",
            // The newest buckets: within a day, across days, with more keys, renamed, filtered first.
            "SELECT time_bucket(INTERVAL '1 hour', time) AS h, max(value) FROM iot.r GROUP BY 1 ORDER BY 1 DESC LIMIT 5",
            "SELECT time_bucket(INTERVAL '1 hour', time) AS h, count(*) FROM iot.r GROUP BY 1 ORDER BY h DESC LIMIT 30",
            "SELECT date_bin(INTERVAL '1 minute', time) AS m, device, avg(value) FROM iot.r WHERE device < 3
             GROUP BY 1, 2 ORDER BY 1 DESC LIMIT 4",
            "SELECT b, n FROM (SELECT time_bucket(INTERVAL '6 hours', time) AS b, count(*) AS n FROM iot.r
             WHERE time < TIMESTAMP '2026-01-04 03:00:00' GROUP BY 1) ORDER BY b DESC LIMIT 6",
        ];
        for sql in queries {
            let mut answers = vec![];
            for on in ["true", "false"] {
                let s = Session::new(lake.clone());
                s.sql(&format!("SET timelake.newest_first = {on}")).await.unwrap();
                let plan = answer(&s, &format!("EXPLAIN {sql}")).await.unwrap();
                assert_eq!(plan.contains("NewestFirstExec"), on == "true", "{sql}\n{plan}");
                answers.push(answer(&s, sql).await.unwrap());
            }
            assert_eq!(answers[0], answers[1], "{sql}");
        }
        // Not these: groups dropped after grouping, the oldest buckets, a bucket from another origin.
        let s = Session::new(lake.clone());
        for sql in [
            "SELECT time_bucket(INTERVAL '1 hour', time) AS h, max(value) AS m FROM iot.r GROUP BY 1 HAVING max(value) > 10
             ORDER BY 1 DESC LIMIT 5",
            "SELECT time_bucket(INTERVAL '1 hour', time) AS h, max(value) FROM iot.r GROUP BY 1 ORDER BY 1 LIMIT 5",
            "SELECT date_bin(INTERVAL '1 hour', time, TIMESTAMP '2026-01-01 00:30:00') AS h, max(value) FROM iot.r
             GROUP BY 1 ORDER BY 1 DESC LIMIT 5",
        ] {
            let plan = answer(&s, &format!("EXPLAIN {sql}")).await.unwrap();
            assert!(!plan.contains("NewestFirstExec"), "{sql}\n{plan}");
        }
    }
}
