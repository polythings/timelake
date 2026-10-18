//! Newest-first scans (#72): `ORDER BY time DESC LIMIT k` over a time-partitioned table reads the
//! newest partition first, in parallel within it, then each older one only while it can still
//! hold one of the `k` newest rows: once `k` rows newer than an older partition's newest time are
//! in, the rest is never read. Each partition is planned as the ordinary scan of its files, so
//! pruning, dedup and the sort above work as before; this only orders and stops them.
//!
//! The idea is threshold-style top-k (Fagin, Lotem and Naor, PODS 2001): visit candidates in order
//! of their upper bound, here a partition's newest time, and stop once the k-th result beats every
//! bound left.

use std::any::Any;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{SchemaRef, TimestampMicrosecondType};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, Sort};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream, execute_stream,
};
use futures::StreamExt;

use super::provider::{Kind, TimelakeProvider};
use super::settings::SessionSettings;
use crate::property;

/// The optimizer rule: a sort by the time column, descending, with a limit, over a scan of a
/// time-partitioned Timelake table (through filters and plain projections) makes the scan
/// newest-first.
#[derive(Debug)]
pub(super) struct NewestFirst;

impl OptimizerRule for NewestFirst {
    fn name(&self) -> &'static str {
        "timelake_newest_first"
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        let options = config.options();
        let session = options.extensions.get::<SessionSettings>().and_then(|s| s.flag(property::NEWEST_FIRST));
        plan.transform_down(|node| {
            let LogicalPlan::Sort(sort) = &node else { return Ok(Transformed::no(node)) };
            match newest_first(sort, session)? {
                Some(input) => Ok(Transformed::yes(LogicalPlan::Sort(Sort { input: Arc::new(input), ..sort.clone() }))),
                None => Ok(Transformed::no(node)),
            }
        })
    }
}

/// The sort's input with its scan made newest-first, if the sort qualifies.
/// `session` is the session's `timelake.newest_first`, else the table's setting applies.
fn newest_first(sort: &Sort, session: Option<bool>) -> DfResult<Option<LogicalPlan>> {
    let (Some(k), [by]) = (sort.fetch, sort.expr.as_slice()) else { return Ok(None) };
    let Expr::Column(column) = &by.expr else { return Ok(None) };
    if by.asc {
        return Ok(None);
    }
    // Down to the scan through what keeps every row's time: filters, column projections, aliases.
    let mut input = sort.input.as_ref();
    loop {
        input = match input {
            LogicalPlan::Filter(f) => &f.input,
            LogicalPlan::SubqueryAlias(s) => &s.input,
            LogicalPlan::Projection(p) if p.expr.iter().all(|e| matches!(e, Expr::Column(_))) => &p.input,
            LogicalPlan::TableScan(_) => break,
            _ => return Ok(None),
        };
    }
    let LogicalPlan::TableScan(scan) = input else { return Ok(None) };
    let provider = source_as_provider(&scan.source)?;
    let Some(p) = (provider.as_ref() as &dyn Any).downcast_ref::<TimelakeProvider>() else { return Ok(None) };
    let contract = p.table().contract();
    let partitioned = contract.chunk.is_some();
    if !session.unwrap_or(contract.newest_first) {
        return Ok(None);
    }
    if p.kind() != Kind::Data || p.is_routed() || !partitioned || contract.time_column.as_deref() != Some(&column.name)
    {
        return Ok(None);
    }
    let newest = provider_as_source(Arc::new(TimelakeProvider::newest(p.table().clone(), k)));
    let rewritten = sort.input.as_ref().clone().transform_up(|node| match node {
        LogicalPlan::TableScan(mut s) if s.table_name == scan.table_name => {
            s.source = Arc::clone(&newest);
            Ok(Transformed::yes(LogicalPlan::TableScan(s)))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(Some(rewritten.data))
}

/// The partitions' scans, newest first, read one after another until the rest can't matter.
#[derive(Debug)]
pub(super) struct NewestFirstExec {
    /// Each partition's scan and the newest time (µs) it can hold, newest first.
    waves: Vec<(Arc<dyn ExecutionPlan>, i64)>,
    k: usize,
    /// The time column in the scans' output, if it was read (else every partition is read).
    time: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl NewestFirstExec {
    pub(super) fn try_new(waves: Vec<(Arc<dyn ExecutionPlan>, i64)>, k: usize, time: &str) -> DfResult<Self> {
        let schema = waves
            .first()
            .map(|(p, _)| p.schema())
            .ok_or_else(|| DataFusionError::Internal("a newest-first scan needs at least one partition".to_string()))?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let time = schema.index_of(time).ok();
        Ok(Self { waves, k, time, properties: Arc::new(properties) })
    }

    fn schema(&self) -> SchemaRef {
        self.waves[0].0.schema()
    }
}

impl DisplayAs for NewestFirstExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NewestFirstExec: {} partitions, newest first, stop at {} rows", self.waves.len(), self.k)
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
        Ok(Arc::new(Self { waves, k: self.k, time: self.time, properties: Arc::clone(&self.properties) }))
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

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DfResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!("NewestFirstExec has one partition, not {partition}")));
        }
        let state = NewestFirstRead {
            waves: self.waves.clone(),
            next: 0,
            current: None,
            newest: BinaryHeap::new(),
            k: self.k,
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
    next: usize,
    current: Option<SendableRecordBatchStream>,
    /// The `k` newest times read so far (a min-heap: its top is the k-th newest).
    newest: BinaryHeap<Reverse<i64>>,
    k: usize,
    time: Option<usize>,
    context: Arc<TaskContext>,
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
            // The partitions left are no newer than this one: none can hold one of the k newest.
            if self.time.is_some() && self.newest.len() >= self.k && self.newest.peek().is_some_and(|t| t.0 > *newest) {
                return Ok(None);
            }
            self.current = Some(execute_stream(Arc::clone(plan), Arc::clone(&self.context))?);
            self.next += 1;
        }
    }

    fn observe(&mut self, batch: &RecordBatch) {
        let Some(times) = self.time.map(|i| batch.column(i)) else { return };
        let Some(times) = times.as_primitive_opt::<TimestampMicrosecondType>() else { return };
        for t in times.iter().flatten() {
            if self.newest.len() < self.k {
                self.newest.push(Reverse(t));
            } else if self.newest.peek().is_some_and(|m| t > m.0) {
                self.newest.pop();
                self.newest.push(Reverse(t));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{T0, batch, faulty_lake, opts};
    use crate::{AppendOptions, Session};

    const MINUTE: i64 = 60_000_000;

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
        ];
        for sql in queries {
            let mut answers = vec![];
            for on in ["true", "false"] {
                let s = Session::new(lake.clone());
                s.sql(&format!("SET timelake.newest_first = {on}")).await.unwrap();
                let plan = s.sql(&format!("EXPLAIN {sql}")).await.unwrap().collect().await.unwrap();
                let plan = arrow::util::pretty::pretty_format_batches(&plan).unwrap().to_string();
                assert_eq!(plan.contains("NewestFirstExec"), on == "true", "{sql}\n{plan}");
                let rows = s.sql(sql).await.unwrap().collect().await.unwrap();
                answers.push(arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string());
            }
            assert_eq!(answers[0], answers[1], "{sql}");
        }
    }
}
