//! Series order for `time_weighted_mean`: its segments are exact when each series' rows come in
//! runs that don't overlap in time. A table with dedup off whose files overlap on the series and
//! time, or aren't sorted, can give them out of order; for those, the scans under a time-weighted
//! mean merge such files in `(series…, time)` order (the dedup scan's merge, keeping every row),
//! each series in one slice.
//!
//! Setting `time-weighted-mean`: `auto` (the default) merges for those tables only, `segments`
//! never (an overlap is then an error), `merge` always. The tables at risk are found before
//! planning ([`prepare`]); optimizer rules can't read file lists. (A window over every reading
//! was the other way measured: 550 s and 20 GB for hourly means of 370 M readings, against 2 s.)

use std::any::Any;
use std::collections::HashSet;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;

use super::PLANNING_REUSE;
use super::dedup::overlapping_groups;
use super::provider::{Kind, TimelakeProvider};
use super::rollup::unalias;
use super::settings::SessionSettings;
use crate::lake::Lake;
use crate::name::TableName;
use crate::property;

/// Tables whose rows of one series may come out of time order in a scan.
pub(super) type OutOfOrder = Arc<RwLock<HashSet<TableName>>>;

/// Records whether table `name` may give one series' rows out of time order: dedup on read is off
/// (`read_dedup` is the session's setting, if it has one) and some files overlap on the series and
/// time, or aren't sorted.
pub(super) async fn prepare(
    lake: &Lake,
    name: &TableName,
    interval: Option<Duration>,
    read_dedup: Option<bool>,
    tables: &OutOfOrder,
) {
    let at_risk = async {
        let table = lake.table_recent(name, interval, PLANNING_REUSE).await.ok()?;
        let contract = table.contract();
        if contract.time_column.is_none() || (contract.dedup && read_dedup.unwrap_or(contract.read_dedup)) {
            return Some(false);
        }
        let files = table.files().await.ok()?;
        let sort_order = i32::try_from(table.metadata().default_sort_order_id()).ok();
        if files.iter().any(|f| f.data_file.sort_order_id() != sort_order) {
            return Some(true);
        }
        let schema = table.schema();
        let keys: Vec<usize> =
            contract.sort_columns().iter().map(|k| schema.index_of(k)).collect::<Result<_, _>>().ok()?;
        Some(!overlapping_groups(&schema, &keys, &files).is_empty())
    }
    .await;
    let mut set = tables.write().unwrap_or_else(PoisonError::into_inner);
    match at_risk {
        Some(false) => set.remove(name),
        // Unknown counts as at risk: merging is right either way.
        _ => set.insert(name.clone()),
    };
}

/// The optimizer rule: the Timelake scans under an aggregate with a `time_weighted_mean` read in
/// series order where the setting says so.
#[derive(Debug)]
pub(super) struct SeriesOrder {
    pub(super) out_of_order: OutOfOrder,
}

impl OptimizerRule for SeriesOrder {
    fn name(&self) -> &'static str {
        "timelake_series_order"
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        let options = config.options();
        let session = options.extensions.get::<SessionSettings>().and_then(|s| s.get(property::TIME_WEIGHTED_MEAN));
        let session = session.map(str::to_ascii_lowercase);
        plan.transform_down(|node| {
            let LogicalPlan::Aggregate(a) = &node else { return Ok(Transformed::no(node)) };
            if !a.aggr_expr.iter().any(is_time_weighted) {
                return Ok(Transformed::no(node));
            }
            let input = a.input.as_ref().clone().transform_up(|n| self.order(n, session.as_deref()))?;
            if !input.transformed {
                return Ok(Transformed::no(node));
            }
            let LogicalPlan::Aggregate(mut a) = node else { unreachable!("matched above") };
            a.input = Arc::new(input.data);
            Ok(Transformed::new(LogicalPlan::Aggregate(a), true, TreeNodeRecursion::Jump))
        })
    }
}

impl SeriesOrder {
    /// `node` with its Timelake scan reading in series order, if the setting says so.
    fn order(&self, node: LogicalPlan, session: Option<&str>) -> DfResult<Transformed<LogicalPlan>> {
        let LogicalPlan::TableScan(scan) = &node else { return Ok(Transformed::no(node)) };
        let provider = source_as_provider(&scan.source)?;
        let Some(p) = (provider.as_ref() as &dyn Any).downcast_ref::<TimelakeProvider>() else {
            return Ok(Transformed::no(node));
        };
        let setting = session.map_or_else(|| p.table().contract().time_weighted_mean.clone(), str::to_string);
        let merge = match setting.as_str() {
            "merge" => true,
            "segments" => false,
            _ => self.out_of_order.read().unwrap_or_else(PoisonError::into_inner).contains(p.table().name()),
        };
        if !merge || p.kind() != Kind::Data || p.reads_in_series_order() {
            return Ok(Transformed::no(node));
        }
        let mut scan = scan.clone();
        scan.source = provider_as_source(Arc::new(p.in_series_order()));
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    }
}

fn is_time_weighted(e: &Expr) -> bool {
    matches!(unalias(e), Expr::AggregateFunction(AggregateFunction { func, .. }) if func.name() == "time_weighted_mean")
}

#[cfg(test)]
mod tests {
    use crate::testing::{T0, batch, faulty_lake, opts, schema};
    use crate::{AppendOptions, Session, TableOptions};

    const SECOND: i64 = 1_000_000;

    async fn answer(s: &Session, sql: &str) -> crate::Result<String> {
        let rows = s.sql(sql).await?.collect().await?;
        Ok(arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string())
    }

    /// Two appends whose readings interleave in time for the same devices, dedup off: the segments
    /// would overlap, so `auto` merges the files in series order, which gives the definition's
    /// answer; the segments form refuses. On rows in order, both forms agree.
    #[tokio::test]
    async fn interleaved_files_are_merged_in_series_order() {
        let (lake, _) = faulty_lake();
        let o = TableOptions { dedup: Some(false), ..opts() };
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        let no_maintenance = AppendOptions { maintain: Some(false), ..Default::default() };
        // Device 1: values 0, 10, 20… every 10 s from one append, 5, 15… at the odd 5 s from another.
        let even: Vec<(i64, i32, f64)> = (0..30).map(|i| (T0 + i * 10 * SECOND, 1, (i * 10) as f64)).collect();
        let odd: Vec<(i64, i32, f64)> = (0..30).map(|i| (T0 + (i * 10 + 5) * SECOND, 1, (i * 10 + 5) as f64)).collect();
        let (t, _) = t.append(&[batch(&even)], no_maintenance.clone()).await.unwrap();
        t.append(&[batch(&odd)], no_maintenance).await.unwrap();

        let sql =
            "SELECT device, time_weighted_mean(time, value) AS ff, time_weighted_mean(time, value, 'linear') AS lin,
                          count(*) AS n FROM iot.r GROUP BY device";
        let session = |mode: &'static str| {
            let lake = lake.clone();
            async move {
                let s = Session::new(lake);
                s.sql(&format!("SET timelake.time_weighted_mean = '{mode}'")).await.unwrap();
                s
            }
        };
        let auto = answer(&session("auto").await, sql).await.unwrap();
        assert_eq!(auto, answer(&session("merge").await, sql).await.unwrap());
        // Readings 0, 5, 10, … 295 every 5 s: forward fill holds each for 5 s over 0..295 s, so the
        // mean is that of 0..290 (145), linear the midpoint (147.5); 60 rows.
        assert!(auto.contains("| 145.0 | 147.5 | 60 |"), "{auto}");
        let err = answer(&session("segments").await, sql).await.unwrap_err().to_string();
        assert!(err.contains("overlap in time"), "{err}");
    }

    /// Readings in order give the same means merged or not.
    #[tokio::test]
    async fn merging_ordered_readings_changes_nothing() {
        let (lake, _) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let rows: Vec<(i64, i32, f64)> =
            (0..500).map(|i| (T0 + i * 7 * SECOND, i32::try_from(i % 3).unwrap(), ((i * 37) % 101) as f64)).collect();
        t.append(&[batch(&rows)], AppendOptions::default()).await.unwrap();
        let sql = "SELECT device, time_bucket(INTERVAL '10 minutes', time) AS b, time_weighted_mean(time, value) AS ff,
                          time_weighted_mean(time, value, 'linear') AS lin, max(value) AS m
                   FROM iot.r GROUP BY 1, 2 ORDER BY 1, 2";
        let mut answers = vec![];
        for mode in ["segments", "merge"] {
            let s = Session::new(lake.clone());
            s.sql(&format!("SET timelake.time_weighted_mean = '{mode}'")).await.unwrap();
            answers.push(answer(&s, sql).await.unwrap());
        }
        assert_eq!(answers[0], answers[1]);
    }
}
