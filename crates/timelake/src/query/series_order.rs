//! Series order for the run aggregates ([`super::runs`]: `time_weighted_mean`, `integral`,
//! `counter_diff`, `counter_rate`, `gap_count`, `gap_seconds`). Each partition sums its runs of a series' readings, and the runs of all
//! partitions are joined in time order: exact when no partition has readings of the series inside
//! another's run. A scan under a signal aggregate gives that:
//! - files that must be merged (copies to remove with dedup on read; without, files that overlap
//!   another on the series and time or aren't sorted) are merged in `(series…, time)` order, each
//!   series in one slice, with every file in the time they span (the dedup scan's merge, keeping
//!   every row without dedup);
//! - the files before and after that time are read as they are, each partition a stretch of time
//!   in time order, and kept to its files (DataFusion otherwise lets idle partitions take others').
//!
//! Setting `series-order`: `auto` (the default) merges what has to be, `segments` only copies
//! (overlaps are then an error), `merge` everything. Measured alternatives: a window over every
//! reading (550 s and 20 GB for hourly means of 370 M readings, against 2 s); files spread over
//! partitions by size, as other scans do, which makes runs with gaps that other partitions fill.
//!
//! A window over each series in time order (`lead(…) OVER (PARTITION BY <series key> ORDER BY
//! time)`) can read every file merged the same way: split by series and sorted, so DataFusion
//! neither repartitions nor sorts. Setting `window-series-order`: on the industrial benchmark,
//! gaps of a day's 1-second readings (one file) 265 → 217 ms, but of a week (seven) 2.0 → 3.0 s:
//! merging sorted files costs more than DataFusion's sort once there are several. `auto` (the
//! default) merges when the files are in one time partition, `true` always, `false` never.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr::Sort;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;

use super::provider::{Kind, TimelakeProvider};
use super::rollup::unalias;
use super::runs::over_readings;
use super::settings::SessionSettings;
use crate::property;

/// How a scan under a signal aggregate reads each series in time order (setting `series-order`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Order {
    /// Only copies merged (with dedup on read): files that overlap are the aggregate's error.
    Segments,
    /// Files that overlap or aren't sorted merged too, with the files in the time they span.
    Auto,
    /// All files merged.
    Merge,
    /// All files merged if they're in one time partition (a window's `auto`); else read as other
    /// scans are, which DataFusion then splits by series and sorts.
    MergeOnePartition,
}

impl Order {
    fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "segments" => Self::Segments,
            "merge" => Self::Merge,
            _ => Self::Auto,
        }
    }
}

/// The optimizer rule: the Timelake scans under a signal aggregate over readings read in series
/// order.
#[derive(Debug)]
pub(super) struct SeriesOrder;

impl OptimizerRule for SeriesOrder {
    fn name(&self) -> &'static str {
        "timelake_series_order"
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> DfResult<Transformed<LogicalPlan>> {
        let options = config.options();
        let session = options.extensions.get::<SessionSettings>().and_then(|s| s.get(property::SERIES_ORDER));
        let session = session.map(Order::parse);
        let settings = options.extensions.get::<SessionSettings>();
        let windows_session = settings.and_then(|s| s.get(property::WINDOW_SERIES_ORDER)).map(str::to_ascii_lowercase);
        plan.transform_down(|node| {
            let wants: Box<Wants> = match &node {
                LogicalPlan::Aggregate(a) if a.aggr_expr.iter().any(|e| over_readings(e, a.input.schema())) => {
                    Box::new(move |p| Some(session.unwrap_or_else(|| Order::parse(&p.table().contract().series_order))))
                }
                LogicalPlan::Window(w) => {
                    let windows: Vec<(HashSet<String>, String)> =
                        w.window_expr.iter().filter_map(series_window).collect();
                    if windows.is_empty() {
                        return Ok(Transformed::no(node));
                    }
                    let session = windows_session.clone();
                    Box::new(move |p| {
                        let contract = p.table().contract();
                        let keys: HashSet<String> = contract.series_key.iter().cloned().collect();
                        let fits = windows
                            .iter()
                            .any(|(by, order)| *by == keys && contract.time_column.as_ref() == Some(order));
                        match session.as_deref().unwrap_or(&contract.window_series_order) {
                            "false" => None,
                            "true" => fits.then_some(Order::Merge),
                            _ => fits.then_some(Order::MergeOnePartition),
                        }
                    })
                }
                _ => return Ok(Transformed::no(node)),
            };
            let marked = node.map_children(|child| child.transform_up(|n| mark(n, &wants)))?;
            Ok(Transformed::new(marked.data, marked.transformed, TreeNodeRecursion::Jump))
        })
    }
}

/// How a node needs a Timelake scan read, if it does.
type Wants = dyn Fn(&TimelakeProvider) -> Option<Order>;

/// `node` with its Timelake scan reading in series order, if `wants` says so.
fn mark(node: LogicalPlan, wants: &Wants) -> DfResult<Transformed<LogicalPlan>> {
    let LogicalPlan::TableScan(scan) = &node else { return Ok(Transformed::no(node)) };
    let provider = source_as_provider(&scan.source)?;
    let Some(p) = (provider.as_ref() as &dyn Any).downcast_ref::<TimelakeProvider>() else {
        return Ok(Transformed::no(node));
    };
    if p.kind() != Kind::Data || p.reads_in_series_order() {
        return Ok(Transformed::no(node));
    }
    let Some(order) = wants(p) else { return Ok(Transformed::no(node)) };
    let mut scan = scan.clone();
    scan.source = provider_as_source(Arc::new(p.in_series_order(order)));
    Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
}

/// The columns a window partitions by and the one it orders by, if it partitions by columns and
/// orders by one, ascending.
fn series_window(e: &Expr) -> Option<(HashSet<String>, String)> {
    let Expr::WindowFunction(w) = unalias(e) else { return None };
    let by = w.params.partition_by.iter().map(|e| match e {
        Expr::Column(c) => Some(c.name.clone()),
        _ => None,
    });
    let order = match w.params.order_by.as_slice() {
        [Sort { expr: Expr::Column(c), asc: true, .. }] => c.name.clone(),
        _ => return None,
    };
    Some((by.collect::<Option<_>>()?, order))
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
                          counter_diff(time, value) AS grew, count(*) AS n FROM iot.r GROUP BY device";
        let session = |mode: &'static str| {
            let lake = lake.clone();
            async move {
                let s = Session::new(lake);
                s.sql(&format!("SET timelake.series_order = '{mode}'")).await.unwrap();
                s
            }
        };
        let auto = answer(&session("auto").await, sql).await.unwrap();
        assert_eq!(auto, answer(&session("merge").await, sql).await.unwrap());
        // Readings 0, 5, 10, … 295 every 5 s: forward fill holds each for 5 s over 0..295 s, so the
        // mean is that of 0..290 (145), linear the midpoint (147.5); the counter grew 295; 60 rows.
        assert!(auto.contains("| 145.0 | 147.5 | 295.0 | 60 |"), "{auto}");
        let err = answer(&session("segments").await, sql).await.unwrap_err().to_string();
        assert!(err.contains("overlap in time"), "{err}");
    }

    /// More files than partitions, a day each, as a table of many days has: each partition reads
    /// a stretch of days, so its runs hold no other partition's readings and join (spread by size,
    /// a partition's run went over days other partitions held: an overlap). A second copy of a
    /// day (a copy with dedup on, an overlap without) merges that day only, which gives the same.
    #[tokio::test]
    async fn partitions_read_stretches_of_time() {
        const MINUTE: i64 = 60 * SECOND;
        let day = |d: i64| -> Vec<(i64, i32, f64)> {
            (d * 1440..(d + 1) * 1440).flat_map(|i| [1, 2].map(|dev| (T0 + i * MINUTE, dev, i as f64))).collect()
        };
        let sql = "SELECT device, time_weighted_mean(time, value) AS mean, counter_diff(time, value) AS grew
                   FROM iot.r GROUP BY device ORDER BY device";
        // Values 0, 1, … 17279 a minute apart: forward fill holds each for a minute up to the last.
        let expected = "| 1      | 8639.0 | 17279.0 |";
        for dedup in [true, false] {
            let (lake, _) = faulty_lake();
            let t = lake.create_table("iot.r", &schema(), TableOptions { dedup: Some(dedup), ..opts() }).await.unwrap();
            let mut t = t;
            for d in 0..12 {
                t = t
                    .append(&[batch(&day(d))], AppendOptions { maintain: Some(false), ..Default::default() })
                    .await
                    .unwrap()
                    .0;
            }
            let modes: &[&str] = &["auto", "segments", "merge"];
            for copy in [false, true] {
                if copy {
                    t.append(&[batch(&day(5))], AppendOptions { maintain: Some(false), ..Default::default() })
                        .await
                        .unwrap();
                }
                for mode in modes.iter().filter(|m| !(copy && !dedup && **m == "segments")) {
                    let s = Session::new(lake.clone());
                    s.sql("SET datafusion.execution.target_partitions = 4").await.unwrap();
                    s.sql(&format!("SET timelake.series_order = '{mode}'")).await.unwrap();
                    let got = answer(&s, sql).await.unwrap();
                    if copy && *mode == "auto" {
                        // Only the copied day is merged; the days before and after are read as they are.
                        let plan = answer(&s, &format!("EXPLAIN {sql}")).await.unwrap();
                        let merged = plan
                            .lines()
                            .skip_while(|l| !l.contains("DedupExec"))
                            .find(|l| l.contains("DataSourceExec"));
                        let days: Vec<&str> = merged.unwrap().split("time_day=").skip(1).map(|d| &d[..10]).collect();
                        assert!(!days.is_empty() && days.iter().all(|d| *d == "2026-01-06"), "{plan}");
                        assert_eq!(plan.matches("DataSourceExec").count(), 3, "{plan}");
                    }
                    assert!(got.contains(expected), "dedup {dedup}, copy {copy}, {mode}\n{got}");
                }
            }
        }
    }

    /// A window over each series in time order gives the same rows merged or not; merged, the plan
    /// doesn't sort. `auto` merges one day's files, not several days'.
    #[tokio::test]
    async fn windows_read_each_series_in_order() {
        let (lake, _) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let rows: Vec<(i64, i32, f64)> =
            (0..3000).map(|i| (T0 + i * 60 * SECOND, i32::try_from(i % 4).unwrap(), ((i * 37) % 101) as f64)).collect();
        t.append(&[batch(&rows)], AppendOptions::default()).await.unwrap();
        let window = |filter: &str| {
            format!(
                "SELECT device, time, value, lead(value) OVER (PARTITION BY device ORDER BY time) AS next
                 FROM iot.r {filter} ORDER BY device, time"
            )
        };
        for (filter, auto_merges) in [("", false), ("WHERE time < TIMESTAMP '2026-01-02 00:00:00'", true)] {
            let sql = window(filter);
            let mut answers = vec![];
            for (mode, merges) in [("true", true), ("false", false), ("auto", auto_merges)] {
                let s = Session::new(lake.clone());
                s.sql(&format!("SET timelake.window_series_order = '{mode}'")).await.unwrap();
                let plan = answer(&s, &format!("EXPLAIN {sql}")).await.unwrap();
                let window_input = plan.lines().skip_while(|l| !l.contains("WindowAggExec")).nth(1).unwrap();
                assert_eq!(!window_input.contains("SortExec"), merges, "{mode} {filter}\n{plan}");
                answers.push(answer(&s, &sql).await.unwrap());
            }
            assert!(answers.iter().all(|a| *a == answers[0]));
        }
    }

    /// Readings in order give the same answers merged or not.
    #[tokio::test]
    async fn merging_ordered_readings_changes_nothing() {
        let (lake, _) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let rows: Vec<(i64, i32, f64)> =
            (0..500).map(|i| (T0 + i * 7 * SECOND, i32::try_from(i % 3).unwrap(), ((i * 37) % 101) as f64)).collect();
        t.append(&[batch(&rows)], AppendOptions::default()).await.unwrap();
        let sql = "SELECT device, time_bucket(INTERVAL '10 minutes', time) AS b, time_weighted_mean(time, value) AS ff,
                          time_weighted_mean(time, value, 'linear') AS lin, integral(time, value) AS area,
                          counter_diff(time, value) AS grew, counter_rate(time, value) AS rate, max(value) AS m
                   FROM iot.r GROUP BY 1, 2 ORDER BY 1, 2";
        let mut answers = vec![];
        for mode in ["segments", "merge"] {
            let s = Session::new(lake.clone());
            s.sql(&format!("SET timelake.series_order = '{mode}'")).await.unwrap();
            answers.push(answer(&s, sql).await.unwrap());
        }
        assert_eq!(answers[0], answers[1]);
    }
}
