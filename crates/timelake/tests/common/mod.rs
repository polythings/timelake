//! Helpers shared by the integration tests: a small IoT schema, generated batches, SQL shortcuts,
//! and plan inspection. Test files declare it `pub mod common;`: its items are public then, so
//! those a test binary doesn't use aren't dead code.

#![expect(clippy::unwrap_used, reason = "test helpers")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use timelake::datafusion::datasource::memory::MemorySourceConfig;
use timelake::datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use timelake::datafusion::datasource::source::DataSourceExec;
use timelake::datafusion::physical_plan::ExecutionPlan;
use timelake::datafusion::physical_plan::empty::EmptyExec;
use timelake::{AppendOptions, Lake, Session, TableOptions};

pub const DAY: i64 = 86_400_000_000;
pub const T0: i64 = 1_767_225_600_000_000; // 2026-01-01T00:00:00Z

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("device", DataType::Int32, true),
        Field::new("site", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// `devices` devices, one point every `step` µs from `start`, `n` points each, in time order.
pub fn batch(start: i64, n: i64, step: i64, devices: i32, value: impl Fn(i32, i64) -> f64) -> RecordBatch {
    let points: Vec<(i64, i32)> = (0..n).flat_map(|i| (0..devices).map(move |d| (i, d))).collect();
    let times = points.iter().map(|&(i, _)| start + i * step);
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from_iter_values(times).with_timezone("UTC")),
            Arc::new(Int32Array::from_iter_values(points.iter().map(|&(_, d)| d))),
            Arc::new(StringArray::from_iter_values(points.iter().map(|&(_, d)| format!("site-{}", d % 3)))),
            Arc::new(Float64Array::from_iter_values(points.iter().map(|&(i, d)| value(d, i)))),
        ],
    )
    .unwrap()
}

/// A time-series table keyed by device.
pub fn opts() -> TableOptions {
    TableOptions { time_column: Some("time".into()), series_key: vec!["device".into()], ..Default::default() }
}

/// An append without inline maintenance, so a test controls the files.
pub fn no_maintenance() -> AppendOptions {
    AppendOptions { maintain: Some(false), ..AppendOptions::default() }
}

/// A lake in a fresh temporary directory (deleted when the guard drops).
pub fn temp_lake() -> (tempfile::TempDir, Lake) {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(dir.path().to_str().unwrap(), HashMap::new()).unwrap();
    (dir, lake)
}

pub async fn rows(s: &Session, sql: &str) -> Vec<RecordBatch> {
    s.sql(sql).await.unwrap().collect().await.unwrap()
}

/// The first value of the first column, as i64.
pub async fn scalar_i64(s: &Session, sql: &str) -> i64 {
    let b = rows(s, sql).await;
    let c = arrow::compute::cast(b[0].column(0), &DataType::Int64).unwrap();
    c.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
}

/// One string column of a result, concatenated across batches.
pub fn strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let c = arrow::compute::cast(b.column_by_name(column).unwrap(), &DataType::Utf8).unwrap();
            let c = c.as_any().downcast_ref::<StringArray>().unwrap().clone();
            (0..c.len()).map(move |i| c.value(i).to_string())
        })
        .collect()
}

/// What a scan in a physical plan reads.
#[derive(Debug, PartialEq, Eq)]
pub enum Scan {
    /// The last-value index (an in-memory batch).
    LastValueIndex,
    /// This many data files.
    Files(usize),
    /// Nothing: every file was pruned.
    Nothing,
}

/// The scans and operator names of a query's physical plan.
#[derive(Debug)]
pub struct Plan {
    pub scans: Vec<Scan>,
    pub operators: Vec<String>,
}

impl Plan {
    pub fn has(&self, operator: &str) -> bool {
        self.operators.iter().any(|o| o == operator)
    }
}

pub async fn plan(s: &Session, sql: &str) -> Plan {
    fn walk(node: &Arc<dyn ExecutionPlan>, plan: &mut Plan) {
        plan.operators.push(node.name().to_string());
        if node.downcast_ref::<EmptyExec>().is_some() {
            plan.scans.push(Scan::Nothing);
        }
        if let Some(exec) = node.downcast_ref::<DataSourceExec>() {
            let source = exec.data_source();
            if let Some(files) = source.downcast_ref::<FileScanConfig>() {
                plan.scans.push(Scan::Files(files.file_groups.iter().map(FileGroup::len).sum()));
            } else if source.downcast_ref::<MemorySourceConfig>().is_some() {
                plan.scans.push(Scan::LastValueIndex);
            }
        }
        for child in node.children() {
            walk(child, plan);
        }
    }
    let physical = s.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
    let mut plan = Plan { scans: vec![], operators: vec![] };
    walk(&physical, &mut plan);
    plan
}

/// The `value` column of a result (or its only column), concatenated across batches.
pub fn f64_values(batches: &[RecordBatch]) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|b| {
            let c = b.column_by_name("value").unwrap_or_else(|| b.column(0));
            c.as_any().downcast_ref::<Float64Array>().unwrap().values().to_vec()
        })
        .collect()
}

/// Every file under `dir`, at any depth.
pub fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = vec![];
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path);
        }
    }
    out
}
