//! Benchmark mode of `timelake sql` (used by the benchmark scripts).

use std::time::Instant;

use anyhow::{Context, Result};
use timelake::Session;
use timelake::arrow::array::{ArrayRef, AsArray, RecordBatch};
use timelake::arrow::compute::{cast, sum};
use timelake::arrow::datatypes::{DataType, Float64Type, TimeUnit};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Runs `sql` 1 + `runs` times and prints `{"times": [...], "rows": n, "sums": [...], "setup_s": s}` as JSON,
/// with block cache hits and misses per run when the cache is on, and request and byte counts per run when an
/// s3proxy admin address is given.
pub(crate) async fn run(session: &Session, sql: &str, runs: usize, setup_s: f64, proxy: Option<&str>) -> Result<()> {
    let (mut times, mut requests, mut bytes, mut rows, mut check) = (vec![], vec![], vec![], 0usize, vec![]);
    let (mut hits, mut misses) = (vec![], vec![]);
    let cache = || session.lake().storage().cache_stats();
    for _ in 0..=runs {
        let before = match proxy {
            Some(p) => Some(proxy_stats(p).await?),
            None => None,
        };
        let cache_before = cache();
        let started = Instant::now();
        let batches = session.sql(sql).await?.collect().await?;
        times.push(started.elapsed().as_secs_f64());
        rows = batches.iter().map(RecordBatch::num_rows).sum();
        check = sums(&batches);
        if let (Some(c0), Some(c1)) = (cache_before, cache()) {
            hits.push(c1.memory_hits + c1.disk_hits - c0.memory_hits - c0.disk_hits);
            misses.push(c1.misses - c0.misses);
        }
        if let (Some((r0, b0)), Some(p)) = (before, proxy) {
            let (r1, b1) = proxy_stats(p).await?;
            // A proxy restarted mid-run starts again from zero.
            requests.push(r1.saturating_sub(r0));
            bytes.push(b1.saturating_sub(b0));
        }
    }
    let mut out = serde_json::json!({ "times": times, "rows": rows, "sums": check, "setup_s": setup_s });
    if !hits.is_empty() {
        out["cache_hits"] = hits.into();
        out["cache_misses"] = misses.into();
    }
    if proxy.is_some() {
        out["requests"] = requests.into();
        out["bytes"] = bytes.into();
    }
    println!("{out}");
    Ok(())
}

/// Per result column, the sum of its values, which the benchmark compares between systems to check answers:
/// timestamps and dates in epoch seconds, booleans as the count of trues, `None` for other types.
fn sums(batches: &[RecordBatch]) -> Vec<Option<f64>> {
    let columns = batches.first().map_or(0, RecordBatch::num_columns);
    (0..columns).map(|i| batches.iter().map(|b| column_sum(b.column(i))).sum()).collect()
}

fn column_sum(column: &ArrayRef) -> Option<f64> {
    let (as_number, scale) = match column.data_type() {
        DataType::Timestamp(unit, _) => {
            let per_second = match unit {
                TimeUnit::Second => 1.0,
                TimeUnit::Millisecond => 1e3,
                TimeUnit::Microsecond => 1e6,
                TimeUnit::Nanosecond => 1e9,
            };
            (cast(column, &DataType::Int64).ok()?, 1.0 / per_second)
        }
        DataType::Date32 => (cast(column, &DataType::Int32).ok()?, 86_400.0),
        DataType::Date64 => (cast(column, &DataType::Int64).ok()?, 1e-3),
        DataType::Boolean => (cast(column, &DataType::Int8).ok()?, 1.0),
        t if t.is_numeric() => (column.clone(), 1.0),
        _ => return None,
    };
    let values = cast(&as_number, &DataType::Float64).ok()?;
    Some(sum(values.as_primitive::<Float64Type>()).unwrap_or(0.0) * scale)
}

/// `(requests, bytes)` counters from tools/s3proxy's admin endpoint.
async fn proxy_stats(addr: &str) -> Result<(u64, u64)> {
    let mut s = tokio::net::TcpStream::connect(addr).await?;
    s.write_all(format!("GET /stats HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes()).await?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).await?;
    let body = resp.split("\r\n\r\n").nth(1).context("bad proxy response")?;
    let v: serde_json::Value = serde_json::from_str(body.trim())?;
    Ok((v["requests"].as_u64().unwrap_or(0), v["bytes"].as_u64().unwrap_or(0)))
}
