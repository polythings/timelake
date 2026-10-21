//! The block cache end to end (spec 22): the same answers with it on, through appends,
//! compaction and expiry, from memory and from a disk cache another lake (process) filled.

#![expect(clippy::unwrap_used, reason = "test code")]

pub mod common;

use std::collections::HashMap;

use common::{DAY, T0, batch, no_maintenance, opts, rows};
use timelake::{Lake, MaintainOptions, Session, property};

const QUERIES: &[&str] = &[
    "SELECT device, count(*), sum(value) FROM iot.r GROUP BY 1 ORDER BY 1",
    "SELECT time, value FROM iot.r WHERE device = 3 AND time >= TIMESTAMP '2026-01-02 06:00:00' ORDER BY time",
    "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY time DESC) AS rn FROM iot.r) WHERE rn = 1 ORDER BY device",
];

fn open(dir: &std::path::Path, cache: &[(&str, &str)]) -> Lake {
    let options: HashMap<String, String> = cache.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
    Lake::open(dir.to_str().unwrap(), options).unwrap()
}

async fn answers(lake: &Lake) -> Vec<String> {
    let s = Session::new(lake.clone());
    let mut out = vec![];
    for q in QUERIES {
        out.push(arrow::util::pretty::pretty_format_batches(&rows(&s, q).await).unwrap().to_string());
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn cached_reads_give_the_same_answers() {
    let warehouse = tempfile::tempdir().unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let disk = [("timelake_cache", "disk"), ("timelake_cache_dir", cache_dir.path().to_str().unwrap())];
    let plain = open(warehouse.path(), &[]);
    let cached = open(warehouse.path(), &disk);
    assert!(plain.storage().cache_stats().is_none(), "off by default on local disk");

    let mut o = opts();
    o.properties.insert(property::SNAPSHOT_RETENTION.into(), "0s".into());
    o.properties.insert(property::MIN_SNAPSHOTS.into(), "1".into());
    let mut t = cached.create_table("iot.r", &common::schema(), o).await.unwrap();
    for i in 0..6 {
        let b = batch(T0 + i * DAY / 2, 720, 60_000_000, 8, |d, j| f64::from(d) * 100.0 + j as f64);
        t = t.append(&[b], no_maintenance()).await.unwrap().0;
    }
    assert_eq!(answers(&cached).await, answers(&plain).await);
    // Compaction rewrites the files and expiry deletes the old ones: still the same answers.
    t.maintain(MaintainOptions::default()).await.unwrap();
    // A fresh lake: `plain` may reuse the version it loaded within the last second, whose files
    // expiry just deleted.
    let expected = answers(&open(warehouse.path(), &[])).await;
    assert_eq!(answers(&cached).await, expected);
    let warm = cached.storage().cache_stats().unwrap();
    assert!(warm.memory_hits > 0, "{warm:?}");

    // Another process with the same disk cache reads the data files from disk.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let other = open(warehouse.path(), &disk);
    assert_eq!(answers(&other).await, expected);
    let stats = other.storage().cache_stats().unwrap();
    assert!(stats.disk_hits > 0, "{stats:?}");
}

#[test]
fn cache_options_are_checked() {
    let dir = tempfile::tempdir().unwrap();
    let open = |kv: &[(&str, &str)]| {
        let options = kv.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
        Lake::open(dir.path().to_str().unwrap(), options)
    };
    assert!(open(&[("timelake_cache", "memory")]).unwrap().storage().cache_stats().is_some());
    for (kv, msg) in [
        (("timelake_cache", "always"), "off, memory or disk"),
        (("timelake_cache_memory_bytes", "lots"), "number of bytes"),
        (("timelake_cache_block_bytes", "100"), "at least"),
    ] {
        let err = open(&[kv]).unwrap_err().to_string();
        assert!(err.contains(msg), "{kv:?}: {err}");
    }
}
