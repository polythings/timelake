//! End-to-end table lifecycle on local storage: create, append, query, last-value index and
//! query rewrite, compaction with dedup, retention, snapshot expiry, concurrent commits.

pub mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::{
    DAY, Scan, T0, batch, f64_values, no_maintenance, opts, plan, rows, scalar_i64, schema, strings, temp_lake,
};
use timelake::{AppendOptions, MaintainOptions, Session, TableOptions, property};

#[tokio::test(flavor = "multi_thread")]
async fn append_query_latest_compact_expire() {
    let (dir, lake) = temp_lake();
    // Expiry at the end should keep only the current snapshot.
    let mut o = opts();
    o.properties.insert(property::SNAPSHOT_RETENTION.into(), "0s".into());
    o.properties.insert(property::MIN_SNAPSHOTS.into(), "1".into());
    let mut t = lake.create_table("iot.readings", &schema(), o.clone()).await.unwrap();
    assert!(lake.create_table("iot.readings", &schema(), o).await.is_err());

    // 3 days of data in 12 appends of 6 hours each; no auto-maintenance yet.
    for i in 0..12 {
        let b = batch(T0 + i * DAY / 4, 360, 60_000_000, 10, |d, j| f64::from(d) * 1000.0 + j as f64);
        let (nt, r) = t.append(&[b], no_maintenance()).await.unwrap();
        assert_eq!(r.rows, 3600);
        t = nt;
    }
    assert_eq!(t.metadata().snapshots().len(), 12);
    assert_eq!(t.files().await.unwrap().len(), 12);
    // Manifests are merged as commits accumulate.
    let manifests = t.manifests().await.unwrap().len();
    assert!(manifests <= 9, "{manifests} manifests");

    let s = Session::new(lake.clone());
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings").await, 43_200);
    // File pruning on time: only the files of the last day remain in the plan.
    let last_day = "FROM iot.readings WHERE time >= TIMESTAMP '2026-01-03 00:00:00'";
    assert_eq!(plan(&s, &format!("SELECT avg(value) {last_day}")).await.scans, [Scan::Files(4)]);
    assert_eq!(scalar_i64(&s, &format!("SELECT count(*) {last_day}")).await, 14_400);
    let hourly = "SELECT time_bucket(INTERVAL '1 hour', time) h, avg(value) FROM iot.readings GROUP BY 1";
    assert_eq!(scalar_i64(&s, &format!("SELECT count(*) FROM ({hourly})")).await, 72);

    // Latest per device via the index, and the automatic rewrites.
    let latest = s.latest("iot.readings").await.unwrap().collect().await.unwrap();
    assert_eq!(latest.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
    let rn_sql = "SELECT device, value FROM (SELECT device, value, ROW_NUMBER() OVER (PARTITION BY device ORDER BY time DESC) AS rn FROM iot.readings) WHERE rn = 1 ORDER BY device";
    let p = plan(&s, rn_sql).await;
    assert_eq!(p.scans, [Scan::LastValueIndex], "{p:?}");
    assert!(!p.has("BoundedWindowAggExec") && !p.has("WindowAggExec"), "{p:?}");
    let values = f64_values(&rows(&s, rn_sql).await);
    assert!((values[3] - (3000.0 + 359.0)).abs() < f64::EPSILON); // device 3, last point of the last append
    // Extra predicates next to rn = 1 (applied after picking the latest row) still use the index.
    let rn_and = "SELECT device FROM (SELECT device, value, ROW_NUMBER() OVER (PARTITION BY device ORDER BY time DESC) AS rn FROM iot.readings) WHERE rn = 1 AND value > 5000";
    assert_eq!(plan(&s, rn_and).await.scans, [Scan::LastValueIndex]);
    assert_eq!(scalar_i64(&s, &format!("SELECT count(*) FROM ({rn_and})")).await, 5);
    let top1 = "SELECT time, value FROM iot.readings WHERE device = 7 ORDER BY time DESC LIMIT 1";
    assert_eq!(plan(&s, top1).await.scans, [Scan::LastValueIndex]);
    assert_eq!(f64_values(&rows(&s, top1).await), [7359.0]);
    // Filters on non-key columns must not use the index.
    let p = plan(&s, "SELECT time FROM iot.readings WHERE value < 5 ORDER BY time DESC LIMIT 1").await;
    assert!(!p.scans.is_empty() && p.scans.iter().all(|s| matches!(s, Scan::Files(_))), "{p:?}");

    // Duplicates + late data: readers see the newest write of each key at once (dedup on read),
    // then compaction removes the older copies (latest write wins).
    let (nt, _) = t.append(&[batch(T0, 10, 60_000_000, 10, |_, _| -1.0)], no_maintenance()).await.unwrap();
    t = nt;
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings").await, 43_200);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings WHERE value = -1").await, 100);
    // A filter on another column must not let an older copy through.
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings WHERE value >= 0").await, 43_100);
    assert_eq!(scalar_i64(&s, "SELECT count(DISTINCT site) FROM iot.readings WHERE value >= 0").await, 3);
    let p = plan(&s, "SELECT avg(value) FROM iot.readings").await;
    assert!(p.has("DedupExec"), "{p:?}");
    // Filters on the key reach the merged files (row-group pruning); others stay above the dedup.
    let explain =
        strings(&rows(&s, "EXPLAIN SELECT avg(value) FROM iot.readings WHERE device = 4 AND value > 0").await, "plan");
    let merged =
        explain.iter().flat_map(|p| p.lines()).find(|l| l.contains("DataSourceExec") && l.contains("__timelake_rank"));
    let merged = merged.unwrap_or_default();
    assert!(merged.contains("predicate=device@") && !merged.contains("value@"), "{explain:?}");
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings WHERE device = 4").await, 4320);
    // The switch: a session can read the copies as they are, and SET turns it back.
    let raw = Session::with_config(lake.clone(), Session::config().set_str("timelake.read_dedup", "false"));
    assert_eq!(scalar_i64(&raw, "SELECT count(*) FROM iot.readings").await, 43_300);
    assert!(!plan(&raw, "SELECT avg(value) FROM iot.readings").await.has("DedupExec"));
    rows(&raw, "SET timelake.read_dedup = true").await;
    assert_eq!(scalar_i64(&raw, "SELECT count(*) FROM iot.readings").await, 43_200);
    // Strategy keys-first gives the same answers: older copies listed from the keys, then left out
    // of a scan that every filter may prune.
    rows(&raw, "SET timelake.read_dedup_strategy = 'keys-first'").await;
    let shown = rows(&raw, "SHOW timelake.read_dedup_strategy").await;
    assert_eq!(strings(&shown, "value"), ["keys-first"]);
    assert!(raw.sql("SET timelake.min_snapshots = 1").await.is_err(), "a table setting, not a session one");
    for (sql, n) in [
        ("SELECT count(*) FROM iot.readings", 43_200),
        ("SELECT count(*) FROM iot.readings WHERE value = -1", 100),
        ("SELECT count(*) FROM iot.readings WHERE value >= 0", 43_100),
        ("SELECT count(*) FROM iot.readings WHERE device = 4 AND value >= 0", 4310),
    ] {
        assert_eq!(scalar_i64(&raw, sql).await, n, "{sql}");
    }
    let explain = strings(&rows(&raw, "EXPLAIN SELECT avg(value) FROM iot.readings WHERE value >= 0").await, "plan");
    assert!(explain.iter().any(|p| p.contains("keep older copies") && p.contains("RightAnti")), "{explain:?}");
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings WHERE device < 5 AND value = -1").await, 50);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings WHERE device = 4 AND value >= 0").await, 4310);
    // Only the files that overlap the late append are merged; the other days aren't.
    assert!(!plan(&s, &format!("SELECT avg(value) {last_day}")).await.has("DedupExec"));
    let (nt, rep) = t.maintain(MaintainOptions { expire: false, ..Default::default() }).await.unwrap();
    t = nt;
    assert_eq!(rep.partitions_compacted, 3, "{rep:?}");
    assert_eq!(rep.duplicates_removed, 100, "{rep:?}");
    assert_eq!(t.files().await.unwrap().len(), 3);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings").await, 43_200);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings WHERE value = -1").await, 100);
    // Compaction keeps the index.
    assert!(t.last_value_index().is_some());
    // Single-device query reads sorted files; result is correct.
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings WHERE device = 4").await, 4320);

    // Expire everything but the current snapshot: the replaced small files are deleted.
    let (t2, rep) = t.maintain(MaintainOptions { compact: false, ..Default::default() }).await.unwrap();
    assert_eq!(rep.snapshots_expired, 13, "{rep:?}");
    assert!(rep.objects_deleted >= 13, "{rep:?}");
    assert_eq!(t2.metadata().snapshots().len(), 1);
    let data_files = std::fs::read_dir(dir.path().join("iot/readings/data"))
        .unwrap()
        .flat_map(|d| std::fs::read_dir(d.unwrap().path()).unwrap())
        .count();
    assert_eq!(data_files, 3);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings").await, 43_200);
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_maintain_retention_idempotency_concurrency() {
    let (_dir, lake) = temp_lake();
    let mut o = opts();
    o.properties.insert(property::COMPACT_MIN_FILES.into(), "4".into());
    o.retention = Some(Duration::from_hours(3650 * 24));
    let mut t = lake.create_table("m", &schema(), o).await.unwrap();
    assert_eq!(t.metadata().properties().get(property::RETENTION).map(String::as_str), Some("3650d"));

    // Auto-maintenance compacts once 4 small files accumulate in a partition.
    let now = chrono::Utc::now().timestamp_micros() / DAY * DAY;
    let mut compactions = 0;
    for i in 0..9 {
        let b = batch(now + i * 60_000_000, 1, 1, 5, |_, _| i as f64);
        let (nt, r) = t.append(&[b], AppendOptions::default()).await.unwrap();
        compactions += usize::from(matches!(r.maintenance, Some(Ok(_))));
        t = nt;
    }
    assert_eq!(compactions, 2);
    assert!(t.files().await.unwrap().len() <= 3);

    // Idempotent batch ids.
    let b = batch(now + DAY / 2, 1, 1, 5, |_, _| 0.0);
    let id = AppendOptions { batch_id: Some("batch-42".into()), maintain: Some(false), ..Default::default() };
    let (t1, r1) = t.append(std::slice::from_ref(&b), id.clone()).await.unwrap();
    let (t2, r2) = t1.append(&[b], id).await.unwrap();
    assert!(!r1.commit.duplicate && r2.commit.duplicate);
    assert_eq!(t1.version(), t2.version());

    // Concurrent appends against the same base all land.
    let base = t2.clone();
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let base = base.clone();
            tokio::spawn(async move {
                let b = batch(now + DAY / 3 + i * 1000, 1, 1, 5, |_, _| 1.0);
                base.append(&[b], no_maintenance()).await.map(|(_, r)| r.commit.attempts)
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap().unwrap();
    }
    let t3 = lake.table("m").await.unwrap();
    let s = Session::new(lake.clone());
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM m").await, 45 + 5 + 40);
    assert_eq!(t3.version(), t2.version() + 8);

    // Retention drops partitions older than the window.
    let (t4, _) = t3.append(&[batch(T0 - 4000 * DAY, 2, 1, 5, |_, _| 9.0)], no_maintenance()).await.unwrap();
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM m").await, 100);
    let (_, rep) = t4.maintain(MaintainOptions { compact: false, expire: false, ..Default::default() }).await.unwrap();
    assert_eq!(rep.partitions_dropped, 1);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM m").await, 90);
}

#[tokio::test(flavor = "multi_thread")]
async fn dimension_tables_and_schema_alignment() {
    let (_dir, lake) = temp_lake();
    let tags =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true), Field::new("name", DataType::Utf8, true)]));
    let t = lake.create_table("iot.tags", &tags, TableOptions::default()).await.unwrap();
    let b = RecordBatch::try_new(
        tags.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .unwrap();
    t.append(&[b], AppendOptions::default()).await.unwrap();

    // Columns in another order and a missing nullable column are fine; unknown columns are not.
    let r = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let partial = Arc::new(Schema::new(vec![
        Field::new("device", DataType::Int64, true),
        Field::new("time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
    ]));
    let b = RecordBatch::try_new(
        partial,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![T0 / 1000, T0 / 1000])),
        ],
    )
    .unwrap();
    r.append(&[b], AppendOptions::default()).await.unwrap();

    let s = Session::new(lake.clone());
    assert_eq!(
        scalar_i64(&s, "SELECT count(*) FROM iot.r r JOIN iot.tags t ON r.device = t.id WHERE t.name = 'b'").await,
        1
    );
    let names: Vec<_> = lake.all_tables().await.unwrap().iter().map(ToString::to_string).collect();
    assert_eq!(names, ["iot.r", "iot.tags"]);
}

/// Expiry deletes nothing when a snapshot it keeps can't be read: that snapshot's files would
/// count as unused, and the table could no longer be repaired.
#[tokio::test(flavor = "multi_thread")]
async fn expiry_deletes_nothing_when_a_kept_snapshot_is_broken() {
    let (dir, lake) = temp_lake();
    let mut o = opts();
    o.properties.insert(property::SNAPSHOT_RETENTION.into(), "0s".into());
    o.properties.insert(property::MIN_SNAPSHOTS.into(), "1".into());
    let mut t = lake.create_table("iot.r", &schema(), o).await.unwrap();
    for i in 0..3 {
        t = t.append(&[batch(T0 + i, 1, 1, 2, |_, _| 1.0)], no_maintenance()).await.unwrap().0;
    }
    let data_files = || {
        std::fs::read_dir(dir.path().join("iot/r/data"))
            .unwrap()
            .flat_map(|d| std::fs::read_dir(d.unwrap().path()).unwrap())
            .count()
    };
    assert_eq!(data_files(), 3);
    let current = t.metadata().current_snapshot().unwrap().clone();
    std::fs::remove_file(current.manifest_list().strip_prefix("file://").unwrap()).unwrap();

    // Another process: nothing cached.
    let lake = timelake::Lake::open(dir.path().to_str().unwrap(), std::collections::HashMap::new()).unwrap();
    let only_expire = MaintainOptions { compact: false, retention: false, ..Default::default() };
    let err = lake.table("iot.r").await.unwrap().maintain(only_expire).await.unwrap_err().to_string();
    assert!(err.contains(&current.snapshot_id().to_string()), "names the broken snapshot: {err}");
    assert_eq!(data_files(), 3, "every data file is still there");
    assert_eq!(lake.table("iot.r").await.unwrap().metadata().snapshots().count(), 3, "nothing expired");
}
