//! Materialized rollups end to end (spec 27): created from SQL, refreshed, and routed to,
//! always giving the answer the raw table gives.

#![allow(clippy::unwrap_used, reason = "test code")]

mod common;

use arrow::array::RecordBatch;
use common::{DAY, T0, batch, opts, rows, scalar_i64, temp_lake};
use timelake::{AppendOptions, Lake, Session, property};

const HOUR: i64 = DAY / 24;
const MINUTE: i64 = HOUR / 60;

const CREATE: &str = "CREATE MATERIALIZED ROLLUP iot.r_1h AS
    SELECT device, time_bucket(INTERVAL '1 hour', time) AS bucket,
           avg(value) AS avg_value, min(value), max(value), sum(value), count(*) AS n, count(value),
           first_value(value ORDER BY time) AS first, last_value(value ORDER BY time) AS last,
           m4(time, value) AS points, time_weighted_mean(time, value) AS twa,
           time_weighted_mean(time, value, 'linear') AS twa_linear, gap_count(time, INTERVAL '90 seconds') AS gaps
    FROM iot.r GROUP BY device, bucket
    WITH (refresh_interval = 'off')";

/// Queries the rollup can answer (all or part of), and one it can't.
const QUERIES: &[(&str, bool)] = &[
    ("SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, avg(value), count(*) FROM iot.r GROUP BY 1, 2", true),
    ("SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, m4(time, value) FROM iot.r GROUP BY 1, 2", true),
    (
        "SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, time_weighted_mean(time, value),
                time_weighted_mean(time, value, 'linear') FROM iot.r GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT time_bucket(INTERVAL '1 day', time) AS d, device, time_weighted_mean(time, value, 'linear')
         FROM iot.r WHERE time >= TIMESTAMP '2026-01-01 05:30:00' GROUP BY 1, 2",
        true,
    ),
    // The other signal aggregates, from the time-weighted means' states.
    (
        "SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, integral(time, value),
                integral(time, value, 'linear'), counter_diff(time, value), counter_rate(time, value)
         FROM iot.r GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT device, time_bucket(INTERVAL '1 day', time) AS d, counter_diff(time, value),
                integral(time, value, 'linear')
         FROM iot.r WHERE time >= TIMESTAMP '2026-01-01 05:30:00' GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT time_bucket(INTERVAL '1 hour', time) AS h, device, max(value) AS m, sum(value) FROM iot.r
         WHERE time >= TIMESTAMP '2026-01-01 05:30:00' AND time < TIMESTAMP '2026-01-02 03:00:00' GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT device, date_bin(INTERVAL '60 minutes', time) AS h, min(value), count(value),
                first_value(value ORDER BY time), last_value(value ORDER BY time)
         FROM iot.r WHERE device IN (1, 2) AND time > TIMESTAMP '2026-01-01 10:00:00' GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, avg(value) FROM iot.r
         WHERE time > now() - INTERVAL '400 days' GROUP BY 1, 2",
        true,
    ),
    // Keys from another table (`IN (SELECT …)`, planned as a semi-join): the rollup's rows are kept the same way.
    (
        "SELECT device, time_bucket(INTERVAL '1 day', time) AS d, avg(value), count(*) FROM iot.r
         WHERE device IN (SELECT d FROM (VALUES (1), (3)) AS v(d)) GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT device, time_weighted_mean(time, value), max(value) FROM iot.r
         WHERE time >= TIMESTAMP '2026-01-01 05:30:00' AND device IN (SELECT d FROM (VALUES (2)) AS v(d))
         GROUP BY device",
        true,
    ),
    (
        "SELECT t.device, time_bucket(INTERVAL '1 hour', t.time) AS h, max(t.value) FROM iot.r t
         GROUP BY 1, 2 HAVING max(t.value) > 15000",
        true,
    ),
    // The gap aggregates, from the stored gap state of the same threshold; not from another threshold's.
    (
        "SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, gap_count(time, INTERVAL '90 seconds'),
                gap_seconds(time, INTERVAL '90 seconds') FROM iot.r GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT device, time_bucket(INTERVAL '1 day', time) AS d, gap_seconds(time, INTERVAL '90 seconds')
         FROM iot.r WHERE time >= TIMESTAMP '2026-01-01 05:30:00' GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, gap_count(time, INTERVAL '30 seconds')
         FROM iot.r GROUP BY 1, 2",
        false,
    ),
    // A time range without a bucket: the rollup's hours inside it, the raw rows at its edges, merged.
    (
        "SELECT device, avg(value), min(value), max(value), count(*), sum(value), count(value), m4(time, value),
                time_weighted_mean(time, value), integral(time, value, 'linear'), counter_diff(time, value)
         FROM iot.r WHERE time >= TIMESTAMP '2026-01-01 05:30:00' AND time < TIMESTAMP '2026-01-02 20:10:00'
         GROUP BY device",
        true,
    ),
    ("SELECT device, max(value), count(*) FROM iot.r GROUP BY device", true),
    ("SELECT count(*), max(value) FROM iot.r WHERE time > TIMESTAMP '2026-01-01 10:00:00'", true),
    ("SELECT device, first_value(value ORDER BY time) FROM iot.r GROUP BY device", false),
    // Wider buckets and fewer keys: the rollup's hours merged.
    (
        "SELECT device, time_bucket(INTERVAL '1 day', time) AS d, avg(value), min(value), max(value), count(*),
                sum(value), count(value), first_value(value ORDER BY time), last_value(value ORDER BY time),
                m4(time, value)
         FROM iot.r GROUP BY 1, 2",
        true,
    ),
    (
        "SELECT time_bucket(INTERVAL '6 hours', time) AS b, count(*) AS n, avg(value), max(value) FROM iot.r
         GROUP BY 1",
        true,
    ),
    (
        "SELECT device, time_bucket(INTERVAL '3 hours', time) AS b, avg(value) FROM iot.r
         WHERE time >= TIMESTAMP '2026-01-01 04:30:00' AND time < TIMESTAMP '2026-01-02 20:00:00' GROUP BY 1, 2",
        true,
    ),
    ("SELECT device, time_bucket(INTERVAL '90 minutes', time) AS h, avg(value) FROM iot.r GROUP BY 1, 2", false),
    ("SELECT device, time_bucket(INTERVAL '30 minutes', time) AS h, avg(value) FROM iot.r GROUP BY 1, 2", false),
    (
        "SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, avg(value) FROM iot.r WHERE value > 5 GROUP BY 1, 2",
        false,
    ),
];

/// A query's rows, sorted, as text.
async fn answer(s: &Session, sql: &str) -> String {
    let batches: Vec<RecordBatch> = rows(s, &format!("SELECT * FROM ({sql}) ORDER BY 1, 2")).await;
    arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string()
}

async fn reads_rollup(s: &Session, sql: &str) -> bool {
    let plan = s.sql(sql).await.unwrap().into_optimized_plan().unwrap();
    plan.display_indent().to_string().contains("r_1h")
}

/// Every query gives the same rows with routing on and off, and is routed when it should be.
async fn check_answers(lake: &Lake) {
    let routed = Session::new(lake.clone());
    let unrouted = Session::new(lake.clone());
    unrouted.sql("SET timelake.rollup_routing = false").await.unwrap();
    for &(sql, routable) in QUERIES {
        assert_eq!(reads_rollup(&routed, sql).await, routable, "{sql}");
        assert!(!reads_rollup(&unrouted, sql).await, "{sql}");
        assert_eq!(answer(&routed, sql).await, answer(&unrouted, sql).await, "{sql}");
    }
}

async fn lake_with_rollup() -> (tempfile::TempDir, Lake) {
    let (dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &common::schema(), opts()).await.unwrap();
    // Two days, a reading a minute from 4 devices; whole numbers, so sums are exact.
    let b = batch(T0, 2 * 24 * 60, MINUTE, 4, |d, i| f64::from(d) * 10_000.0 + i as f64);
    t.append(&[b], AppendOptions::default()).await.unwrap();
    let s = Session::new(lake.clone());
    let created = rows(&s, CREATE).await;
    let text = arrow::util::pretty::pretty_format_batches(&created).unwrap().to_string();
    assert!(text.contains("iot.r_1h") && text.contains("192"), "4 devices × 48 hours\n{text}");
    (dir, lake)
}

#[tokio::test(flavor = "multi_thread")]
async fn rollups_answer_like_the_raw_table() {
    let (_dir, lake) = lake_with_rollup().await;
    let raw = lake.table("iot.r").await.unwrap();
    assert_eq!(raw.contract().rollups, ["iot.r_1h"]);
    let s = Session::new(lake.clone());
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r_1h").await, 192);
    // Columns in the order the query named them.
    let rollup = lake.table("iot.r_1h").await.unwrap();
    let schema = rollup.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "device",
            "bucket",
            "avg_value",
            "min_value",
            "max_value",
            "sum_value",
            "n",
            "count_value",
            "first",
            "last",
            "points",
            "twa",
            "twa_linear",
            "gaps"
        ]
    );
    check_answers(&lake).await;
}

/// A rollup with averages and counts but no sums answers wider buckets by weighting each
/// bucket's average by its count.
#[tokio::test(flavor = "multi_thread")]
async fn averages_merge_from_average_and_count() {
    let (_dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &common::schema(), opts()).await.unwrap();
    let b = batch(T0, 2 * 24 * 60, MINUTE, 4, |d, i| f64::from(d) * 10_000.0 + i as f64);
    t.append(&[b], AppendOptions::default()).await.unwrap();
    let s = Session::new(lake.clone());
    rows(
        &s,
        "CREATE MATERIALIZED ROLLUP iot.r_1h AS SELECT device, time_bucket(INTERVAL '1 hour', time) AS bucket,
         avg(value) AS mean, count(value) AS n FROM iot.r GROUP BY device, bucket WITH (refresh_interval = 'off')",
    )
    .await;
    let sql = "SELECT device, time_bucket(INTERVAL '1 day', time) AS d, avg(value) FROM iot.r GROUP BY 1, 2";
    let unrouted = Session::new(lake.clone());
    unrouted.sql("SET timelake.rollup_routing = false").await.unwrap();
    assert!(reads_rollup(&s, sql).await);
    assert_eq!(answer(&s, sql).await, answer(&unrouted, sql).await);
}

/// A daily rollup next to an hourly one is computed from the hourly rollup where that is up to
/// date, and holds what computing it from the raw rows gives: at creation and after a refresh.
#[tokio::test(flavor = "multi_thread")]
async fn a_wider_rollup_is_computed_from_a_narrower_one() {
    let (_dir, lake) = lake_with_rollup().await;
    let s = Session::new(lake.clone());
    rows(
        &s,
        "CREATE MATERIALIZED ROLLUP iot.r_1d AS SELECT device, time_bucket(INTERVAL '1 day', time) AS bucket,
         avg(value) AS avg_value, max(value), count(*) AS n FROM iot.r GROUP BY device, bucket
         WITH (refresh_interval = 'off')",
    )
    .await;
    let expected = "SELECT device, time_bucket(INTERVAL '1 day', time) AS bucket, avg(value) AS avg_value,
                           max(value) AS max_value, count(*) AS n
                    FROM iot.r GROUP BY 1, 2";
    let stored = "SELECT device, bucket, avg_value, max_value, n FROM iot.r_1d";
    let unrouted = Session::new(lake.clone());
    unrouted.sql("SET timelake.rollup_routing = false").await.unwrap();
    assert_eq!(answer(&unrouted, stored).await, answer(&unrouted, expected).await);

    // A third day, then a refresh of both: the hourly rollup first, the daily from it.
    let t = lake.table("iot.r").await.unwrap();
    let b = batch(T0 + 2 * DAY, 24 * 60, MINUTE, 4, |d, i| f64::from(d) * 10_000.0 + i as f64);
    t.append(&[b], AppendOptions { maintain: Some(false), ..AppendOptions::default() }).await.unwrap();
    rows(&s, "REFRESH MATERIALIZED ROLLUP iot.r_1h").await;
    rows(&s, "REFRESH MATERIALIZED ROLLUP iot.r_1d").await;
    assert_eq!(scalar_i64(&unrouted, "SELECT count(*) FROM iot.r_1d").await, 12);
    assert_eq!(answer(&unrouted, stored).await, answer(&unrouted, expected).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn changes_since_the_refresh_come_from_the_raw_table() {
    let (_dir, lake) = lake_with_rollup().await;
    let t = lake.table("iot.r").await.unwrap();
    // A newer write of an old reading (dedup keeps it), and a new hour, without refreshing.
    let late = batch(T0 + 3 * HOUR, 1, MINUTE, 2, |_, _| -1.0);
    let new = batch(T0 + 2 * DAY, 30, MINUTE, 4, |d, i| f64::from(d) + i as f64);
    let no_maintenance = AppendOptions { maintain: Some(false), ..Default::default() };
    let (t, _) = t.append(&[late], no_maintenance.clone()).await.unwrap();
    let (t, _) = t.append(&[new], no_maintenance).await.unwrap();
    check_answers(&lake).await;

    let s = Session::new(lake.clone());
    let refreshed = rows(&s, "REFRESH MATERIALIZED ROLLUP iot.r_1h").await;
    let text = arrow::util::pretty::pretty_format_batches(&refreshed).unwrap().to_string();
    // A changed hour is recomputed for every device: the late hour and the new one, 4 devices each.
    assert!(text.contains("| 8 "), "{text}");
    let bucket = "SELECT min_value FROM iot.r_1h WHERE device = 0 AND bucket = TIMESTAMP '2026-01-01 03:00:00'";
    assert_eq!(scalar_i64(&s, bucket).await, -1);
    check_answers(&lake).await;
    assert_eq!(rows(&s, "REFRESH MATERIALIZED ROLLUP iot.r_1h").await[0].num_rows(), 1);

    // Inline maintenance refreshes a rollup whose last refresh is older than its interval.
    let rollup = lake.table("iot.r_1h").await.unwrap();
    rollup.set_properties(&[("rollup-refresh-interval", Some("0s"))]).await.unwrap();
    let (_, result) = t
        .refresh()
        .await
        .unwrap()
        .append(&[batch(T0 + 2 * DAY + HOUR, 1, MINUTE, 1, |_, _| 7.0)], AppendOptions::default())
        .await
        .unwrap();
    let report = result.maintenance.unwrap().unwrap();
    assert_eq!((report.rollups_refreshed, report.rollup_rows), (1, 1));
    check_answers(&lake).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_definitions_and_drop() {
    let (_dir, lake) = lake_with_rollup().await;
    let s = Session::new(lake.clone());
    let create = |select: &str| format!("CREATE MATERIALIZED ROLLUP iot.bad AS {select}");
    for (select, msg) in [
        (
            "SELECT device, time_bucket(INTERVAL '1 hour', time) b, avg(value) FROM iot.r WHERE device = 1 GROUP BY 1, 2",
            "WHERE",
        ),
        ("SELECT site, time_bucket(INTERVAL '1 hour', time) b, avg(value) FROM iot.r GROUP BY 1, 2", "series-key"),
        ("SELECT device, time_bucket(INTERVAL '1 month', time) b, avg(value) FROM iot.r GROUP BY 1, 2", "time_bucket"),
        ("SELECT device, time_bucket(INTERVAL '1 hour', time) b, median(value) FROM iot.r GROUP BY 1, 2", "median"),
        ("SELECT device, avg(value) FROM iot.r GROUP BY 1", "time_bucket"),
        ("SELECT time_bucket(INTERVAL '1 hour', time) b, avg(value) FROM iot.r GROUP BY device, b", "grouping column"),
    ] {
        let err = s.sql(&create(select)).await.unwrap_err().to_string();
        assert!(err.contains(msg), "{select}: {err}");
    }
    assert!(lake.table("iot.bad").await.is_err(), "nothing is left behind");
    assert!(s.sql(CREATE).await.is_err(), "exists");
    rows(&s, &CREATE.replace("ROLLUP iot", "ROLLUP IF NOT EXISTS iot")).await;
    assert!(s.sql("DROP MATERIALIZED ROLLUP iot.r").await.unwrap_err().to_string().contains("not a rollup"));

    rows(&s, "DROP MATERIALIZED ROLLUP iot.r_1h").await;
    assert!(lake.table("iot.r_1h").await.is_err());
    let raw = lake.table("iot.r").await.unwrap();
    assert_eq!(raw.contract().rollups, Vec::<String>::new());
    assert!(!raw.metadata().properties().contains_key(property::ROLLUPS));
    assert!(s.sql("DROP MATERIALIZED ROLLUP iot.r_1h").await.is_err());
    rows(&s, "DROP MATERIALIZED ROLLUP IF EXISTS iot.r_1h").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_of_the_raw_table_rebuilds_the_rollup() {
    let (_dir, lake) = lake_with_rollup().await;
    let t = lake.table("iot.r").await.unwrap();
    let before = t.metadata().current_snapshot_id().unwrap();
    let s = Session::new(lake.clone());
    t.append(&[batch(T0 + 2 * DAY, 60, MINUTE, 4, |_, _| 1.0)], AppendOptions::default()).await.unwrap();
    rows(&s, "REFRESH MATERIALIZED ROLLUP iot.r_1h").await;
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r_1h").await, 196);

    // The rollup now holds an hour the raw table no longer has: it isn't used until rebuilt.
    lake.table("iot.r").await.unwrap().rollback(before).await.unwrap();
    let sql = QUERIES[0].0;
    let unrouted = Session::new(lake.clone());
    unrouted.sql("SET timelake.rollup_routing = false").await.unwrap();
    assert!(!reads_rollup(&s, sql).await);
    assert_eq!(answer(&s, sql).await, answer(&unrouted, sql).await);

    rows(&s, "REFRESH MATERIALIZED ROLLUP iot.r_1h").await;
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r_1h").await, 192);
    check_answers(&lake).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollups_keep_what_the_raw_table_drops() {
    let (_dir, lake) = lake_with_rollup().await;
    // The data is from January: a 30-day retention drops all of it from the raw table.
    let t = lake.table("iot.r").await.unwrap().set_properties(&[("retention", Some("30d"))]).await.unwrap();
    let (_, report) = t.maintain(timelake::MaintainOptions::default()).await.unwrap();
    assert!(report.partitions_dropped > 0);
    let s = Session::new(lake.clone());
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r").await, 0);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r_1h").await, 192);
    let unrouted = Session::new(lake.clone());
    unrouted.sql("SET timelake.rollup_routing = false").await.unwrap();
    // Buckets older than the raw table's retention aren't read from the rollup.
    for &(sql, _) in QUERIES {
        assert_eq!(answer(&s, sql).await, answer(&unrouted, sql).await, "{sql}");
    }
}

/// Gaps that cross bucket boundaries, and buckets full of gaps, merge from the stored gap state
/// into what the raw readings give, at the rollup's width and wider.
#[tokio::test(flavor = "multi_thread")]
async fn gaps_merge_across_buckets() {
    let (_dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &common::schema(), opts()).await.unwrap();
    // 20 hours a minute apart, nothing for 6 hours, then 11 hours two minutes apart.
    let first = batch(T0, 20 * 60, MINUTE, 4, |d, i| f64::from(d) + i as f64);
    let second = batch(T0 + 26 * HOUR, 11 * 30, 2 * MINUTE, 4, |d, i| f64::from(d) + i as f64);
    t.append(&[first, second], AppendOptions::default()).await.unwrap();
    let s = Session::new(lake.clone());
    rows(
        &s,
        "CREATE MATERIALIZED ROLLUP iot.r_1h AS SELECT device, time_bucket(INTERVAL '1 hour', time) AS bucket,
         gap_count(time, INTERVAL '90 seconds') AS gaps
         FROM iot.r GROUP BY device, bucket WITH (refresh_interval = 'off')",
    )
    .await;
    let unrouted = Session::new(lake.clone());
    unrouted.sql("SET timelake.rollup_routing = false").await.unwrap();
    for sql in [
        "SELECT device, time_bucket(INTERVAL '1 hour', time) AS h, gap_count(time, INTERVAL '90 seconds') AS n,
                gap_seconds(time, INTERVAL '90 seconds') AS s FROM iot.r GROUP BY 1, 2",
        "SELECT device, time_bucket(INTERVAL '1 day', time) AS d, gap_count(time, INTERVAL '90 seconds') AS n,
                gap_seconds(time, INTERVAL '90 seconds') AS s FROM iot.r GROUP BY 1, 2",
        "SELECT device, gap_count(time, INTERVAL '90 seconds') AS n, gap_seconds(time, INTERVAL '90 seconds') AS s
         FROM iot.r WHERE time >= TIMESTAMP '2026-01-01 10:30:20' GROUP BY 1",
    ] {
        assert!(reads_rollup(&s, sql).await, "{sql}");
        assert_eq!(answer(&s, sql).await, answer(&unrouted, sql).await, "{sql}");
    }
    // Device 0: the 6-hour silence, then 329 two-minute steps between its 330 later readings.
    let all = "SELECT gap_count(time, INTERVAL '90 seconds') FROM iot.r WHERE device = 0";
    assert_eq!(scalar_i64(&unrouted, all).await, 330);
}

/// A rollup that holds few readings per row answers plain aggregates, but not signal aggregates,
/// whose stored states cost more to merge than the readings do to read.
#[tokio::test(flavor = "multi_thread")]
async fn signal_states_need_enough_readings_per_row() {
    let (_dir, lake) = lake_with_rollup().await;
    let s = Session::new(lake.clone());
    rows(
        &s,
        "CREATE MATERIALIZED ROLLUP iot.r_1m AS SELECT device, time_bucket(INTERVAL '1 minute', time) AS bucket,
         avg(value) AS a, count(*) AS n, time_weighted_mean(time, value) AS twa
         FROM iot.r GROUP BY device, bucket WITH (refresh_interval = 'off')",
    )
    .await;
    let reads = |sql: &'static str| {
        let s = Session::new(lake.clone());
        async move { s.sql(sql).await.unwrap().into_optimized_plan().unwrap().display_indent().to_string() }
    };
    let avg = reads("SELECT device, time_bucket(INTERVAL '1 minute', time), avg(value) FROM iot.r GROUP BY 1, 2");
    assert!(avg.await.contains("r_1m"));
    let twa = reads(
        "SELECT device, time_bucket(INTERVAL '1 minute', time), time_weighted_mean(time, value) FROM iot.r GROUP BY 1, 2",
    );
    assert!(!twa.await.contains("r_1m"), "a reading a minute: one per row");
    // The hourly rollup holds 60 a row: enough.
    let hourly = reads("SELECT device, time_weighted_mean(time, value) FROM iot.r GROUP BY 1");
    assert!(hourly.await.contains("r_1h"));
}
