//! SQL over a lake: catalog listing, statement handling, and the latest-row query.

pub mod common;

use common::{T0, batch, opts, rows, scalar_i64, schema, strings, temp_lake};
use timelake::{AppendOptions, Session, TableOptions};

/// `SHOW TABLES` and `information_schema` list the lake without blocking the runtime, so they
/// work on a single-threaded runtime too.
#[tokio::test(flavor = "current_thread")]
async fn information_schema_on_current_thread_runtime() {
    let (_dir, lake) = temp_lake();
    lake.create_table("iot.readings", &schema(), opts()).await.unwrap();
    lake.create_table("tags", &schema(), TableOptions::default()).await.unwrap();
    let s = Session::new(lake.clone());

    let tables = rows(
        &s,
        "SELECT table_schema, table_name FROM information_schema.tables \
         WHERE table_catalog = 'timelake' AND table_schema <> 'information_schema' ORDER BY 1, 2",
    )
    .await;
    assert_eq!(strings(&tables, "table_schema"), ["default", "iot"]);
    assert_eq!(strings(&tables, "table_name"), ["tags", "readings"]);

    let shown = rows(&s, "SHOW TABLES").await;
    let names = strings(&shown, "table_name");
    assert!(names.contains(&"readings".to_string()) && names.contains(&"tags".to_string()), "{names:?}");

    // A table created after the first listing shows up in the next one.
    lake.create_table("iot.events", &schema(), opts()).await.unwrap();
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'iot'").await, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn queries_run_on_current_thread_runtime() {
    let (_dir, lake) = temp_lake();
    let t = lake.create_table("iot.readings", &schema(), opts()).await.unwrap();
    t.append(&[batch(T0, 10, 1_000_000, 3, |_, _| 1.0)], AppendOptions::default()).await.unwrap();
    let s = Session::new(lake);
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.readings").await, 30);
}
