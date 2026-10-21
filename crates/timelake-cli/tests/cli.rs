//! Smoke test of the `timelake` binary on a temporary local warehouse.

#![expect(clippy::unwrap_used, reason = "test code")]

use std::path::Path;
use std::process::Command;

/// Runs `timelake -w <warehouse> <args…>` and returns stdout and stderr; panics if it fails.
fn timelake(warehouse: &Path, args: &[&str]) -> (String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_timelake"))
        .arg("--warehouse")
        .arg(warehouse)
        .args(args)
        .env_remove("TIMELAKE_WAREHOUSE")
        .output()
        .unwrap();
    let (stdout, stderr) = (String::from_utf8(out.stdout).unwrap(), String::from_utf8(out.stderr).unwrap());
    assert!(out.status.success(), "timelake {args:?} failed:\n{stderr}");
    (stdout, stderr)
}

#[test]
fn create_import_query_maintain() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let csv = dir.path().join("readings.csv");
    std::fs::write(
        &csv,
        "time,device,value\n\
         2026-01-01T00:00:00Z,1,1.5\n\
         2026-01-01T00:01:00Z,1,2.5\n\
         2026-01-01T00:00:00Z,2,10.0\n\
         2026-01-02T00:00:00Z,2,11.0\n",
    )
    .unwrap();
    let csv = csv.to_str().unwrap();

    let (out, _) =
        timelake(&warehouse, &["import", "iot.readings", csv, "--time-column", "time", "--series-key", "device"]);
    assert!(out.contains("iot.readings: appended 4 rows in 1 commits"), "{out}");

    let (out, _) = timelake(&warehouse, &["ls"]);
    assert_eq!(out.trim(), "iot.readings");

    // Statements are split by the SQL parser, so a ';' inside a string literal stays put.
    let (out, _) = timelake(
        &warehouse,
        &["sql", "--format", "csv", "SELECT 'a;b' AS s; SELECT count(*) AS n FROM iot.readings WHERE device = 2"],
    );
    assert_eq!(out, "s\na;b\nn\n2\n");

    let (out, _) = timelake(&warehouse, &["latest", "iot.readings", "--format", "json"]);
    assert_eq!(out.lines().count(), 2, "{out}");
    assert!(out.contains(r#""value":2.5"#) && out.contains(r#""value":11.0"#), "{out}");

    // Appending the same batch id again is skipped and reported as such.
    let (out, _) = timelake(&warehouse, &["append", "iot.readings", csv, "--batch-id", "b1"]);
    assert!(out.contains("appended 4 rows in 1 commits"), "{out}");
    let (out, err) = timelake(&warehouse, &["append", "iot.readings", csv, "--batch-id", "b1"]);
    assert!(out.contains("appended 0 rows in 0 commits, skipped 1 duplicate batch,"), "{out}");
    assert!(err.contains("duplicate batch, skipped"), "{err}");

    let (out, _) = timelake(&warehouse, &["maintain"]);
    assert!(out.starts_with("iot.readings: compacted 2 partitions"), "{out}");

    let (out, _) = timelake(&warehouse, &["info", "iot.readings", "--json"]);
    let info: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(info["rows"], 4, "{info}");
    assert_eq!(info["series_key"], serde_json::json!(["device"]));
    assert_eq!(info["partitions"].as_array().unwrap().len(), 2);

    let (out, _) = timelake(&warehouse, &["info", "iot.readings"]);
    assert!(out.contains("data       4 rows in 2 files"), "{out}");

    let (out, _) = timelake(&warehouse, &["drop", "iot.readings"]);
    assert_eq!(out.trim(), "dropped iot.readings");
}

#[test]
fn rollups_from_sql() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let csv = dir.path().join("readings.csv");
    let write = |rows: &str| std::fs::write(&csv, format!("time,device,value\n{rows}")).unwrap();
    write(
        "2026-01-01T00:00:00Z,1,1.5\n2026-01-01T06:00:00Z,1,2.5\n2026-01-01T00:00:00Z,2,10.0\n2026-01-02T00:00:00Z,2,11.0\n",
    );
    let csv_path = csv.to_str().unwrap();
    timelake(&warehouse, &["import", "iot.readings", csv_path, "--time-column", "time", "--series-key", "device"]);

    let create = "CREATE MATERIALIZED ROLLUP iot.daily AS \
                  SELECT device, time_bucket(INTERVAL '1 day', time) AS day, max(value) AS peak \
                  FROM iot.readings GROUP BY device, day; \
                  SELECT device, peak FROM iot.daily ORDER BY device, day";
    let (out, _) = timelake(&warehouse, &["sql", "--format", "csv", create]);
    assert_eq!(out, "rollup,rows\niot.daily,3\ndevice,peak\n1,2.5\n2,10.0\n2,11.0\n");

    write("2026-01-01T12:00:00Z,1,9.0\n");
    timelake(&warehouse, &["append", "iot.readings", csv_path]);
    let (out, _) = timelake(&warehouse, &["maintain", "iot.readings"]);
    // The changed day is recomputed for both devices.
    assert!(out.contains("refreshed 1 rollups (2 rows)"), "{out}");
    let (out, _) = timelake(&warehouse, &["sql", "--format", "csv", "SELECT peak FROM iot.daily WHERE device = 1"]);
    assert_eq!(out, "peak\n9.0\n");

    let (out, _) = timelake(&warehouse, &["settings", "iot.readings"]);
    assert!(out.lines().any(|l| l.starts_with("rollups") && l.contains("iot.daily")), "{out}");
    timelake(&warehouse, &["sql", "DROP MATERIALIZED ROLLUP iot.daily"]);
    let (out, _) = timelake(&warehouse, &["ls"]);
    assert_eq!(out.trim(), "iot.readings");
}

/// Commits the table's metadata again without `timelake.format-version`, as Timelake 0.2 wrote it.
/// Timelake never removes the property itself, so the test edits the metadata files directly.
fn as_written_by_timelake_0_2(metadata: &Path) {
    let version: u64 = std::fs::read_to_string(metadata.join("version-hint.text")).unwrap().trim().parse().unwrap();
    let current = metadata.join(format!("v{version}.metadata.json"));
    let mut doc: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&current).unwrap()).unwrap();
    doc["properties"].as_object_mut().unwrap().remove("timelake.format-version");
    let next = metadata.join(format!("v{}.metadata.json", version + 1));
    std::fs::write(next, doc.to_string()).unwrap();
    std::fs::write(metadata.join("version-hint.text"), (version + 1).to_string()).unwrap();
}

#[test]
fn info_shows_sub_day_retention() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let create = ["create", "iot.r", "--schema", "time:timestamp,device:int", "--time-column", "time"];
    timelake(&warehouse, &[&create[..], &["--prop", "retention=12h"]].concat());

    let (out, _) = timelake(&warehouse, &["info", "iot.r"]);
    assert!(out.lines().any(|l| l == "retention  12h"), "{out}");
    let (out, _) = timelake(&warehouse, &["info", "iot.r", "--json"]);
    let info: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(info["retention"], "12h");
}

#[test]
fn settings_and_alter() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let create = ["create", "iot.r", "--schema", "time:timestamp,device:int", "--time-column", "time"];
    timelake(&warehouse, &[&create[..], &["--series-key", "device", "--prop", "retention=90d"]].concat());

    let (out, _) = timelake(&warehouse, &["settings"]);
    assert!(out.lines().any(|l| l.starts_with("read-dedup ") && l.contains("table,session")), "{out}");
    timelake(&warehouse, &["alter", "iot.r", "--set", "timelake.read_dedup=false", "--unset", "retention"]);
    let (out, _) = timelake(&warehouse, &["settings", "iot.r", "--json"]);
    let settings: serde_json::Value = serde_json::from_str(&out).unwrap();
    let value = |name: &str| settings.as_array().unwrap().iter().find(|s| s["name"] == name).unwrap()["value"].clone();
    assert_eq!((value("read-dedup"), value("retention")), (serde_json::json!("false"), serde_json::Value::Null));

    let fails = |args: &[&str]| {
        let out = Command::new(env!("CARGO_BIN_EXE_timelake")).arg("-w").arg(&warehouse).args(args).output().unwrap();
        assert!(!out.status.success(), "{args:?} succeeded");
        String::from_utf8(out.stderr).unwrap()
    };
    assert!(fails(&["alter", "iot.r", "--set", "chunk-interval=hour"]).contains("fixed when the table is created"));
    assert!(fails(&["sql", "--set", "timelake.nope=1", "SELECT 1"]).contains("unknown setting"));
    // A query error says its message once, not once per layer that wraps it.
    let err = fails(&["sql", "SELECT nope FROM iot.r"]);
    assert_eq!(err.matches("No field named nope").count(), 1, "{err}");
}

#[test]
fn upgrade_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let csv = dir.path().join("r.csv");
    std::fs::write(&csv, "time,device,value\n2026-01-01T00:00:00Z,1,1.5\n").unwrap();
    let csv = csv.to_str().unwrap();
    let import = ["import", "iot.r", csv, "--time-column", "time", "--series-key", "device"];
    // A format 0.1.0 table, as Timelake 0.2 wrote them.
    timelake(&warehouse, &import);
    as_written_by_timelake_0_2(&warehouse.join("iot/r/metadata"));
    timelake(&warehouse, &["append", "iot.r", csv]);

    let (out, _) = timelake(&warehouse, &["upgrade", "--dry-run"]);
    assert!(out.contains("iot.r: would:") && out.contains("set table format 0.1.0 → 0.3.0"), "{out}");
    let (out, _) = timelake(&warehouse, &["upgrade", "iot.r"]);
    let undo = out.lines().find_map(|l| l.split("undo: ").nth(1)).unwrap_or_else(|| panic!("{out}"));
    let (out, _) = timelake(&warehouse, &["upgrade", "iot.r"]);
    assert!(out.contains("already current"), "{out}");

    let args: Vec<&str> = undo.split_whitespace().skip(1).collect(); // "timelake rollback iot.r <id>"
    let (out, _) = timelake(&warehouse, &args);
    assert!(out.contains("is current again"), "{out}");
}

/// Runs `timelake -w <warehouse> <args…>`, expecting it to fail; returns stderr.
fn fails(warehouse: &Path, args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_timelake")).arg("-w").arg(warehouse).args(args).output().unwrap();
    assert!(!out.status.success(), "{args:?} succeeded");
    String::from_utf8(out.stderr).unwrap()
}

#[test]
fn file_names_in_any_case() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let csv = dir.path().join("readings.CSV");
    std::fs::write(&csv, "time,device,value\n2026-01-01T00:00:00Z,1,1.5\n").unwrap();
    let csv = csv.to_str().unwrap();
    let (out, _) = timelake(&warehouse, &["import", "iot.r", csv, "--time-column", "time", "--series-key", "device"]);
    assert!(out.contains("appended 1 rows"), "{out}");
}

#[test]
fn empty_results_keep_their_columns() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let empty = "SELECT 1 AS a, 'x' AS b WHERE false";
    let (out, _) = timelake(&warehouse, &["sql", "--format", "csv", empty]);
    assert_eq!(out, "a,b\n");
    let (out, _) = timelake(&warehouse, &["sql", empty]);
    assert!(out.contains("| a | b |"), "{out}");
    let (out, _) = timelake(&warehouse, &["sql", "--format", "json", empty]);
    assert_eq!(out, "");
}

#[test]
fn session_settings_take_any_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let show = "SELECT value FROM information_schema.df_settings WHERE name = 'timelake.read_dedup'";
    for key in ["read_dedup", "read-dedup", "timelake.read_dedup"] {
        let (out, _) = timelake(&warehouse, &["sql", "--format", "csv", "--set", &format!("{key}=false"), show]);
        assert_eq!(out, "value\nfalse\n", "{key}");
    }
    assert!(fails(&warehouse, &["sql", "--set", "nope=1", "SELECT 1"]).contains("unknown setting"));
}

#[test]
fn options_are_checked_when_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let create = ["create", "iot.r", "--schema", "time:timestamp", "--time-column", "time"];
    let err = fails(&warehouse, &[&create[..], &["--chunk", "fortnight"]].concat());
    assert!(err.contains("invalid value 'fortnight' for '--chunk"), "{err}");
    let err = fails(&warehouse, &[&create[..], &["--retention", "soon"]].concat());
    assert!(err.contains("invalid value 'soon' for '--retention"), "{err}");
}

#[test]
fn info_prints_numbers_and_utc_times() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let csv = dir.path().join("r.csv");
    std::fs::write(&csv, "time,device,value\n2026-01-01T00:00:00Z,1,1.5\n").unwrap();
    let csv = csv.to_str().unwrap();
    timelake(&warehouse, &["import", "iot.r", csv, "--time-column", "time", "--series-key", "device"]);
    let (out, _) = timelake(&warehouse, &["info", "iot.r", "--json"]);
    let info: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(info["format_version"], 2, "{info}");
    assert_eq!(info["snapshots"][0]["operation"], "append", "{info}");
    let (out, _) = timelake(&warehouse, &["info", "iot.r"]);
    assert!(out.contains("Iceberg v2"), "{out}");
    let snapshot = out.lines().find(|l| l.contains("← current")).unwrap_or_else(|| panic!("{out}"));
    assert!(snapshot.contains("Z append"), "{snapshot}");
}
