//! Reads a table the engine wrote: `tests/fixtures/lake`, made from `readings.csv` with
//!
//! ```sh
//! timelake -w http://127.0.0.1/lake -o timelake_local_root=tests/fixtures import iot.readings \
//!   tests/fixtures/readings.csv --schema time:timestamptz,tags_id:int,velocity:double \
//!   --time-column time --series-key tags_id
//! ```
//!
//! Regenerate it when the table format changes.
#![expect(clippy::unwrap_used, reason = "test helpers panic on bad fixtures")]

use std::path::PathBuf;

use serde_json::{Value, json};
use timelake_wasm::{
    Collector, DataFile, FileReader, Head, ManifestContent, QuerySpec, TableMeta, parse_manifest, parse_manifest_list,
    plan_files,
};

const URL: &str = "http://127.0.0.1/";

/// The bytes behind a URL recorded in the table's metadata.
fn fetch(url: &str) -> Vec<u8> {
    let path = url.strip_prefix(URL).unwrap_or_else(|| panic!("unexpected URL {url}"));
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(path);
    std::fs::read(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()))
}

fn load() -> (TableMeta, Vec<DataFile>) {
    let meta = TableMeta::parse(
        std::str::from_utf8(&fetch(&format!("{URL}lake/iot/readings/metadata/v2.metadata.json"))).unwrap(),
    )
    .unwrap();
    let manifests = parse_manifest_list(&fetch(meta.manifest_list.as_deref().unwrap())).unwrap();
    assert!(manifests.iter().all(|m| m.content == ManifestContent::Data));
    let files = manifests
        .iter()
        .flat_map(|m| parse_manifest(&fetch(&m.path), &meta.schema, m.sequence_number).unwrap())
        .collect();
    (meta, files)
}

/// Runs a query over the planned files the way the client does: footer, plan, ranges, decode.
fn query(spec: Value) -> (usize, Value) {
    let (_, files) = load();
    let spec: QuerySpec = serde_json::from_value(spec).unwrap();
    let planned = plan_files(&files, &spec);
    let mut collector = Collector::new(spec.clone()).unwrap();
    for f in &planned {
        let bytes = fetch(&f.path);
        let mut reader = FileReader::new(f.size, &bytes).unwrap();
        for r in reader.plan(&spec) {
            let range = usize::try_from(r.start).unwrap()..usize::try_from(r.end).unwrap();
            reader.add_range(r.start, bytes[range].to_vec());
        }
        collector.push(&reader).unwrap();
    }
    (planned.len(), serde_json::from_str(&collector.finish().unwrap()).unwrap())
}

#[test]
fn parses_metadata_and_manifests() {
    let (meta, files) = load();
    let fields: Vec<_> = meta.schema.iter().map(|f| (f.name.as_str(), f.ty.as_str(), f.required)).collect();
    assert_eq!(fields, [("time", "timestamptz", true), ("tags_id", "int", false), ("velocity", "double", false)]);
    assert_eq!(meta.time_column.as_deref(), Some("time"));
    assert_eq!(meta.series_key, ["tags_id"]);
    assert_eq!(meta.chunk.as_deref(), Some("day"));
    assert!(meta.last_value_index.is_some());
    assert_eq!(meta.snapshot_count, 1);
    assert_eq!(meta.current_snapshot.as_ref().map(|s| s.operation.as_str()), Some("append"));

    let mut days: Vec<_> = files.iter().map(|f| (f.partition, f.records)).collect();
    days.sort_unstable();
    // Day partitions: days since the epoch.
    assert_eq!(days, [(Some(20_454), 8), (Some(20_455), 8)]);
    assert!(files.iter().all(|f| f.lower.contains_key("time") && f.upper.contains_key("tags_id")));
}

#[test]
fn reads_a_time_range_from_one_file() {
    let spec = json!({
        "timeColumn": "time", "series": { "tags_id": [2] },
        "from": 1_767_312_000_000_000_i64, "to": 1_767_333_600_000_001_i64, // 2026-01-02 00:00 to 06:00
        "columns": ["time", "velocity"],
    });
    let (files, rows) = query(spec);
    assert_eq!(files, 1);
    assert_eq!(
        rows,
        json!([
            { "time": "2026-01-02T00:00:00Z", "velocity": 24.5 },
            { "time": "2026-01-02T06:00:00Z", "velocity": 25.5 },
        ])
    );
}

#[test]
fn aggregates_buckets_across_files() {
    let aggs = json!([{ "column": "velocity", "op": "avg" }, { "column": "*", "op": "count" }]);
    let spec = json!({ "timeColumn": "time", "series": { "tags_id": [1] }, "bucket": { "every": 86_400_000_000_i64, "aggs": aggs } });
    let (files, rows) = query(spec);
    assert_eq!(files, 2);
    assert_eq!(
        rows,
        json!([
            { "bucket": "2026-01-01T00:00:00Z", "velocity_avg": 11.5, "count": 4 },
            { "bucket": "2026-01-02T00:00:00Z", "velocity_avg": 47.0 / 3.0, "count": 4 },
        ])
    );
}

#[test]
fn scans_for_the_latest_rows_like_the_index() {
    let by_tag = |rows: Value| {
        let mut rows = rows.as_array().unwrap().clone();
        rows.sort_by_key(|r| r["tags_id"].as_i64());
        Value::Array(rows)
    };
    let (_, latest) =
        query(json!({ "timeColumn": "time", "latestBy": ["tags_id"], "columns": ["tags_id", "velocity"] }));
    let latest = by_tag(latest);
    assert_eq!(latest, json!([{ "tags_id": 1, "velocity": 17.0 }, { "tags_id": 2, "velocity": 27.5 }]));

    // The engine's last-value index holds the same rows.
    let (meta, _) = load();
    let bytes = fetch(meta.last_value_index.as_deref().unwrap());
    let spec: QuerySpec =
        serde_json::from_value(json!({ "timeColumn": "time", "columns": ["tags_id", "velocity"] })).unwrap();
    let mut reader = FileReader::new(bytes.len() as u64, &bytes).unwrap();
    reader.plan(&spec);
    reader.add_range(0, bytes);
    let mut collector = Collector::new(spec).unwrap();
    collector.push(&reader).unwrap();
    assert_eq!(by_tag(serde_json::from_str(&collector.finish().unwrap()).unwrap()), latest);
}

/// The head describes the same version as the metadata chain: files, bounds, the last-value
/// index, and footers that open each file without reading its tail.
#[test]
fn head_matches_the_metadata() {
    let head = Head::parse(&fetch(&format!("{URL}lake/iot/readings/metadata/timelake-head"))).unwrap();
    let (meta, mut files) = load();
    assert_eq!(head.version, 2);
    assert_eq!(head.meta.manifest_list, meta.manifest_list);
    let mut from_head = head.files.clone();
    from_head.sort_by(|a, b| a.path.cmp(&b.path));
    files.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(from_head, files);

    let spec: QuerySpec =
        serde_json::from_value(json!({ "timeColumn": "time", "columns": ["tags_id", "velocity"] })).unwrap();
    let mut collector = Collector::new(spec.clone()).unwrap();
    collector.push_batch(head.last_values.as_ref().unwrap()).unwrap();
    let rows: Value = serde_json::from_str(&collector.finish().unwrap()).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);

    // Reading a file from its footer in the head gives what reading its tail gives.
    let read = |mut reader: FileReader, bytes: &[u8]| {
        for r in reader.plan(&spec) {
            let range = usize::try_from(r.start).unwrap()..usize::try_from(r.end).unwrap();
            reader.add_range(r.start, bytes[range].to_vec());
        }
        let mut c = Collector::new(spec.clone()).unwrap();
        c.push(&reader).unwrap();
        c.finish().unwrap()
    };
    for f in &files {
        let bytes = fetch(&f.path);
        let footer = head.footers.get(&f.path).unwrap();
        let from_footer = FileReader::from_footer(f.size, footer).unwrap();
        assert_eq!(read(from_footer, &bytes), read(FileReader::new(f.size, &bytes).unwrap(), &bytes));
    }
}
