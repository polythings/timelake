//! Time columns in nanoseconds or milliseconds: queries give times in microseconds, so the reader
//! scales the column, its row-group statistics and its file bounds to match.
#![expect(clippy::unwrap_used, reason = "test helpers panic on bad fixtures")]

use std::sync::Arc;

use std::collections::BTreeMap;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampMillisecondArray, TimestampNanosecondArray};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use timelake_format::head::built_in;
use timelake_format::head::content::HeadBuilder;
use timelake_format::iceberg::{DataFile, Entry, Status};
use timelake_wasm::{Collector, FileReader, Head, QuerySpec, plan_files};

/// 2026-01-01T00:00:00Z in microseconds.
const T0: i64 = 1_767_225_600_000_000;
const HOUR: i64 = 3_600_000_000;

/// Four hourly rows in two row groups, the time column in `unit`.
fn file(times: ArrayRef) -> Vec<u8> {
    let batch =
        RecordBatch::try_from_iter([("time", times), ("v", Arc::new(Int64Array::from(vec![0, 1, 2, 3])))]).unwrap();
    let props = WriterProperties::builder().set_max_row_group_row_count(Some(2)).build();
    let mut out = vec![];
    let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    out
}

fn query(bytes: &[u8], spec: Value) -> (usize, Value) {
    let spec: QuerySpec = serde_json::from_value(spec).unwrap();
    let mut reader = FileReader::new(bytes.len() as u64, bytes).unwrap();
    let ranges = reader.plan(&spec);
    for r in &ranges {
        let range = usize::try_from(r.start).unwrap()..usize::try_from(r.end).unwrap();
        reader.add_range(r.start, bytes[range].to_vec());
    }
    let mut collector = Collector::new(spec).unwrap();
    collector.push(&reader).unwrap();
    (ranges.len(), serde_json::from_str(&collector.finish().unwrap()).unwrap())
}

fn check(bytes: &[u8]) {
    let range =
        json!({ "timeColumn": "time", "from": T0 + 2 * HOUR, "to": T0 + 3 * HOUR + 1, "columns": ["time", "v"] });
    let (_, rows) = query(bytes, range);
    assert_eq!(rows, json!([{ "time": "2026-01-01T02:00:00Z", "v": 2 }, { "time": "2026-01-01T03:00:00Z", "v": 3 }]));

    let later = json!({ "timeColumn": "time", "from": T0 + 10 * HOUR, "columns": ["v"] });
    assert_eq!(query(bytes, later), (0, json!([])), "the statistics rule out every row group");

    let aggs = json!([{ "column": "*", "op": "count" }]);
    let buckets = json!({ "timeColumn": "time", "bucket": { "every": 2 * HOUR, "aggs": aggs } });
    let (_, rows) = query(bytes, buckets);
    assert_eq!(
        rows,
        json!([{ "bucket": "2026-01-01T00:00:00Z", "count": 2 }, { "bucket": "2026-01-01T02:00:00Z", "count": 2 }])
    );
}

#[test]
fn nanosecond_time_columns() {
    let nanos: Vec<i64> = (0..4).map(|h| (T0 + h * HOUR) * 1000).collect();
    check(&file(Arc::new(TimestampNanosecondArray::from(nanos).with_timezone("+00:00"))));
}

#[test]
fn millisecond_time_columns() {
    let millis: Vec<i64> = (0..4).map(|h| (T0 + h * HOUR) / 1000).collect();
    check(&file(Arc::new(TimestampMillisecondArray::from(millis).with_timezone("+00:00"))));
}

/// A head listing one file whose `timestamptz_ns` column spans `[lower, upper]` nanoseconds.
fn head_with_nanosecond_bounds(lower: i64, upper: i64) -> Vec<u8> {
    let metadata = json!({
        "format-version": 2, "location": "s3://b/t", "current-schema-id": 0,
        "schemas": [{ "schema-id": 0, "fields": [{ "id": 1, "name": "time", "type": "timestamptz_ns", "required": true }] }],
        "properties": { "timelake.time-column": "time" },
    });
    let file = DataFile {
        path: "s3://b/t/data/a.parquet".into(),
        record_count: 2,
        file_size: 100,
        lower_bounds: BTreeMap::from([(1, lower.to_le_bytes().to_vec())]),
        upper_bounds: BTreeMap::from([(1, upper.to_le_bytes().to_vec())]),
        ..DataFile::default()
    };
    let entry = Entry {
        status: Status::Added,
        snapshot_id: 1,
        sequence_number: 1,
        file_sequence_number: 1,
        file: Arc::new(file),
    };
    let head = HeadBuilder::new(1, built_in("arrow").unwrap(), metadata.to_string(), &[entry], 0).unwrap();
    head.finish().unwrap()
}

/// A head's file bounds are scaled like a manifest's: a file of nanosecond times is planned for a
/// microsecond range inside it, and skipped for one after it.
#[test]
fn head_bounds_of_nanosecond_columns() {
    let head = Head::parse(&head_with_nanosecond_bounds(T0 * 1000, (T0 + HOUR) * 1000 + 1)).unwrap();
    let planned = |spec: Value| plan_files(&head.files, &serde_json::from_value(spec).unwrap()).len();
    assert_eq!(planned(json!({ "timeColumn": "time", "from": T0 + 1, "to": T0 + 2 })), 1);
    assert_eq!(planned(json!({ "timeColumn": "time", "from": T0 + HOUR + 1 })), 1, "rounded up");
    assert_eq!(planned(json!({ "timeColumn": "time", "from": T0 + HOUR + 2 })), 0);
}
