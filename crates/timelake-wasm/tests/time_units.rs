//! Time columns in nanoseconds or milliseconds: queries give times in microseconds, so the reader
//! scales the column, its row-group statistics and its file bounds to match.
#![expect(clippy::unwrap_used, reason = "test helpers panic on bad fixtures")]

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampMillisecondArray, TimestampNanosecondArray};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use timelake_wasm::{Collector, FileReader, QuerySpec};

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
