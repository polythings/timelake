//! Time-bucket aggregation, streamed across files.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};

use crate::error::{Error, Result};
use crate::filter::micros;
use crate::keys::KeyEncoder;
use crate::output::{iso, to_json};
use crate::spec::{AggInput, AggOp, BucketSpec};

/// Start of the bucket holding time `t`: buckets are `every` wide, with a boundary at `origin`.
pub fn bucket_start(t: i64, every: i64, origin: i64) -> i64 {
    (t - origin).div_euclid(every) * every + origin
}

/// Bytes of a bucket start that sort like the number (big-endian, sign bit flipped).
fn bucket_bytes(bucket: i64) -> [u8; 8] {
    (bucket.cast_unsigned() ^ (1 << 63)).to_be_bytes()
}

fn bucket_from_bytes(bytes: &[u8]) -> i64 {
    let mut b = [0; 8];
    b.copy_from_slice(&bytes[..8]);
    (u64::from_be_bytes(b) ^ (1 << 63)).cast_signed()
}

/// Running state of one aggregate in one group.
#[derive(Debug, Clone, Default)]
struct Acc {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
    first: Option<(i64, f64)>,
    last: Option<(i64, f64)>,
}

impl Acc {
    fn add(&mut self, t: i64, v: f64) {
        self.count += 1;
        self.sum += v;
        self.min = Some(self.min.map_or(v, |m| m.min(v)));
        self.max = Some(self.max.map_or(v, |m| m.max(v)));
        // On equal times, the first value seen stays first and the last seen becomes last.
        if self.first.is_none_or(|(ft, _)| t < ft) {
            self.first = Some((t, v));
        }
        if self.last.is_none_or(|(lt, _)| t >= lt) {
            self.last = Some((t, v));
        }
    }

    fn value(&self, op: AggOp) -> Option<f64> {
        #[expect(clippy::cast_precision_loss, reason = "counts beyond 2^53 rows are out of reach")]
        let count = self.count as f64;
        match op {
            AggOp::Count => Some(count),
            AggOp::Sum => (self.count > 0).then_some(self.sum),
            AggOp::Avg => (self.count > 0).then(|| self.sum / count),
            AggOp::Min => self.min,
            AggOp::Max => self.max,
            AggOp::First => self.first.map(|(_, v)| v),
            AggOp::Last => self.last.map(|(_, v)| v),
        }
    }
}

/// Aggregates rows into time buckets (and `by` groups), one file's batches at a time.
#[derive(Debug)]
pub(crate) struct Aggregator {
    spec: BucketSpec,
    time_column: String,
    keys: KeyEncoder,
    /// Group key (bucket bytes, then the encoded `by` values) → one accumulator per aggregate.
    /// Ordered, so the output is sorted by bucket, then by the `by` values.
    groups: BTreeMap<Box<[u8]>, Vec<Acc>>,
}

impl Aggregator {
    pub(crate) fn new(spec: BucketSpec, time_column: String) -> Result<Self> {
        if spec.every <= 0 {
            return Err(Error::invalid("bucket width must be positive"));
        }
        if let Some(a) = spec.aggs.iter().find(|a| a.column == AggInput::Rows && a.op != AggOp::Count) {
            return Err(Error::invalid(format!("{} needs a column, not *", a.op.as_str())));
        }
        Ok(Self { keys: KeyEncoder::new(spec.by.clone()), spec, time_column, groups: BTreeMap::new() })
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        let time = batch
            .column_by_name(&self.time_column)
            .ok_or_else(|| Error::invalid(format!("unknown time column {}", self.time_column)))?;
        let time = micros(time)?;
        let by = self.keys.encode(batch)?;
        // Aggregated columns as f64, cast once per batch. `None` for `*` and for columns the file lacks.
        let values: Vec<Option<Float64Array>> = self
            .spec
            .aggs
            .iter()
            .map(|a| match &a.column {
                AggInput::Column(c) => batch
                    .column_by_name(c)
                    .map(|c| Ok(cast(c, &DataType::Float64)?.as_primitive::<Float64Type>().clone()))
                    .transpose(),
                AggInput::Rows => Ok(None),
            })
            .collect::<Result<_>>()?;
        let mut key = Vec::new();
        for row in (0..batch.num_rows()).filter(|&r| time.is_valid(r)) {
            let t = time.value(row);
            key.clear();
            key.extend_from_slice(&bucket_bytes(bucket_start(t, self.spec.every, self.spec.origin.unwrap_or(0))));
            if let Some(by) = &by {
                key.extend_from_slice(by.row(row).data());
            }
            if !self.groups.contains_key(key.as_slice()) {
                self.groups.insert(key.as_slice().into(), vec![Acc::default(); self.spec.aggs.len()]);
            }
            let Some(accs) = self.groups.get_mut(key.as_slice()) else { continue };
            for ((acc, agg), values) in accs.iter_mut().zip(&self.spec.aggs).zip(&values) {
                match values {
                    Some(v) if v.is_valid(row) => acc.add(t, v.value(row)),
                    None if agg.column == AggInput::Rows => acc.count += 1,
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// The groups as a JSON array: `{bucket, <by columns>, <aggregates>}` per group.
    pub(crate) fn finish(&self) -> Result<String> {
        let mut fields = vec![Field::new("bucket", DataType::Utf8, false)];
        let buckets = StringArray::from_iter_values(self.groups.keys().map(|k| iso(bucket_from_bytes(k))));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(buckets)];
        for (name, array) in self.spec.by.iter().zip(self.keys.decode(self.groups.keys().map(|k| &k[8..]))?) {
            fields.push(Field::new(name, array.data_type().clone(), true));
            columns.push(array);
        }
        for (i, agg) in self.spec.aggs.iter().enumerate() {
            let accs = self.groups.values().map(|accs| &accs[i]);
            let array: ArrayRef = match agg.op {
                AggOp::Count => Arc::new(accs.map(|a| a.count).collect::<UInt64Array>()),
                op => Arc::new(accs.map(|a| a.value(op).filter(|v| v.is_finite())).collect::<Float64Array>()),
            };
            fields.push(Field::new(agg.name(), array.data_type().clone(), true));
            columns.push(array);
        }
        to_json(&[RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?])
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, TimestampMicrosecondArray};
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn buckets_align_to_origin() {
        assert_eq!(bucket_start(0, 10, 0), 0);
        assert_eq!(bucket_start(9, 10, 0), 0);
        assert_eq!(bucket_start(10, 10, 0), 10);
        // Times before the epoch (or the origin) round down, not toward zero.
        assert_eq!(bucket_start(-1, 10, 0), -10);
        assert_eq!(bucket_start(-10, 10, 0), -10);
        assert_eq!(bucket_start(-11, 10, 0), -20);
        // Boundaries sit at origin + k * every, also for negative origins.
        assert_eq!(bucket_start(7, 10, 3), 3);
        assert_eq!(bucket_start(2, 10, 3), -7);
        assert_eq!(bucket_start(14, 10, -3), 7);
        assert_eq!(bucket_start(-4, 10, -3), -13);
    }

    #[test]
    fn bucket_bytes_sort_like_numbers() {
        let mut values = [i64::MIN, -10, -1, 0, 1, 10, i64::MAX];
        let mut bytes: Vec<_> = values.iter().map(|&v| bucket_bytes(v)).collect();
        bytes.sort_unstable();
        values.sort_unstable();
        assert_eq!(bytes.iter().map(|b| bucket_from_bytes(b)).collect::<Vec<_>>(), values);
    }

    fn batch(rows: &[(i64, i32, Option<f64>)]) -> RecordBatch {
        RecordBatch::try_from_iter([
            ("time", Arc::new(TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef),
            ("tag", Arc::new(rows.iter().map(|r| r.1).collect::<Int32Array>())),
            ("v", Arc::new(rows.iter().map(|r| r.2).collect::<Float64Array>())),
        ])
        .unwrap()
    }

    fn aggregate(spec: Value, batches: &[RecordBatch]) -> Value {
        let mut agg = Aggregator::new(serde_json::from_value(spec).unwrap(), "time".into()).unwrap();
        for b in batches {
            agg.push(b).unwrap();
        }
        serde_json::from_str(&agg.finish().unwrap()).unwrap()
    }

    #[test]
    fn computes_every_op_across_batches() {
        let ops = ["count", "sum", "avg", "min", "max", "first", "last"];
        let mut aggs: Vec<Value> = ops.iter().map(|op| json!({ "column": "v", "op": op })).collect();
        aggs.push(json!({ "column": "*", "op": "count" }));
        let spec = json!({ "every": 10, "aggs": aggs });
        // Out of time order and split across batches; nulls count for count(*) only.
        let a = batch(&[(5, 1, Some(3.0)), (1, 1, Some(1.0)), (12, 1, None)]);
        let b = batch(&[(3, 1, None), (8, 1, Some(5.0)), (15, 1, Some(2.0))]);
        assert_eq!(
            aggregate(spec, &[a, b]),
            json!([
                { "bucket": "1970-01-01T00:00:00Z", "v_count": 3, "v_sum": 9.0, "v_avg": 3.0, "v_min": 1.0,
                  "v_max": 5.0, "v_first": 1.0, "v_last": 5.0, "count": 4 },
                { "bucket": "1970-01-01T00:00:00.000010Z", "v_count": 1, "v_sum": 2.0, "v_avg": 2.0, "v_min": 2.0,
                  "v_max": 2.0, "v_first": 2.0, "v_last": 2.0, "count": 2 },
            ])
        );
    }

    #[test]
    fn groups_by_columns_in_value_order() {
        let spec = json!({ "every": 100, "by": ["tag"], "aggs": [{ "column": "v", "op": "sum" }] });
        let rows =
            aggregate(spec, &[batch(&[(1, 10, Some(1.0)), (2, 2, Some(2.0)), (3, 10, Some(3.0)), (150, 2, None)])]);
        assert_eq!(
            rows,
            json!([
                { "bucket": "1970-01-01T00:00:00Z", "tag": 2, "v_sum": 2.0 },
                { "bucket": "1970-01-01T00:00:00Z", "tag": 10, "v_sum": 4.0 },
                { "bucket": "1970-01-01T00:00:00.000100Z", "tag": 2, "v_sum": null },
            ])
        );
    }

    #[test]
    fn rejects_bad_specs() {
        let new = |spec: Value| Aggregator::new(serde_json::from_value(spec).unwrap(), "time".into());
        assert!(new(json!({ "every": 0, "aggs": [] })).is_err());
        assert!(new(json!({ "every": 10, "aggs": [{ "column": "*", "op": "avg" }] })).is_err());
    }
}
