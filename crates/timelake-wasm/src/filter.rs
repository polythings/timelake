//! Exact row filters: the time range and the series values.

use std::sync::Arc;

use arrow_arith::boolean::{and_kleene, or_kleene};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, Scalar, StringArray};
use arrow_cast::cast;
use arrow_ord::cmp::{eq, gt_eq, lt};
use arrow_schema::{DataType, TimeUnit};
use arrow_select::filter::filter_record_batch;
use serde_json::Value as Json;

use crate::error::Result;
use crate::spec::QuerySpec;

/// A time column as microseconds since the epoch, whatever unit it is stored in.
pub(crate) fn micros(col: &ArrayRef) -> Result<Int64Array> {
    let col = match col.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) | DataType::Int64 => col,
        DataType::Timestamp(..) => &cast(col, &DataType::Timestamp(TimeUnit::Microsecond, None))?,
        _ => col,
    };
    Ok(cast(col, &DataType::Int64)?.as_primitive::<Int64Type>().clone())
}

/// `v` as a one-row array of the type it naturally has in JSON.
fn json_array(v: &Json) -> Option<ArrayRef> {
    Some(match v {
        Json::Number(n) => match n.as_i64() {
            Some(i) => Arc::new(Int64Array::from(vec![i])),
            None => Arc::new(Float64Array::from(vec![n.as_f64()?])),
        },
        Json::String(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Json::Bool(b) => Arc::new(BooleanArray::from(vec![*b])),
        Json::Null | Json::Array(_) | Json::Object(_) => return None,
    })
}

/// `v` as a scalar of type `ty`, or `None` when `ty` can't hold it exactly (`1.5` or `2^40` for
/// an `int` column): such a value matches no row.
fn exact_scalar(v: &Json, ty: &DataType) -> Result<Option<Scalar<ArrayRef>>> {
    let Some(value) = json_array(v) else { return Ok(None) };
    // Casting is lossy (it truncates floats, and an impossible cast gives null); a value is exact
    // when it survives the round trip unchanged.
    let converted = cast(&value, ty)?;
    if converted.is_null(0) {
        return Ok(None);
    }
    let same = eq(&cast(&converted, value.data_type())?, &value)?;
    Ok((same.is_valid(0) && same.value(0)).then(|| Scalar::new(converted)))
}

/// Rows of `col` equal to any of `values`.
fn eq_any(col: &ArrayRef, values: &[Json]) -> Result<BooleanArray> {
    let mut mask = BooleanArray::from(vec![false; col.len()]);
    for v in values {
        if let Some(scalar) = exact_scalar(v, col.data_type())? {
            mask = or_kleene(&mask, &eq(col, &scalar)?)?;
        }
    }
    Ok(mask)
}

/// Keeps the rows in the query's time range whose series columns match its values.
pub(crate) fn filter(batch: &RecordBatch, spec: &QuerySpec) -> Result<RecordBatch> {
    let mut mask: Option<BooleanArray> = None;
    let mut and = |m: BooleanArray| -> Result<()> {
        mask = Some(match mask.take() {
            None => m,
            Some(acc) => and_kleene(&acc, &m)?,
        });
        Ok(())
    };
    if let Some(time) = spec.time_column.as_ref().and_then(|tc| batch.column_by_name(tc))
        && (spec.from.is_some() || spec.to.is_some())
    {
        let time = micros(time)?;
        if let Some(from) = spec.from {
            and(gt_eq(&time, &Int64Array::new_scalar(from))?)?;
        }
        if let Some(to) = spec.to {
            and(lt(&time, &Int64Array::new_scalar(to))?)?;
        }
    }
    for (name, values) in &spec.series {
        if let Some(col) = batch.column_by_name(name)
            && !values.is_empty()
        {
            and(eq_any(col, values)?)?;
        }
    }
    match mask {
        None => Ok(batch.clone()),
        Some(mask) => Ok(filter_record_batch(batch, &mask)?),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema};
    use serde_json::json;

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("time", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("tag", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("speed", DataType::Float64, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![10, 20, 30, 40, 50]).with_timezone("UTC")),
                Arc::new(Int32Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("7"), Some("b"), None])),
                Arc::new(Float64Array::from(vec![0.5, 1.5, 2.0, 3.0, 4.0])),
            ],
        )
        .unwrap()
    }

    /// Times of the rows matching `spec`.
    fn times(spec: serde_json::Value) -> Vec<i64> {
        let spec: QuerySpec = serde_json::from_value(spec).unwrap();
        let out = filter(&batch(), &spec).unwrap();
        micros(out.column_by_name("time").unwrap()).unwrap().values().to_vec()
    }

    #[test]
    fn time_range_is_half_open() {
        assert_eq!(times(json!({ "timeColumn": "time", "from": 20, "to": 40 })), [20, 30]);
        assert_eq!(times(json!({ "timeColumn": "time", "from": 45 })), [50]);
        assert_eq!(times(json!({ "timeColumn": "time", "to": 11 })), [10]);
        assert_eq!(times(json!({ "from": 20 })), [10, 20, 30, 40, 50], "no time column, no time filter");
    }

    #[test]
    fn series_values_combine_with_time() {
        assert_eq!(times(json!({ "series": { "tag": [1] } })), [20, 50]);
        assert_eq!(times(json!({ "series": { "tag": [0, 2] } })), [10, 30]);
        assert_eq!(times(json!({ "series": { "tag": [1] }, "timeColumn": "time", "to": 30 })), [20]);
        assert_eq!(times(json!({ "series": { "tag": [1], "name": ["b"] } })), [20]);
        assert_eq!(times(json!({ "series": { "tag": [] } })), [10, 20, 30, 40, 50]);
    }

    #[test]
    fn values_a_column_cannot_hold_match_nothing() {
        // B3: 1.5 used to become 0 and 2^32 + 1 wrapped to 1.
        assert_eq!(times(json!({ "series": { "tag": [1.5] } })), Vec::<i64>::new());
        assert_eq!(times(json!({ "series": { "tag": [4_294_967_297_i64] } })), Vec::<i64>::new());
        assert_eq!(times(json!({ "series": { "tag": [-2_147_483_649_i64] } })), Vec::<i64>::new());
        assert_eq!(times(json!({ "series": { "tag": ["x"] } })), Vec::<i64>::new());
        assert_eq!(times(json!({ "series": { "tag": [null] } })), Vec::<i64>::new());
        // Exact conversions still match.
        assert_eq!(times(json!({ "series": { "tag": [2.0] } })), [30]);
        assert_eq!(times(json!({ "series": { "tag": ["2"] } })), [30]);
        assert_eq!(times(json!({ "series": { "name": [7] } })), [30]);
        assert_eq!(times(json!({ "series": { "speed": [1.5, 2] } })), [20, 30]);
    }
}
