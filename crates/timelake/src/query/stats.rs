//! Statistics from Iceberg manifests: per-file min/max/null counts for pruning and planning.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::stats::Precision;
use datafusion::common::{Column, ColumnStatistics, ScalarValue, Statistics};
use datafusion::physical_optimizer::pruning::PruningStatistics;
use iceberg::spec::{Datum, PrimitiveLiteral};

use crate::table::LiveEntry;

/// Converts an Iceberg bound to a scalar of the Arrow column type (`None` if the types don't
/// match or aren't used for pruning).
pub(super) fn datum_to_scalar(d: &Datum, dt: &DataType) -> Option<ScalarValue> {
    use PrimitiveLiteral as L;
    let v = match (d.literal(), dt) {
        (L::Boolean(b), DataType::Boolean) => ScalarValue::Boolean(Some(*b)),
        (L::Int(i), DataType::Int32) => ScalarValue::Int32(Some(*i)),
        (L::Int(i), DataType::Date32) => ScalarValue::Date32(Some(*i)),
        (L::Long(i), DataType::Int64) => ScalarValue::Int64(Some(*i)),
        (L::Long(i), DataType::Timestamp(TimeUnit::Microsecond, tz)) => {
            ScalarValue::TimestampMicrosecond(Some(*i), tz.clone())
        }
        (L::Long(i), DataType::Timestamp(TimeUnit::Nanosecond, tz)) => {
            ScalarValue::TimestampNanosecond(Some(*i), tz.clone())
        }
        (L::Float(f), DataType::Float32) => ScalarValue::Float32(Some(f.0)),
        (L::Double(f), DataType::Float64) => ScalarValue::Float64(Some(f.0)),
        (L::String(s), DataType::Utf8) => ScalarValue::Utf8(Some(s.clone())),
        _ => return None,
    };
    Some(v)
}

/// The Iceberg field id of a column (carried as Parquet field id metadata).
pub(super) fn field_id(schema: &SchemaRef, idx: usize) -> Option<i32> {
    schema.field(idx).metadata().get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)?.parse().ok()
}

fn exact(n: u64) -> Precision<usize> {
    usize::try_from(n).map_or(Precision::Absent, Precision::Exact)
}

/// Statistics of one data file, for DataFusion's planner.
pub(super) fn file_statistics(schema: &SchemaRef, f: &LiveEntry) -> Statistics {
    let df = &f.data_file;
    let columns = (0..schema.fields().len())
        .map(|i| {
            let mut cs = ColumnStatistics::new_unknown();
            if let Some(id) = field_id(schema, i) {
                let dt = schema.field(i).data_type();
                if let Some(v) = df.lower_bounds().get(&id).and_then(|d| datum_to_scalar(d, dt)) {
                    cs.min_value = Precision::Inexact(v);
                }
                if let Some(v) = df.upper_bounds().get(&id).and_then(|d| datum_to_scalar(d, dt)) {
                    cs.max_value = Precision::Inexact(v);
                }
                if let Some(n) = df.null_value_counts().get(&id) {
                    cs.null_count = exact(*n);
                }
            }
            cs
        })
        .collect();
    Statistics {
        num_rows: exact(df.record_count()),
        total_byte_size: exact(df.file_size_in_bytes()).to_inexact(),
        column_statistics: columns,
    }
}

/// Per-file min/max/null/row counts for `PruningPredicate`, one container per file.
#[derive(Debug)]
pub(super) struct FileStats {
    mins: HashMap<String, ArrayRef>,
    maxs: HashMap<String, ArrayRef>,
    nulls: HashMap<String, ArrayRef>,
    rows: ArrayRef,
    files: usize,
}

impl FileStats {
    pub(super) fn new(schema: &SchemaRef, files: &[LiveEntry]) -> Self {
        let (mut mins, mut maxs, mut nulls) = (HashMap::new(), HashMap::new(), HashMap::new());
        for (i, field) in schema.fields().iter().enumerate() {
            let Some(id) = field_id(schema, i) else { continue };
            let dt = field.data_type();
            let Ok(null) = ScalarValue::try_from(dt) else { continue };
            let column = |bound: &dyn Fn(&LiveEntry) -> Option<&Datum>| {
                let values =
                    files.iter().map(|f| bound(f).and_then(|d| datum_to_scalar(d, dt)).unwrap_or_else(|| null.clone()));
                ScalarValue::iter_to_array(values).ok()
            };
            if let Some(a) = column(&|f| f.data_file.lower_bounds().get(&id)) {
                mins.insert(field.name().clone(), a);
            }
            if let Some(a) = column(&|f| f.data_file.upper_bounds().get(&id)) {
                maxs.insert(field.name().clone(), a);
            }
            let counts: UInt64Array = files.iter().map(|f| f.data_file.null_value_counts().get(&id).copied()).collect();
            nulls.insert(field.name().clone(), Arc::new(counts) as ArrayRef);
        }
        let rows: UInt64Array = files.iter().map(|f| Some(f.data_file.record_count())).collect();
        Self { mins, maxs, nulls, rows: Arc::new(rows), files: files.len() }
    }
}

impl PruningStatistics for FileStats {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.mins.get(&column.name).cloned()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.maxs.get(&column.name).cloned()
    }

    fn num_containers(&self) -> usize {
        self.files
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.nulls.get(&column.name).cloned()
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        Some(self.rows.clone())
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_convert_to_the_column_type() {
        let utc = Some(Arc::from(timelake_format::rows::UTC));
        let cases = [
            (Datum::bool(true), DataType::Boolean, ScalarValue::Boolean(Some(true))),
            (Datum::int(7), DataType::Int32, ScalarValue::Int32(Some(7))),
            (Datum::date(20_000), DataType::Date32, ScalarValue::Date32(Some(20_000))),
            (Datum::long(-3), DataType::Int64, ScalarValue::Int64(Some(-3))),
            (
                Datum::timestamptz_micros(1_700_000_000_000_000),
                DataType::Timestamp(TimeUnit::Microsecond, utc.clone()),
                ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), utc.clone()),
            ),
            (
                Datum::timestamp_micros(5),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ScalarValue::TimestampMicrosecond(Some(5), None),
            ),
            (
                Datum::timestamp_nanos(5),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                ScalarValue::TimestampNanosecond(Some(5), None),
            ),
            (Datum::float(0.5_f32), DataType::Float32, ScalarValue::Float32(Some(0.5))),
            (Datum::double(1.5), DataType::Float64, ScalarValue::Float64(Some(1.5))),
            (Datum::string("truck_1"), DataType::Utf8, ScalarValue::Utf8(Some("truck_1".into()))),
        ];
        for (datum, dt, expected) in cases {
            assert_eq!(datum_to_scalar(&datum, &dt), Some(expected), "{datum:?} as {dt}");
        }
    }

    #[test]
    fn mismatched_or_unsupported_types_are_not_used() {
        assert_eq!(datum_to_scalar(&Datum::int(7), &DataType::Int64), None);
        assert_eq!(datum_to_scalar(&Datum::long(7), &DataType::Timestamp(TimeUnit::Millisecond, None)), None);
        assert_eq!(datum_to_scalar(&Datum::string("x"), &DataType::LargeUtf8), None);
        assert_eq!(datum_to_scalar(&Datum::double(1.0), &DataType::Float32), None);
    }
}
