//! Skipping files and row groups whose statistics rule out every row of a query.

use std::cmp::Ordering;

use parquet::basic::{ConvertedType, IntType, LogicalType, TimeUnit};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescriptor;
use serde_json::Value as Json;
use timelake_format::bounds::Bound;

use crate::meta::DataFile;
use crate::spec::QuerySpec;

/// Compares a bound with a filter value; `None` when they are not comparable.
fn bound_cmp(b: &Bound, v: &Json) -> Option<Ordering> {
    match (b, v) {
        (Bound::Int(x), Json::Number(n)) => match n.as_i64() {
            Some(n) => Some(x.cmp(&n)),
            #[expect(clippy::cast_precision_loss, reason = "an approximate order only widens what may match")]
            None => (*x as f64).partial_cmp(&n.as_f64()?),
        },
        (Bound::Float(x), Json::Number(n)) => x.partial_cmp(&n.as_f64()?),
        (Bound::Str(x), Json::String(s)) => Some(x.as_str().cmp(s)),
        _ => None,
    }
}

/// Whether `[lower, upper]` may contain `v`. Missing or incomparable bounds can't rule it out.
fn may_contain(lower: Option<&Bound>, upper: Option<&Bound>, v: &Json) -> bool {
    let above_lower = lower.and_then(|l| bound_cmp(l, v)).is_none_or(|o| o != Ordering::Greater);
    let below_upper = upper.and_then(|u| bound_cmp(u, v)).is_none_or(|o| o != Ordering::Less);
    above_lower && below_upper
}

/// Whether times in `[lower, upper]` may fall into the query's `[from, to)`.
fn time_overlaps(lower: Option<i64>, upper: Option<i64>, spec: &QuerySpec) -> bool {
    let ends_before = matches!((spec.from, upper), (Some(from), Some(hi)) if hi < from);
    let starts_after = matches!((spec.to, lower), (Some(to), Some(lo)) if lo >= to);
    !ends_before && !starts_after
}

/// Whether column statistics (looked up by `bounds`) allow rows matching the query.
fn may_match(spec: &QuerySpec, bounds: impl Fn(&str) -> (Option<Bound>, Option<Bound>)) -> bool {
    let time_ok = spec.time_column.as_deref().is_none_or(|tc| {
        let as_int = |b| match b {
            Some(Bound::Int(x)) => Some(x),
            _ => None,
        };
        let (lo, hi) = bounds(tc);
        time_overlaps(as_int(lo), as_int(hi), spec)
    });
    time_ok
        && spec.series.iter().all(|(col, values)| {
            let (lo, hi) = bounds(col);
            values.is_empty() || values.iter().any(|v| may_contain(lo.as_ref(), hi.as_ref(), v))
        })
}

/// Files that may hold matching rows, judged by their Iceberg column bounds, oldest data first
/// (by sequence number): pushed in this order, a later row of the same key and time is a later
/// write, as the newest-row and last-value queries assume.
pub fn plan_files<'a>(files: &'a [DataFile], spec: &QuerySpec) -> Vec<&'a DataFile> {
    let mut planned: Vec<&DataFile> =
        files.iter().filter(|f| may_match(spec, |c| (f.lower.get(c).cloned(), f.upper.get(c).cloned()))).collect();
    planned.sort_by_key(|f| f.sequence_number);
    planned
}

/// Whether a row group may hold matching rows, judged by its Parquet column statistics.
/// `column_index` maps a column name to its leaf index.
pub(crate) fn row_group_may_match(
    rg: &RowGroupMetaData,
    spec: &QuerySpec,
    column_index: impl Fn(&str) -> Option<usize>,
) -> bool {
    may_match(spec, |c| {
        let Some(column) = column_index(c).map(|i| rg.column(i)) else { return (None, None) };
        column.statistics().map_or((None, None), |s| stat_bounds(s, stats_kind(column.column_descr())))
    })
}

/// How a column's Parquet statistics compare with the values of a query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatsKind {
    /// As stored: plain numbers, dates and text.
    Plain,
    /// Timestamps in this unit, compared in microseconds.
    Time(TimeUnit),
    /// Not at all: decimals (stored unscaled), unsigned integers and other logical types.
    Opaque,
}

fn stats_kind(column: &ColumnDescriptor) -> StatsKind {
    match column.logical_type_ref() {
        Some(LogicalType::Timestamp(timestamp)) => StatsKind::Time(timestamp.unit),
        Some(LogicalType::Integer(IntType { is_signed: true, .. }) | LogicalType::Date | LogicalType::String) => {
            StatsKind::Plain
        }
        Some(_) => StatsKind::Opaque,
        None => match column.converted_type() {
            ConvertedType::NONE
            | ConvertedType::UTF8
            | ConvertedType::DATE
            | ConvertedType::INT_8
            | ConvertedType::INT_16
            | ConvertedType::INT_32
            | ConvertedType::INT_64 => StatsKind::Plain,
            ConvertedType::TIMESTAMP_MILLIS => StatsKind::Time(TimeUnit::MILLIS),
            ConvertedType::TIMESTAMP_MICROS => StatsKind::Time(TimeUnit::MICROS),
            _ => StatsKind::Opaque,
        },
    }
}

/// A timestamp `x` stored in `unit`, in microseconds: rounded down for a lower bound and up for
/// an upper one, so the bounds still contain every row.
pub(crate) fn scale_to_micros(x: i64, unit: TimeUnit, upper: bool) -> i64 {
    match unit {
        TimeUnit::MILLIS => x.saturating_mul(1000),
        TimeUnit::MICROS => x,
        TimeUnit::NANOS => x.div_euclid(1000) + i64::from(upper && x.rem_euclid(1000) != 0),
    }
}

fn stat_bounds(s: &Statistics, kind: StatsKind) -> (Option<Bound>, Option<Bound>) {
    let str_bound = |b: Option<&parquet::data_type::ByteArray>| b?.as_utf8().ok().map(|s| Bound::Str(s.to_string()));
    let time = |x: i64, upper| match kind {
        StatsKind::Time(unit) => Bound::Int(scale_to_micros(x, unit, upper)),
        _ => Bound::Int(x),
    };
    match s {
        _ if kind == StatsKind::Opaque => (None, None),
        Statistics::Int32(v) => {
            (v.min_opt().map(|x| Bound::Int(i64::from(*x))), v.max_opt().map(|x| Bound::Int(i64::from(*x))))
        }
        Statistics::Int64(v) => (v.min_opt().map(|x| time(*x, false)), v.max_opt().map(|x| time(*x, true))),
        Statistics::Double(v) => (v.min_opt().map(|x| Bound::Float(*x)), v.max_opt().map(|x| Bound::Float(*x))),
        Statistics::ByteArray(v) => (str_bound(v.min_opt()), str_bound(v.max_opt())),
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    fn file(path: &str, time: (i64, i64), tag: (i64, i64)) -> DataFile {
        DataFile {
            path: path.into(),
            size: 0,
            records: 0,
            partition: None,
            sequence_number: 0,
            lower: HashMap::from([("time".into(), Bound::Int(time.0)), ("tag".into(), Bound::Int(tag.0))]),
            upper: HashMap::from([("time".into(), Bound::Int(time.1)), ("tag".into(), Bound::Int(tag.1))]),
        }
    }

    fn spec(v: serde_json::Value) -> QuerySpec {
        serde_json::from_value(v).unwrap()
    }

    fn planned(files: &[DataFile], s: &QuerySpec) -> Vec<String> {
        plan_files(files, s).into_iter().map(|f| f.path.clone()).collect()
    }

    #[test]
    fn prunes_by_time_range() {
        let files = [file("a", (0, 99), (1, 5)), file("b", (100, 199), (1, 5))];
        // `to` is exclusive, `from` inclusive.
        assert_eq!(planned(&files, &spec(json!({ "timeColumn": "time", "to": 100 }))), ["a"]);
        assert_eq!(planned(&files, &spec(json!({ "timeColumn": "time", "from": 99 }))), ["a", "b"]);
        assert_eq!(planned(&files, &spec(json!({ "timeColumn": "time", "from": 100, "to": 101 }))), ["b"]);
        assert_eq!(planned(&files, &spec(json!({ "from": 500 }))), ["a", "b"], "no time column, no pruning");
    }

    #[test]
    fn prunes_by_series_values() {
        let files = [file("a", (0, 9), (1, 5)), file("b", (0, 9), (6, 9))];
        assert_eq!(planned(&files, &spec(json!({ "series": { "tag": [7] } }))), ["b"]);
        assert_eq!(planned(&files, &spec(json!({ "series": { "tag": [2, 7] } }))), ["a", "b"]);
        assert_eq!(planned(&files, &spec(json!({ "series": { "tag": [] } }))), ["a", "b"]);
        assert_eq!(planned(&files, &spec(json!({ "series": { "tag": [10] } }))), Vec::<String>::new());
        // Fractional values compare numerically; strings don't compare with ints, so keep the file.
        assert_eq!(planned(&files, &spec(json!({ "series": { "tag": [5.5] } }))), Vec::<String>::new());
        assert_eq!(planned(&files, &spec(json!({ "series": { "tag": ["x"] } }))), ["a", "b"]);
    }

    /// Files come oldest data first, so a later row of a key is a later write when they are
    /// pushed in this order (heads and manifest lists list the newest first).
    #[test]
    fn plans_oldest_data_first() {
        let mut files = [file("new", (0, 9), (1, 5)), file("old", (0, 9), (1, 5)), file("mid", (0, 9), (1, 5))];
        for (f, seq) in files.iter_mut().zip([7, 2, 5]) {
            f.sequence_number = seq;
        }
        assert_eq!(planned(&files, &spec(json!({}))), ["old", "mid", "new"]);
    }

    /// The metadata of a one-row Parquet file holding `column` as `c`.
    fn row_group(column: arrow_array::ArrayRef) -> parquet::file::metadata::ParquetMetaData {
        let batch = arrow_array::RecordBatch::try_from_iter([("c", column)]).unwrap();
        let mut bytes = vec![];
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        parquet::file::metadata::ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::from(bytes)).unwrap()
    }

    /// Decimals are stored unscaled (1.5 as 15) and unsigned integers as signed ones: their
    /// statistics can't rule out a value, so the row group is read.
    #[test]
    fn statistics_of_decimals_and_unsigned_integers_never_prune() {
        use arrow_array::{Decimal128Array, Int32Array, UInt32Array};

        let may_match = |column: arrow_array::ArrayRef, v: serde_json::Value| {
            let meta = row_group(column);
            row_group_may_match(meta.row_group(0), &spec(json!({ "series": { "c": [v] } })), |_| Some(0))
        };
        let decimal = || std::sync::Arc::new(Decimal128Array::from(vec![15]).with_precision_and_scale(5, 1).unwrap());
        assert!(may_match(decimal(), json!(1.5)));
        let unsigned = std::sync::Arc::new(UInt32Array::from(vec![u32::MAX]));
        assert!(may_match(unsigned, json!(u32::MAX)));
        // Plain integers still prune.
        assert!(!may_match(std::sync::Arc::new(Int32Array::from(vec![15])), json!(1.5)));
    }
}
