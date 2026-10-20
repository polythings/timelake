//! Skipping files and row groups whose statistics rule out every row of a query.

use std::cmp::Ordering;

use parquet::basic::{ConvertedType, LogicalType, TimeUnit};
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

/// Files that may hold matching rows, judged by their Iceberg column bounds.
pub fn plan_files<'a>(files: &'a [DataFile], spec: &QuerySpec) -> Vec<&'a DataFile> {
    files.iter().filter(|f| may_match(spec, |c| (f.lower.get(c).cloned(), f.upper.get(c).cloned()))).collect()
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
        column.statistics().map_or((None, None), |s| stat_bounds(s, timestamp_unit(column.column_descr())))
    })
}

/// The unit of a timestamp column; `None` for other columns.
fn timestamp_unit(column: &ColumnDescriptor) -> Option<TimeUnit> {
    match column.logical_type_ref() {
        Some(LogicalType::Timestamp { unit, .. }) => Some(*unit),
        Some(_) => None,
        None => match column.converted_type() {
            ConvertedType::TIMESTAMP_MILLIS => Some(TimeUnit::MILLIS),
            ConvertedType::TIMESTAMP_MICROS => Some(TimeUnit::MICROS),
            _ => None,
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

fn stat_bounds(s: &Statistics, unit: Option<TimeUnit>) -> (Option<Bound>, Option<Bound>) {
    let str_bound = |b: Option<&parquet::data_type::ByteArray>| b?.as_utf8().ok().map(|s| Bound::Str(s.to_string()));
    let time = |x: i64, upper| Bound::Int(unit.map_or(x, |u| scale_to_micros(x, u, upper)));
    match s {
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
}
