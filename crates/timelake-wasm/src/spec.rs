//! The query a host asks for, deserialized from a plain JavaScript object.

use std::collections::HashMap;

use arrow_schema::Schema;
use serde::Deserialize;
use serde_json::Value as Json;

use crate::error::{Error, Result};
use crate::meta::Field;

/// What to read. Times are microseconds since the epoch; the range is `[from, to)`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuerySpec {
    pub time_column: Option<String>,
    /// Column → allowed values (series key filters). An empty list allows every value.
    #[serde(default)]
    pub series: HashMap<String, Vec<Json>>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    /// Columns to return (empty = all).
    #[serde(default)]
    pub columns: Vec<String>,
    pub limit: Option<usize>,
    /// Order rows by time descending (default ascending).
    #[serde(default)]
    pub descending: bool,
    /// Aggregate into time buckets instead of returning rows.
    pub bucket: Option<BucketSpec>,
    /// Return only the newest row per distinct value of these columns (the series key).
    pub latest_by: Option<Vec<String>>,
}

/// Time-bucket aggregation, like SQL `date_bin(every, time, origin)` grouped with `by`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BucketSpec {
    /// Bucket width in microseconds.
    pub every: i64,
    /// A bucket boundary, in microseconds since the epoch (default 0, the epoch itself).
    pub origin: Option<i64>,
    /// Extra group-by columns (typically the series key).
    #[serde(default)]
    pub by: Vec<String>,
    pub aggs: Vec<AggSpec>,
}

/// One aggregate; its output column is `<column>_<op>`, or `count` for `count(*)`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggSpec {
    pub column: AggInput,
    pub op: AggOp,
}

/// What an aggregate reads: one column, or every row (`"*"`, only for `count`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum AggInput {
    Rows,
    Column(String),
}

impl From<String> for AggInput {
    fn from(name: String) -> Self {
        if name == "*" { Self::Rows } else { Self::Column(name) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggOp {
    /// Non-null values (or rows, for `*`).
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// The value at the earliest time.
    First,
    /// The value at the latest time.
    Last,
}

impl AggOp {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::First => "first",
            Self::Last => "last",
        }
    }
}

impl AggSpec {
    /// The output column name.
    pub fn name(&self) -> String {
        match &self.column {
            AggInput::Rows => "count".into(),
            AggInput::Column(c) => format!("{c}_{}", self.op.as_str()),
        }
    }
}

impl QuerySpec {
    /// Errors when the query names a column the table (`schema`) doesn't have: a filter on it
    /// would otherwise keep every row, and a projection drop it unnoticed.
    pub fn check_columns(&self, schema: &[Field]) -> Result<()> {
        let aggregated = self.bucket.iter().flat_map(|b| {
            let read = b.aggs.iter().filter_map(|a| match &a.column {
                AggInput::Column(c) => Some(c),
                AggInput::Rows => None,
            });
            b.by.iter().chain(read)
        });
        let mut named = (self.time_column.iter())
            .chain(self.series.keys())
            .chain(&self.columns)
            .chain(self.latest_by.iter().flatten())
            .chain(aggregated);
        match named.find(|c| !schema.iter().any(|f| &f.name == *c)) {
            Some(c) => Err(Error::invalid(format!("the table has no column {c:?}"))),
            None => Ok(()),
        }
    }

    /// Columns of `schema` that must be decoded: output, filters, buckets and aggregates.
    pub(crate) fn needed_columns(&self, schema: &Schema) -> Vec<String> {
        let mut cols: Vec<String> = vec![];
        let mut add = |c: &str| {
            if schema.field_with_name(c).is_ok() && !cols.iter().any(|x| x == c) {
                cols.push(c.to_string());
            }
        };
        match &self.bucket {
            Some(b) => {
                b.by.iter().for_each(|c| add(c));
                for a in &b.aggs {
                    if let AggInput::Column(c) = &a.column {
                        add(c);
                    }
                }
            }
            None if self.columns.is_empty() => schema.fields().iter().for_each(|f| add(f.name())),
            None => self.columns.iter().for_each(|c| add(c)),
        }
        self.latest_by.iter().flatten().for_each(|c| add(c));
        if let Some(tc) = &self.time_column {
            add(tc);
        }
        self.series.keys().for_each(|c| add(c));
        cols
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_aggregates() {
        let spec: AggSpec = serde_json::from_value(json!({ "column": "*", "op": "count" })).unwrap();
        assert_eq!(spec, AggSpec { column: AggInput::Rows, op: AggOp::Count });
        assert_eq!(spec.name(), "count");
        let spec: AggSpec = serde_json::from_value(json!({ "column": "speed", "op": "avg" })).unwrap();
        assert_eq!(spec.name(), "speed_avg");
    }

    #[test]
    fn columns_the_table_lacks_are_errors() {
        let schema =
            ["time", "tag", "v"].map(|name| Field { id: 1, name: name.into(), ty: "long".into(), required: false });
        let check = |v: serde_json::Value| serde_json::from_value::<QuerySpec>(v).unwrap().check_columns(&schema);
        assert!(check(json!({ "timeColumn": "time", "series": { "tag": [1] }, "columns": ["v"] })).is_ok());
        let bucket = json!({ "every": 1, "by": ["tag"], "aggs": [{ "column": "*", "op": "count" }] });
        assert!(check(json!({ "timeColumn": "time", "bucket": bucket })).is_ok());
        for bad in [
            json!({ "timeColumn": "tme" }),
            json!({ "series": { "tags": [1] } }),
            json!({ "columns": ["w"] }),
            json!({ "timeColumn": "time", "latestBy": ["device"] }),
            json!({ "timeColumn": "time", "bucket": { "every": 1, "aggs": [{ "column": "w", "op": "sum" }] } }),
        ] {
            let err = check(bad.clone()).unwrap_err().to_string();
            assert!(err.contains("has no column"), "{bad}: {err}");
        }
    }

    #[test]
    fn rejects_unknown_ops_and_fields() {
        assert!(serde_json::from_value::<AggSpec>(json!({ "column": "speed", "op": "median" })).is_err());
        assert!(serde_json::from_value::<QuerySpec>(json!({ "form": 1 })).is_err());
    }
}
