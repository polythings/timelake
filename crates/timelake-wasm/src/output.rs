//! Query results as JSON.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use arrow_cast::cast;
use arrow_json::writer::JsonArray;
use arrow_ord::sort::{SortOptions, sort_to_indices};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat;
use arrow_select::interleave::interleave_record_batch;

use crate::error::{Error, Result};
use crate::spec::QuerySpec;

/// RFC 3339 in UTC, like the engine's JSON output (`2026-01-03T23:00:00Z`).
pub(crate) fn iso(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        .unwrap_or_default()
}

/// Batches as one JSON array of objects, with nulls written out.
pub(crate) fn to_json(batches: &[RecordBatch]) -> Result<String> {
    let mut buf = vec![];
    let mut writer = arrow_json::WriterBuilder::new().with_explicit_nulls(true).build::<_, JsonArray>(&mut buf);
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    drop(writer);
    String::from_utf8(buf).map_err(|e| Error::invalid(e.to_string()))
}

/// The wider of two types of one column in different files, as Iceberg's type promotions widen
/// them (`int` to `long`, `float` to `double`, a decimal's precision).
fn wider(one: &DataType, other: &DataType) -> Option<DataType> {
    use DataType::{Decimal128, Float32, Float64, Int32, Int64};
    Some(match (one, other) {
        _ if one == other => one.clone(),
        (Int32 | Int64, Int32 | Int64) => Int64,
        (Float32 | Float64, Float32 | Float64) => Float64,
        (Decimal128(p1, scale), Decimal128(p2, scale2)) if scale == scale2 => Decimal128(*p1.max(p2), *scale),
        _ => return None,
    })
}

/// `batches` in one schema, columns matched by name: files written with different schemas, or
/// projected to different columns, hold different columns or narrower types. A column a batch
/// lacks is null there.
pub(crate) fn unify(batches: Vec<RecordBatch>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let Some(first) = batches.first() else { return Ok((Arc::new(Schema::empty()), vec![])) };
    let schema = first.schema();
    if batches.iter().all(|b| b.schema() == schema) {
        return Ok((schema, batches));
    }
    let mut fields: Vec<Field> = vec![];
    for f in batches.iter().flat_map(|b| b.schema().fields().iter().cloned().collect::<Vec<_>>()) {
        match fields.iter_mut().find(|g| g.name() == f.name()) {
            Some(g) => {
                let ty = wider(g.data_type(), f.data_type()).ok_or_else(|| {
                    Error::invalid(format!(
                        "column {} is {} in one file and {} in another",
                        f.name(),
                        g.data_type(),
                        f.data_type()
                    ))
                })?;
                *g = g.clone().with_data_type(ty);
            }
            // Nullable: a batch without the column fills it with nulls.
            None => fields.push(f.as_ref().clone().with_nullable(true)),
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let aligned = batches
        .iter()
        .map(|b| {
            let columns = schema
                .fields()
                .iter()
                .map(|f| match b.column_by_name(f.name()) {
                    Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
                    Some(c) => Ok(cast(c, f.data_type())?),
                    None => Ok(new_null_array(f.data_type(), b.num_rows())),
                })
                .collect::<Result<Vec<ArrayRef>>>()?;
            let options = RecordBatchOptions::new().with_row_count(Some(b.num_rows()));
            Ok(RecordBatch::try_new_with_options(schema.clone(), columns, &options)?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((schema, aligned))
}

/// The rows a query returns, in its order and within its limit: by time when the batches have the time column,
/// else as they came. Takes the batches, so each is freed once its rows are copied out.
pub(crate) fn top_rows(batches: Vec<RecordBatch>, spec: &QuerySpec) -> Result<Vec<RecordBatch>> {
    let (schema, batches) = unify(batches)?;
    let Some(time) = spec.time_column.as_ref().and_then(|tc| schema.index_of(tc).ok()) else {
        return Ok(first_rows(batches, spec.limit));
    };
    // Sort the time column alone, then copy the rows out in that order: no copy of every row first.
    let times: Vec<&dyn Array> = batches.iter().map(|b| b.column(time).as_ref()).collect();
    let options = SortOptions { descending: spec.descending, nulls_first: false };
    let order = sort_to_indices(&concat(&times)?, Some(options), spec.limit)?;
    let starts: Vec<usize> = batches
        .iter()
        .scan(0, |start, b| {
            let this = *start;
            *start += b.num_rows();
            Some(this)
        })
        .collect();
    let picks: Vec<(usize, usize)> = order
        .values()
        .iter()
        .map(|&row| {
            let row = row as usize;
            let batch = starts.partition_point(|&start| start <= row) - 1;
            (batch, row - starts[batch])
        })
        .collect();
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    Ok(vec![interleave_record_batch(&refs, &picks)?])
}

/// The first `limit` rows of `batches`, in order.
pub(crate) fn first_rows(batches: Vec<RecordBatch>, limit: Option<usize>) -> Vec<RecordBatch> {
    let mut left = limit.unwrap_or(usize::MAX);
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        if left == 0 {
            break;
        }
        let n = b.num_rows().min(left);
        left -= n;
        out.push(if n < b.num_rows() { b.slice(0, n) } else { b });
    }
    out
}

/// Rows ordered by time, limited and projected as the query asks, as a JSON array of objects.
pub(crate) fn rows_json(batches: Vec<RecordBatch>, spec: &QuerySpec) -> Result<String> {
    let rows = top_rows(batches, spec)?;
    let projected = rows
        .iter()
        .map(|b| {
            if spec.columns.is_empty() {
                return Ok(b.clone());
            }
            let schema = b.schema();
            let indices: Vec<usize> = spec.columns.iter().filter_map(|c| schema.index_of(c).ok()).collect();
            Ok(b.project(&indices)?)
        })
        .collect::<Result<Vec<_>>>()?;
    drop(rows);
    to_json(&projected)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Files with different columns, or a column at a narrower type, give one table of rows.
    #[test]
    fn rows_of_files_with_different_columns() {
        use arrow_array::{Int32Array, Int64Array, StringArray};

        let old = RecordBatch::try_from_iter([("v", Arc::new(Int32Array::from(vec![1])) as ArrayRef)]).unwrap();
        let new = RecordBatch::try_from_iter([
            ("tag", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
            ("v", Arc::new(Int64Array::from(vec![1 << 40]))),
        ])
        .unwrap();
        let json = rows_json(vec![old, new], &QuerySpec::default()).unwrap();
        assert_eq!(json, r#"[{"v":1,"tag":null},{"v":1099511627776,"tag":"a"}]"#);
    }

    #[test]
    fn formats_times_like_the_engine() {
        assert_eq!(iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso(1_767_225_600_000_000), "2026-01-01T00:00:00Z");
        assert_eq!(iso(1_500), "1970-01-01T00:00:00.001500Z");
        assert_eq!(iso(-1_000_000), "1969-12-31T23:59:59Z");
    }
}
