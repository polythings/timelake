//! Preparing rows for a table, shared by the engine and the WASM writer: aligning batches to the
//! table schema, stamping the insertion time, sorting by `(partition, series…, time, insertion
//! time)`, splitting by partition, and keeping the latest write of each `(series…, time)` — every
//! file is then sorted and free of duplicates on its own, which dedup on read and streaming
//! compaction rely on.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, TimestampMicrosecondArray, UInt32Array, new_null_array};
use arrow_cast::{CastOptions, cast, cast_with_options};
use arrow_ord::partition::partition;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use arrow_select::zip::zip;

use crate::Contract;
use crate::error::{WriteError, WriteResult};
use crate::merge::{Merge, Step};

/// Ascending with nulls first: the order Timelake writes and declares for `(series…, time)`.
pub const ASCENDING: SortOptions = SortOptions { descending: false, nulls_first: true };

/// The time zone timestamps with any zone are stored in.
pub const UTC: &str = "+00:00";

/// Rows of one time partition (`None` for unpartitioned tables), sorted by the table's write order,
/// in one or more batches.
pub type PartitionRows = (Option<i32>, Vec<RecordBatch>);

/// Casts and reorders a batch to the table schema by column name. Missing nullable columns are
/// filled with nulls; extra columns are an error. Only conversions that keep every value are made
/// (see [`conversion`]); a value that doesn't fit is an error, never a null.
pub fn align_batch(batch: &RecordBatch, schema: &SchemaRef) -> WriteResult<RecordBatch> {
    let input = batch.schema();
    if let Some(extra) = input.fields().iter().find(|f| schema.field_with_name(f.name()).is_err()) {
        return Err(WriteError::invalid(format!("column {:?} is not in the table schema", extra.name())));
    }
    let columns = schema
        .fields()
        .iter()
        .map(|f| match input.index_of(f.name()) {
            Ok(i) if batch.column(i).data_type() == f.data_type() => Ok(batch.column(i).clone()),
            Ok(i) => convert(batch.column(i), f),
            Err(_) if f.is_nullable() => Ok(new_null_array(f.data_type(), batch.num_rows())),
            Err(_) => Err(WriteError::invalid(format!("required column {:?} is missing", f.name()))),
        })
        .collect::<WriteResult<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| WriteError::invalid(format!("batch does not fit the table schema: {e}")))
}

/// How a column of one type becomes another without losing values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Conversion {
    /// A cast that errs on a value it can't convert (an integer out of range, text that doesn't
    /// parse) rather than making it null.
    Checked,
    /// Each value must come back unchanged when cast back (between integers, floats and decimals,
    /// where a cast may round, and to a coarser time unit, where it truncates).
    RoundTrip,
}

/// How a column of type `from` is written to a table column of type `to`, if it can be.
fn conversion(from: &DataType, to: &DataType) -> Option<Conversion> {
    use DataType::{
        Binary, BinaryView, Boolean, Date32, Date64, Decimal128, Decimal256, Dictionary, Float16, Float32, Float64,
        LargeBinary, LargeList, LargeUtf8, List, Null, Struct, Timestamp, Utf8, Utf8View,
    };
    let text = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
    let int = DataType::is_integer;
    let float_bits = |t: &DataType| match t {
        Float16 => Some(16),
        Float32 => Some(32),
        Float64 => Some(64),
        _ => None,
    };
    let number = |t: &DataType| int(t) || float_bits(t).is_some() || matches!(t, Decimal128(..) | Decimal256(..));
    Some(match (from, to) {
        (Dictionary(_, values), _) => conversion(values, to)?,
        // Nested field names and metadata (Parquet field ids) don't matter.
        _ if from.equals_datatype(to) => Conversion::Checked,
        (List(a) | LargeList(a), List(b) | LargeList(b)) => conversion(a.data_type(), b.data_type())?,
        (Struct(a), Struct(b)) if a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.name() == y.name()) => {
            let each = a.iter().zip(b).map(|(x, y)| conversion(x.data_type(), y.data_type()));
            each.collect::<Option<Vec<_>>>()?.into_iter().max().unwrap_or(Conversion::Checked)
        }
        (Null, _) => Conversion::Checked,
        // A finer unit holds every value (a cast errs on overflow); a coarser one truncates.
        (Timestamp(..) | Date32 | Date64, Timestamp(..)) | (Date32 | Date64, Date32 | Date64) => {
            if tick_nanos(from)? >= tick_nanos(to)? { Conversion::Checked } else { Conversion::RoundTrip }
        }
        _ if int(from) && int(to) => Conversion::Checked,
        _ if float_bits(from).zip(float_bits(to)).is_some_and(|(f, t)| f <= t) => Conversion::Checked,
        _ if number(from) && number(to) => Conversion::RoundTrip,
        _ if text(from) && (text(to) || number(to) || matches!(to, Boolean | Date32 | Timestamp(..))) => {
            Conversion::Checked
        }
        (Binary | LargeBinary | BinaryView, Binary | LargeBinary | BinaryView) => Conversion::Checked,
        _ => return None,
    })
}

/// Nanoseconds in one unit of a date or timestamp type.
fn tick_nanos(t: &DataType) -> Option<i64> {
    Some(match t {
        DataType::Date32 => 86_400_000_000_000,
        DataType::Date64 | DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000_000,
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        _ => return None,
    })
}

/// `column` as `field`'s type, or an error naming the column if a value would be lost.
fn convert(column: &ArrayRef, field: &Field) -> WriteResult<ArrayRef> {
    let (from, to) = (column.data_type(), field.data_type());
    let name = field.name();
    let Some(how) = conversion(from, to) else {
        return Err(WriteError::invalid(format!("column {name:?}: {from} can't be written to the table's {to}")));
    };
    let strict = CastOptions { safe: false, ..CastOptions::default() };
    let cast = |a: &ArrayRef, t: &DataType| {
        cast_with_options(a, t, &strict)
            .map_err(|e| WriteError::invalid(format!("column {name:?}: {from} can't be cast to the table's {to}: {e}")))
    };
    let out = cast(column, to)?;
    if how == Conversion::RoundTrip && cast(&out, from)?.as_ref() != column.as_ref() {
        return Err(WriteError::invalid(format!(
            "column {name:?}: some {from} values are not exactly {to}, the table's type; cast them first"
        )));
    }
    Ok(out)
}

/// `batch` with its insertion-time `column` set to `now` (µs since the epoch) where it is missing or
/// null; values a caller supplied (an import keeping arrival times) stay.
pub fn with_ingest_time(batch: &RecordBatch, column: &str, now: i64) -> WriteResult<RecordBatch> {
    let n = batch.num_rows();
    let fill = || Arc::new(TimestampMicrosecondArray::from_value(now, n).with_timezone(UTC)) as ArrayRef;
    let ty = DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()));
    let schema = batch.schema();
    let Ok(i) = schema.index_of(column) else {
        let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(column, ty, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(fill());
        return Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?);
    };
    if batch.column(i).null_count() == 0 {
        return Ok(batch.clone());
    }
    // Strict, so a value that doesn't convert is an error rather than a null stamped with `now`.
    let given = cast_with_options(batch.column(i), &ty, &CastOptions { safe: false, ..CastOptions::default() })?;
    let filled = zip(&arrow_arith::boolean::is_not_null(&given)?, &given, &fill())?;
    let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
    fields[i] = Arc::new(Field::new(column, filled.data_type().clone(), false));
    let mut columns = batch.columns().to_vec();
    columns[i] = filled;
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

/// Input is sorted in groups of at most about this many bytes: a group is concatenated and sorted
/// at once, and an append of several groups merges its sorted groups. An Arrow string or binary
/// array holds at most 2 GiB, so no single sort may hold more.
const SORT_GROUP_BYTES: usize = 1 << 30;

/// The partition of each row, as a key column while sorted groups are merged.
const PARTITION: &str = "_timelake_partition";

/// Aligns `batches` to `schema`, stamps the insertion time `now`, sorts by `(partition, series…,
/// time, insertion time)` with input order as the tiebreak, splits by partition and, when dedup
/// is on, keeps the last of rows with the same `(series…, time)`.
pub fn sort_and_split(
    schema: &SchemaRef,
    contract: &Contract,
    batches: &[RecordBatch],
    now: i64,
) -> WriteResult<Vec<PartitionRows>> {
    sort_and_split_in_groups(schema, contract, batches, now, SORT_GROUP_BYTES)
}

fn sort_and_split_in_groups(
    schema: &SchemaRef,
    contract: &Contract,
    batches: &[RecordBatch],
    now: i64,
    group_bytes: usize,
) -> WriteResult<Vec<PartitionRows>> {
    let ingest = contract.ingest_time_column.as_deref();
    let aligned = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| align_batch(&ingest.map_or_else(|| Ok(b.clone()), |c| with_ingest_time(b, c, now))?, schema))
        .collect::<WriteResult<Vec<_>>>()?;
    let groups = groups_of(aligned, group_bytes);
    if let [group] = groups.as_slice() {
        return sort_group(schema, contract, group);
    }
    merge_groups(schema, contract, &groups)
}

/// `batches` in consecutive groups of at most `bytes` each (a larger batch is a group of its own).
fn groups_of(batches: Vec<RecordBatch>, bytes: usize) -> Vec<Vec<RecordBatch>> {
    let mut groups: Vec<Vec<RecordBatch>> = vec![];
    let mut size = 0;
    for batch in batches {
        let n = batch.get_array_memory_size();
        match groups.last_mut() {
            Some(group) if size + n <= bytes => {
                size += n;
                group.push(batch);
            }
            _ => {
                size = n;
                groups.push(vec![batch]);
            }
        }
    }
    groups
}

/// One group's rows, sorted, split by partition and deduplicated.
fn sort_group(schema: &SchemaRef, contract: &Contract, group: &[RecordBatch]) -> WriteResult<Vec<PartitionRows>> {
    let all = concat_batches(schema, group)?;
    let partitions = partition_values(&all, contract)?;
    let mut keys: Vec<ArrayRef> =
        partitions.iter().map(|p| Arc::new(Int32Array::from(p.clone())) as ArrayRef).collect();
    keys.extend(columns(&all, &contract.write_order())?);
    let (sorted, runs) = if keys.is_empty() {
        let rows = all.num_rows();
        (all, vec![(None, rows)])
    } else {
        let order = stable_sort_indices(&keys, all.num_rows())?;
        let sorted = take_record_batch(&all, &order)?;
        let runs = match partitions {
            None => vec![(None, sorted.num_rows())],
            // Rows are now grouped by partition: one run per partition.
            Some(p) => {
                let sorted_partitions: Vec<i32> = order.values().iter().map(|&i| p[i as usize]).collect();
                sorted_partitions.chunk_by(|a, b| a == b).map(|run| (Some(run[0]), run.len())).collect()
            }
        };
        (sorted, runs)
    };
    let dedup = contract.dedup && !contract.sort_columns().is_empty();
    let mut out = vec![];
    let mut offset = 0;
    for (partition, len) in runs {
        let rows = sorted.slice(offset, len);
        let rows = if dedup { last_per_group(&rows, &contract.sort_columns())? } else { rows };
        out.push((partition, vec![rows]));
        offset += len;
    }
    Ok(out)
}

/// Sorts each group with its partition as a leading key column, then merges the groups in that
/// order (with dedup: the last write of each key kept), split by partition into batches of
/// `row_group_rows`.
fn merge_groups(
    schema: &SchemaRef,
    contract: &Contract,
    groups: &[Vec<RecordBatch>],
) -> WriteResult<Vec<PartitionRows>> {
    let partitioned = contract.time_column.is_some() && contract.chunk.is_some();
    let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
    if partitioned {
        fields.push(Arc::new(Field::new(PARTITION, DataType::Int32, false)));
    }
    let keyed = Arc::new(Schema::new(fields));
    let mut keys: Vec<String> = partitioned.then(|| PARTITION.to_string()).into_iter().collect();
    keys.extend(contract.write_order());
    let mut runs = vec![];
    for group in groups {
        let all = concat_batches(schema, group)?;
        let mut columns = all.columns().to_vec();
        if let Some(p) = partition_values(&all, contract)? {
            columns.push(Arc::new(Int32Array::from(p)));
        }
        let all = RecordBatch::try_new(keyed.clone(), columns)?;
        runs.push(Some(stable_sort(&all, &keys)?));
    }
    let sort_columns = contract.sort_columns();
    let unique =
        if contract.dedup && !sort_columns.is_empty() { usize::from(partitioned) + sort_columns.len() } else { 0 };
    let names: Vec<&str> = keys.iter().map(String::as_str).collect();
    let mut merge = Merge::new(keyed, &names, unique, runs.len(), contract.row_group_rows)?;
    let mut out: Vec<PartitionRows> = vec![];
    loop {
        match merge.step()? {
            Step::Need(i) => merge.push(i, runs[i].take())?,
            Step::Output(batch) => {
                for (partition, rows) in split_partitions(&batch, partitioned)? {
                    match out.last_mut() {
                        Some((p, batches)) if *p == partition => batches.push(rows),
                        _ => out.push((partition, vec![rows])),
                    }
                }
            }
            Step::Done => break,
        }
    }
    Ok(out)
}

/// A merged batch's rows by partition, without the partition column.
fn split_partitions(batch: &RecordBatch, partitioned: bool) -> WriteResult<Vec<(Option<i32>, RecordBatch)>> {
    if !partitioned {
        return Ok(vec![(None, batch.clone())]);
    }
    let last = batch.num_columns() - 1;
    let values = batch.column(last).as_any().downcast_ref::<Int32Array>().expect("the partition column");
    let rows = batch.project(&(0..last).collect::<Vec<_>>())?;
    let mut out = vec![];
    let mut offset = 0;
    for run in values.values().chunk_by(|a, b| a == b) {
        out.push((Some(run[0]), rows.slice(offset, run.len())));
        offset += run.len();
    }
    Ok(out)
}

/// Partition value of every row (`None` for unpartitioned tables).
fn partition_values(batch: &RecordBatch, contract: &Contract) -> WriteResult<Option<Vec<i32>>> {
    let (Some(time_column), Some(chunk)) = (&contract.time_column, contract.chunk) else { return Ok(None) };
    let times =
        cast(batch.column(batch.schema().index_of(time_column)?), &DataType::Timestamp(TimeUnit::Microsecond, None))?;
    let Some(times) = times.as_any().downcast_ref::<TimestampMicrosecondArray>() else {
        return Err(WriteError::invalid(format!("time column {time_column:?} is not a timestamp")));
    };
    if times.null_count() > 0 {
        return Err(WriteError::invalid(format!("time column {time_column:?} contains nulls")));
    }
    let values = times.values().iter().map(|&t| chunk.value(t)).collect::<crate::Result<_>>()?;
    Ok(Some(values))
}

/// The columns of `batch` named `names`, in that order.
pub fn columns(batch: &RecordBatch, names: &[String]) -> WriteResult<Vec<ArrayRef>> {
    let schema = batch.schema();
    names.iter().map(|n| Ok(batch.column(schema.index_of(n)?).clone())).collect()
}

/// Row indices that sort by `keys` (ascending, nulls first), keeping input order among equal keys.
pub fn stable_sort_indices(keys: &[ArrayRef], rows: usize) -> WriteResult<UInt32Array> {
    let rows = u32::try_from(rows).map_err(|_| too_many_rows(rows))?;
    let mut columns: Vec<SortColumn> =
        keys.iter().map(|k| SortColumn { values: k.clone(), options: Some(ASCENDING) }).collect();
    columns.push(SortColumn { values: Arc::new(UInt32Array::from_iter_values(0..rows)), options: None });
    Ok(lexsort_to_indices(&columns, None)?)
}

/// `batch` sorted by its columns `names`, keeping input order among equal keys.
pub fn stable_sort(batch: &RecordBatch, names: &[String]) -> WriteResult<RecordBatch> {
    if names.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let indices = stable_sort_indices(&columns(batch, names)?, batch.num_rows())?;
    Ok(take_record_batch(batch, &indices)?)
}

/// The last row of each run of equal values in the columns `names` of a sorted batch (the last
/// row overall if `names` is empty).
pub fn last_per_group(sorted: &RecordBatch, names: &[String]) -> WriteResult<RecordBatch> {
    let rows = sorted.num_rows();
    if rows == 0 {
        return Ok(sorted.clone());
    }
    if names.is_empty() {
        return Ok(sorted.slice(rows - 1, 1));
    }
    let groups = partition(&columns(sorted, names)?)?.ranges();
    if groups.len() == rows {
        return Ok(sorted.clone());
    }
    let last: UInt32Array =
        groups.iter().map(|g| u32::try_from(g.end - 1)).collect::<Result<_, _>>().map_err(|_| too_many_rows(rows))?;
    Ok(take_record_batch(sorted, &last)?)
}

/// Newest row per series among `batches`, or `None` without a time column or rows. On equal
/// times the later insertion wins, then the later row (later input position), matching dedup's
/// "latest write wins".
pub fn latest_per_series(
    schema: &SchemaRef,
    contract: &Contract,
    batches: &[RecordBatch],
) -> WriteResult<Option<RecordBatch>> {
    if contract.time_column.is_none() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(None);
    }
    let sorted = stable_sort(&concat_batches(schema, batches)?, &contract.write_order())?;
    Ok(Some(last_per_group(&sorted, &contract.series_key)?))
}

/// Newest row per series among `parts`, each already the newest rows of something, one per series
/// and sorted by the write order (as indexes are): merged, not sorted again, so extending an index
/// of a million series by an append costs a pass over it. Later parts win ties, as in
/// [`latest_per_series`].
pub fn merge_latest(
    schema: &SchemaRef,
    contract: &Contract,
    parts: &[RecordBatch],
) -> WriteResult<Option<RecordBatch>> {
    if contract.series_key.is_empty() || contract.time_column.is_none() {
        return latest_per_series(schema, contract, parts);
    }
    let parts: Vec<&RecordBatch> = parts.iter().filter(|p| p.num_rows() > 0).collect();
    let rows: usize = parts.iter().map(|p| p.num_rows()).sum();
    if rows == 0 {
        return Ok(None);
    }
    let order = contract.write_order();
    let keys: Vec<&str> = order.iter().map(String::as_str).collect();
    let mut merge = Merge::new(schema.clone(), &keys, contract.series_key.len(), parts.len(), rows)?;
    let mut inputs: Vec<Option<RecordBatch>> = parts.into_iter().cloned().map(Some).collect();
    let mut out = vec![];
    loop {
        match merge.step()? {
            Step::Need(i) => merge.push(i, inputs[i].take())?,
            Step::Output(batch) => out.push(batch),
            Step::Done => break,
        }
    }
    Ok(Some(concat_batches(schema, &out)?))
}

/// Newest row per series among partitions that [`sort_and_split`] returned: each is sorted by the
/// write order, so its last row per series is its newest, and only those few rows are sorted to
/// pick the newest across partitions. The same rows as [`latest_per_series`] of the partitions,
/// without sorting the whole append again.
pub fn latest_of_sorted(
    schema: &SchemaRef,
    contract: &Contract,
    partitions: &[PartitionRows],
) -> WriteResult<Option<RecordBatch>> {
    if contract.time_column.is_none() {
        return Ok(None);
    }
    let lasts = partitions
        .iter()
        .flat_map(|(_, batches)| batches)
        .filter(|rows| rows.num_rows() > 0)
        .map(|rows| last_per_group(rows, &contract.series_key))
        .collect::<WriteResult<Vec<_>>>()?;
    latest_per_series(schema, contract, &lasts)
}

fn too_many_rows(rows: usize) -> WriteError {
    WriteError::invalid(format!("{rows} rows are too many to sort at once"))
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch(keys: &[Option<i32>], tags: &[&str]) -> RecordBatch {
        let schema =
            Schema::new(vec![Field::new("k", DataType::Int32, true), Field::new("tag", DataType::Utf8, false)]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(keys.to_vec())), Arc::new(StringArray::from(tags.to_vec()))],
        )
        .unwrap()
    }

    fn tags(b: &RecordBatch) -> Vec<&str> {
        let tags = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        (0..tags.len()).map(|i| tags.value(i)).collect()
    }

    /// `values` (one column, `from`) aligned to a table column of type `to`.
    fn convert(values: ArrayRef, to: DataType) -> WriteResult<ArrayRef> {
        let input = Schema::new(vec![Field::new("c", values.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(input), vec![values]).unwrap();
        let table = Arc::new(Schema::new(vec![Field::new("c", to, true)]));
        Ok(align_batch(&batch, &table)?.column(0).clone())
    }

    #[test]
    fn converts_only_without_loss() {
        use arrow_array::{
            BooleanArray, Date32Array, Date64Array, DictionaryArray, Float32Array, Float64Array, Int8Array, Int64Array,
            LargeStringArray, TimestampNanosecondArray, types::Int8Type,
        };
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()));
        let allowed: Vec<(ArrayRef, DataType)> = vec![
            (Arc::new(Int8Array::from(vec![-3, 7])), DataType::Int64),
            (Arc::new(Int64Array::from(vec![-3, 7])), DataType::Int32),
            (Arc::new(Float32Array::from(vec![1.5])), DataType::Float64),
            (Arc::new(Float64Array::from(vec![1.5, 2.0])), DataType::Float32),
            (Arc::new(Int64Array::from(vec![1 << 53])), DataType::Float64),
            (Arc::new(Float64Array::from(vec![3.0, -1.0])), DataType::Int32),
            (Arc::new(TimestampNanosecondArray::from(vec![1_000]).with_timezone("Europe/Kyiv")), ts.clone()),
            (Arc::new(StringArray::from(vec!["2026-01-01T00:00:00Z"])), ts.clone()),
            (Arc::new(StringArray::from(vec!["12", "-4"])), DataType::Int32),
            (Arc::new(Date32Array::from(vec![20_454])), ts.clone()),
            (Arc::new(TimestampNanosecondArray::from(vec![2_000, 3_000])), ts.clone()),
            (Arc::new(Date64Array::from(vec![2 * 86_400_000])), DataType::Date32),
            (Arc::new(Date32Array::from(vec![2])), DataType::Date64),
            (Arc::new(LargeStringArray::from(vec!["a"])), DataType::Utf8),
            (Arc::new(DictionaryArray::<Int8Type>::from_iter(["a", "b", "a"])), DataType::Utf8),
            (new_null_array(&DataType::Null, 2), DataType::Float64),
        ];
        for (values, to) in allowed {
            let from = values.data_type().clone();
            let out = convert(values, to.clone()).unwrap_or_else(|e| panic!("{from} → {to}: {e}"));
            assert_eq!(out.data_type(), &to);
        }

        let rejected: Vec<(ArrayRef, DataType, &str)> = vec![
            (Arc::new(Int64Array::from(vec![1 << 40])), DataType::Int32, "can't be cast"),
            (Arc::new(Float64Array::from(vec![1.5])), DataType::Int32, "not exactly"),
            (Arc::new(Float64Array::from(vec![0.1])), DataType::Float32, "not exactly"),
            (Arc::new(Int64Array::from(vec![(1 << 53) + 1])), DataType::Float64, "not exactly"),
            (Arc::new(StringArray::from(vec!["12", "twelve"])), DataType::Int32, "can't be cast"),
            (Arc::new(StringArray::from(vec!["yesterday"])), ts.clone(), "can't be cast"),
            (Arc::new(BooleanArray::from(vec![true])), DataType::Int32, "can't be written"),
            (Arc::new(TimestampNanosecondArray::from(vec![1])), DataType::Int64, "can't be written"),
            // Two values a truncating cast would make one.
            (Arc::new(TimestampNanosecondArray::from(vec![1_001, 1_002])), ts.clone(), "not exactly"),
            (Arc::new(Date64Array::from(vec![86_400_000 + 1])), DataType::Date32, "not exactly"),
            (Arc::new(Int32Array::from(vec![1])), DataType::Utf8, "can't be written"),
        ];
        for (values, to, msg) in rejected {
            let from = values.data_type().clone();
            let err = convert(values, to.clone()).map(|_| ()).unwrap_err().to_string();
            assert!(err.contains("column \"c\"") && err.contains(msg), "{from} → {to}: {err}");
        }
    }

    /// A supplied insertion time that doesn't parse is an error, not a null stamped with `now`.
    #[test]
    fn a_bad_ingest_time_is_an_error() {
        let schema = Schema::new(vec![Field::new("_ingest_ts", DataType::Utf8, true)]);
        let given = StringArray::from(vec![Some("2026-01-01T00:00:00Z"), None, Some("soon")]);
        let b = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(given)]).unwrap();
        assert!(with_ingest_time(&b, "_ingest_ts", 7).is_err());
    }

    #[test]
    fn sorts_nulls_first_and_keeps_input_order_on_ties() {
        let b = batch(&[Some(2), None, Some(1), Some(2), Some(1)], &["a", "b", "c", "d", "e"]);
        let sorted = stable_sort(&b, &["k".into()]).unwrap();
        assert_eq!(tags(&sorted), ["b", "c", "e", "a", "d"]);
        assert_eq!(tags(&last_per_group(&sorted, &["k".into()]).unwrap()), ["b", "e", "d"]);
    }

    #[test]
    fn without_keys_keeps_the_last_row() {
        let b = batch(&[Some(1), Some(2)], &["a", "b"]);
        assert_eq!(tags(&stable_sort(&b, &[]).unwrap()), ["a", "b"]);
        assert_eq!(tags(&last_per_group(&b, &[]).unwrap()), ["b"]);
    }

    /// The newest row per series from sorted partitions is what sorting everything gives.
    #[test]
    fn latest_of_sorted_partitions_matches_sorting_everything() {
        use std::collections::HashMap;

        use arrow_array::{Float64Array, TimestampMicrosecondArray};

        const DAY: i64 = 86_400_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(UTC.into())), false),
            Field::new("device", DataType::Int32, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let contract = Contract::from_properties(&HashMap::from([
            (crate::property::TIME_COLUMN.to_string(), "time".to_string()),
            (crate::property::SERIES_KEY.to_string(), "device".to_string()),
            (crate::property::CHUNK.to_string(), "day".to_string()),
        ]))
        .unwrap();
        let rows: Vec<(i64, i32, f64)> = (0..200)
            .map(|i| {
                ((i * 7919) % 5 * DAY + i % 13, i32::try_from(i % 6).unwrap(), f64::from(i32::try_from(i).unwrap()))
            })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.0)).with_timezone(UTC)),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .unwrap();
        let partitions = sort_and_split(&schema, &contract, &[batch], 0).unwrap();
        assert!(partitions.len() > 1);
        let all: Vec<RecordBatch> = partitions.iter().flat_map(|(_, b)| b.clone()).collect();
        let expected = latest_per_series(&schema, &contract, &all).unwrap();
        assert_eq!(latest_of_sorted(&schema, &contract, &partitions).unwrap(), expected);
    }

    /// An append sorted in several groups and merged gives the rows sorting it at once gives, with
    /// dedup across groups (the later write wins) and with dedup off.
    #[test]
    fn merged_groups_match_one_sort() {
        use std::collections::HashMap;

        use arrow_array::{Float64Array, TimestampMicrosecondArray};

        const DAY: i64 = 86_400_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(UTC.into())), false),
            Field::new("device", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = |from: i64| {
            // Times repeat across batches (every batch writes the same keys), over three days.
            let rows: Vec<i64> = (0..300).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(
                        TimestampMicrosecondArray::from_iter_values(rows.iter().map(|i| i % 3 * DAY + i % 50))
                            .with_timezone(UTC),
                    ),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|i| format!("d{}", (i * 7) % 11)))),
                    Arc::new(Float64Array::from_iter_values(rows.iter().map(|i| (from + i) as f64))),
                ],
            )
            .unwrap()
        };
        let batches: Vec<RecordBatch> = (0..4).map(|b| batch(b * 1000)).collect();
        for dedup in ["true", "false"] {
            let contract = Contract::from_properties(&HashMap::from([
                (crate::property::TIME_COLUMN.to_string(), "time".to_string()),
                (crate::property::SERIES_KEY.to_string(), "device".to_string()),
                (crate::property::CHUNK.to_string(), "day".to_string()),
                (crate::property::DEDUP.to_string(), dedup.to_string()),
                (crate::property::ROW_GROUP_ROWS.to_string(), "64".to_string()),
            ]))
            .unwrap();
            let flat = |p: Vec<PartitionRows>| -> Vec<(Option<i32>, RecordBatch)> {
                p.into_iter().map(|(p, b)| (p, concat_batches(&schema, &b).unwrap())).collect()
            };
            let once = flat(sort_and_split_in_groups(&schema, &contract, &batches, 0, usize::MAX).unwrap());
            let merged = sort_and_split_in_groups(&schema, &contract, &batches, 0, 1).unwrap();
            assert!(merged.iter().any(|(_, b)| b.len() > 1), "batches of row_group_rows");
            assert_eq!(flat(merged), once, "dedup {dedup}");
            assert_eq!(once.len(), 3);
        }
    }

    #[test]
    fn empty_input() {
        let b = batch(&[], &[]);
        assert_eq!(last_per_group(&stable_sort(&b, &["k".into()]).unwrap(), &["k".into()]).unwrap().num_rows(), 0);
    }

    /// Merging indexes gives what sorting their rows gives: the newest per series, later parts
    /// winning ties, late rows of a part losing to newer ones of another.
    #[test]
    fn merged_indexes_match_sorted_ones() {
        use std::collections::HashMap;

        use arrow_array::Float64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("device", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let props = HashMap::from([
            (crate::property::TIME_COLUMN.to_string(), "time".to_string()),
            (crate::property::SERIES_KEY.to_string(), "device".to_string()),
        ]);
        let contract = Contract::from_properties(&props).unwrap();
        let part = |rows: &[(i64, Option<&str>, f64)]| {
            let b = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow_array::Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                    Arc::new(rows.iter().map(|r| r.1).collect::<StringArray>()),
                    Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
                ],
            )
            .unwrap();
            latest_per_series(&schema, &contract, &[b]).unwrap().unwrap()
        };
        let base = part(&[(5, Some("a"), 1.0), (7, Some("b"), 2.0), (1, None, 3.0), (9, Some("d"), 4.0)]);
        let late = part(&[(4, Some("a"), 5.0), (7, Some("b"), 6.0), (2, None, 7.0), (3, Some("c"), 8.0)]);
        let newer = part(&[(8, Some("a"), 9.0), (1, Some("e"), 10.0)]);
        let parts = [base, late, newer];
        let merged = merge_latest(&schema, &contract, &parts).unwrap().unwrap();
        assert_eq!(merged, latest_per_series(&schema, &contract, &parts).unwrap().unwrap());
        let values = merged.column(2).as_any().downcast_ref::<Float64Array>().unwrap().values().to_vec();
        assert_eq!(values, [7.0, 9.0, 6.0, 8.0, 4.0, 10.0]);
    }

    /// Batches matched to a table by column name (moved from the engine with `align_batch`).
    mod align {
        use arrow_array::{Array, Int8Array, Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        use super::super::*;

        fn table_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]))
        }

        #[test]
        fn aligns_by_name_and_casts() {
            let input = Arc::new(Schema::new(vec![Field::new("id", DataType::Int8, false)]));
            let batch = RecordBatch::try_new(input, vec![Arc::new(Int8Array::from(vec![1, 2]))]).unwrap();
            let aligned = align_batch(&batch, &table_schema()).unwrap();
            assert_eq!(aligned.schema(), table_schema());
            assert_eq!(aligned.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values(), &[1, 2]);
            assert_eq!(aligned.column(1).null_count(), 2);

            let reordered = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("id", DataType::Int32, false),
                ])),
                vec![Arc::new(StringArray::from(vec!["a"])), Arc::new(Int32Array::from(vec![7]))],
            )
            .unwrap();
            let aligned = align_batch(&reordered, &table_schema()).unwrap();
            assert_eq!(aligned.column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0), 7);
        }

        #[test]
        fn rejects_extra_and_missing_required_columns() {
            let extra = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("x", DataType::Int32, true),
                ])),
                vec![Arc::new(Int32Array::from(vec![1])), Arc::new(Int32Array::from(vec![1]))],
            )
            .unwrap();
            assert!(align_batch(&extra, &table_schema()).is_err());

            let missing = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(vec!["a"]))],
            )
            .unwrap();
            assert!(align_batch(&missing, &table_schema()).is_err());
        }
    }

    /// The newest row per series (moved from the engine with `latest_per_series`).
    mod latest {
        use std::collections::HashMap;

        use arrow_array::{Array, Float64Array, Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        use super::super::*;
        use crate::property;

        fn schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("time", DataType::Int64, false),
                Field::new("device", DataType::Utf8, true),
                Field::new("value", DataType::Float64, true),
            ]))
        }

        fn batch(rows: &[(i64, Option<&str>, f64)]) -> RecordBatch {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                    Arc::new(rows.iter().map(|r| r.1).collect::<StringArray>()),
                    Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
                ],
            )
            .unwrap()
        }

        fn contract(time: Option<&str>, key: &str) -> Contract {
            let mut p = HashMap::from([(property::SERIES_KEY.to_string(), key.to_string())]);
            if let Some(t) = time {
                p.insert(property::TIME_COLUMN.into(), t.into());
            }
            Contract::from_properties(&p).unwrap()
        }

        fn values(b: &RecordBatch) -> Vec<f64> {
            b.column(2).as_any().downcast_ref::<Float64Array>().unwrap().values().to_vec()
        }

        #[test]
        fn newest_row_per_series() {
            let c = contract(Some("time"), "device");
            let b = batch(&[(2, Some("a"), 1.0), (1, Some("b"), 2.0), (3, Some("a"), 3.0), (1, Some("b"), 4.0)]);
            let latest = latest_per_series(&schema(), &c, &[b]).unwrap().unwrap();
            // Device b has two rows at time 1: the later one wins.
            assert_eq!(values(&latest), [3.0, 4.0]);
        }

        #[test]
        fn equal_times_across_batches_take_the_later_batch() {
            let c = contract(Some("time"), "device");
            let first = batch(&[(5, Some("a"), 1.0)]);
            let second = batch(&[(5, Some("a"), 2.0), (4, Some("a"), 3.0)]);
            let latest = latest_per_series(&schema(), &c, &[first, second]).unwrap().unwrap();
            assert_eq!(values(&latest), [2.0]);
        }

        #[test]
        fn null_series_key_is_its_own_series() {
            let c = contract(Some("time"), "device");
            let b = batch(&[(1, None, 1.0), (2, Some("a"), 2.0), (3, None, 3.0)]);
            let latest = latest_per_series(&schema(), &c, &[b]).unwrap().unwrap();
            assert_eq!(values(&latest), [3.0, 2.0]);
            assert!(latest.column(1).is_null(0));
        }

        #[test]
        fn empty_series_key_keeps_the_single_newest_row() {
            let c = contract(Some("time"), "");
            let b = batch(&[(2, Some("a"), 1.0), (9, Some("b"), 2.0), (9, Some("c"), 3.0), (1, Some("d"), 4.0)]);
            let latest = latest_per_series(&schema(), &c, &[b]).unwrap().unwrap();
            assert_eq!(values(&latest), [3.0]);
        }

        #[test]
        fn nothing_without_time_column_or_rows() {
            let b = batch(&[(1, Some("a"), 1.0)]);
            assert!(latest_per_series(&schema(), &contract(None, "device"), &[b]).unwrap().is_none());
            let c = contract(Some("time"), "device");
            assert!(latest_per_series(&schema(), &c, &[]).unwrap().is_none());
            assert!(latest_per_series(&schema(), &c, &[batch(&[])]).unwrap().is_none());
        }
    }
}
