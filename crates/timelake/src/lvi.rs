//! Last-value index: one small Parquet file per snapshot holding the latest row of every series.
//!
//! "Current state of every device" is the most common IoT query and the one where lakes lose to
//! TimescaleDB by ~100× (SPEC §17.5): a generic engine has to scan everything to find each
//! series' newest row. The index makes it a single small read. It is maintained incrementally on
//! every append (previous index ⊕ newest rows of the appended data), referenced from the snapshot
//! summary (`timelake.last-value-index`), and plain Parquet, so `DuckDB` can read it too.

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::contract::Contract;
use crate::error::Result;
use crate::schema::align_batch;
use crate::table::Table;

/// Newest row per series among `batches`, or `None` without a time column or rows. On equal
/// times the later insertion wins, then the later row (later input position), matching dedup's
/// "latest write wins".
pub fn latest_per_series(
    schema: &SchemaRef,
    contract: &Contract,
    batches: &[RecordBatch],
) -> Result<Option<RecordBatch>> {
    Ok(timelake_format::rows::latest_per_series(schema, contract, batches)?)
}

/// Reads an index (from the lake's cache when possible).
pub(crate) async fn read(table: &Table, uri: &str) -> Result<RecordBatch> {
    let cache = &table.lake().caches().last_value_indexes;
    if let Some(batch) = cache.get(uri) {
        return Ok(batch);
    }
    let bytes = table.lake().storage().read(uri).await?;
    let schema = table.schema();
    let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)?
        .build()?
        .map(|b| align_batch(&b?, &schema))
        .collect::<Result<Vec<_>>>()?;
    let batch = concat_batches(&schema, &batches)?;
    cache.insert(uri.to_string(), batch.clone());
    Ok(batch)
}

/// A fresh location for the index of a new snapshot.
pub(crate) fn new_uri(table: &Table, snapshot_id: i64) -> String {
    format!("{}/metadata/lvi-{snapshot_id}-{}.parquet", table.location(), uuid::Uuid::new_v4())
}

/// The index as a Parquet file.
pub(crate) fn encode(batch: &RecordBatch) -> Result<Bytes> {
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?)).build();
    let mut buf = vec![];
    let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    w.write(batch)?;
    w.close()?;
    Ok(Bytes::from(buf))
}

/// The index for a new snapshot: the base snapshot's index merged with the newest rows of the
/// appended data. `None` if the table has no time column.
pub(crate) async fn merged(base: &Table, delta: &RecordBatch) -> Result<Option<RecordBatch>> {
    let mut parts = vec![];
    if let Some(uri) = base.last_value_index() {
        parts.push(read(base, &uri).await?);
    }
    parts.push(delta.clone());
    Ok(timelake_format::rows::merge_latest(&base.schema(), base.contract(), &parts)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
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
