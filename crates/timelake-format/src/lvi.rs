//! The last-value index's file: the newest row of every series, as one small Parquet file per
//! snapshot (`timelake.last-value-index` in its summary), so `DuckDB` can read it too.

use arrow_array::{Int64Array, RecordBatch};
use arrow_cast::cast;
use arrow_ord::cmp::gt_eq;
use arrow_schema::{DataType, SchemaRef};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::error::WriteResult;
use crate::file::ZSTD_LEVEL;
use crate::rows::align_batch;

/// The index as a Parquet file.
pub fn encode(batch: &RecordBatch) -> WriteResult<Vec<u8>> {
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?)).build();
    let mut buf = vec![];
    let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    w.write(batch)?;
    w.close()?;
    Ok(buf)
}

/// An index file, its columns aligned to the table's `schema`.
pub fn decode(bytes: Bytes, schema: &SchemaRef) -> WriteResult<RecordBatch> {
    let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)?
        .build()?
        .map(|b| align_batch(&b?, schema))
        .collect::<WriteResult<Vec<_>>>()?;
    Ok(concat_batches(schema, &batches)?)
}

/// The rows of `index` whose `time_column` is at or after `cutoff` (µs since the epoch): what
/// retention leaves of it.
pub fn retain_since(index: &RecordBatch, time_column: &str, cutoff: i64) -> WriteResult<RecordBatch> {
    let times = cast(index.column(index.schema().index_of(time_column)?), &DataType::Int64)?;
    let keep = gt_eq(&times, &Int64Array::new_scalar(cutoff))?;
    Ok(filter_record_batch(index, &keep)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn indexes_round_trip_into_the_table_schema() {
        let file = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(file, vec![Arc::new(Float64Array::from(vec![1.5]))]).unwrap();
        let table = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, true),
            Field::new("v", DataType::Float64, true),
        ]));
        let read = decode(Bytes::from(encode(&batch).unwrap()), &table).unwrap();
        assert_eq!(read.schema(), table);
        assert_eq!(read.column(0).null_count(), 1);
        assert_eq!(read.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 1.5);
    }

    #[test]
    fn retention_keeps_series_seen_since_the_cutoff() {
        use arrow_array::TimestampMicrosecondArray;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        )]));
        let times = TimestampMicrosecondArray::from(vec![1, 5, 9]);
        let index = RecordBatch::try_new(schema, vec![Arc::new(times)]).unwrap();
        assert_eq!(retain_since(&index, "t", 5).unwrap().num_rows(), 2);
    }
}
