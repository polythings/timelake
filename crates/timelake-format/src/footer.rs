//! Parquet footers (file metadata and page indexes) kept apart from their files, so the head can
//! carry them and a cold scan doesn't fetch each file's footer first (spec 7, P2). A footer is
//! stored as `ParquetMetaDataWriter` writes it: page indexes, metadata, length, magic.

use bytes::Bytes;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter};

/// Bytes read from the end of a data file first: holds the whole footer of typical files.
pub const PREFETCH: u64 = 64 << 10;

/// A file's footer as the head stores it.
pub fn encode(metadata: &ParquetMetaData) -> Result<Vec<u8>, ParquetError> {
    let mut buf = vec![];
    ParquetMetaDataWriter::new(&mut buf, metadata).finish()?;
    Ok(buf)
}

/// A footer [`encode`] wrote, with its offset index if it has one, and its column index as
/// `column_index` says: scans prune pages with it, compaction only needs the offsets.
pub fn decode(bytes: &Bytes, column_index: PageIndexPolicy) -> Result<ParquetMetaData, ParquetError> {
    ParquetMetaDataReader::new()
        .with_column_index_policy(column_index)
        .with_offset_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};

    use super::*;

    #[test]
    fn footers_round_trip() {
        let batch = RecordBatch::try_from_iter([("x", Arc::new(Int64Array::from(vec![1, 2, 3])) as _)]).unwrap();
        let mut file = vec![];
        let mut w = ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(Bytes::from(file), options).unwrap();
        let bytes = Bytes::from(encode(reader.metadata()).unwrap());
        let all = decode(&bytes, PageIndexPolicy::Optional).unwrap();
        assert_eq!(all.file_metadata().num_rows(), 3);
        assert!(all.offset_index().is_some() && all.column_index().is_some());
        let offsets = decode(&bytes, PageIndexPolicy::Skip).unwrap();
        assert!(offsets.offset_index().is_some() && offsets.column_index().is_none());
    }
}
