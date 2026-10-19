//! Parquet footers (file metadata and page indexes) kept apart from their files, so the head
//! object can carry them and a cold scan doesn't fetch each file's footer first (spec 18, P2).
//! A footer is stored as `ParquetMetaDataWriter` writes it: page indexes, metadata, length, magic.

use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter};

use crate::error::Result;
use crate::lake::Lake;

/// Bytes read from the end of a file first: holds the whole footer of typical files.
const PREFETCH: usize = 64 << 10;

/// The footer of the data file at `uri`, of `size` bytes, from the lake's cache or the store.
pub(crate) async fn read(lake: &Lake, uri: &str, size: u64) -> Result<Bytes> {
    if let Some(bytes) = lake.caches().footers.get(uri) {
        return Ok(bytes);
    }
    let storage = lake.storage();
    let mut reader = ParquetObjectReader::new(storage.object_store(), storage.path(uri)?).with_file_size(size);
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_prefetch_hint(Some(PREFETCH))
        .load_and_finish(&mut reader, size)
        .await?;
    let bytes = encode(&metadata)?;
    lake.caches().footers.insert(uri.to_string(), bytes.clone());
    Ok(bytes)
}

pub(crate) fn encode(metadata: &ParquetMetaData) -> Result<Bytes> {
    let mut buf = vec![];
    ParquetMetaDataWriter::new(&mut buf, metadata).finish()?;
    Ok(Bytes::from(buf))
}

pub(crate) fn decode(bytes: &Bytes) -> Result<Arc<ParquetMetaData>> {
    let reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional);
    Ok(Arc::new(reader.parse_and_finish(bytes)?))
}
