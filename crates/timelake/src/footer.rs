//! Reading the Parquet footers the head carries (spec 7, P2) from the store, through the lake's
//! cache. The encoding is `timelake_format::footer`'s.

use bytes::Bytes;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use timelake_format::footer::{self, PREFETCH};

use crate::error::Result;
use crate::lake::Lake;

/// The footer of the data file at `uri`, of `size` bytes, from the lake's cache or the store.
pub(crate) async fn read(lake: &Lake, uri: &str, size: u64) -> Result<Bytes> {
    if let Some(bytes) = lake.caches().footers.get(uri) {
        return Ok(bytes);
    }
    let storage = lake.storage();
    let mut reader = ParquetObjectReader::new(storage.object_store(), storage.path(uri)?).with_file_size(size);
    let prefetch = usize::try_from(PREFETCH).expect("64 KiB fits a usize");
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_prefetch_hint(Some(prefetch))
        .load_and_finish(&mut reader, size)
        .await?;
    let bytes = Bytes::from(footer::encode(&metadata)?);
    lake.caches().footers.insert(uri.to_string(), bytes.clone());
    Ok(bytes)
}
