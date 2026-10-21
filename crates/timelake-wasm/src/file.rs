//! Reading a Parquet file through byte ranges the host fetched: plan the ranges, then decode.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::{Buf, Bytes};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};

use crate::error::{Error, Result};
use crate::filter::filter;
use crate::prune::row_group_may_match;
use crate::spec::QuerySpec;

/// Ranges closer than this are fetched as one: a request costs more than the bytes in between.
pub const COALESCE_GAP: u64 = 64 * 1024;
/// Rows per decoded batch.
const BATCH_ROWS: usize = 64 * 1024;
/// The footer ends with a 4-byte metadata length and the `PAR1` magic.
const FOOTER_TRAILER: usize = 8;

/// The fetched parts of a file, as a sparse file the Parquet reader can read from.
#[derive(Debug, Clone, Default)]
struct SparseFile {
    len: u64,
    chunks: BTreeMap<u64, Bytes>,
}

impl SparseFile {
    fn bytes(&self, start: u64, length: u64) -> Result<Bytes, ParquetError> {
        let end = start + length;
        let not_fetched = || ParquetError::General(format!("bytes {start}..{end} were not fetched"));
        let (&offset, chunk) = self.chunks.range(..=start).next_back().ok_or_else(not_fetched)?;
        let from = usize::try_from(start - offset).map_err(|_| not_fetched())?;
        let to = usize::try_from(end - offset).map_err(|_| not_fetched())?;
        if to > chunk.len() {
            return Err(not_fetched());
        }
        Ok(chunk.slice(from..to))
    }
}

impl Length for SparseFile {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for SparseFile {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let (&offset, chunk) = self
            .chunks
            .range(..=start)
            .next_back()
            .ok_or_else(|| ParquetError::General(format!("byte {start} was not fetched")))?;
        let length = (offset + chunk.len() as u64).saturating_sub(start);
        Ok(self.bytes(start, length)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        self.bytes(start, length as u64)
    }
}

/// How many trailing bytes hold the footer (metadata and trailer), given at least the last 8.
pub fn footer_len(tail: &[u8]) -> Result<u64> {
    let [.., l0, l1, l2, l3, b'P', b'A', b'R', b'1'] = *tail else {
        return Err(Error::invalid("not a Parquet file"));
    };
    Ok(u64::from(u32::from_le_bytes([l0, l1, l2, l3])) + FOOTER_TRAILER as u64)
}

/// Sorts ranges and merges those less than `gap` bytes apart.
pub fn coalesce(mut ranges: Vec<Range<u64>>, gap: u64) -> Vec<Range<u64>> {
    ranges.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match merged.last_mut() {
            Some(last) if r.start <= last.end + gap => last.end = last.end.max(r.end),
            _ => merged.push(r),
        }
    }
    merged
}

/// A Parquet file opened from its footer. [`plan`](Self::plan) picks row groups and columns and
/// says which bytes to fetch; after [`add_range`](Self::add_range), [`read`](Self::read) decodes.
#[derive(Debug)]
pub struct FileReader {
    data: SparseFile,
    meta: Arc<ParquetMetaData>,
    reader_meta: ArrowReaderMetadata,
    row_groups: Vec<usize>,
    leaves: Vec<usize>,
}

impl FileReader {
    /// Opens a file of `size` bytes; `tail` must hold at least the last [`footer_len`] bytes.
    pub fn new(size: u64, tail: &[u8]) -> Result<Self> {
        let need = footer_len(tail)?;
        let metadata = usize::try_from(need)
            .ok()
            .and_then(|need| tail.len().checked_sub(need))
            .map(|start| &tail[start..tail.len() - FOOTER_TRAILER])
            .ok_or_else(|| Error::invalid(format!("need the last {need} bytes for the footer")))?;
        Self::with_metadata(size, Arc::new(ParquetMetaDataReader::decode_metadata(metadata)?))
    }

    /// Opens a file of `size` bytes from a footer stored apart from it, as the head object carries
    /// them (written by `ParquetMetaDataWriter`: page indexes, metadata, length, magic).
    pub fn from_footer(size: u64, footer: &Bytes) -> Result<Self> {
        let reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional);
        Self::with_metadata(size, Arc::new(reader.parse_and_finish(footer)?))
    }

    fn with_metadata(size: u64, meta: Arc<ParquetMetaData>) -> Result<Self> {
        let reader_meta = ArrowReaderMetadata::try_new(meta.clone(), ArrowReaderOptions::new())?;
        Ok(Self {
            data: SparseFile { len: size, chunks: BTreeMap::new() },
            meta,
            reader_meta,
            row_groups: vec![],
            leaves: vec![],
        })
    }

    /// Selects the row groups whose statistics may match and the needed columns; returns the
    /// byte ranges of their column chunks, coalesced.
    pub fn plan(&mut self, spec: &QuerySpec) -> Vec<Range<u64>> {
        let descr = self.meta.file_metadata().schema_descr();
        // Timelake tables are flat: one leaf per top-level column.
        let leaf = |name: &str| (0..descr.num_columns()).find(|&i| descr.column(i).name() == name);
        self.leaves = spec.needed_columns(self.reader_meta.schema()).iter().filter_map(|c| leaf(c)).collect();
        self.row_groups = (0..self.meta.num_row_groups())
            .filter(|&rg| row_group_may_match(self.meta.row_group(rg), spec, leaf))
            .collect();
        let (meta, leaves) = (&self.meta, &self.leaves);
        let ranges = self.row_groups.iter().flat_map(|&rg| {
            leaves.iter().map(move |&c| {
                let (start, len) = meta.row_group(rg).column(c).byte_range();
                start..start + len
            })
        });
        coalesce(ranges.collect(), COALESCE_GAP)
    }

    /// Adds fetched bytes starting at offset `start`.
    pub fn add_range(&mut self, start: u64, bytes: Vec<u8>) {
        self.data.chunks.insert(start, Bytes::from(bytes));
    }

    /// Decodes the planned row groups and columns a batch at a time, with the query's filters applied; batches
    /// without matching rows are left out.
    pub fn batches<'a>(&self, spec: &'a QuerySpec) -> Result<impl Iterator<Item = Result<RecordBatch>> + 'a> {
        let mask = ProjectionMask::leaves(self.meta.file_metadata().schema_descr(), self.leaves.iter().copied());
        let reader = (!self.row_groups.is_empty())
            .then(|| {
                ParquetRecordBatchReaderBuilder::new_with_metadata(self.data.clone(), self.reader_meta.clone())
                    .with_row_groups(self.row_groups.clone())
                    .with_projection(mask)
                    .with_batch_size(BATCH_ROWS)
                    .build()
            })
            .transpose()?;
        Ok(reader.into_iter().flatten().filter_map(move |batch| match batch.map_err(Error::from) {
            Ok(batch) => match filter(&batch, spec) {
                Ok(batch) if batch.num_rows() == 0 => None,
                result => Some(result),
            },
            Err(e) => Some(Err(e)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_footer_length() {
        let mut tail = vec![0xAA; 4];
        tail.extend_from_slice(&300u32.to_le_bytes());
        tail.extend_from_slice(b"PAR1");
        assert_eq!(footer_len(&tail).unwrap(), 308);
        assert_eq!(footer_len(&tail[4..]).unwrap(), 308, "the last 8 bytes are enough");
        assert!(footer_len(&tail[5..]).is_err(), "too short");
        assert!(footer_len(b"12345678PAR2").is_err(), "bad magic");
    }

    #[test]
    fn coalesces_close_ranges() {
        assert_eq!(coalesce(vec![], 10), Vec::<Range<u64>>::new());
        // Unsorted input; 20..30 is within the gap of 0..10, 100..110 is not.
        assert_eq!(coalesce(vec![100..110, 20..30, 0..10], 10), [0..30, 100..110]);
        // Overlapping and contained ranges merge.
        assert_eq!(coalesce(vec![0..50, 10..20, 40..60, 100..101], 0), [0..60, 100..101]);
        // Adjacent ranges merge even with no gap allowed.
        assert_eq!(coalesce(vec![0..10, 10..20, 21..30], 0), [0..20, 21..30]);
    }

    #[test]
    fn sparse_file_serves_only_fetched_bytes() {
        let mut f = SparseFile { len: 1000, chunks: BTreeMap::new() };
        f.chunks.insert(100, Bytes::from_static(b"0123456789"));
        f.chunks.insert(500, Bytes::from_static(b"abcdef"));
        assert_eq!(f.get_bytes(102, 3).unwrap(), "234");
        assert_eq!(f.get_bytes(500, 6).unwrap(), "abcdef");
        assert!(f.get_bytes(108, 5).is_err(), "runs past the chunk");
        assert!(f.get_bytes(50, 1).is_err(), "before any chunk");
        assert!(f.get_bytes(300, 1).is_err(), "in a gap");
        assert!(f.get_read(507).is_err(), "past the end of a chunk");
        let mut rest = String::new();
        std::io::Read::read_to_string(&mut f.get_read(503).unwrap(), &mut rest).unwrap();
        assert_eq!(rest, "def");
    }
}
