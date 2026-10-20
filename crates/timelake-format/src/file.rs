//! Writing Timelake data files without I/O, shared by the engine and the WASM writer (spec 13):
//! the Parquet settings for time series, a file writer whose bytes the caller drains and uploads,
//! and the Iceberg statistics of a written file (lakebed's, as iceberg-rust's writer computes them).
//!
//! On native targets a row group's columns are encoded on threads of their own; on WebAssembly,
//! one after another.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use lakebed_iceberg::stats::{ParquetStats, field_id};
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory, compute_leaves,
};
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, SortingColumn};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesPtr};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::ColumnPath;

use crate::Contract;

/// ZSTD level for data files: fast to write, close to the best ratio.
pub const ZSTD_LEVEL: i32 = 3;

/// Rows encoded between checks of a row group's size against its byte cap.
const BYTE_CHECK_ROWS: usize = 8 * 1024;

/// Rows below which a batch's columns are encoded on one thread: threads cost more than they save.
#[cfg(not(target_arch = "wasm32"))]
const PARALLEL_MIN_ROWS: usize = 8 * 1024;

/// The Parquet codec named by Iceberg's `write.parquet.compression-codec` and `-level`.
/// Measured on TSBS IoT (repeat queries, local disk): ZSTD 3 11.5 ms and 200 MB, ZSTD 1 11.1 ms
/// and 204 MB, LZ4 12.3 ms and 242 MB, Snappy 10.8 ms and 277 MB: decoding isn't the bottleneck,
/// and on object storage bytes are, so ZSTD 3 is the default (`bench/results/PERFORMANCE.md`).
/// Codecs this build lacks (WebAssembly has ZSTD and Snappy) fail when a file is written.
pub fn compression(codec: &str, level: Option<i32>) -> Result<Compression, ParquetError> {
    Ok(match codec {
        "zstd" => Compression::ZSTD(ZstdLevel::try_new(level.unwrap_or(ZSTD_LEVEL))?),
        "lz4" | "lz4_raw" => Compression::LZ4_RAW,
        "snappy" => Compression::SNAPPY,
        "gzip" => Compression::GZIP(GzipLevel::try_new(level.map_or(Ok(6), u32::try_from).unwrap_or(6))?),
        "brotli" => Compression::BROTLI(BrotliLevel::try_new(level.map_or(Ok(1), u32::try_from).unwrap_or(1))?),
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => {
            return Err(ParquetError::General(format!(
                "write.parquet.compression-codec {other:?}: use zstd, lz4, snappy, gzip, brotli or uncompressed"
            )));
        }
    })
}

/// Rows per data page when the table doesn't set `data-page-rows`: Parquet's own default.
pub const DATA_PAGE_ROWS: usize = 20_000;

/// Parquet settings for a table's data files: ZSTD, page statistics and page index (for row-group
/// and page pruning), delta encoding for the time column, dictionary encoding for everything else
/// (floats falling back to byte-stream-split, or byte-stream-split only, by `float-encoding`), and
/// the sort order recorded.
/// Pages hold the table's `data-page-rows`, else `default_page_rows`.
pub fn writer_properties(
    schema: &SchemaRef,
    contract: &Contract,
    created_by: &str,
    default_page_rows: usize,
) -> Result<WriterProperties, ParquetError> {
    let mut b = WriterProperties::builder()
        .set_compression(compression(&contract.compression_codec, contract.compression_level)?)
        .set_max_row_group_row_count(Some(contract.row_group_rows))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(contract.data_page_rows.unwrap_or(default_page_rows).max(1))
        .set_created_by(created_by.to_string());
    for f in schema.fields() {
        let path = ColumnPath::from(f.name().as_str());
        match f.data_type() {
            // A dictionary falls back to byte-stream-split once it outgrows its page.
            DataType::Float32 | DataType::Float64 if contract.float_encoding == "byte-stream-split" => {
                b = b
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path, Encoding::BYTE_STREAM_SPLIT);
            }
            DataType::Float32 | DataType::Float64 => b = b.set_column_encoding(path, Encoding::BYTE_STREAM_SPLIT),
            DataType::Timestamp(_, _) if Some(f.name()) == contract.time_column.as_ref() => {
                b = b
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path, Encoding::DELTA_BINARY_PACKED);
            }
            _ => {}
        }
    }
    let sorting = contract
        .sort_columns()
        .iter()
        .map(|c| {
            let column_idx = schema.index_of(c).map_err(|e| ParquetError::ArrowError(e.to_string()))?;
            let column_idx = i32::try_from(column_idx).map_err(|_| ParquetError::General("too many columns".into()))?;
            Ok(SortingColumn { column_idx, descending: false, nulls_first: true })
        })
        .collect::<Result<Vec<_>, ParquetError>>()?;
    if !sorting.is_empty() {
        b = b.set_sorting_columns(Some(sorting));
    }
    Ok(b.build())
}

/// Bytes written and not yet taken by the caller.
#[derive(Clone, Debug, Default)]
struct Buffer(Arc<Mutex<Vec<Vec<u8>>>>);

impl Buffer {
    /// The chunks written, in order: kept apart rather than in one growing buffer, which on
    /// WebAssembly leaves memory the allocator can't reuse.
    fn take(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn len(&self) -> usize {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).iter().map(Vec::len).sum()
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Writes one Parquet file from sorted batches. Encoded row groups accumulate as bytes that the
/// caller takes with [`FileWriter::take`] and uploads as parts; memory is the row group being
/// encoded plus what wasn't taken.
pub struct FileWriter {
    schema: SchemaRef,
    writer: SerializedFileWriter<Buffer>,
    buffer: Buffer,
    factory: ArrowRowGroupWriterFactory,
    /// The current row group's column writers, one per leaf column.
    columns: Option<Vec<ArrowColumnWriter>>,
    rows_in_group: usize,
    row_group_rows: usize,
    /// Flush a row group once its encoded columns hold this many bytes, before `row_group_rows`.
    max_group_bytes: Option<usize>,
    /// Bytes taken by the caller so far.
    taken: u64,
    /// NaN counts of float columns, by field id.
    nan_counts: BTreeMap<i32, u64>,
    field_ids: Vec<Option<i32>>,
    parallel: bool,
}

impl std::fmt::Debug for FileWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileWriter")
            .field("rows_in_group", &self.rows_in_group)
            .field("taken", &self.taken)
            .finish_non_exhaustive()
    }
}

impl FileWriter {
    /// A writer of files with columns `schema` (whose fields may carry Iceberg field ids as
    /// `PARQUET:field_id` metadata) and settings `props` (see [`writer_properties`]).
    pub fn new(schema: SchemaRef, props: WriterProperties) -> Result<Self, ParquetError> {
        let mut props = props;
        parquet::arrow::add_encoded_arrow_schema_to_metadata(&schema, &mut props);
        let props: WriterPropertiesPtr = Arc::new(props);
        let parquet_schema = ArrowSchemaConverter::new().with_coerce_types(props.coerce_types()).convert(&schema)?;
        let field_ids = parquet_schema.columns().iter().map(|c| field_id(c)).collect();
        let row_group_rows = props.max_row_group_row_count().unwrap_or(usize::MAX).max(1);
        let buffer = Buffer::default();
        let writer = SerializedFileWriter::new(buffer.clone(), parquet_schema.root_schema_ptr(), props)?;
        let factory = ArrowRowGroupWriterFactory::new(&writer, schema.clone());
        Ok(Self {
            schema,
            writer,
            buffer,
            factory,
            columns: None,
            rows_in_group: 0,
            row_group_rows,
            max_group_bytes: None,
            taken: 0,
            nan_counts: BTreeMap::new(),
            field_ids,
            parallel: cfg!(not(target_arch = "wasm32")),
        })
    }

    /// Ends row groups once their encoded columns hold `bytes`, even before the row count, to
    /// bound memory where it is scarce (128 MB in a Cloudflare Worker).
    #[must_use]
    pub fn with_max_row_group_bytes(mut self, bytes: Option<usize>) -> Self {
        self.max_group_bytes = bytes;
        self
    }

    /// Encodes the columns on one thread (as on WebAssembly), for measuring.
    #[must_use]
    pub fn sequential(mut self) -> Self {
        self.parallel = false;
        self
    }

    /// Writes rows that sort after every row written so far.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        if batch.schema().fields() != self.schema.fields() {
            return Err(ParquetError::General("batch schema differs from the file's".into()));
        }
        self.count_nans(batch);
        // With a byte cap, encode in slices small enough to notice when the group is full.
        let step = if self.max_group_bytes.is_some() { BYTE_CHECK_ROWS } else { usize::MAX };
        let mut offset = 0;
        while offset < batch.num_rows() {
            let n = (self.row_group_rows - self.rows_in_group).min(batch.num_rows() - offset).min(step);
            self.encode(&batch.slice(offset, n))?;
            offset += n;
            let full = self.max_group_bytes.is_some_and(|max| self.group_memory() >= max);
            if self.rows_in_group == self.row_group_rows || full {
                self.flush_row_group()?;
            }
        }
        Ok(())
    }

    /// The bytes encoded since the last call, to be uploaded in order.
    pub fn take(&mut self) -> Vec<u8> {
        self.take_chunks().concat()
    }

    /// Like [`FileWriter::take`], as the chunks written (no copy into one buffer).
    pub fn take_chunks(&mut self) -> Vec<Vec<u8>> {
        let chunks = self.buffer.take();
        self.taken += chunks.iter().map(Vec::len).sum::<usize>() as u64;
        chunks
    }

    /// Bytes the file has so far, including the open row group's estimated size.
    pub fn size(&self) -> u64 {
        let open: usize = self.columns.iter().flatten().map(ArrowColumnWriter::get_estimated_total_bytes).sum();
        self.writer.bytes_written() as u64 + open as u64
    }

    fn group_memory(&self) -> usize {
        self.columns.iter().flatten().map(ArrowColumnWriter::memory_size).sum()
    }

    /// Memory the writer holds: the open row group and bytes not yet taken.
    pub fn memory_size(&self) -> usize {
        let open: usize = self.columns.iter().flatten().map(ArrowColumnWriter::memory_size).sum();
        open + self.buffer.len()
    }

    /// Finishes the file: the last bytes to upload, and the file's statistics.
    pub fn finish(self) -> Result<(Vec<u8>, FileStats), ParquetError> {
        let (rest, stats) = self.finish_chunks()?;
        Ok((rest.concat(), stats))
    }

    /// Like [`FileWriter::finish`], with the last bytes as the chunks written.
    pub fn finish_chunks(mut self) -> Result<(Vec<Vec<u8>>, FileStats), ParquetError> {
        if self.rows_in_group > 0 {
            self.flush_row_group()?;
        }
        let metadata = self.writer.finish()?;
        let rest = self.take_chunks();
        Ok((rest, FileStats::new(metadata, self.taken, std::mem::take(&mut self.nan_counts))))
    }

    fn encode(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        if self.columns.is_none() {
            let index = self.writer.flushed_row_groups().len();
            self.columns = Some(self.factory.create_column_writers(index)?);
        }
        let Some(columns) = self.columns.as_mut() else { return Ok(()) };
        let mut leaves: Vec<ArrowLeafColumn> = Vec::with_capacity(columns.len());
        for (field, array) in self.schema.fields().iter().zip(batch.columns()) {
            leaves.extend(compute_leaves(field, array)?);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if self.parallel && batch.num_rows() >= PARALLEL_MIN_ROWS && columns.len() > 1 {
            std::thread::scope(|s| {
                let handles: Vec<_> =
                    columns.iter_mut().zip(&leaves).map(|(w, leaf)| s.spawn(move || w.write(leaf))).collect();
                handles.into_iter().try_for_each(|h| h.join().unwrap_or_else(|_| Err(panicked())))
            })?;
            self.rows_in_group += batch.num_rows();
            return Ok(());
        }
        for (w, leaf) in columns.iter_mut().zip(&leaves) {
            w.write(leaf)?;
        }
        self.rows_in_group += batch.num_rows();
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<(), ParquetError> {
        let Some(columns) = self.columns.take() else { return Ok(()) };
        let chunks = self.close_columns(columns)?;
        let mut group = self.writer.next_row_group()?;
        for chunk in chunks {
            chunk.append_to_row_group(&mut group)?;
        }
        group.close()?;
        self.rows_in_group = 0;
        Ok(())
    }

    fn close_columns(&self, columns: Vec<ArrowColumnWriter>) -> Result<Vec<ArrowColumnChunk>, ParquetError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.parallel && columns.len() > 1 {
            return std::thread::scope(|s| {
                let handles: Vec<_> = columns.into_iter().map(|w| s.spawn(move || w.close())).collect();
                handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(panicked()))).collect()
            });
        }
        columns.into_iter().map(ArrowColumnWriter::close).collect()
    }

    fn count_nans(&mut self, batch: &RecordBatch) {
        for (i, column) in batch.columns().iter().enumerate() {
            let nans = nan_count(column);
            if let (Some(nans), Some(id)) = (nans, self.leaf_field_id(i)) {
                *self.nan_counts.entry(id).or_default() += nans;
            }
        }
    }

    /// The field id of top-level column `i`, if every column is a single leaf (so column `i` is
    /// leaf `i`) and it has one.
    fn leaf_field_id(&self, i: usize) -> Option<i32> {
        let flat = self.schema.fields().iter().all(|f| !f.data_type().is_nested());
        if flat { self.field_ids.get(i).copied().flatten() } else { None }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn panicked() -> ParquetError {
    ParquetError::General("a column encoder panicked".into())
}

/// NaNs in a float column; `None` for other types.
fn nan_count(column: &ArrayRef) -> Option<u64> {
    let count = |nans: usize| Some(nans as u64);
    match column.data_type() {
        DataType::Float64 => {
            let a = column.as_any().downcast_ref::<Float64Array>()?;
            count(a.iter().filter(|v| v.is_some_and(f64::is_nan)).count())
        }
        DataType::Float32 => {
            let a = column.as_any().downcast_ref::<Float32Array>()?;
            count(a.iter().filter(|v| v.is_some_and(f32::is_nan)).count())
        }
        _ => None,
    }
}

/// What Iceberg records about a written data file ([`ParquetStats`]: counts, bounds, split offsets),
/// with its footer, for readers that take footers from elsewhere (the head, spec 7).
#[derive(Clone, Debug)]
pub struct FileStats {
    /// The statistics.
    pub iceberg: ParquetStats,
    /// The footer.
    pub metadata: Arc<ParquetMetaData>,
}

impl FileStats {
    /// The statistics of a file of `file_size` bytes with footer `metadata`.
    pub fn new(metadata: ParquetMetaData, file_size: u64, nan_counts: BTreeMap<i32, u64>) -> Self {
        Self { iceberg: ParquetStats::new(&metadata, file_size, nan_counts), metadata: Arc::new(metadata) }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn field(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, true).with_metadata(HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]))
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            field("time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), 1),
            field("device", DataType::Int32, 2),
            field("value", DataType::Float64, 3),
            field("name", DataType::Utf8, 4),
        ]))
    }

    fn batch(from: i64, n: i64) -> RecordBatch {
        let time = TimestampMicrosecondArray::from_iter_values(from..from + n).with_timezone("UTC");
        let device = Int32Array::from_iter_values((from..from + n).map(|i| i32::try_from(i % 7).unwrap() - 3));
        let value: Float64Array =
            (from..from + n).map(|i| Some(if i % 5 == 0 { f64::NAN } else { i as f64 })).collect();
        let name: StringArray = (from..from + n).map(|i| (i % 3 != 0).then(|| format!("n{}", i % 11))).collect();
        RecordBatch::try_new(schema(), vec![Arc::new(time), Arc::new(device), Arc::new(value), Arc::new(name)]).unwrap()
    }

    fn props(rows: usize) -> WriterProperties {
        let contract = Contract::from_properties(&HashMap::from([
            ("timelake.time-column".to_string(), "time".to_string()),
            ("timelake.row-group-rows".to_string(), rows.to_string()),
        ]))
        .unwrap();
        writer_properties(&schema(), &contract, "test", DATA_PAGE_ROWS).unwrap()
    }

    fn write(parallel: bool, rows: usize) -> (Vec<u8>, FileStats) {
        let mut w = FileWriter::new(schema(), props(rows)).unwrap();
        if !parallel {
            w = w.sequential();
        }
        let mut bytes = vec![];
        for from in [0, 30_000, 60_000] {
            w.write(&batch(from, 30_000)).unwrap();
            bytes.extend(w.take());
        }
        let (rest, stats) = w.finish().unwrap();
        bytes.extend(rest);
        (bytes, stats)
    }

    #[test]
    fn files_read_back_with_their_statistics() {
        let (bytes, stats) = write(true, 40_000);
        assert_eq!((stats.iceberg.record_count, stats.iceberg.file_size), (90_000, bytes.len() as u64));
        assert_eq!(stats.iceberg.split_offsets.len(), 3, "row groups of 40,000 rows");
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap().build().unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 90_000);
        assert_eq!(stats.iceberg.lower_bounds[&1], 0i64.to_le_bytes());
        assert_eq!(stats.iceberg.upper_bounds[&1], 89_999i64.to_le_bytes());
        assert_eq!(
            (stats.iceberg.lower_bounds[&2].as_slice(), stats.iceberg.upper_bounds[&2].as_slice()),
            (&(-3i32).to_le_bytes()[..], &3i32.to_le_bytes()[..])
        );
        assert_eq!(stats.iceberg.nan_counts[&3], 18_000);
        assert_eq!(stats.iceberg.null_counts[&4], 30_000);
        assert_eq!(
            (stats.iceberg.lower_bounds[&4].as_slice(), stats.iceberg.upper_bounds[&4].as_slice()),
            (&b"n0"[..], &b"n9"[..])
        );
        assert_eq!(stats.iceberg.value_counts[&4], 90_000);
    }

    /// Parallel and sequential encoding write the same bytes.
    #[test]
    fn parallel_encoding_writes_the_same_file() {
        assert_eq!(write(true, 40_000).0, write(false, 40_000).0);
    }

    /// Floats get a dictionary falling back to byte-stream-split, or byte-stream-split alone.
    #[test]
    fn float_encodings() {
        let schema = Arc::new(Schema::new(vec![field("v", DataType::Float64, 1)]));
        let path = ColumnPath::from("v");
        for (value, dictionary) in [(None, true), (Some("byte-stream-split"), false)] {
            let properties: HashMap<String, String> =
                value.map(|v| (crate::property::FLOAT_ENCODING.to_string(), v.to_string())).into_iter().collect();
            let contract = Contract::from_properties(&properties).unwrap();
            let props = writer_properties(&schema, &contract, "test", DATA_PAGE_ROWS).unwrap();
            assert_eq!(props.dictionary_enabled(&path), dictionary, "{value:?}");
            assert_eq!(props.encoding(&path), Some(Encoding::BYTE_STREAM_SPLIT), "{value:?}");
        }
    }
}
