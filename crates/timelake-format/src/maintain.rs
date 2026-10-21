//! Maintenance for the shared write path (the WASM writer): retention, compaction by streaming
//! merge (spec 19), and snapshot expiry with file cleanup, as the engine's `maintain` does them.
//! Compaction reads each input one row group at a time through ranged reads, so memory is a row
//! group per input and the row group being written; a cap on the bytes one run rewrites keeps a
//! run within a request's CPU time.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};

use crate::append::PartitionFiles;
use crate::bounds::{self, Bound};
use crate::commit::{Change, commit, put_metadata};
use crate::duration_micros;
use crate::error::{WriteError, WriteResult};
use crate::footer;
use crate::host::Host;
use crate::iceberg::{DataFile, Entry, read_manifest, read_manifest_list};
use crate::layout::{hint_uri, metadata_prefix, metadata_uri, metadata_version};
use crate::merge::{Merge, Step};
use crate::rows::{align_batch, stable_sort};
use crate::table::TableState;

/// Files smaller than this fraction of the target size count as small.
const SMALL_FILE_NUMERATOR: u64 = 3;
const SMALL_FILE_DENOMINATOR: u64 = 4;
/// Size tiers of small files: tier `n` holds files of `2^16 × 4^n` bytes and up.
const TIER_BASE_SHIFT: u32 = 16;
const TIER_GROWTH: u64 = 4;
/// A partition holding at most this many target sizes is rewritten whole.
const REWRITE_WHOLE_PARTITION_TARGETS: u64 = 2;
/// Rows per batch read from an input, and per batch the merge hands the writer. Small, because a
/// merge holds a batch per input (measured: 1,024 and 8,192 rows peak at 77 MiB merging 24 files
/// of 50,000 TSBS rows, against 97 MiB with 4,096 and 65,536, for 15% more time).
const READ_BATCH_ROWS: usize = 1024;
const MERGED_BATCH_ROWS: usize = 8 * 1024;
/// The most files one merge reads at once: a merge holds a window of each, so this bounds its
/// memory. A larger group is merged in parts, which later runs merge again.
const MAX_MERGE_INPUTS: usize = 24;
/// Compressed bytes of an input read per window (the pages covering a window's rows).
const WINDOW_BYTES: u64 = 128 << 10;
/// Inline maintenance expires snapshots in batches of about this many.
const EXPIRE_BATCH: usize = 20;

/// Options for [`maintain`]. The default does everything, rewriting each partition into sorted
/// files of the target size.
#[derive(Clone, Debug)]
pub struct MaintainOptions {
    /// Merge small or unsorted files.
    pub compact: bool,
    /// Drop partitions older than the table's retention.
    pub retention: bool,
    /// Expire old snapshots and delete the files only they used.
    pub expire: bool,
    /// Compact a partition once it has this many small files.
    pub min_small_files: usize,
    /// Only consider these partitions (`None`: all).
    pub partitions: Option<HashSet<Option<i32>>>,
    /// Merge size tiers of small files instead of rewriting whole partitions.
    pub tiered: bool,
    /// In tiered mode, rewrite partitions this far behind the newest data whole.
    pub cold_after: Option<Duration>,
    /// Stop compacting once the inputs of this run reach this many bytes (at least one group of
    /// files is compacted); the next run continues.
    pub max_bytes: Option<u64>,
}

impl Default for MaintainOptions {
    fn default() -> Self {
        Self {
            compact: true,
            retention: true,
            expire: true,
            min_small_files: 2,
            partitions: None,
            tiered: false,
            cold_after: None,
            max_bytes: None,
        }
    }
}

impl MaintainOptions {
    /// What maintenance runs inline after an append that touched `partitions`: size tiers of those
    /// partitions and cold ones, retention, and expiry in batches (the engine's choices).
    pub fn inline(table: &TableState, partitions: HashSet<Option<i32>>, now_ms: i64) -> Self {
        let c = &table.contract;
        let snapshots = table.metadata.snapshot_list();
        let oldest = snapshots.iter().map(|s| s.timestamp_ms).min().unwrap_or(i64::MAX);
        let cutoff = c.snapshot_cutoff_ms(now_ms);
        let n = snapshots.len();
        Self {
            compact: true,
            retention: c.retention.is_some(),
            expire: n >= c.max_snapshots.saturating_add(EXPIRE_BATCH)
                || (n >= c.min_snapshots.saturating_add(EXPIRE_BATCH) && oldest < cutoff),
            min_small_files: c.compact_min_files,
            partitions: Some(partitions),
            tiered: true,
            cold_after: c.compact_cold_after,
            max_bytes: None,
        }
    }
}

/// What [`maintain`] did.
#[derive(Clone, Debug, Default)]
pub struct MaintainReport {
    /// Partitions compacted.
    pub partitions_compacted: usize,
    /// Data files replaced by compaction.
    pub files_rewritten: usize,
    /// Data files compaction wrote.
    pub files_written: usize,
    /// Rows read by compaction.
    pub rows_rewritten: u64,
    /// Bytes of the files compaction read.
    pub bytes_rewritten: u64,
    /// Duplicate `(series, time)` rows compaction removed.
    pub duplicates_removed: u64,
    /// Compaction stopped at `max_bytes` with more left to do.
    pub more_to_compact: bool,
    /// Partitions dropped by retention.
    pub partitions_dropped: usize,
    /// Data files dropped by retention.
    pub files_dropped: usize,
    /// Snapshots expired.
    pub snapshots_expired: usize,
    /// Objects deleted after expiry.
    pub objects_deleted: usize,
}

impl MaintainReport {
    /// Whether nothing changed.
    pub fn is_noop(&self) -> bool {
        self.files_rewritten == 0 && self.files_dropped == 0 && self.snapshots_expired == 0 && self.objects_deleted == 0
    }
}

/// Runs maintenance: retention, then compaction, then snapshot expiry. Returns the new version.
pub async fn maintain<H: Host>(
    host: &H,
    table: TableState,
    opts: &MaintainOptions,
) -> WriteResult<(TableState, MaintainReport)> {
    let mut report = MaintainReport::default();
    let mut table = table;
    if opts.retention {
        table = retention(host, table, &mut report).await?;
    }
    if opts.compact {
        table = compact(host, table, opts, &mut report).await?;
    }
    if opts.expire {
        table = expire(host, table, &mut report).await?;
    }
    Ok((table, report))
}

/// Drops whole time partitions entirely older than the retention period.
async fn retention<H: Host>(host: &H, mut table: TableState, report: &mut MaintainReport) -> WriteResult<TableState> {
    let now_micros = host.now_ms().saturating_mul(1000);
    let (Some(cutoff), Some(chunk)) = (table.contract.retention_cutoff(now_micros), table.contract.chunk) else {
        return Ok(table);
    };
    let expired = |p: i32| chunk.end_micros(p).is_ok_and(|end| end <= cutoff);
    let removed: Vec<Entry> =
        table.live_files(host).await?.iter().filter(|e| e.file.partition.is_some_and(expired)).cloned().collect();
    if removed.is_empty() {
        return Ok(table);
    }
    report.partitions_dropped += removed.iter().map(|e| e.file.partition).collect::<HashSet<_>>().len();
    report.files_dropped += removed.len();
    let change = Change::Delete { removed, lvi_cutoff: Some(cutoff) };
    Ok(commit(host, table, &change, &HashMap::<String, String>::new()).await?.0)
}

/// Rewrites groups of small (or unsorted) files into sorted files of the target size, removing
/// duplicate `(series, time)` rows (latest write wins) when dedup is on.
async fn compact<H: Host>(
    host: &H,
    mut table: TableState,
    opts: &MaintainOptions,
    report: &mut MaintainReport,
) -> WriteResult<TableState> {
    let plan = compaction_plan(host, &mut table, opts).await?;
    // Groups larger than a merge takes are split into balanced parts, consecutive in commit order.
    let mut plan: Vec<(Option<i32>, Vec<Entry>)> = plan
        .into_iter()
        .flat_map(|(p, files)| {
            let size = files.len().div_ceil(files.len().div_ceil(MAX_MERGE_INPUTS));
            files.chunks(size.max(1)).map(|c| (p, c.to_vec())).collect::<Vec<_>>()
        })
        .collect();
    if let Some(max) = opts.max_bytes {
        let mut total = 0;
        let keep = plan
            .iter()
            .take_while(|(_, files)| {
                let first = total == 0;
                total += files.iter().map(|e| e.file.file_size).sum::<u64>();
                first || total <= max
            })
            .count();
        report.more_to_compact = keep < plan.len();
        plan.truncate(keep);
    }
    if plan.is_empty() {
        return Ok(table);
    }
    let (mut removed, mut added, mut sequence_numbers, mut written) = (vec![], vec![], vec![], vec![]);
    let mut outcome = Ok(());
    for (partition, files) in plan {
        // The files are a run in commit order; the new files take the run's place in that order.
        let sequence_number = files.iter().map(|f| f.sequence_number).max().unwrap_or_default();
        match rewrite(host, &table, partition, &files, report).await {
            Ok(out) => {
                for (file, footer) in out {
                    written.push((file.path.clone(), footer));
                    sequence_numbers.push(sequence_number);
                    added.push(file);
                }
                removed.extend(files);
            }
            Err(e) => {
                outcome = Err(e);
                break;
            }
        }
    }
    report.files_written += added.len();
    let paths: Vec<String> = written.iter().map(|(p, _)| p.clone()).collect();
    let committed = match outcome {
        Ok(()) => {
            let change = Change::Replace { removed, added, sequence_numbers };
            commit(host, table, &change, &HashMap::<String, String>::new()).await
        }
        Err(e) => Err(e),
    };
    match committed {
        Ok((mut table, _)) => {
            table.footers.extend(written);
            let _ = table.write_head(host).await;
            Ok(table)
        }
        Err(e) => {
            // Unreferenced, unless the commit's outcome is unknown: a failed cleanup only wastes space.
            if !matches!(e, WriteError::CommitUnknown(_)) {
                let _ = host.delete(&paths).await;
            }
            Err(e)
        }
    }
}

/// The time column's field id in the table's schema.
fn time_field_id(table: &TableState) -> Option<i32> {
    let time = table.contract.time_column.as_ref()?;
    let field = table.schema.field_with_name(time).ok()?;
    field.metadata().get(crate::metadata::FIELD_ID_KEY)?.parse().ok()
}

/// Which files of which partitions to rewrite (the engine's plan). Each group is a run of files
/// next to each other in commit order.
async fn compaction_plan<H: Host>(
    host: &H,
    table: &mut TableState,
    opts: &MaintainOptions,
) -> WriteResult<Vec<(Option<i32>, Vec<Entry>)>> {
    let c = table.contract.clone();
    let small = c.target_file_size / SMALL_FILE_DENOMINATOR * SMALL_FILE_NUMERATOR;
    let is_small = |e: &Entry| e.file.file_size < small;
    let sort_order_id = Some(table.metadata.sort_order_id());
    let sorted_table = !c.sort_columns().is_empty();
    let min_files = opts.min_small_files.max(2);
    let time_id = time_field_id(table);
    let files = table.live_files(host).await?.to_vec();

    let cold: HashSet<Option<i32>> = match (opts.tiered, opts.cold_after, c.chunk, time_id) {
        (true, Some(cold_after), Some(chunk), Some(id)) => {
            // Every timestamp bound is a long in Iceberg's encoding, whatever its unit.
            let newest = files
                .iter()
                .filter_map(|f| match bounds::decode("timestamptz", f.file.upper_bounds.get(&id)?) {
                    Some(Bound::Int(t)) => Some(t),
                    _ => None,
                })
                .max();
            newest.map_or_else(HashSet::new, |newest| {
                let cutoff = newest.saturating_sub(duration_micros(cold_after));
                let ended = |p: i32| chunk.end_micros(p).is_ok_and(|end| end <= cutoff);
                files.iter().filter_map(|f| f.file.partition).filter(|&p| ended(p)).map(Some).collect()
            })
        }
        _ => HashSet::new(),
    };
    let mut by_partition: BTreeMap<Option<i32>, Vec<Entry>> = BTreeMap::new();
    for e in files {
        let p = e.file.partition;
        if opts.partitions.as_ref().is_none_or(|ps| ps.contains(&p)) || cold.contains(&p) {
            by_partition.entry(p).or_default().push(e);
        }
    }
    let mut plan = vec![];
    for (partition, mut files) in by_partition {
        files.sort_by(|a, b| (a.sequence_number, a.file.path.as_str()).cmp(&(b.sequence_number, b.file.path.as_str())));
        let is_cold = cold.contains(&partition);
        let min_files = if is_cold { 2 } else { min_files };
        if opts.tiered && !is_cold {
            let tier = |e: &Entry| is_small(e).then(|| (e.file.file_size >> TIER_BASE_SHIFT).max(1).ilog(TIER_GROWTH));
            let runs = files.chunk_by(|a, b| tier(a) == tier(b));
            plan.extend(
                runs.filter(|run| tier(&run[0]).is_some() && run.len() >= min_files)
                    .map(|run| (partition, run.to_vec())),
            );
            continue;
        }
        let n_small = files.iter().filter(|e| is_small(e)).count();
        let unsorted = sorted_table && files.iter().any(|e| e.file.sort_order_id != sort_order_id);
        if n_small < min_files && !(unsorted && min_files <= 2) {
            continue;
        }
        let total: u64 = files.iter().map(|e| e.file.file_size).sum();
        if total <= REWRITE_WHOLE_PARTITION_TARGETS.saturating_mul(c.target_file_size) || unsorted {
            plan.push((partition, files));
            continue;
        }
        let runs = files.chunk_by(|a, b| is_small(a) == is_small(b)).filter(|run| is_small(&run[0]) && run.len() >= 2);
        plan.extend(runs.map(|run| (partition, run.to_vec())));
    }
    Ok(plan)
}

/// Merges the rows of `files` (oldest commit first) into sorted, deduplicated new files.
async fn rewrite<H: Host>(
    host: &H,
    table: &TableState,
    partition: Option<i32>,
    files: &[Entry],
    report: &mut MaintainReport,
) -> WriteResult<Vec<(DataFile, Vec<u8>)>> {
    let c = &table.contract;
    let order = c.write_order();
    let unique = if c.dedup { c.sort_columns().len() } else { 0 };
    let sort_order_id = Some(table.metadata.sort_order_id());
    let mut inputs = Vec::with_capacity(files.len());
    for e in files {
        let footer = match table.footers.get(&e.file.path) {
            Some(bytes) => Arc::new(footer::decode(&Bytes::copy_from_slice(bytes), PageIndexPolicy::Skip)?),
            None => read_footer(host, &e.file.path, e.file.file_size).await?,
        };
        let sorted = order.is_empty() || e.file.sort_order_id == sort_order_id;
        inputs.push(Input::new(e.file.path.clone(), e.file.file_size, footer, sorted));
    }
    let keys: Vec<&str> = order.iter().map(String::as_str).collect();
    let mut merge = Merge::new(table.schema.clone(), &keys, unique, files.len(), MERGED_BATCH_ROWS)?;
    let mut out = PartitionFiles::new(host, table, partition)?;
    let merged = async {
        loop {
            match merge.step()? {
                Step::Need(i) => {
                    let batch = inputs[i].next(host, table, &order).await?;
                    merge.push(i, batch)?;
                }
                Step::Output(batch) => out.write(&batch).await?,
                Step::Done => return Ok(()),
            }
        }
    }
    .await;
    if let Err(e) = merged {
        out.abort().await;
        return Err(e);
    }
    let (rows_in, rows_out, _) = merge.stats();
    report.duplicates_removed += rows_in - rows_out;
    report.rows_rewritten += rows_in;
    report.bytes_rewritten += files.iter().map(|e| e.file.file_size).sum::<u64>();
    report.partitions_compacted += 1;
    report.files_rewritten += files.len();
    out.finish().await
}

/// One input of a compaction, read a window of rows at a time: with an offset index (Timelake
/// writes one), only the pages covering the window, one request per column; without, a row group.
struct Input {
    path: String,
    size: u64,
    metadata: Arc<ParquetMetaData>,
    /// Sorted in the table's order (else read whole and sorted first).
    sorted: bool,
    /// The row group being read, and its next row.
    group: usize,
    row: usize,
    /// Dictionary pages of the current row group, fetched once for all its windows.
    dictionaries: Vec<(u64, Bytes)>,
    batches: Batches,
}

impl Input {
    fn new(path: String, size: u64, metadata: Arc<ParquetMetaData>, sorted: bool) -> Self {
        let batches: Batches = Box::new(std::iter::empty());
        Self { path, size, metadata, sorted, group: 0, row: 0, dictionaries: vec![], batches }
    }

    /// The next batch, aligned to the table schema; `None` at the end.
    async fn next<H: Host>(
        &mut self,
        host: &H,
        table: &TableState,
        order: &[String],
    ) -> WriteResult<Option<RecordBatch>> {
        loop {
            if let Some(batch) = self.batches.next() {
                return Ok(Some(align_batch(&batch?, &table.schema)?));
            }
            let groups = self.metadata.num_row_groups();
            if self.group >= groups {
                return Ok(None);
            }
            if !self.sorted {
                // Another engine's file: read it whole and sort it on its own.
                let all = read_row_groups(host, &self.path, self.size, &self.metadata, 0..groups).await?;
                let aligned = all.map(|b| align_batch(&b?, &table.schema)).collect::<WriteResult<Vec<_>>>()?;
                let sorted = stable_sort(&concat_batches(&table.schema, &aligned)?, order)?;
                let n = sorted.num_rows();
                let slices: Vec<_> =
                    (0..n).step_by(READ_BATCH_ROWS).map(|i| sorted.slice(i, READ_BATCH_ROWS.min(n - i))).collect();
                self.batches = Box::new(slices.into_iter().map(Ok));
                self.group = groups;
                continue;
            }
            let rows = usize::try_from(self.metadata.row_group(self.group).num_rows()).unwrap_or(0);
            if self.row >= rows {
                (self.group, self.row) = (self.group + 1, 0);
                self.dictionaries.clear();
                continue;
            }
            if self.metadata.offset_index().is_none() {
                let group = self.group..self.group + 1;
                self.batches = read_row_groups(host, &self.path, self.size, &self.metadata, group).await?;
                self.row = rows;
                continue;
            }
            let end = (self.row + self.window_rows()).min(rows);
            self.batches = self.read_window(host, self.row, end).await?;
            self.row = end;
        }
    }

    /// Rows per window: about `WINDOW_BYTES` of the row group's compressed data.
    fn window_rows(&self) -> usize {
        let g = self.metadata.row_group(self.group);
        let (rows, bytes) = (g.num_rows().unsigned_abs(), g.compressed_size().unsigned_abs().max(1));
        usize::try_from(rows.saturating_mul(WINDOW_BYTES) / bytes).unwrap_or(usize::MAX).max(READ_BATCH_ROWS)
    }

    /// Rows `start..end` of the current row group, from the pages that cover them.
    async fn read_window<H: Host>(&mut self, host: &H, start: usize, end: usize) -> WriteResult<Batches> {
        let group = self.metadata.row_group(self.group);
        let rows = group.num_rows().unsigned_abs();
        let Some(index) = self.metadata.offset_index().and_then(|i| i.get(self.group)) else {
            return Err(crate::error::WriteError::Corrupt("offset index without the row group".into()));
        };
        let (mut ranges, mut dictionaries) = (vec![], vec![]);
        for (column, offsets) in group.columns().iter().zip(index) {
            let pages = offsets.page_locations();
            let covering: Vec<_> = pages
                .iter()
                .enumerate()
                .filter(|(i, p)| {
                    let next = pages.get(i + 1).map_or(rows, |n| n.first_row_index.unsigned_abs());
                    p.first_row_index.unsigned_abs() < end as u64 && next > start as u64
                })
                .map(|(_, p)| p)
                .collect();
            if let (Some(first), Some(last)) = (covering.first(), covering.last()) {
                let from = first.offset.unsigned_abs();
                ranges.push((
                    from,
                    last.offset.unsigned_abs() + u64::from(last.compressed_page_size.unsigned_abs()) - from,
                ));
            }
            let chunk_start = column.byte_range().0;
            if self.dictionaries.is_empty()
                && let Some(first) = pages.first()
                && first.offset.unsigned_abs() > chunk_start
            {
                dictionaries.push((chunk_start, first.offset.unsigned_abs() - chunk_start));
            }
        }
        let path = self.path.as_str();
        let fetch = |(from, len): (u64, u64)| async move {
            Ok::<_, crate::error::WriteError>((from, Bytes::from(host.get_range(path, from, len).await?)))
        };
        let (pages, new_dictionaries) = futures_util::future::join(
            futures_util::future::try_join_all(ranges.into_iter().map(fetch)),
            futures_util::future::try_join_all(dictionaries.into_iter().map(fetch)),
        )
        .await;
        self.dictionaries.extend(new_dictionaries?);
        let mut parts = pages?;
        parts.extend(self.dictionaries.iter().cloned());
        let reader = Sparse { parts, file_len: self.size };
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let metadata = ArrowReaderMetadata::try_new(Arc::clone(&self.metadata), options)?;
        let selection = RowSelection::from(vec![RowSelector::skip(start), RowSelector::select(end - start)]);
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(reader, metadata)
            .with_row_groups(vec![self.group])
            .with_row_selection(selection)
            .with_batch_size(READ_BATCH_ROWS)
            .build()?;
        Ok(Box::new(reader))
    }
}

/// Byte ranges of a file that were fetched, as a Parquet chunk reader: reads must fall in one.
struct Sparse {
    parts: Vec<(u64, Bytes)>,
    file_len: u64,
}

impl Length for Sparse {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl ChunkReader for Sparse {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let (offset, bytes) = self.part(start, 0)?;
        let from = usize::try_from(start - offset).unwrap_or(0);
        Ok(bytes::Buf::reader(bytes.slice(from..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        let (offset, bytes) = self.part(start, length)?;
        let from = usize::try_from(start - offset).unwrap_or(0);
        Ok(bytes.slice(from..from + length))
    }
}

impl Sparse {
    fn part(&self, start: u64, length: usize) -> Result<(u64, &Bytes), ParquetError> {
        self.parts
            .iter()
            .find(|(offset, bytes)| *offset <= start && start + length as u64 <= *offset + bytes.len() as u64)
            .map(|(offset, bytes)| (*offset, bytes))
            .ok_or_else(|| ParquetError::General(format!("bytes {start}+{length} were not fetched")))
    }
}

/// The batches of row groups `groups` of the file at `path`, read with one ranged request.
async fn read_row_groups<H: Host>(
    host: &H,
    path: &str,
    file_len: u64,
    metadata: &Arc<ParquetMetaData>,
    groups: std::ops::Range<usize>,
) -> WriteResult<Batches> {
    let ranges = groups.clone().flat_map(|g| {
        metadata.row_group(g).columns().iter().map(parquet::file::metadata::ColumnChunkMetaData::byte_range)
    });
    let (start, end) = ranges.fold((u64::MAX, 0), |(s, e), (off, len)| (s.min(off), e.max(off + len)));
    if start >= end {
        return Ok(Box::new(std::iter::empty()));
    }
    let bytes = Bytes::from(host.get_range(path, start, end - start).await?);
    let reader = Sparse { parts: vec![(start, bytes)], file_len };
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
    let reader_metadata = ArrowReaderMetadata::try_new(Arc::clone(metadata), options)?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(reader, reader_metadata)
        .with_row_groups(groups.collect())
        .with_batch_size(READ_BATCH_ROWS)
        .build()?;
    // Decoded a batch at a time as the merge asks.
    Ok(Box::new(reader))
}

/// Batches decoded on demand.
type Batches = Box<dyn Iterator<Item = Result<RecordBatch, arrow_schema::ArrowError>>>;

/// Reads the footer of the file at `path` of `size` bytes (one request for typical files).
async fn read_footer<H: Host>(host: &H, path: &str, size: u64) -> WriteResult<Arc<ParquetMetaData>> {
    let mut want = footer::PREFETCH.min(size);
    loop {
        let tail = Bytes::from(host.get_range(path, size - want, want).await?);
        let mut reader = ParquetMetaDataReader::new()
            .with_column_index_policy(PageIndexPolicy::Skip)
            .with_offset_index_policy(PageIndexPolicy::Optional);
        match reader.try_parse_sized(&tail, size) {
            Ok(()) => return Ok(Arc::new(reader.finish()?)),
            Err(ParquetError::NeedMoreData(n)) if (n as u64) > want && (n as u64) <= size => want = n as u64,
            Err(e) => return Err(e.into()),
        }
    }
}

// --- expiry ----------------------------------------------------------------------------------

/// Expires old snapshots and deletes the files only they referenced.
async fn expire<H: Host>(host: &H, table: TableState, report: &mut MaintainReport) -> WriteResult<TableState> {
    let mut table = table;
    loop {
        let mut snapshots = table.metadata.snapshot_list();
        snapshots.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms));
        let expired = expired_snapshots(&table, &snapshots, host.now_ms());
        if expired.is_empty() {
            return Ok(table);
        }
        let ids: Vec<i64> = expired.iter().map(|s| s.id).collect();
        // What the kept snapshots use, read in full before anything changes: a file left out
        // would be deleted.
        let remaining: Vec<_> = snapshots.iter().filter(|s| !ids.contains(&s.id)).cloned().collect();
        let kept = reachable(host, &remaining, Missing::Error).await?;
        let previous = metadata_uri(&table.location, table.version);
        let metadata = table.metadata.without_snapshots(&ids, &previous, host.now_ms())?;
        let version = table.version + 1;
        if !put_metadata(host, &metadata_uri(&table.location, version), metadata.to_json().into_bytes()).await? {
            table = TableState::load(host, &table.location).await?;
            continue;
        }
        report.snapshots_expired += expired.len();
        let mut expired_table = table.clone();
        expired_table.version = version;
        expired_table.metadata = metadata;
        let _ = host.put(&hint_uri(&table.location), version.to_string().into_bytes()).await;
        let gone = reachable(host, &expired, Missing::Skip).await?;
        let doomed: Vec<String> = gone.difference(&kept).cloned().collect();
        host.delete(&doomed).await?;
        report.objects_deleted += doomed.len();
        report.objects_deleted += prune_metadata_files(host, &expired_table).await?;
        let _ = expired_table.write_head(host).await;
        return Ok(expired_table);
    }
}

/// Snapshots (newest first) to expire: past the newest `min_snapshots`, and either older than the
/// snapshot retention or beyond `max_snapshots`. The current snapshot, and any a branch or tag
/// points at (another engine's), always stay.
fn expired_snapshots(
    table: &TableState,
    newest_first: &[crate::metadata::SnapshotRef],
    now_ms: i64,
) -> Vec<crate::metadata::SnapshotRef> {
    let c = &table.contract;
    if c.keep_snapshots_until.is_some_and(|until| now_ms < until) {
        return vec![];
    }
    let current = table.metadata.current_snapshot_id();
    let referenced = table.metadata.referenced_snapshots();
    let cutoff = c.snapshot_cutoff_ms(now_ms);
    newest_first
        .iter()
        .enumerate()
        .filter(|(i, s)| {
            Some(s.id) != current
                && !referenced.contains(&s.id)
                && *i >= c.min_snapshots
                && (s.timestamp_ms < cutoff || *i >= c.max_snapshots)
        })
        .map(|(_, s)| s.clone())
        .collect()
}

/// What [`reachable`] does with a manifest list or manifest that isn't there.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Missing {
    /// Already deleted by an earlier, interrupted expiry: nothing more to find.
    Skip,
    /// A snapshot that is kept can't be read: an error, so that nothing is deleted.
    Error,
}

/// Every object (manifest lists, manifests, data files, last-value indexes) `snapshots` use.
async fn reachable<H: Host>(
    host: &H,
    snapshots: &[crate::metadata::SnapshotRef],
    missing: Missing,
) -> WriteResult<HashSet<String>> {
    let absent = |snapshot: i64, uri: &str| {
        WriteError::NotFound(format!(
            "{uri}, used by snapshot {snapshot}, is missing, so expiry deletes nothing; roll back past the snapshot or \
             restore the file"
        ))
    };
    let mut out = HashSet::new();
    let mut manifests = BTreeMap::new();
    for s in snapshots {
        out.insert(s.manifest_list.clone());
        if let Some(index) = &s.last_value_index {
            out.insert(index.clone());
        }
        let Some(bytes) = host.get(&s.manifest_list).await? else {
            if missing == Missing::Error {
                return Err(absent(s.id, &s.manifest_list));
            }
            continue;
        };
        for m in read_manifest_list(&bytes)? {
            out.insert(m.path.clone());
            manifests.entry(m.path.clone()).or_insert((s.id, m));
        }
    }
    for (snapshot, m) in manifests.values() {
        let Some(bytes) = host.get(&m.path).await? else {
            if missing == Missing::Error {
                return Err(absent(*snapshot, &m.path));
            }
            continue;
        };
        for e in read_manifest(m, &bytes)? {
            // Entries marked deleted only record a removal.
            if e.status != crate::iceberg::Status::Deleted {
                out.insert(e.file.path.clone());
            }
        }
    }
    Ok(out)
}

/// Keeps the newest metadata files (those in the metadata log plus the current one).
async fn prune_metadata_files<H: Host>(host: &H, table: &TableState) -> WriteResult<usize> {
    let keep_from = table.version.saturating_sub(table.metadata.metadata_log_len() as u64 + 1);
    let doomed: Vec<String> = host
        .list(&metadata_prefix(&table.location))
        .await?
        .into_iter()
        .map(|o| o.uri)
        .filter(|uri| metadata_version(uri).is_some_and(|v| v < keep_from))
        .collect();
    host.delete(&doomed).await?;
    Ok(doomed.len())
}
