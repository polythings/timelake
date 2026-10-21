//! Maintenance for the shared write path (the WASM writer): retention, compaction by streaming
//! merge (spec 19), and snapshot expiry with file cleanup, planned as the engine's `maintain`
//! plans them ([`plan`]). Compaction reads each input a window of pages at a time through ranged
//! reads, so memory is a window per input and the row group being written; a cap on the bytes one
//! run rewrites keeps a run within a request's CPU time. Rollups are left to the engine.

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
use crate::commit::{Change, MAX_ATTEMPTS, backoff_millis, commit, put_metadata};
use crate::error::{WriteError, WriteResult};
use crate::footer;
use crate::host::Host;
use crate::iceberg::{DataFile, Entry, read_manifest, read_manifest_list};
use crate::layout::{hint_uri, metadata_prefix, metadata_uri};
use crate::merge::{Merge, Step};
use crate::metadata::SnapshotRef;
use crate::rows::{align_batch, stable_sort};
use crate::table::TableState;

mod plan;

pub use plan::*;

/// Rows per batch read from an input, and per batch the merge hands the writer: the WebAssembly
/// writer's, small because a merge holds a batch per input (measured: 1,024 and 8,192 rows peak at
/// 77 MiB merging 24 files of 50,000 TSBS rows, against 97 MiB with the engine's 4,096 and 65,536,
/// for 15% more time).
const READ_BATCH_ROWS: usize = 1024;
const MERGED_BATCH_ROWS: usize = 8 * 1024;
/// Compressed bytes of an input read per window (the pages covering a window's rows).
const WINDOW_BYTES: u64 = 128 << 10;

/// Runs maintenance: retention, then compaction, then snapshot expiry (not rollups: `opts.rollups`
/// is the engine's). Returns the new version.
pub async fn maintain<H: Host>(
    host: &H,
    table: TableState,
    opts: &MaintainOptions,
) -> WriteResult<(TableState, MaintainReport)> {
    let started = host.now_ms();
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
    report.duration = Duration::from_millis(u64::try_from(host.now_ms().saturating_sub(started)).unwrap_or(0));
    Ok((table, report))
}

/// Drops whole time partitions entirely older than the retention period.
async fn retention<H: Host>(host: &H, mut table: TableState, report: &mut MaintainReport) -> WriteResult<TableState> {
    let now_micros = host.now_ms().saturating_mul(1000);
    let files = table.live_files(host).await?.to_vec();
    let Some(plan) = retention_plan(&table.contract, now_micros, files.iter().map(|e| e.file.partition)) else {
        return Ok(table);
    };
    if plan.files.is_empty() {
        return Ok(table);
    }
    let removed: Vec<Entry> = plan.files.iter().map(|&i| files[i].clone()).collect();
    report.partitions_dropped += plan.partitions;
    report.files_dropped += removed.len();
    let change = Change::Delete { removed, lvi_cutoff: Some(plan.cutoff) };
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
    let plan = plan_compaction(host, &mut table, opts).await?;
    report.more_to_compact = plan.more_to_compact;
    if plan.groups.is_empty() {
        return Ok(table);
    }
    report.partitions_compacted += plan.groups.iter().map(|g| g.partition).collect::<HashSet<_>>().len();
    let files = table.live_files(host).await?.to_vec();
    let (mut removed, mut added, mut sequence_numbers, mut written) = (vec![], vec![], vec![], vec![]);
    let mut outcome = Ok(());
    for group in plan.groups {
        let files: Vec<Entry> = group.files.iter().map(|&i| files[i].clone()).collect();
        // The files are a run in commit order; the new files take the run's place in that order.
        let sequence_number = files.iter().map(|f| f.sequence_number).max().unwrap_or_default();
        match rewrite(host, &table, group.partition, &files, report).await {
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

/// Which live files to merge ([`compaction_plan`]); groups index [`TableState::live_files`].
async fn plan_compaction<H: Host>(
    host: &H,
    table: &mut TableState,
    opts: &MaintainOptions,
) -> WriteResult<CompactionPlan> {
    let time_id = time_field_id(table);
    let sort_order_id = Some(table.metadata.sort_order_id());
    let contract = table.contract.clone();
    let files: Vec<PlanFile<'_>> = table.live_files(host).await?.iter().map(|e| plan_file(e, time_id)).collect();
    Ok(compaction_plan(&contract, sort_order_id, &files, opts))
}

/// A live file as compaction planning sees it; `time_id` is the time column's field id.
fn plan_file(e: &Entry, time_id: Option<i32>) -> PlanFile<'_> {
    // Every timestamp bound is a long in Iceberg's encoding, whatever its unit.
    let newest_time = time_id.and_then(|id| match bounds::decode("timestamptz", e.file.upper_bounds.get(&id)?) {
        Some(Bound::Int(t)) => Some(t),
        _ => None,
    });
    PlanFile {
        path: &e.file.path,
        partition: e.file.partition,
        size: e.file.file_size,
        sequence_number: e.sequence_number,
        sort_order_id: e.file.sort_order_id,
        newest_time,
    }
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
    report.merged(files.len(), files.iter().map(|e| e.file.file_size).sum(), rows_in, rows_out);
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
    // Built again on the newer version when another writer publishes first, as a commit is.
    for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
            host.sleep(backoff_millis(attempt, host.random())).await;
            table = TableState::load(host, &table.location).await?;
        }
        let (expired_table, expired, kept) = match publish_expiry(host, &table).await? {
            Expiry::Nothing => return Ok(table),
            Expiry::Lost => continue,
            Expiry::Published { table, expired, kept } => (*table, expired, kept),
        };
        report.snapshots_expired += expired.len();
        let _ = host.put(&hint_uri(&expired_table.location), expired_table.version.to_string().into_bytes()).await;
        let gone = reachable(host, &expired, Missing::Skip).await?;
        let doomed: Vec<String> = gone.difference(&kept).cloned().collect();
        host.delete(&doomed).await?;
        report.objects_deleted += doomed.len();
        report.objects_deleted += prune_metadata_files(host, &expired_table).await?;
        let _ = expired_table.write_head(host).await;
        return Ok(expired_table);
    }
    Err(WriteError::Conflict(format!("{}: gave up after {MAX_ATTEMPTS} attempts", table.location)))
}

/// What one attempt at expiry did.
enum Expiry {
    /// No snapshot is due.
    Nothing,
    /// Another writer published this version first.
    Lost,
    /// The new version, the snapshots it expired, and every object the kept snapshots use.
    Published { table: Box<TableState>, expired: Vec<SnapshotRef>, kept: HashSet<String> },
}

/// Publishes `table` without the snapshots due to expire ([`expired_snapshots`]).
async fn publish_expiry<H: Host>(host: &H, table: &TableState) -> WriteResult<Expiry> {
    let snapshots = table.metadata.snapshot_list();
    let times: Vec<SnapshotTime> =
        snapshots.iter().map(|s| SnapshotTime { id: s.id, timestamp_ms: s.timestamp_ms }).collect();
    let (current, referenced) = (table.metadata.current_snapshot_id(), table.metadata.referenced_snapshots());
    let ids = expired_snapshots(&table.contract, &times, current, &referenced, host.now_ms());
    if ids.is_empty() {
        return Ok(Expiry::Nothing);
    }
    let (expired, remaining): (Vec<_>, Vec<_>) = snapshots.into_iter().partition(|s| ids.contains(&s.id));
    // What the kept snapshots use, read in full before anything changes: a file left out would be
    // deleted.
    let kept = reachable(host, &remaining, Missing::Error).await?;
    let previous = metadata_uri(&table.location, table.version);
    let metadata = table.metadata.without_snapshots(&ids, &previous, host.now_ms())?;
    // Loaded first: metadata this Timelake can't load is never committed.
    let next = TableState::from_metadata(&table.location, table.version + 1, metadata)?;
    let uri = metadata_uri(&table.location, next.version);
    if !put_metadata(host, &uri, next.metadata.to_json().into_bytes()).await? {
        return Ok(Expiry::Lost);
    }
    // Expiry keeps the current snapshot: its files, index and footers stay.
    let (files, last_values, footers) = (table.files.clone(), table.last_values.clone(), table.footers.clone());
    let table = Box::new(TableState { files, last_values, footers, ..next });
    Ok(Expiry::Published { table, expired, kept })
}

/// Every object (manifest lists, manifests, data files, last-value indexes) `snapshots` use.
async fn reachable<H: Host>(host: &H, snapshots: &[SnapshotRef], missing: Missing) -> WriteResult<HashSet<String>> {
    let absent =
        |snapshot: i64, uri: &str| missing.check(snapshot, uri).map_err(|e| WriteError::NotFound(e.to_string()));
    let mut out = HashSet::new();
    let mut manifests = BTreeMap::new();
    for s in snapshots {
        out.insert(s.manifest_list.clone());
        if let Some(index) = &s.last_value_index {
            out.insert(index.clone());
        }
        let Some(bytes) = host.get(&s.manifest_list).await? else {
            absent(s.id, &s.manifest_list)?;
            continue;
        };
        for m in read_manifest_list(&bytes)? {
            out.insert(m.path.clone());
            manifests.entry(m.path.clone()).or_insert((s.id, m));
        }
    }
    for (snapshot, m) in manifests.values() {
        let Some(bytes) = host.get(&m.path).await? else {
            absent(*snapshot, &m.path)?;
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

/// Deletes the metadata files older than the metadata log ([`pruned_metadata`]).
async fn prune_metadata_files<H: Host>(host: &H, table: &TableState) -> WriteResult<usize> {
    let listed = host.list(&metadata_prefix(&table.location)).await?.into_iter().map(|o| o.uri);
    let doomed = pruned_metadata(listed, table.version, table.metadata.metadata_log_len());
    host.delete(&doomed).await?;
    Ok(doomed.len())
}
