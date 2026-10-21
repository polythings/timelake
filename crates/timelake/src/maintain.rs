//! Maintenance: retention, compaction with dedup, and snapshot expiry with file cleanup. This
//! replaces the "custom Glue jobs" (spec 20): appends run it inline as thresholds are crossed,
//! or [`Table::maintain`] runs it as one stateless command.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use futures::{StreamExt, TryStreamExt};
use iceberg::spec::{DataFile, Datum, ManifestStatus, PrimitiveLiteral, SnapshotRef, TableMetadata};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::file::properties::WriterProperties;
use timelake_format::duration_micros;
use timelake_format::layout::{metadata_dir, metadata_version};
use timelake_format::merge::{Merge, Step};
use tokio::sync::mpsc;

use crate::commit::{Change, commit, publish_retrying};
use crate::error::{Error, Result};
use crate::rollup::{RollupRefresh, refresh_all};
use crate::schema::align_batch;
use crate::sort::stable_sort;
use crate::storage::Uploads;
use crate::summary;
use crate::table::{LiveEntry, Table};
use crate::write::{PartitionWriter, file_partition, row_groups, writer_properties};

/// Files smaller than this fraction of the target size count as small (worth compacting).
const SMALL_FILE_NUMERATOR: u64 = 3;
const SMALL_FILE_DENOMINATOR: u64 = 4;
/// Tiered compaction groups small files by size: tier `n` holds files of
/// `TIER_BASE × TIER_GROWTH^n` bytes and up (64 KiB, 256 KiB, 1 MiB, …). The idea is Apache
/// Cassandra's size-tiered compaction strategy.
const TIER_BASE_SHIFT: u32 = 16;
const TIER_GROWTH: u64 = 4;
/// A partition whose files total at most this many target sizes is rewritten whole.
const REWRITE_WHOLE_PARTITION_TARGETS: u64 = 2;
/// Rows per batch when reading a data file back for compaction. Larger batches measured no faster
/// and used more memory.
const READ_BATCH_ROWS: usize = 4 * 1024;
/// Batches each input of a compaction reads ahead of the merge (decoding overlaps merging).
const READ_AHEAD: usize = 1;
/// Rows per batch the merge hands the writer.
const MERGED_BATCH_ROWS: usize = 64 * 1024;
/// How many manifest lists or manifests expiry reads at once.
const CONCURRENT_METADATA_READS: usize = 16;

/// Options for [`Table::maintain`]. The default does everything, rewriting each partition into
/// sorted files of the target size.
#[derive(Clone, Debug)]
pub struct MaintainOptions {
    /// Merge small or unsorted files.
    pub compact: bool,
    /// Drop partitions older than the table's retention.
    pub retention: bool,
    /// Expire old snapshots and delete the files only they used.
    pub expire: bool,
    /// Compact a partition once it has this many small files (default: 2, i.e. whenever useful).
    pub min_small_files: usize,
    /// Only consider these partitions (None = all).
    pub partitions: Option<HashSet<Option<i32>>>,
    /// Size-tiered mode (inline auto-maintenance): merge groups of similarly sized small files
    /// instead of rewriting whole partitions, so write amplification stays logarithmic while
    /// micro-batches stream in. `maintain` without it produces one sorted file per partition.
    pub tiered: bool,
    /// In tiered mode, partitions whose end is this much older than the newest data in the table
    /// are rewritten whole once they have two small files, touched by the append or not: data
    /// stops arriving there, and one sorted, duplicate-free file reads fastest.
    pub cold_after: Option<Duration>,
    /// Which of the table's rollups to bring up to date, after compaction (spec 27).
    pub rollups: RollupRefresh,
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
            rollups: RollupRefresh::Always,
        }
    }
}

/// What [`Table::maintain`] did.
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
    /// Duplicate `(series, time)` rows compaction removed.
    pub duplicates_removed: u64,
    /// Partitions dropped by retention.
    pub partitions_dropped: usize,
    /// Data files dropped by retention.
    pub files_dropped: usize,
    /// Snapshots expired.
    pub snapshots_expired: usize,
    /// Objects deleted after expiry (data files, manifests, metadata files).
    pub objects_deleted: usize,
    /// Rollups that took new rows.
    pub rollups_refreshed: usize,
    /// Rows appended to them.
    pub rollup_rows: u64,
    /// Time taken.
    pub duration: Duration,
}

impl MaintainReport {
    /// Whether nothing changed.
    pub fn is_noop(&self) -> bool {
        self.files_rewritten == 0
            && self.files_dropped == 0
            && self.snapshots_expired == 0
            && self.objects_deleted == 0
            && self.rollups_refreshed == 0
    }
}

impl Table {
    /// Runs maintenance: retention, then compaction, then refreshing rollups, then snapshot expiry
    /// and file cleanup. Returns the new table version.
    pub async fn maintain(&self, opts: MaintainOptions) -> Result<(Table, MaintainReport)> {
        let started = Instant::now();
        let mut report = MaintainReport::default();
        let mut table = self.clone();
        if opts.retention {
            table = retention(&table, &mut report).await?;
        }
        if opts.compact {
            table = compact(&table, &opts, &mut report).await?;
        }
        let refreshed = refresh_all(&table, opts.rollups).await?;
        (report.rollups_refreshed, report.rollup_rows) = (refreshed.rollups, refreshed.rows);
        if opts.expire {
            table = expire(&table, &mut report).await?;
        }
        report.duration = started.elapsed();
        Ok((table, report))
    }
}

fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

/// Drops whole time partitions that are entirely older than the retention period.
async fn retention(table: &Table, report: &mut MaintainReport) -> Result<Table> {
    let c = table.contract();
    let (Some(cutoff), Some(chunk)) = (c.retention_cutoff(now_micros()), c.chunk) else { return Ok(table.clone()) };
    let expired = |p: i32| chunk.end_micros(p).is_ok_and(|end| end <= cutoff);
    let removed: Vec<LiveEntry> =
        table.files().await?.into_iter().filter(|e| file_partition(&e.data_file).is_some_and(expired)).collect();
    if removed.is_empty() {
        return Ok(table.clone());
    }
    report.partitions_dropped += removed.iter().map(|e| file_partition(&e.data_file)).collect::<HashSet<_>>().len();
    report.files_dropped += removed.len();
    let change = Change::Delete { removed, lvi_cutoff: Some(cutoff) };
    let (table, _) = commit(table, change, HashMap::new(), Uploads::default()).await?;
    Ok(table)
}

/// Rewrites partitions with many small (or unsorted) files into sorted files of the target size,
/// removing duplicate `(series_key, time)` rows (latest write wins) when dedup is on.
async fn compact(table: &Table, opts: &MaintainOptions, report: &mut MaintainReport) -> Result<Table> {
    let plan = compaction_plan(table, opts).await?;
    if plan.is_empty() {
        return Ok(table.clone());
    }
    let props = writer_properties(table)?;
    let uploads = Uploads::default();
    let (mut removed, mut added, mut sequence_numbers) = (vec![], vec![], vec![]);
    let mut outcome = Ok(());
    for (partition, files) in plan {
        // The files are a run in commit order (older first, so "later row wins" means "latest write
        // wins"); the new files take the run's place in that order.
        let sequence_number = files.iter().map(|f| f.sequence_number).max().unwrap_or_default();
        match rewrite_partition(table, partition, &files, &props, &uploads, report).await {
            Ok(written) => {
                sequence_numbers.extend(std::iter::repeat_n(sequence_number, written.len()));
                added.extend(written);
                removed.extend(files);
            }
            Err(e) => {
                outcome = Err(e);
                break;
            }
        }
    }
    report.files_written += added.len();
    let paths: Vec<String> = added.iter().map(|f| f.file_path().to_string()).collect();
    let committed = match outcome {
        Ok(()) => commit(table, Change::Replace { removed, added, sequence_numbers }, HashMap::new(), uploads).await,
        Err(e) => {
            uploads.abort().await;
            Err(e)
        }
    };
    if committed.as_ref().is_err_and(|e| !matches!(e, Error::CommitUnknown(_))) {
        // Unreferenced: a failed cleanup only wastes space, and the original error matters more.
        let _ = table.lake().storage().delete_many(paths).await;
    }
    Ok(committed?.0)
}

/// Merges the rows of `files` (oldest commit first) into sorted, deduplicated new files, as
/// streams (spec 19): memory is a few batches per file and the row group being written,
/// however large the partition.
async fn rewrite_partition(
    table: &Table,
    partition: Option<i32>,
    files: &[LiveEntry],
    props: &WriterProperties,
    uploads: &Uploads,
    report: &mut MaintainReport,
) -> Result<Vec<DataFile>> {
    let contract = table.contract();
    let order = contract.write_order();
    let unique = if contract.dedup { contract.sort_columns().len() } else { 0 };
    let sort_order_id = i32::try_from(table.metadata().default_sort_order_id()).ok();
    let mut inputs: Vec<_> = files
        .iter()
        .map(|e| {
            let sorted = order.is_empty() || e.data_file.sort_order_id() == sort_order_id;
            read_ahead(table, &e.data_file, (!sorted).then(|| order.clone()))
        })
        .collect();
    let keys: Vec<&str> = order.iter().map(String::as_str).collect();
    let mut merge = Merge::new(table.schema(), &keys, unique, files.len(), MERGED_BATCH_ROWS)?;
    let mut writer = PartitionWriter::new(table, partition, props, uploads);
    let merged = async {
        loop {
            match merge.step()? {
                Step::Need(i) => {
                    let batch = inputs[i].recv().await.transpose()?;
                    merge.push(i, batch)?;
                }
                Step::Output(batch) => writer.write(&batch).await?,
                Step::Done => return Ok::<_, Error>(()),
            }
        }
    }
    .await;
    if let Err(e) = merged {
        writer.abort().await;
        return Err(e);
    }
    let (rows_in, rows_out, _) = merge.stats();
    report.duplicates_removed += rows_in - rows_out;
    report.rows_rewritten += rows_in;
    report.partitions_compacted += 1;
    report.files_rewritten += files.len();
    writer.finish().await
}

/// Which files of which partitions to rewrite. Each group is a run of files next to each other in
/// commit order: a file committed between two merged files may rewrite a key they hold, and must
/// stay newer than the merged file.
pub(crate) async fn compaction_plan(
    table: &Table,
    opts: &MaintainOptions,
) -> Result<Vec<(Option<i32>, Vec<LiveEntry>)>> {
    let c = table.contract();
    let small = c.target_file_size / SMALL_FILE_DENOMINATOR * SMALL_FILE_NUMERATOR;
    let is_small = |e: &LiveEntry| e.data_file.file_size_in_bytes() < small;
    let sort_order_id = i32::try_from(table.metadata().default_sort_order_id()).ok();
    let sorted_table = !c.sort_columns().is_empty();
    let min_files = opts.min_small_files.max(2);

    let files = table.files().await?;
    let cold = cold_partitions(table, &files, opts);
    let mut by_partition: BTreeMap<Option<i32>, Vec<LiveEntry>> = BTreeMap::new();
    for e in files {
        let p = file_partition(&e.data_file);
        if opts.partitions.as_ref().is_none_or(|ps| ps.contains(&p)) || cold.contains(&p) {
            by_partition.entry(p).or_default().push(e);
        }
    }
    let mut plan = vec![];
    for (partition, mut files) in by_partition {
        files.sort_by(|a, b| {
            (a.sequence_number, a.data_file.file_path()).cmp(&(b.sequence_number, b.data_file.file_path()))
        });
        let min_files = if cold.contains(&partition) { 2 } else { min_files };
        if opts.tiered && !cold.contains(&partition) {
            let tier = |e: &LiveEntry| {
                is_small(e).then(|| (e.data_file.file_size_in_bytes() >> TIER_BASE_SHIFT).max(1).ilog(TIER_GROWTH))
            };
            let runs = files.chunk_by(|a, b| tier(a) == tier(b));
            let runs = runs.filter(|run| tier(&run[0]).is_some() && run.len() >= min_files);
            plan.extend(runs.map(|run| (partition, run.to_vec())));
            continue;
        }
        let n_small = files.iter().filter(|e| is_small(e)).count();
        let unsorted = sorted_table && files.iter().any(|e| e.data_file.sort_order_id() != sort_order_id);
        if n_small < min_files && !(unsorted && min_files <= 2) {
            continue;
        }
        let total: u64 = files.iter().map(|e| e.data_file.file_size_in_bytes()).sum();
        if total <= REWRITE_WHOLE_PARTITION_TARGETS.saturating_mul(c.target_file_size) || unsorted {
            plan.push((partition, files));
            continue;
        }
        let runs = files.chunk_by(|a, b| is_small(a) == is_small(b)).filter(|run| is_small(&run[0]) && run.len() >= 2);
        plan.extend(runs.map(|run| (partition, run.to_vec())));
    }
    Ok(plan)
}

/// In tiered mode, the partitions whose end is `opts.cold_after` or more before the newest time in
/// the table (the files' upper bounds): data has moved on from them.
fn cold_partitions(table: &Table, files: &[LiveEntry], opts: &MaintainOptions) -> HashSet<Option<i32>> {
    let c = table.contract();
    let (true, Some(cold_after), Some(chunk), Some(time)) = (opts.tiered, opts.cold_after, c.chunk, &c.time_column)
    else {
        return HashSet::new();
    };
    let Some(id) = table.metadata().current_schema().field_by_name(time).map(|f| f.id) else { return HashSet::new() };
    let newest = files.iter().filter_map(|f| match f.data_file.upper_bounds().get(&id).map(Datum::literal) {
        Some(PrimitiveLiteral::Long(t)) => Some(*t),
        _ => None,
    });
    let Some(newest) = newest.max() else { return HashSet::new() };
    let cutoff = newest.saturating_sub(duration_micros(cold_after));
    let ended_before_cutoff = |p: i32| chunk.end_micros(p).is_ok_and(|end| end <= cutoff);
    files.iter().filter_map(|f| file_partition(&f.data_file)).filter(|&p| ended_before_cutoff(p)).map(Some).collect()
}

/// Reads a data file in a task of its own, a few batches ahead of the merge, aligned to the table
/// schema. A file not in the table's sort order is read whole and sorted by `sort` first.
fn read_ahead(table: &Table, df: &DataFile, sort: Option<Vec<String>>) -> mpsc::Receiver<Result<RecordBatch>> {
    let (tx, rx) = mpsc::channel(READ_AHEAD);
    let storage = table.lake().storage().clone();
    let (schema, uri, size) = (table.schema(), df.file_path().to_string(), df.file_size_in_bytes());
    tokio::spawn(async move {
        let read = async {
            let reader = ParquetObjectReader::new(storage.object_store(), storage.path(&uri)?).with_file_size(size);
            let stream =
                ParquetRecordBatchStreamBuilder::new(reader).await?.with_batch_size(READ_BATCH_ROWS).build()?;
            Ok::<_, Error>(stream.map(|b| align_batch(&b?, &schema)))
        };
        let mut stream = match read.await {
            Ok(s) => s.boxed(),
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        };
        if let Some(order) = sort {
            let sorted = async {
                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                // A whole file's sort is CPU work: off the async workers.
                let schema = Arc::clone(&schema);
                let sort = move || stable_sort(&concat_batches(&schema, &batches)?, &order);
                tokio::task::spawn_blocking(sort).await.map_err(std::io::Error::other)?
            };
            stream = match sorted.await {
                Ok(b) => futures::stream::iter(row_groups(b, READ_BATCH_ROWS).map(Ok).collect::<Vec<_>>()).boxed(),
                Err(e) => futures::stream::iter([Err(e)]).boxed(),
            };
        }
        while let Some(batch) = stream.next().await {
            // The merge stopped (an error elsewhere): stop reading.
            if tx.send(batch).await.is_err() {
                return;
            }
        }
    });
    rx
}

/// Expires old snapshots and deletes the files only they referenced.
async fn expire(table: &Table, report: &mut MaintainReport) -> Result<Table> {
    // Built again on the newer version when another writer publishes first.
    let (expired_table, Some((base, expired, kept))) = publish_retrying(table, |base| {
        Box::pin(async move {
            let mut snapshots: Vec<SnapshotRef> = base.metadata().snapshots().cloned().collect();
            snapshots.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms()));
            let expired = expired_snapshots(base, &snapshots)?;
            if expired.is_empty() {
                return Ok(None);
            }
            let ids: Vec<i64> = expired.iter().map(|s| s.snapshot_id()).collect();
            // What the kept snapshots use, read in full before anything changes: a file left out
            // would be deleted.
            let remaining: Vec<SnapshotRef> =
                snapshots.iter().filter(|s| !ids.contains(&s.snapshot_id())).cloned().collect();
            let kept = reachable(base, &remaining, Missing::Error).await?;
            let metadata = base.next_metadata().remove_snapshots(&ids).build()?.metadata;
            // Expiry keeps the current snapshot, so its files stay the same.
            Ok(Some((metadata, base.known_files().cloned(), (base.clone(), expired, kept))))
        })
    })
    .await?
    else {
        return Ok(table.clone());
    };
    report.snapshots_expired += expired.len();

    // Files reachable from the expired snapshots but not from the remaining ones.
    let gone = reachable(&base, &expired, Missing::Skip).await?;
    let storage = base.lake().storage();
    report.objects_deleted += storage.delete_many(gone.difference(&kept).cloned().collect()).await?;
    report.objects_deleted += prune_metadata_files(&expired_table).await?;
    Ok(expired_table)
}

/// Snapshots (newest first) to expire: past the newest `min_snapshots`, and either older than
/// the snapshot retention or beyond `max_snapshots`. The current snapshot, and any a branch or tag
/// points at (another engine's), always stay.
fn expired_snapshots(table: &Table, newest_first: &[SnapshotRef]) -> Result<Vec<SnapshotRef>> {
    let c = table.contract();
    let now_ms = chrono::Utc::now().timestamp_millis();
    if c.keep_snapshots_until.is_some_and(|until| now_ms < until) {
        return Ok(vec![]);
    }
    let current = table.metadata().current_snapshot_id();
    let referenced = referenced_snapshots(table.metadata())?;
    let cutoff = c.snapshot_cutoff_ms(now_ms);
    Ok(newest_first
        .iter()
        .enumerate()
        .filter(|(i, s)| {
            Some(s.snapshot_id()) != current
                && !referenced.contains(&s.snapshot_id())
                && *i >= c.min_snapshots
                && (s.timestamp_ms() < cutoff || *i >= c.max_snapshots)
        })
        .map(|(_, s)| s.clone())
        .collect())
}

/// The snapshots a branch or tag points at, read from the metadata as written (iceberg-rust has
/// no accessor for every ref).
fn referenced_snapshots(metadata: &TableMetadata) -> Result<HashSet<i64>> {
    let json = serde_json::to_value(metadata)?;
    let Some(serde_json::Value::Object(refs)) = json.get("refs") else { return Ok(HashSet::new()) };
    Ok(refs.values().filter_map(|r| r.get("snapshot-id")?.as_i64()).collect())
}

/// Every object (manifest lists, manifests, data files, last-value indexes) a set of snapshots uses.
/// What [`reachable`] does with a manifest list that isn't there.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Missing {
    /// Already deleted by an earlier, interrupted expiry: nothing more to find.
    Skip,
    /// A snapshot that is kept can't be read: an error, so that nothing is deleted.
    Error,
}

async fn reachable(table: &Table, snapshots: &[SnapshotRef], missing: Missing) -> Result<HashSet<String>> {
    let lake = table.lake();
    let mut out = HashSet::new();
    for s in snapshots {
        out.insert(s.manifest_list().to_string());
        if let Some(index) = s.summary().additional_properties.get(summary::LAST_VALUE_INDEX) {
            out.insert(index.clone());
        }
    }
    let version = table.metadata().format_version();
    let lists = snapshots.iter().map(|s| async move {
        match lake.manifest_list(s.manifest_list(), version).await {
            Ok(list) => Ok(Some(list)),
            Err(Error::ObjectStore(object_store::Error::NotFound { .. })) if missing == Missing::Skip => Ok(None),
            Err(Error::ObjectStore(object_store::Error::NotFound { .. })) => Err(Error::NotFound(format!(
                "{}: the manifest list of snapshot {} ({}) is missing, so expiry deletes nothing; roll back past \
                 it or restore the file",
                table.name(),
                s.snapshot_id(),
                s.manifest_list(),
            ))),
            Err(e) => Err(e),
        }
    });
    let lists: Vec<_> = futures::stream::iter(lists.collect::<Vec<_>>())
        .buffered(CONCURRENT_METADATA_READS)
        .try_collect::<Vec<_>>()
        .await?;
    let mut manifests = HashMap::new();
    for manifest in lists.iter().flatten().flat_map(|l| l.iter()) {
        out.insert(manifest.manifest_path.clone());
        manifests.entry(manifest.manifest_path.as_str()).or_insert(manifest);
    }
    let loads: Vec<_> = manifests.into_values().map(|m| lake.manifest(m)).collect();
    let entries: Vec<_> = futures::stream::iter(loads).buffered(CONCURRENT_METADATA_READS).try_collect().await?;
    // Entries marked deleted only record a removal; the file itself is unreferenced.
    for e in entries.iter().flat_map(|es| es.iter()).filter(|e| e.status != ManifestStatus::Deleted) {
        out.insert(e.data_file.file_path().to_string());
    }
    Ok(out)
}

/// Keeps the newest metadata files (those in the metadata log plus the current one).
async fn prune_metadata_files(table: &Table) -> Result<usize> {
    let keep_from = table.version().saturating_sub(table.metadata().metadata_log().len() as u64 + 1);
    let storage = table.lake().storage();
    let doomed = storage
        .list(&metadata_dir(table.location()))
        .await?
        .into_iter()
        .map(|o| o.uri)
        .filter(|uri| metadata_version(uri).is_some_and(|v| v < keep_from))
        .collect();
    storage.delete_many(doomed).await
}

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array;

    use super::*;
    use crate::append::AppendOptions;
    use crate::testing::{T0, batch, faulty_lake, opts, schema};
    use crate::{Lake, Session};

    fn no_maintenance() -> AppendOptions {
        AppendOptions { maintain: Some(false), ..AppendOptions::default() }
    }

    /// A compaction whose new file fails to upload leaves no multipart upload open.
    #[tokio::test]
    async fn a_failed_compaction_aborts_its_upload() {
        let (lake, store) = crate::testing::faulty_lake_multipart();
        let mut o = opts();
        o.properties.insert(crate::property::ROW_GROUP_ROWS.into(), "100".into());
        let mut t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        for i in 0..2 {
            let rows: Vec<(i64, i32, f64)> = (0..2000).map(|j: i32| (T0 + i, j, f64::from(j).sin())).collect();
            t = t.append(&[batch(&rows)], no_maintenance()).await.unwrap().0;
        }
        let before = store.paths().await;
        store.fail(crate::testing::Op::PutPart, "/data/");
        assert!(t.maintain(MaintainOptions::default()).await.is_err());
        assert_eq!(store.open_uploads(), 0);
        assert_eq!(store.paths().await, before);
    }

    async fn values_of_device_1(lake: &Lake) -> Vec<f64> {
        let batches = Session::new(lake.clone()).sql("SELECT value FROM iot.r WHERE device = 1").await.unwrap();
        let batches = batches.collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| b.column(0).as_any().downcast_ref::<Float64Array>().unwrap().values().to_vec())
            .collect()
    }

    /// Inline maintenance rewrites a partition whole once newer data has moved past it, even when
    /// the append didn't touch it; with `compact-cold-after = off` it leaves it to the size tiers.
    #[tokio::test]
    async fn cold_partitions_are_rewritten_whole() {
        const DAY: i64 = 86_400_000_000;
        for (setting, files_on_day_one) in [(None, 1), (Some("off"), 3)] {
            let (lake, _store) = faulty_lake();
            let mut o = opts();
            if let Some(v) = setting {
                o.properties.insert(crate::property::COMPACT_COLD_AFTER.into(), v.into());
            }
            let mut t = lake.create_table("iot.r", &schema(), o).await.unwrap();
            for i in 0..3 {
                t = t.append(&[batch(&[(T0 + i, 1, 1.0)])], AppendOptions::default()).await.unwrap().0;
            }
            assert_eq!(t.files().await.unwrap().len(), 3, "below compact-min-files: no tier merge");
            // Two days later: day one is cold, untouched by this append.
            let (t, _) = t.append(&[batch(&[(T0 + 2 * DAY, 1, 2.0)])], AppendOptions::default()).await.unwrap();
            let files = t.files().await.unwrap();
            let first = files.iter().map(|f| file_partition(&f.data_file)).min().unwrap();
            let on_day_one = files.iter().filter(|f| file_partition(&f.data_file) == first).count();
            assert_eq!(on_day_one, files_on_day_one, "{setting:?}");
        }
    }

    /// Tiered compaction merges files of a similar size. When a file of another size lies between
    /// them in commit order and rewrites a key, its newer value must still win.
    #[tokio::test]
    async fn compaction_keeps_the_latest_write_when_it_skips_a_file() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        // A bigger file (another size tier) with a newer value for device 1.
        let mut rows = vec![(T0, 1, 2.0)];
        rows.extend((1..200_000u32).map(|i| (T0 + i64::from(i), 2, f64::from(i.wrapping_mul(2_654_435_761)) / 3.0)));
        let (t, _) = t.append(&[batch(&rows)], no_maintenance()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0 + 1, 3, 3.0)])], no_maintenance()).await.unwrap();

        let tiered = MaintainOptions { tiered: true, expire: false, retention: false, ..MaintainOptions::default() };
        let (t, _) = t.maintain(tiered).await.unwrap();
        assert_eq!(values_of_device_1(&lake).await, [2.0]);
        let (t, _) = t.maintain(MaintainOptions { expire: false, ..MaintainOptions::default() }).await.unwrap();
        assert_eq!(t.files().await.unwrap().len(), 1);
        assert_eq!(values_of_device_1(&lake).await, [2.0]);
    }

    /// Compaction streams sorted files and sorts an unsorted one (another engine's, repeating a
    /// key) on its own first; the merged file is sorted, and the latest write of each key wins.
    #[tokio::test]
    async fn compaction_merges_sorted_and_unsorted_files() {
        use crate::testing::append_unsorted;

        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) =
            t.append(&[batch(&[(T0, 1, 1.0), (T0 + 2, 1, 1.0), (T0, 2, 1.0)])], no_maintenance()).await.unwrap();
        let day = crate::write::sort_and_split(&t, &[batch(&[(T0, 1, 0.0)])]).unwrap()[0].0;
        let t = append_unsorted(&t, day, batch(&[(T0 + 2, 1, 2.0), (T0 + 1, 1, 2.0), (T0 + 2, 1, 3.0)])).await;
        let (t, _) = t.append(&[batch(&[(T0 + 1, 1, 4.0), (T0 + 3, 2, 4.0)])], no_maintenance()).await.unwrap();

        let (t, report) = t.maintain(MaintainOptions { expire: false, ..MaintainOptions::default() }).await.unwrap();
        assert_eq!((report.files_rewritten, report.files_written), (3, 1));
        assert_eq!((report.rows_rewritten, report.duplicates_removed), (8, 3));
        assert_eq!(t.files().await.unwrap().len(), 1);
        // The unsorted file's rows carry insertion time 0, so at T0 + 2 the first append's row is
        // the latest write; at T0 + 1 the last append's is.
        assert_eq!(values_of_device_1(&lake).await, [1.0, 4.0, 1.0]);
        let sql = "SELECT device, time FROM iot.r";
        let rows = Session::new(lake.clone()).sql(sql).await.unwrap().collect().await.unwrap();
        let devices: Vec<i32> = rows
            .iter()
            .flat_map(|b| b.column(0).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap().values().to_vec())
            .collect();
        assert_eq!(devices, [1, 1, 1, 2, 2], "the merged file is sorted by series, then time");
    }
}
