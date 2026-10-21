//! Maintenance: retention, compaction with dedup, and snapshot expiry with file cleanup. This
//! replaces the "custom Glue jobs" (spec 20): appends run it inline as thresholds are crossed,
//! or [`Table::maintain`] runs it as one stateless command.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use futures::{StreamExt, TryStreamExt};
use iceberg::spec::{DataFile, ManifestStatus, SnapshotRef, TableMetadata};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use timelake_format::layout::metadata_prefix;
use timelake_format::maintain::{
    CompactionPlan, MAX_MERGE_INPUTS, Missing, PlanFile, SnapshotTime, compaction_plan as plan, expired_snapshots,
    pruned_metadata, retention_plan,
};
pub use timelake_format::maintain::{MaintainOptions, MaintainReport, MaintainSteps};
use timelake_format::merge::{Merge, MergeOptions, Step};
use timelake_format::rows::{align_batch, stable_sort};
use tokio::sync::{Semaphore, mpsc};

use crate::commit::{Change, commit, publish_retrying};
use crate::error::{Error, Result};
use crate::rollup::refresh_all;
use crate::storage::Uploads;
use crate::summary;
use crate::table::{LiveEntry, Table};
use crate::write::{PartitionWriter, file_partition, row_groups, writer_properties};

/// Rows per batch when reading a data file back for compaction. Batches of 1,024 to 16,384 rows
/// measured the same time and memory on a server (spec 19); the WebAssembly writer reads smaller
/// ones for its memory limit.
const READ_BATCH_ROWS: usize = 4 * 1024;
/// Batches each input of a compaction reads ahead of the merge (decoding overlaps merging).
const READ_AHEAD: usize = 1;
/// Rows per batch the merge hands the writer.
const MERGED_BATCH_ROWS: usize = 64 * 1024;
/// Groups compaction rewrites at once, at most: each holds the row group it is writing.
const CONCURRENT_REWRITES: usize = 4;
/// How many manifest lists or manifests expiry reads at once.
const CONCURRENT_METADATA_READS: usize = 16;

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

/// Drops whole time partitions that are entirely older than the retention period.
async fn retention(table: &Table, report: &mut MaintainReport) -> Result<Table> {
    let files = table.files().await?;
    let partitions = files.iter().map(|e| file_partition(&e.data_file));
    let Some(plan) = retention_plan(table.contract(), chrono::Utc::now().timestamp_micros(), partitions) else {
        return Ok(table.clone());
    };
    if plan.files.is_empty() {
        return Ok(table.clone());
    }
    let removed: Vec<LiveEntry> = plan.files.iter().map(|&i| files[i].clone()).collect();
    report.partitions_dropped += plan.partitions;
    report.files_dropped += removed.len();
    let change = Change::Delete { removed, lvi_cutoff: Some(plan.cutoff) };
    let (table, _) = commit(table, change, HashMap::new(), Uploads::default()).await?;
    Ok(table)
}

/// Rewrites partitions with many small (or unsorted) files into sorted files of the target size,
/// removing duplicate `(series_key, time)` rows (latest write wins) when dedup is on.
async fn compact(table: &Table, opts: &MaintainOptions, report: &mut MaintainReport) -> Result<Table> {
    let plan = compaction_plan(table, opts).await?;
    report.more_to_compact = plan.more_to_compact;
    if plan.groups.is_empty() {
        return Ok(table.clone());
    }
    report.partitions_compacted += plan.groups.iter().map(|(p, _)| p).collect::<HashSet<_>>().len();
    let uploads = Uploads::default();
    let results = rewrite_groups(table, &plan.groups, &uploads).await?;
    let (mut removed, mut added, mut sequence_numbers) = (vec![], vec![], vec![]);
    let mut outcome = Ok(());
    for ((_, files), result) in plan.groups.into_iter().zip(results) {
        let rewritten = match result {
            Ok(rewritten) => rewritten,
            Err(e) => {
                outcome = outcome.and(Err(e));
                continue;
            }
        };
        let bytes = files.iter().map(|e| e.data_file.file_size_in_bytes()).sum();
        report.merged(files.len(), bytes, rewritten.rows_in, rewritten.rows_out);
        // The files are a run in commit order (older first, so "later row wins" means "latest write
        // wins"); the new files take the run's place in that order.
        let sequence_number = files.iter().map(|f| f.sequence_number).max().unwrap_or_default();
        sequence_numbers.extend(std::iter::repeat_n(sequence_number, rewritten.files.len()));
        added.extend(rewritten.files);
        removed.extend(files);
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

/// Rewrites each group of files of a partition, uploads queued in `uploads`. Groups are rewritten
/// at once while they read no more files together than one merge may: the memory one merge is
/// allowed. Every rewrite runs to its end, so one that fails doesn't drop the others halfway with
/// their uploads open.
async fn rewrite_groups(
    table: &Table,
    groups: &[(Option<i32>, Vec<LiveEntry>)],
    uploads: &Uploads,
) -> Result<Vec<Result<Rewritten>>> {
    let props = writer_properties(table)?;
    let inputs = Semaphore::new(MAX_MERGE_INPUTS);
    // The futures are created up front so the returned future stays `Send`.
    let rewrites: Vec<_> = groups
        .iter()
        .map(|(partition, files)| {
            let (inputs, writer) = (&inputs, PartitionWriter::new(table, *partition, &props, uploads));
            async move {
                let n = u32::try_from(files.len().min(MAX_MERGE_INPUTS)).unwrap_or(u32::MAX);
                let _permits = inputs.acquire_many(n).await.expect("the semaphore is never closed");
                rewrite_partition(table, files, writer).await
            }
        })
        .collect();
    Ok(futures::stream::iter(rewrites).buffered(CONCURRENT_REWRITES).collect().await)
}

/// Merges the rows of `files` (oldest commit first), files of one partition, into sorted,
/// deduplicated new files that `writer` writes, as streams (spec 19): memory is a few batches per
/// file and the row group being written, however large the partition.
async fn rewrite_partition(table: &Table, files: &[LiveEntry], mut writer: PartitionWriter<'_>) -> Result<Rewritten> {
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
    let options = MergeOptions { inputs: files.len(), unique, batch_rows: MERGED_BATCH_ROWS };
    let mut merge = Merge::new(table.schema(), &keys, options)?;
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
    Ok(Rewritten { files: writer.finish().await?, rows_in, rows_out })
}

/// What rewriting a group of files wrote, and the rows it merged.
struct Rewritten {
    files: Vec<DataFile>,
    rows_in: u64,
    rows_out: u64,
}

/// What compaction merges: groups of live files, each a run in commit order
/// (`timelake_format::maintain::compaction_plan`).
pub(crate) struct Compaction {
    /// Each group's partition and files.
    pub groups: Vec<(Option<i32>, Vec<LiveEntry>)>,
    /// A later run has more to merge.
    pub more_to_compact: bool,
}

/// Which files of which partitions to rewrite.
pub(crate) async fn compaction_plan(table: &Table, opts: &MaintainOptions) -> Result<Compaction> {
    let files = table.files().await?;
    let time_id = table.time_field_id();
    let planned: Vec<PlanFile<'_>> = files.iter().map(|e| plan_file(e, time_id)).collect();
    let sort_order_id = i32::try_from(table.metadata().default_sort_order_id()).ok();
    let CompactionPlan { groups, more_to_compact } = plan(table.contract(), sort_order_id, &planned, opts);
    let groups = groups.into_iter().map(|g| (g.partition, g.files.iter().map(|&i| files[i].clone()).collect()));
    Ok(Compaction { groups: groups.collect(), more_to_compact })
}

/// A live file as compaction planning sees it; `time_id` is the time column's field id.
fn plan_file(e: &LiveEntry, time_id: Option<i32>) -> PlanFile<'_> {
    let f = &e.data_file;
    let newest_time = time_id.and_then(|id| e.time_bounds(id).1);
    PlanFile {
        path: f.file_path(),
        partition: file_partition(f),
        size: f.file_size_in_bytes(),
        sequence_number: e.sequence_number,
        sort_order_id: f.sort_order_id(),
        newest_time,
    }
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
            Ok::<_, Error>(stream.map(|b| Ok::<_, Error>(align_batch(&b?, &schema)?)))
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
                let sort = move || Ok::<_, Error>(stable_sort(&concat_batches(&schema, &batches)?, &order)?);
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
            let snapshots: Vec<SnapshotRef> = base.metadata().snapshots().cloned().collect();
            let times: Vec<SnapshotTime> = snapshots
                .iter()
                .map(|s| SnapshotTime { id: s.snapshot_id(), timestamp_ms: s.timestamp_ms() })
                .collect();
            let (current, referenced) = (base.metadata().current_snapshot_id(), referenced_snapshots(base.metadata())?);
            let now_ms = chrono::Utc::now().timestamp_millis();
            let ids = expired_snapshots(base.contract(), &times, current, &referenced, now_ms);
            if ids.is_empty() {
                return Ok(None);
            }
            let (expired, remaining): (Vec<SnapshotRef>, Vec<SnapshotRef>) =
                snapshots.into_iter().partition(|s| ids.contains(&s.snapshot_id()));
            // What the kept snapshots use, read in full before anything changes: a file left out
            // would be deleted.
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

/// The snapshots a branch or tag points at, read from the metadata as written (iceberg-rust has
/// no accessor for every ref).
fn referenced_snapshots(metadata: &TableMetadata) -> Result<HashSet<i64>> {
    let json = serde_json::to_value(metadata)?;
    let Some(serde_json::Value::Object(refs)) = json.get("refs") else { return Ok(HashSet::new()) };
    Ok(refs.values().filter_map(|r| r.get("snapshot-id")?.as_i64()).collect())
}

/// Every object (manifest lists, manifests, data files, last-value indexes) `snapshots` use. A
/// manifest list or manifest that isn't there is skipped or an error, as `missing` says.
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
    let absent = |snapshot: i64, uri: &str| -> Result<()> {
        missing.check(snapshot, uri).map_err(|e| Error::NotFound(format!("{}: {e}", table.name())))
    };
    let lists = snapshots.iter().map(|s| async move {
        match lake.manifest_list(s.manifest_list(), version).await {
            Ok(list) => Ok(Some((s.snapshot_id(), list))),
            Err(e) if is_not_found(&e) => absent(s.snapshot_id(), s.manifest_list()).map(|()| None),
            Err(e) => Err(e),
        }
    });
    let lists: Vec<_> = futures::stream::iter(lists.collect::<Vec<_>>())
        .buffered(CONCURRENT_METADATA_READS)
        .try_collect::<Vec<_>>()
        .await?;
    let mut manifests = HashMap::new();
    for (snapshot, list) in lists.iter().flatten() {
        for manifest in list.iter() {
            out.insert(manifest.manifest_path.clone());
            manifests.entry(manifest.manifest_path.as_str()).or_insert((*snapshot, manifest));
        }
    }
    let loads: Vec<_> = manifests
        .into_values()
        .map(|(snapshot, m)| async move {
            match lake.manifest(m).await {
                Ok(entries) => Ok(Some(entries)),
                Err(e) if is_not_found(&e) => absent(snapshot, &m.manifest_path).map(|()| None),
                Err(e) => Err(e),
            }
        })
        .collect();
    let entries: Vec<_> = futures::stream::iter(loads).buffered(CONCURRENT_METADATA_READS).try_collect().await?;
    // Entries marked deleted only record a removal; the file itself is unreferenced.
    for e in entries.iter().flatten().flat_map(|es| es.iter()).filter(|e| e.status != ManifestStatus::Deleted) {
        out.insert(e.data_file.file_path().to_string());
    }
    Ok(out)
}

/// Whether `e` says an object isn't there.
fn is_not_found(e: &Error) -> bool {
    matches!(e, Error::ObjectStore(object_store::Error::NotFound { .. }))
}

/// Deletes the metadata files older than the metadata log (`pruned_metadata`).
async fn prune_metadata_files(table: &Table) -> Result<usize> {
    let storage = table.lake().storage();
    let listed = storage.list_prefix_all(&metadata_prefix(table.location())).await?;
    let doomed = pruned_metadata(listed, table.version(), table.metadata().metadata_log().len());
    storage.delete_many(doomed).await
}

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array;

    use super::*;
    use crate::append::AppendOptions;
    use timelake_format::rows::sort_and_split;

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
        let day = sort_and_split(&t.schema(), t.contract(), &[batch(&[(T0, 1, 0.0)])], 0).unwrap()[0].0;
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
