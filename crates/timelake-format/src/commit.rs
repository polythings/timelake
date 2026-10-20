//! Commits for the shared write path: the engine's commit protocol (`timelake::commit`) over a
//! [`Host`]. Every file of a snapshot (manifests, manifest list, last-value index) is uploaded at
//! once, then `vN+1.metadata.json` is written only if absent. When another writer got there first,
//! the commit reloads the table and tries again on top of it. The version hint and the head are
//! written after the commit; failures there are ignored, as readers don't rely on them.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_cast::cast;
use arrow_ord::cmp::gt_eq;
use arrow_schema::DataType;
use arrow_select::filter::filter_record_batch;

use crate::error::{WriteError, WriteResult};
use crate::host::{Host, uuid_v4};
use crate::iceberg::{
    DataFile, Entry, ManifestContext, ManifestFile, ManifestWrite, Status, manifest_file, write_manifest,
    write_manifest_list,
};
use crate::metadata::NewSnapshot;
use crate::rows::merge_latest;
use crate::summary;
use crate::table::{TableState, encode_index, hint_uri, metadata_uri};

/// Above this many manifests, an append merges the existing ones into one.
const MERGE_MANIFESTS_AT: usize = 8;
/// Attempts before a commit that keeps losing the race gives up.
const MAX_ATTEMPTS: u64 = 10;
/// Wait before attempt `n`: between these milliseconds, times `n`.
const BACKOFF_MILLIS: (u64, u64) = (5, 50);

/// What a commit changes.
#[derive(Clone, Debug)]
pub enum Change {
    /// New data files, and the newest row per series among them (for the last-value index).
    Append {
        /// The files.
        files: Vec<DataFile>,
        /// Newest row per series in them.
        latest: Option<RecordBatch>,
    },
    /// Remove files and add files with the same rows (compaction). Each added file keeps the data
    /// sequence number in `sequence_numbers`.
    Replace {
        /// Files removed.
        removed: Vec<Entry>,
        /// Files added.
        added: Vec<DataFile>,
        /// Data sequence number of each added file.
        sequence_numbers: Vec<i64>,
    },
    /// Remove files (retention). The last-value index drops series older than `lvi_cutoff` (µs).
    Delete {
        /// Files removed.
        removed: Vec<Entry>,
        /// Retention cutoff for the last-value index.
        lvi_cutoff: Option<i64>,
    },
}

impl Change {
    fn added(&self) -> &[DataFile] {
        match self {
            Self::Append { files, .. } => files,
            Self::Replace { added, .. } => added,
            Self::Delete { .. } => &[],
        }
    }

    fn removed(&self) -> &[Entry] {
        match self {
            Self::Append { .. } => &[],
            Self::Replace { removed, .. } | Self::Delete { removed, .. } => removed,
        }
    }

    fn data_sequence(&self, i: usize) -> Option<i64> {
        match self {
            Self::Replace { sequence_numbers, .. } => sequence_numbers.get(i).copied(),
            _ => None,
        }
    }

    fn operation(&self) -> &'static str {
        match self {
            Self::Append { .. } => "append",
            Self::Replace { .. } => "replace",
            Self::Delete { .. } => "delete",
        }
    }
}

/// How a commit went.
#[derive(Clone, Debug, Default)]
pub struct CommitInfo {
    /// The new snapshot (for a duplicate, the current one).
    pub snapshot_id: i64,
    /// The table version after the commit.
    pub version: u64,
    /// Attempts it took.
    pub attempts: u64,
    /// The batch id was already committed; nothing was written.
    pub duplicate: bool,
}

/// Commits `change` on top of the newest version, retrying on conflicts. `props` go into the
/// snapshot summary.
pub async fn commit<H: Host, S: BuildHasher>(
    host: &H,
    base: TableState,
    change: &Change,
    props: &HashMap<String, String, S>,
) -> WriteResult<(TableState, CommitInfo)> {
    let mut base = base;
    for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
            let (lo, hi) = BACKOFF_MILLIS;
            host.sleep((lo + host.random() % (hi - lo)) * attempt).await;
            base = TableState::load(host, &base.location).await?;
        }
        if let (Change::Append { .. }, Some(id)) = (change, props.get(summary::BATCH_ID))
            && base.metadata.history_has_batch(id)
        {
            let info = CommitInfo {
                snapshot_id: base.metadata.current_snapshot_id().unwrap_or_default(),
                version: base.version,
                attempts: attempt,
                duplicate: true,
            };
            return Ok((base, info));
        }
        if let Some((table, snapshot_id)) = attempt_commit(host, &mut base, change, props).await? {
            let info = CommitInfo { snapshot_id, version: table.version, attempts: attempt, duplicate: false };
            return Ok((table, info));
        }
    }
    Err(WriteError::Conflict(format!("{}: gave up after {MAX_ATTEMPTS} attempts", base.location)))
}

/// Writes table metadata `bytes` at `uri` unless a version is there; `false` if another writer's
/// is.
///
/// A conditional PUT that reports failure may have landed all the same: a retry after a first try
/// that landed gets "already exists", and a timeout says nothing. The version is then read back:
/// our own metadata there is the commit, another writer's means it lost. If that can't be told,
/// the error is [`WriteError::CommitUnknown`], and callers must not delete what the metadata
/// references.
pub(crate) async fn put_metadata<H: Host>(host: &H, uri: &str, bytes: Vec<u8>) -> WriteResult<bool> {
    let unknown = |e: &dyn std::fmt::Display| WriteError::CommitUnknown(format!("{uri}: {e}"));
    match host.put_if_absent(uri, bytes.clone()).await {
        Ok(true) => Ok(true),
        // Ours (an earlier try landed) or another writer's. Absent now, it was never ours: our
        // version would still be there.
        Ok(false) => match host.get(uri).await {
            Ok(found) => Ok(found.is_some_and(|found| found == bytes)),
            Err(e) => Err(unknown(&e)),
        },
        // Absent now, the PUT may still land.
        Err(e) => match host.get(uri).await {
            Ok(Some(found)) => Ok(found == bytes),
            Ok(None) | Err(_) => Err(unknown(&e)),
        },
    }
}

/// One attempt: writes the snapshot's files and publishes it. `None` if another writer committed
/// this version first (this attempt's files are deleted). With an unknown outcome
/// ([`put_metadata`]), nothing is deleted.
async fn attempt_commit<H: Host, S: BuildHasher>(
    host: &H,
    base: &mut TableState,
    change: &Change,
    props: &HashMap<String, String, S>,
) -> WriteResult<Option<(TableState, i64)>> {
    let snapshot_id = loop {
        let id = i64::try_from(host.random() >> 1).unwrap_or(1).max(1);
        if !base.metadata.has_snapshot(id) {
            break id;
        }
    };
    let sequence_number = base.metadata.next_sequence_number();
    let mut uploads: Vec<(String, Vec<u8>)> = vec![];
    let result = async {
        let manifests = write_manifests(host, base, change, snapshot_id, &mut uploads).await?;
        let index = new_index(host, base, change, snapshot_id, &mut uploads).await?;
        let list_uri = format!("{}/metadata/snap-{snapshot_id}-{}.avro", base.location, uuid_v4(host));
        let list = write_manifest_list(&manifests, snapshot_id, base.metadata.current_snapshot_id(), sequence_number)?;
        uploads.push((list_uri.clone(), list));
        let puts = uploads.iter().map(|(uri, bytes)| host.put(uri, bytes.clone()));
        futures_util::future::try_join_all(puts).await?;

        let base_summary = base.metadata.summary();
        let mut summary: HashMap<String, String> = props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        // What a rollup reflects stays recorded until a refresh records something newer.
        for key in [summary::ROLLUP_SOURCE_SNAPSHOT, summary::ROLLUP_SOURCE_SEQUENCE] {
            if let (false, Some(v)) = (summary.contains_key(key), base_summary.get(key)) {
                summary.insert(key.into(), v.clone());
            }
        }
        if let Some((location, _)) = &index {
            summary.insert(summary::LAST_VALUE_INDEX.into(), location.clone());
        }
        summary.extend(counters(change, &base_summary));
        let now = host.now_ms();
        let snapshot = NewSnapshot {
            snapshot_id,
            sequence_number,
            timestamp_ms: now,
            manifest_list: list_uri,
            operation: change.operation().into(),
            summary,
        };
        let previous = metadata_uri(&base.location, base.version);
        let metadata = base.metadata.with_snapshot(&snapshot, &previous, now)?;
        let contract = metadata.contract()?;
        let version = base.version + 1;
        let ours = put_metadata(host, &metadata_uri(&base.location, version), metadata.to_json().into_bytes()).await?;
        Ok::<_, WriteError>(ours.then_some((metadata, contract, version, index)))
    }
    .await;
    let Ok(Some((metadata, contract, version, index))) = result else {
        if !matches!(result, Err(WriteError::CommitUnknown(_))) {
            let written: Vec<String> = uploads.into_iter().map(|(uri, _)| uri).collect();
            let _ = host.delete(&written).await;
        }
        return result.map(|_| None);
    };

    let files = base.files.as_ref().map(|files| {
        let removed: HashSet<&str> = change.removed().iter().map(|e| e.file.path.as_str()).collect();
        let mut next: Vec<Entry> = change
            .added()
            .iter()
            .enumerate()
            .map(|(i, f)| Entry {
                status: Status::Added,
                snapshot_id,
                sequence_number: change.data_sequence(i).unwrap_or(sequence_number),
                file_sequence_number: sequence_number,
                file: Arc::new(f.clone()),
            })
            .collect();
        next.extend(files.iter().filter(|e| !removed.contains(e.file.path.as_str())).cloned());
        next
    });
    let last_values = match index {
        Some((_, batch)) => Some(batch),
        None if matches!(change, Change::Replace { .. }) => base.last_values.clone(),
        None => None,
    };
    let mut table = TableState {
        location: base.location.clone(),
        version,
        schema: base.schema.clone(),
        contract,
        metadata,
        files,
        last_values,
        footers: base.footers.clone(),
    };
    let live: HashSet<&str> = table.files.iter().flatten().map(|e| e.file.path.as_str()).collect();
    table.footers.retain(|path, _| live.contains(path.as_str()));
    let _ = host.put(&hint_uri(&table.location), version.to_string().into_bytes()).await;
    Ok(Some((table, snapshot_id)))
}

/// Writes a manifest of the added files, carries the base snapshot's manifests over, and rewrites
/// the ones holding removed files (or all of them once there are too many).
async fn write_manifests<H: Host>(
    host: &H,
    base: &TableState,
    change: &Change,
    snapshot_id: i64,
    uploads: &mut Vec<(String, Vec<u8>)>,
) -> WriteResult<Vec<ManifestFile>> {
    let metadata = &base.metadata;
    let schema_json = metadata.schema_json()?.to_string();
    let (spec_id, spec_fields) = metadata.partition_spec()?;
    let spec_fields_json = spec_fields.to_string();
    let partition = metadata.partition_field()?;
    let ctx = ManifestContext {
        schema_json: &schema_json,
        schema_id: metadata.schema_id(),
        spec_fields_json: &spec_fields_json,
        spec_id,
        partition: partition.as_ref().map(|(name, chunk)| (name.as_str(), *chunk)),
    };
    let mut write = |kind: &str, entries: &[ManifestWrite]| -> WriteResult<ManifestFile> {
        let uri = format!("{}/metadata/{}-{kind}-m.avro", base.location, uuid_v4(host));
        let bytes = write_manifest(&ctx, entries)?;
        let mf = manifest_file(&uri, &bytes, spec_id, snapshot_id, entries, partition.is_some());
        uploads.push((uri, bytes));
        Ok(mf)
    };
    let mut new = vec![];
    let added = change.added();
    if !added.is_empty() {
        let entries: Vec<ManifestWrite> = added
            .iter()
            .enumerate()
            .map(|(i, f)| ManifestWrite {
                status: Status::Added,
                snapshot_id: Some(snapshot_id),
                sequence_number: change.data_sequence(i),
                file_sequence_number: None,
                file: Arc::new(f.clone()),
            })
            .collect();
        new.push(write("added", &entries)?);
    }

    let current = base.manifests(host).await?;
    let removed: HashSet<&str> = change.removed().iter().map(|e| e.file.path.as_str()).collect();
    let merge = removed.is_empty() && current.len() + new.len() > MERGE_MANIFESTS_AT;
    if removed.is_empty() && !merge {
        new.extend(current);
        return Ok(new);
    }
    let loaded = futures_util::future::try_join_all(current.iter().map(|m| TableState::manifest(host, m))).await?;
    let (mut kept, mut deleted, mut carried) = (vec![], vec![], vec![]);
    for (manifest, entries) in current.into_iter().zip(loaded) {
        let live: Vec<&Entry> = entries.iter().filter(|e| e.status != Status::Deleted).collect();
        if !merge && !live.iter().any(|e| removed.contains(e.file.path.as_str())) {
            carried.push(manifest);
            continue;
        }
        for e in live {
            let (status, snapshot) = if removed.contains(e.file.path.as_str()) {
                (Status::Deleted, snapshot_id)
            } else {
                (Status::Existing, e.snapshot_id)
            };
            let w = ManifestWrite {
                status,
                snapshot_id: Some(snapshot),
                sequence_number: Some(e.sequence_number),
                file_sequence_number: Some(e.file_sequence_number),
                file: Arc::clone(&e.file),
            };
            if status == Status::Deleted { deleted.push(w) } else { kept.push(w) }
        }
    }
    if deleted.len() != removed.len() {
        return Err(WriteError::Conflict(format!(
            "{}: {} of {} files to remove are no longer in the table (concurrent maintenance?)",
            base.location,
            removed.len() - deleted.len(),
            removed.len()
        )));
    }
    if !kept.is_empty() {
        new.push(write("merged", &kept)?);
    }
    if !deleted.is_empty() {
        new.push(write("deleted", &deleted)?);
    }
    new.extend(carried);
    Ok(new)
}

/// The last-value index of the new snapshot, if it changes: extended on append, trimmed on delete.
async fn new_index<H: Host>(
    host: &H,
    base: &mut TableState,
    change: &Change,
    snapshot_id: i64,
    uploads: &mut Vec<(String, Vec<u8>)>,
) -> WriteResult<Option<(String, RecordBatch)>> {
    let index = match change {
        Change::Append { latest: Some(latest), .. } => {
            // Only extend an index that covers all existing rows.
            let rows: u64 =
                base.metadata.summary().get(summary::TOTAL_RECORDS).and_then(|v| v.parse().ok()).unwrap_or(0);
            if base.last_value_index().is_none() && rows > 0 {
                return Ok(None);
            }
            let mut parts: Vec<RecordBatch> = base.last_values(host).await?.into_iter().collect();
            parts.push(latest.clone());
            merge_latest(&base.schema, &base.contract, &parts)?
        }
        Change::Append { latest: None, .. } | Change::Replace { .. } => return Ok(None),
        Change::Delete { lvi_cutoff, .. } => {
            let (Some(cutoff), Some(time_column)) = (lvi_cutoff, base.contract.time_column.clone()) else {
                return Ok(None);
            };
            let Some(index) = base.last_values(host).await? else { return Ok(None) };
            let times = cast(index.column(index.schema().index_of(&time_column)?), &DataType::Int64)?;
            let keep = gt_eq(&times, &Int64Array::new_scalar(*cutoff))?;
            Some(filter_record_batch(&index, &keep)?)
        }
    };
    let Some(index) = index else { return Ok(None) };
    let uri = format!("{}/metadata/lvi-{snapshot_id}-{}.parquet", base.location, uuid_v4(host));
    uploads.push((uri.clone(), encode_index(&index)?));
    Ok(Some((uri, index)))
}

/// Iceberg's standard snapshot summary counters.
fn counters(change: &Change, base: &HashMap<String, String>) -> BTreeMap<String, String> {
    let total = |k: &str| base.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    let (added, removed) = (change.added(), change.removed());
    let added_rows: u64 = added.iter().map(|f| f.record_count).sum();
    let added_bytes: u64 = added.iter().map(|f| f.file_size).sum();
    let removed_rows: u64 = removed.iter().map(|e| e.file.record_count).sum();
    let removed_bytes: u64 = removed.iter().map(|e| e.file.file_size).sum();
    let (added_files, removed_files) = (added.len() as u64, removed.len() as u64);
    let mut out = BTreeMap::new();
    for (k, v) in [
        (summary::ADDED_DATA_FILES, added_files),
        (summary::ADDED_RECORDS, added_rows),
        (summary::ADDED_FILES_SIZE, added_bytes),
        (summary::DELETED_DATA_FILES, removed_files),
        (summary::DELETED_RECORDS, removed_rows),
        (summary::REMOVED_FILES_SIZE, removed_bytes),
    ] {
        if v != 0 {
            out.insert(k.to_string(), v.to_string());
        }
    }
    for (k, v) in [
        (summary::TOTAL_DATA_FILES, (total(summary::TOTAL_DATA_FILES) + added_files).saturating_sub(removed_files)),
        (summary::TOTAL_RECORDS, (total(summary::TOTAL_RECORDS) + added_rows).saturating_sub(removed_rows)),
        (summary::TOTAL_FILES_SIZE, (total(summary::TOTAL_FILES_SIZE) + added_bytes).saturating_sub(removed_bytes)),
        (summary::TOTAL_DELETE_FILES, 0),
        (summary::TOTAL_POSITION_DELETES, 0),
        (summary::TOTAL_EQUALITY_DELETES, 0),
    ] {
        out.insert(k.to_string(), v.to_string());
    }
    out
}
