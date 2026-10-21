//! Commits (spec 2). The protocol both writers follow is defined here once, without I/O: what a
//! change records in the snapshot summary, what happens to the last-value index, which manifests
//! are carried over or rewritten, how a conditional metadata PUT is judged, and how long a writer
//! waits between attempts. The engine (`timelake::commit`, with iceberg-rust's types) and
//! [`commit`] below (over a [`Host`]) each do their own I/O around it.
//!
//! [`commit`] uploads every file of a snapshot (manifests, manifest list, last-value index) at
//! once, then writes `vN+1.metadata.json` only if absent. When another writer got there first, it
//! loads the table again and tries on top of it. It writes the version hint after the commit and
//! waits for it (the engine writes it in the background); a failure there is ignored, as readers
//! don't rely on it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;

use crate::error::{WriteError, WriteResult};
use crate::host::{Host, uuid_v4};
use crate::iceberg::{
    DataFile, Entry, ManifestContext, ManifestFile, ManifestWrite, Status, manifest_file, write_manifest,
    write_manifest_list,
};
use crate::layout::{hint_uri, last_value_index_uri, manifest_list_uri, manifest_uri, metadata_uri};
use crate::lvi;
use crate::metadata::NewSnapshot;
use crate::rows::merge_latest;
use crate::summary;
use crate::table::TableState;

/// Above this many manifests, an append merges the existing ones into one, keeping scan planning
/// to a handful of reads no matter how many commits a table has seen.
pub const MERGE_MANIFESTS_AT: usize = 8;
/// Attempts before a commit that keeps losing the race gives up with a conflict.
pub const MAX_ATTEMPTS: usize = 10;
/// Wait before attempt `n`: between these milliseconds, times `n`.
const BACKOFF_MILLIS: (u64, u64) = (5, 50);

/// How long to wait before attempt `attempt` (from 2), given a `random` number: a random number of
/// milliseconds between 5 and 50, times `attempt`, so writers that lost the same race don't meet
/// again.
pub fn backoff_millis(attempt: usize, random: u64) -> u64 {
    let (lo, hi) = BACKOFF_MILLIS;
    (lo + random % (hi - lo)).saturating_mul(u64::try_from(attempt).unwrap_or(u64::MAX))
}

/// What a commit changes. `D` is the writer's data file type, `E` its manifest entry type.
#[derive(Clone, Debug)]
pub enum Change<D, E> {
    /// New data files, and the newest row per series among them (for the last-value index).
    Append {
        /// The files.
        files: Vec<D>,
        /// Newest row per series in them; `None` drops the index (it can't be kept correct).
        latest: Option<RecordBatch>,
    },
    /// Remove files and add files with the same rows (compaction). Each added file keeps the data
    /// sequence number in `sequence_numbers` (the newest of the files it replaces), so files
    /// committed in between still order after the rows it holds.
    Replace {
        /// Files removed.
        removed: Vec<E>,
        /// Files added.
        added: Vec<D>,
        /// Data sequence number of each added file.
        sequence_numbers: Vec<i64>,
    },
    /// Remove files (retention). The last-value index drops series whose newest row is older
    /// than `lvi_cutoff` (µs since the epoch).
    Delete {
        /// Files removed.
        removed: Vec<E>,
        /// Retention cutoff for the last-value index.
        lvi_cutoff: Option<i64>,
    },
    /// Set the last-value index of a table that has none (`timelake upgrade`); data is unchanged.
    Index {
        /// The newest row per series of the whole table.
        latest: RecordBatch,
    },
}

pub use crate::metadata::Operation;

/// What a commit does with the last-value index (the newest row per series).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IndexAction<'a> {
    /// Record the base snapshot's index, if it has one: the newest rows are unchanged.
    Keep,
    /// Record none: an index can't be kept correct.
    Drop,
    /// Merge the base snapshot's index (if any) with these rows and write the result.
    Extend(&'a RecordBatch),
    /// Keep the rows of the base snapshot's index at or after this time (µs) and write them.
    Trim(i64),
    /// Write these rows as the index.
    Set(&'a RecordBatch),
}

impl<D, E> Change<D, E> {
    /// The files added.
    pub fn added(&self) -> &[D] {
        match self {
            Self::Append { files, .. } => files,
            Self::Replace { added, .. } => added,
            Self::Delete { .. } | Self::Index { .. } => &[],
        }
    }

    /// The files removed.
    pub fn removed(&self) -> &[E] {
        match self {
            Self::Append { .. } | Self::Index { .. } => &[],
            Self::Replace { removed, .. } | Self::Delete { removed, .. } => removed,
        }
    }

    /// The data sequence number the `i`th added file keeps, if not the commit's own.
    pub fn data_sequence(&self, i: usize) -> Option<i64> {
        match self {
            Self::Replace { sequence_numbers, .. } => sequence_numbers.get(i).copied(),
            _ => None,
        }
    }

    /// The snapshot's operation.
    pub fn operation(&self) -> Operation {
        match self {
            Self::Append { .. } => Operation::Append,
            Self::Replace { .. } | Self::Index { .. } => Operation::Replace,
            Self::Delete { .. } => Operation::Delete,
        }
    }

    /// The batch id to look for in the table's history (the current snapshot and its ancestors):
    /// an append whose id is there is a duplicate and commits nothing. Only appends have one.
    pub fn batch_id<'p, S: BuildHasher>(&self, props: &'p HashMap<String, String, S>) -> Option<&'p str> {
        match self {
            Self::Append { .. } => props.get(summary::BATCH_ID).map(String::as_str),
            _ => None,
        }
    }

    /// What the commit does with the last-value index, given the base snapshot's summary and
    /// whether the table has a time column: extended on append (only an index that covers every
    /// row), kept on compaction, trimmed to the retention cutoff on delete, and set by
    /// [`Change::Index`].
    pub fn index_action<S: BuildHasher>(
        &self,
        base_summary: &HashMap<String, String, S>,
        time_column: bool,
    ) -> IndexAction<'_> {
        let has_index = base_summary.contains_key(summary::LAST_VALUE_INDEX);
        match self {
            Self::Append { latest: Some(latest), .. } => {
                let rows = base_summary.get(summary::TOTAL_RECORDS).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
                if has_index || rows == 0 { IndexAction::Extend(latest) } else { IndexAction::Drop }
            }
            Self::Append { latest: None, .. } => IndexAction::Drop,
            Self::Delete { lvi_cutoff: Some(cutoff), .. } if has_index && time_column => IndexAction::Trim(*cutoff),
            Self::Replace { .. } | Self::Delete { .. } => IndexAction::Keep,
            Self::Index { latest } => IndexAction::Set(latest),
        }
    }
}

/// Files, rows and bytes of a set of data files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileTotals {
    /// Files.
    pub files: u64,
    /// Rows.
    pub rows: u64,
    /// Bytes.
    pub bytes: u64,
}

impl FileTotals {
    /// The totals of files given as `(rows, bytes)`.
    pub fn of(files: impl IntoIterator<Item = (u64, u64)>) -> Self {
        files.into_iter().fold(Self::default(), |t, (rows, bytes)| Self {
            files: t.files.saturating_add(1),
            rows: t.rows.saturating_add(rows),
            bytes: t.bytes.saturating_add(bytes),
        })
    }
}

/// The new snapshot's summary: the caller's `props`, the rollup source carried over from the base
/// snapshot's summary until a refresh records a newer one, the last-value index's location, and
/// Iceberg's counters for the files `added` and `removed`.
pub fn snapshot_summary<S: BuildHasher, T: BuildHasher>(
    props: &HashMap<String, String, S>,
    base: &HashMap<String, String, T>,
    index: Option<&str>,
    added: FileTotals,
    removed: FileTotals,
) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    for key in [summary::ROLLUP_SOURCE_SNAPSHOT, summary::ROLLUP_SOURCE_SEQUENCE] {
        if let (false, Some(v)) = (out.contains_key(key), base.get(key)) {
            out.insert(key.into(), v.clone());
        }
    }
    if let Some(location) = index {
        out.insert(summary::LAST_VALUE_INDEX.into(), location.to_string());
    }
    out.extend(counters(base, added, removed));
    out
}

/// Iceberg's standard snapshot summary counters: what changed (when anything did) and the totals.
fn counters<S: BuildHasher>(
    base: &HashMap<String, String, S>,
    added: FileTotals,
    removed: FileTotals,
) -> BTreeMap<String, String> {
    let total = |k: &str| base.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    let next = |k: &str, add: u64, remove: u64| total(k).saturating_add(add).saturating_sub(remove);
    let mut out = BTreeMap::new();
    for (k, v) in [
        (summary::ADDED_DATA_FILES, added.files),
        (summary::ADDED_RECORDS, added.rows),
        (summary::ADDED_FILES_SIZE, added.bytes),
        (summary::DELETED_DATA_FILES, removed.files),
        (summary::DELETED_RECORDS, removed.rows),
        (summary::REMOVED_FILES_SIZE, removed.bytes),
    ] {
        if v != 0 {
            out.insert(k.to_string(), v.to_string());
        }
    }
    for (k, v) in [
        (summary::TOTAL_DATA_FILES, next(summary::TOTAL_DATA_FILES, added.files, removed.files)),
        (summary::TOTAL_RECORDS, next(summary::TOTAL_RECORDS, added.rows, removed.rows)),
        (summary::TOTAL_FILES_SIZE, next(summary::TOTAL_FILES_SIZE, added.bytes, removed.bytes)),
        (summary::TOTAL_DELETE_FILES, 0),
        (summary::TOTAL_POSITION_DELETES, 0),
        (summary::TOTAL_EQUALITY_DELETES, 0),
    ] {
        out.insert(k.to_string(), v.to_string());
    }
    out
}

/// A manifest entry, as a commit's manifest plan needs it.
pub trait ManifestEntry {
    /// The data file's URI.
    fn path(&self) -> &str;
    /// Whether the entry only records a removal (the file isn't live).
    fn is_deleted(&self) -> bool;
}

impl ManifestEntry for Entry {
    fn path(&self) -> &str {
        &self.file.path
    }

    fn is_deleted(&self) -> bool {
        self.status == Status::Deleted
    }
}

/// What a commit does with the base snapshot's manifests, besides writing one of the files it
/// adds (listed first).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestPlan {
    /// Carry every one over unchanged.
    Carry,
    /// Read them and [`rewrite_manifests`]: with `merge`, all into one; without, the ones holding
    /// removed files.
    Rewrite {
        /// Merge every manifest (there are too many).
        merge: bool,
    },
}

/// The plan for a commit that removes `removed` files, on a base snapshot of `base` manifests,
/// writing `new` manifests of its own: removals rewrite the manifests holding the files, and an
/// append merges every manifest once there are more than [`MERGE_MANIFESTS_AT`].
pub fn manifest_plan(removed: usize, base: usize, new: usize) -> ManifestPlan {
    let merge = removed == 0 && base.saturating_add(new) > MERGE_MANIFESTS_AT;
    if removed == 0 && !merge { ManifestPlan::Carry } else { ManifestPlan::Rewrite { merge } }
}

/// The base snapshot's manifests after [`ManifestPlan::Rewrite`]: those carried over unchanged (by
/// index), and the live entries of the others, kept (written as existing) or removed (written as
/// deleted by the new snapshot).
#[derive(Debug)]
pub struct Rewritten<'a, E> {
    /// Indexes of the manifests carried over.
    pub carried: Vec<usize>,
    /// Entries that stay.
    pub kept: Vec<&'a E>,
    /// Entries of the files removed.
    pub removed: Vec<&'a E>,
}

/// Some of the files a commit removes are no longer in the table: another writer removed them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotInTable {
    /// Files not found.
    pub missing: usize,
    /// Files to remove.
    pub of: usize,
}

impl fmt::Display for NotInTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { missing, of } = self;
        write!(f, "{missing} of {of} files to remove are no longer in the table (concurrent maintenance?)")
    }
}

/// Splits the entries of the base snapshot's `manifests` (in manifest-list order) for a commit
/// that removes the files in `removed`: a manifest without any is carried over unless `merge`.
pub fn rewrite_manifests<'a, E: ManifestEntry + 'a, S: BuildHasher>(
    manifests: impl IntoIterator<Item = &'a [E]>,
    removed: &HashSet<&str, S>,
    merge: bool,
) -> Result<Rewritten<'a, E>, NotInTable> {
    let mut out = Rewritten { carried: vec![], kept: vec![], removed: vec![] };
    for (i, entries) in manifests.into_iter().enumerate() {
        let live = entries.iter().filter(|e| !e.is_deleted());
        if !merge && !live.clone().any(|e| removed.contains(e.path())) {
            out.carried.push(i);
            continue;
        }
        for e in live {
            if removed.contains(e.path()) { out.removed.push(e) } else { out.kept.push(e) }
        }
    }
    if out.removed.len() != removed.len() {
        return Err(NotInTable { missing: removed.len().saturating_sub(out.removed.len()), of: removed.len() });
    }
    Ok(out)
}

/// Whether a conditional PUT of new metadata that didn't report success is the commit all the
/// same, from what reading its URI back `found` (the object, or `None` if absent): `Ok(true)` if
/// the object is `ours`, `Ok(false)` if another writer's, or the error to report if that can't be
/// told. `put` is the PUT's error, or `None` if it answered "already exists".
///
/// A retry after a first try that landed gets "already exists", and a failure or timeout says
/// nothing. Absent after "already exists", the version was never ours (ours would still be
/// there); absent after a failure, the PUT may still land. With an unknown outcome, the writer
/// must not delete what the metadata references.
pub fn read_back<E>(put: Option<E>, found: Result<Option<&[u8]>, E>, ours: &[u8]) -> Result<bool, E> {
    match (found, put) {
        (Ok(Some(found)), _) => Ok(found == ours),
        (Ok(None), None) => Ok(false),
        (Ok(None) | Err(_), Some(e)) | (Err(e), None) => Err(e),
    }
}

/// How a commit went.
#[derive(Clone, Debug, Default)]
pub struct CommitInfo {
    /// The new snapshot (for a duplicate, the current one; 0 if nothing was committed).
    pub snapshot_id: i64,
    /// The table version after the commit.
    pub version: u64,
    /// Attempts it took: more than one means other writers committed concurrently.
    pub attempts: usize,
    /// Time from the first attempt to the commit.
    pub duration: Duration,
    /// True if the batch id was already committed and nothing was written.
    pub duplicate: bool,
}

// --- the shared write path's commit -------------------------------------------------------------

/// Commits `change` on top of the newest version, retrying on conflicts. `props` go into the
/// snapshot summary.
pub async fn commit<H: Host, S: BuildHasher>(
    host: &H,
    base: TableState,
    change: &Change<DataFile, Entry>,
    props: &HashMap<String, String, S>,
) -> WriteResult<(TableState, CommitInfo)> {
    let started = host.now_ms();
    let elapsed = || Duration::from_millis(u64::try_from(host.now_ms().saturating_sub(started)).unwrap_or(0));
    let mut base = base;
    for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
            host.sleep(backoff_millis(attempt, host.random())).await;
            base = TableState::load(host, &base.location).await?;
        }
        if let Some(id) = change.batch_id(props)
            && base.metadata.history_has_batch(id)
        {
            let info = CommitInfo {
                snapshot_id: base.metadata.current_snapshot_id().unwrap_or_default(),
                version: base.version,
                attempts: attempt,
                duration: elapsed(),
                duplicate: true,
            };
            return Ok((base, info));
        }
        if let Some((table, snapshot_id)) = attempt_commit(host, &mut base, change, props).await? {
            let info = CommitInfo {
                snapshot_id,
                version: table.version,
                attempts: attempt,
                duration: elapsed(),
                duplicate: false,
            };
            return Ok((table, info));
        }
    }
    Err(WriteError::Conflict(format!("{}: gave up after {MAX_ATTEMPTS} attempts", base.location)))
}

/// Writes table metadata `bytes` at `uri` unless a version is there; `false` if another writer's
/// is. An outcome that can't be told ([`read_back`]) is [`WriteError::CommitUnknown`].
pub(crate) async fn put_metadata<H: Host>(host: &H, uri: &str, bytes: Vec<u8>) -> WriteResult<bool> {
    let put = match host.put_if_absent(uri, bytes.clone()).await {
        Ok(true) => return Ok(true),
        put => put.err(),
    };
    let found = host.get(uri).await;
    read_back(put.as_ref(), found.as_ref().map(Option::as_deref), &bytes)
        .map_err(|e| WriteError::CommitUnknown(format!("{uri}: {e}")))
}

/// One attempt: writes the snapshot's files and publishes it. `None` if another writer committed
/// this version first (this attempt's files are deleted). With an unknown outcome
/// ([`put_metadata`]), nothing is deleted.
async fn attempt_commit<H: Host, S: BuildHasher>(
    host: &H,
    base: &mut TableState,
    change: &Change<DataFile, Entry>,
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
        let list_uri = manifest_list_uri(&base.location, snapshot_id, uuid_v4(host));
        let list = write_manifest_list(&manifests, snapshot_id, base.metadata.current_snapshot_id(), sequence_number)?;
        uploads.push((list_uri.clone(), list));
        let puts = uploads.iter().map(|(uri, bytes)| host.put(uri, bytes.clone()));
        futures_util::future::try_join_all(puts).await?;

        let added = FileTotals::of(change.added().iter().map(|f| (f.record_count, f.file_size)));
        let removed = FileTotals::of(change.removed().iter().map(|e| (e.file.record_count, e.file.file_size)));
        let summary = snapshot_summary(props, &base.metadata.summary(), index.location.as_deref(), added, removed);
        let now = host.now_ms();
        let snapshot = NewSnapshot {
            snapshot_id,
            sequence_number,
            timestamp_ms: now,
            manifest_list: list_uri,
            operation: change.operation(),
            summary,
        };
        let previous = metadata_uri(&base.location, base.version);
        let metadata = base.metadata.with_snapshot(&snapshot, &previous, now)?;
        // Loaded first: metadata this Timelake can't load is never committed.
        let next = TableState::from_metadata(&base.location, base.version + 1, metadata)?;
        let uri = metadata_uri(&base.location, next.version);
        let ours = put_metadata(host, &uri, next.metadata.to_json().into_bytes()).await?;
        Ok::<_, WriteError>(ours.then_some((next, index)))
    }
    .await;
    let Ok(Some((mut table, index))) = result else {
        if !matches!(result, Err(WriteError::CommitUnknown(_))) {
            let written: Vec<String> = uploads.into_iter().map(|(uri, _)| uri).collect();
            let _ = host.delete(&written).await;
        }
        return result.map(|_| None);
    };

    table.files = base.files.as_ref().map(|files| {
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
    table.last_values = match index {
        NewIndex { written: Some(batch), .. } => Some(batch),
        NewIndex { location: Some(_), .. } => base.last_values.clone(),
        NewIndex { location: None, .. } => None,
    };
    let live: HashSet<&str> = table.files.iter().flatten().map(|e| e.file.path.as_str()).collect();
    table.footers = base.footers.clone();
    table.footers.retain(|path, _| live.contains(path.as_str()));
    let _ = host.put(&hint_uri(&table.location), table.version.to_string().into_bytes()).await;
    Ok(Some((table, snapshot_id)))
}

/// Writes a manifest of the added files, then carries the base snapshot's manifests over or
/// rewrites them, as [`manifest_plan`] says.
async fn write_manifests<H: Host>(
    host: &H,
    base: &TableState,
    change: &Change<DataFile, Entry>,
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
        partition: partition.as_ref(),
    };
    let mut write = |kind: &str, entries: &[ManifestWrite]| -> WriteResult<ManifestFile> {
        let uri = manifest_uri(&base.location, uuid_v4(host), kind);
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
    let ManifestPlan::Rewrite { merge } = manifest_plan(removed.len(), current.len(), new.len()) else {
        new.extend(current);
        return Ok(new);
    };
    let loaded = futures_util::future::try_join_all(current.iter().map(|m| TableState::manifest(host, m))).await?;
    let rewritten = rewrite_manifests(loaded.iter().map(Vec::as_slice), &removed, merge)
        .map_err(|e| WriteError::Conflict(format!("{}: {e}", base.location)))?;
    let entry = |e: &Entry, status: Status, snapshot: i64| ManifestWrite {
        status,
        snapshot_id: Some(snapshot),
        sequence_number: Some(e.sequence_number),
        file_sequence_number: Some(e.file_sequence_number),
        file: Arc::clone(&e.file),
    };
    if !rewritten.kept.is_empty() {
        let kept: Vec<_> = rewritten.kept.iter().map(|e| entry(e, Status::Existing, e.snapshot_id)).collect();
        new.push(write("merged", &kept)?);
    }
    if !rewritten.removed.is_empty() {
        let deleted: Vec<_> = rewritten.removed.iter().map(|e| entry(e, Status::Deleted, snapshot_id)).collect();
        new.push(write("deleted", &deleted)?);
    }
    new.extend(rewritten.carried.iter().map(|&i| current[i].clone()));
    Ok(new)
}

/// The new snapshot's last-value index: where it is, and its rows if this commit wrote it.
struct NewIndex {
    location: Option<String>,
    written: Option<RecordBatch>,
}

/// The last-value index of the new snapshot, as [`Change::index_action`] says.
async fn new_index<H: Host>(
    host: &H,
    base: &mut TableState,
    change: &Change<DataFile, Entry>,
    snapshot_id: i64,
    uploads: &mut Vec<(String, Vec<u8>)>,
) -> WriteResult<NewIndex> {
    let time_column = base.contract.time_column.clone();
    let index = match change.index_action(&base.metadata.summary(), time_column.is_some()) {
        IndexAction::Keep => return Ok(NewIndex { location: base.last_value_index(), written: None }),
        IndexAction::Drop => None,
        IndexAction::Extend(latest) => {
            let mut parts: Vec<RecordBatch> = base.last_values(host).await?.into_iter().collect();
            parts.push(latest.clone());
            merge_latest(&base.schema, &base.contract, &parts)?
        }
        IndexAction::Trim(cutoff) => match (base.last_values(host).await?, &time_column) {
            (Some(index), Some(time)) => Some(lvi::retain_since(&index, time, cutoff)?),
            _ => return Ok(NewIndex { location: base.last_value_index(), written: None }),
        },
        IndexAction::Set(latest) => Some(latest.clone()),
    };
    let Some(index) = index else { return Ok(NewIndex { location: None, written: None }) };
    let uri = last_value_index_uri(&base.location, snapshot_id, uuid_v4(host));
    uploads.push((uri.clone(), lvi::encode(&index)?));
    Ok(NewIndex { location: Some(uri), written: Some(index) })
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestChange = Change<(u64, u64), TestEntry>;

    #[derive(Debug)]
    struct TestEntry(&'static str, bool);

    impl ManifestEntry for TestEntry {
        fn path(&self) -> &str {
            self.0
        }

        fn is_deleted(&self) -> bool {
            self.1
        }
    }

    fn summary(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    fn batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()))
    }

    #[test]
    fn the_index_is_extended_kept_trimmed_set_or_dropped() {
        let latest = batch();
        let with_index = summary(&[(summary::LAST_VALUE_INDEX, "lvi"), (summary::TOTAL_RECORDS, "5")]);
        let without = summary(&[(summary::TOTAL_RECORDS, "5")]);
        let empty = summary(&[]);
        let append = |latest: Option<RecordBatch>| TestChange::Append { files: vec![], latest };
        let delete = |lvi_cutoff| TestChange::Delete { removed: vec![], lvi_cutoff };
        let replace = TestChange::Replace { removed: vec![], added: vec![], sequence_numbers: vec![] };

        assert_eq!(append(Some(latest.clone())).index_action(&with_index, true), IndexAction::Extend(&latest));
        // An index that doesn't cover the rows already there isn't started by an append.
        assert_eq!(append(Some(latest.clone())).index_action(&without, true), IndexAction::Drop);
        assert_eq!(append(Some(latest.clone())).index_action(&empty, true), IndexAction::Extend(&latest));
        assert_eq!(append(None).index_action(&with_index, true), IndexAction::Drop);
        // Compaction keeps the newest row per series.
        assert_eq!(replace.index_action(&with_index, true), IndexAction::Keep);
        assert_eq!(delete(Some(7)).index_action(&with_index, true), IndexAction::Trim(7));
        assert_eq!(delete(Some(7)).index_action(&without, true), IndexAction::Keep);
        assert_eq!(delete(Some(7)).index_action(&with_index, false), IndexAction::Keep);
        assert_eq!(delete(None).index_action(&with_index, true), IndexAction::Keep);
        let index = TestChange::Index { latest: latest.clone() };
        assert_eq!(index.index_action(&without, true), IndexAction::Set(&latest));
        assert_eq!(index.operation(), Operation::Replace);
    }

    #[test]
    fn only_appends_are_checked_for_their_batch_id() {
        let props = summary(&[(summary::BATCH_ID, "b1")]);
        assert_eq!(TestChange::Append { files: vec![], latest: None }.batch_id(&props), Some("b1"));
        assert_eq!(TestChange::Delete { removed: vec![], lvi_cutoff: None }.batch_id(&props), None);
    }

    #[test]
    fn summaries_carry_totals_rollup_sources_and_the_index() {
        let base = summary(&[
            (summary::TOTAL_RECORDS, "10"),
            (summary::TOTAL_DATA_FILES, "2"),
            (summary::TOTAL_FILES_SIZE, "100"),
            (summary::ROLLUP_SOURCE_SNAPSHOT, "7"),
            (summary::LAST_VALUE_INDEX, "old"),
            (summary::BATCH_ID, "b0"),
        ]);
        let props = summary(&[(summary::BATCH_ID, "b1")]);
        let added = FileTotals::of([(3, 30)]);
        let removed = FileTotals::of([(4, 40), (1, 10)]);
        let s = snapshot_summary(&props, &base, Some("new"), added, removed);
        let get = |k: &str| s.get(k).map(String::as_str);
        assert_eq!(get(summary::BATCH_ID), Some("b1"));
        assert_eq!(get(summary::ROLLUP_SOURCE_SNAPSHOT), Some("7"));
        assert_eq!(get(summary::LAST_VALUE_INDEX), Some("new"));
        assert_eq!((get(summary::ADDED_RECORDS), get(summary::DELETED_DATA_FILES)), (Some("3"), Some("2")));
        assert_eq!(get(summary::TOTAL_RECORDS), Some("8"));
        assert_eq!(get(summary::TOTAL_DATA_FILES), Some("1"));
        assert_eq!(get(summary::TOTAL_FILES_SIZE), Some("80"));
        assert_eq!(get(summary::TOTAL_DELETE_FILES), Some("0"));

        // Nothing added or removed: no change counters, and totals saturate rather than wrap.
        let s = snapshot_summary(&props, &summary(&[]), None, FileTotals::default(), FileTotals::of([(5, 5)]));
        assert_eq!((s.get(summary::ADDED_RECORDS), s.get(summary::LAST_VALUE_INDEX)), (None, None));
        assert_eq!(s.get(summary::TOTAL_RECORDS).map(String::as_str), Some("0"));
    }

    #[test]
    fn appends_merge_manifests_past_the_limit_and_removals_rewrite_theirs() {
        assert_eq!(manifest_plan(0, MERGE_MANIFESTS_AT - 1, 1), ManifestPlan::Carry);
        assert_eq!(manifest_plan(0, MERGE_MANIFESTS_AT, 1), ManifestPlan::Rewrite { merge: true });
        assert_eq!(manifest_plan(2, 1, 0), ManifestPlan::Rewrite { merge: false });

        let manifests =
            [vec![TestEntry("a", false), TestEntry("gone", true)], vec![TestEntry("b", false), TestEntry("c", false)]];
        let slices = || manifests.iter().map(Vec::as_slice);
        let removed: HashSet<&str> = ["c"].into();
        let r = rewrite_manifests(slices(), &removed, false).unwrap();
        assert_eq!(r.carried, [0]);
        assert_eq!(r.kept.iter().map(|e| e.0).collect::<Vec<_>>(), ["b"]);
        assert_eq!(r.removed.iter().map(|e| e.0).collect::<Vec<_>>(), ["c"]);

        let r = rewrite_manifests(slices(), &HashSet::new(), true).unwrap();
        assert!(r.carried.is_empty() && r.removed.is_empty());
        assert_eq!(r.kept.iter().map(|e| e.0).collect::<Vec<_>>(), ["a", "b", "c"], "deleted entries are dropped");

        // A file another writer removed already is a conflict.
        let removed: HashSet<&str> = ["c", "gone"].into();
        assert_eq!(rewrite_manifests(slices(), &removed, false).unwrap_err(), NotInTable { missing: 1, of: 2 });
    }

    #[test]
    fn a_metadata_put_is_judged_by_what_is_read_back() {
        let (ours, theirs): (&[u8], &[u8]) = (b"ours", b"theirs");
        for put in [None, Some("timeout")] {
            assert_eq!(read_back(put, Ok(Some(ours)), ours), Ok(true));
            assert_eq!(read_back(put, Ok(Some(theirs)), ours), Ok(false));
        }
        assert_eq!(read_back::<&str>(None, Ok(None), ours), Ok(false));
        assert_eq!(read_back(Some("timeout"), Ok(None), ours), Err("timeout"));
        assert_eq!(read_back(Some("timeout"), Err("read"), ours), Err("timeout"));
        assert_eq!(read_back(None, Err("read"), ours), Err("read"));
    }

    #[test]
    fn backoff_grows_with_the_attempt() {
        assert_eq!(backoff_millis(2, 0), 10);
        assert_eq!(backoff_millis(3, 44), 147);
        assert_eq!(backoff_millis(usize::MAX, u64::MAX), u64::MAX);
    }
}
