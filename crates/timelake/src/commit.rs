//! Timelake's own Iceberg commit layer (D13, spec 2), built on iceberg-rust's public spec types.
//!
//! iceberg-rust 0.10 only exposes fast-append transactions; compaction and retention need
//! "replace" and "delete" snapshots, and frequent appends need manifest merging so that planning a
//! scan doesn't fetch one manifest per commit. This module writes manifests, manifest lists,
//! snapshots and table metadata directly, then commits through the object-store catalog with
//! optimistic retries. The protocol's rules (summaries, the last-value index, which manifests are
//! rewritten, retries, judging a conditional PUT) are `timelake_format::commit`'s, shared with the
//! WebAssembly writer; this module does the engine's I/O, caching and concurrency around them.
//!
//! A commit takes two round trips: every new file (data, manifests, manifest list, last-value
//! index) is encoded locally and uploaded at once, then the new metadata is written with
//! put-if-absent. The version hint and the head are updated in the background.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use bytes::Bytes;
use iceberg::spec::{
    DataFile, ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriter, ManifestWriterBuilder, Operation,
    Snapshot, Summary, TableMetadata, UNASSIGNED_SEQUENCE_NUMBER,
};

use futures::future::BoxFuture;

pub use timelake_format::commit::CommitInfo;
use timelake_format::commit::{
    FileTotals, IndexAction, MAX_ATTEMPTS, ManifestEntry, ManifestPlan, backoff_millis, manifest_plan, read_back,
    rewrite_manifests, snapshot_summary,
};
use timelake_format::layout::{hint_uri, manifest_list_uri, manifest_uri, metadata_uri};

use crate::error::{Error, Result};
use crate::storage::Uploads;
use crate::table::{LiveEntry, Table};
use crate::{background, head, lvi, summary};

/// What a commit changes, with iceberg-rust's data files and the engine's manifest entries.
pub(crate) type Change = timelake_format::commit::Change<DataFile, LiveEntry>;

impl ManifestEntry for LiveEntry {
    fn path(&self) -> &str {
        self.data_file.file_path()
    }

    fn is_deleted(&self) -> bool {
        self.status == ManifestStatus::Deleted
    }
}

/// Iceberg-rust's name for the operation.
fn operation(change: &Change) -> Operation {
    match change.operation() {
        timelake_format::commit::Operation::Append => Operation::Append,
        timelake_format::commit::Operation::Replace => Operation::Replace,
        timelake_format::commit::Operation::Delete => Operation::Delete,
    }
}

/// Commits `change` on top of the latest version, rebasing and retrying when another writer
/// commits first. `props` go into the snapshot summary. `data` holds the uploads of the files
/// `change` adds: they are sent with the first attempt's metadata, or dropped if nothing commits.
pub(crate) async fn commit(
    table: &Table,
    change: Change,
    props: HashMap<String, String>,
    data: Uploads,
) -> Result<(Table, CommitInfo)> {
    let result = commit_with(table, &change, &props, &data).await;
    data.abort().await;
    result
}

async fn commit_with(
    table: &Table,
    change: &Change,
    props: &HashMap<String, String>,
    data: &Uploads,
) -> Result<(Table, CommitInfo)> {
    let started = Instant::now();
    let mut base = table.clone();
    for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
            back_off(attempt).await;
            base = base.refresh().await?;
        }
        writable(&base)?;
        if is_duplicate(&base, change, props) {
            let snapshot_id = base.metadata().current_snapshot_id().unwrap_or_default();
            let info = CommitInfo {
                snapshot_id,
                version: base.version(),
                attempts: attempt,
                duration: started.elapsed(),
                duplicate: true,
            };
            return Ok((base, info));
        }
        let writer = SnapshotWriter::new(&base, change);
        match writer.commit(props, data).await {
            Ok(Attempt::Committed(table)) => {
                let table = *table;
                let info = CommitInfo {
                    snapshot_id: writer.snapshot_id,
                    version: table.version(),
                    attempts: attempt,
                    duration: started.elapsed(),
                    duplicate: false,
                };
                return Ok((table, info));
            }
            // Another writer committed first: rebase onto it.
            Ok(Attempt::Lost) => writer.discard().await,
            // The metadata may be in the table: what it references stays.
            Err(e @ Error::CommitUnknown(_)) => return Err(e),
            Err(e) => {
                writer.discard().await;
                return Err(e);
            }
        }
    }
    Err(gave_up(table))
}

/// Waits before attempt `attempt` (from 2), as [`backoff_millis`] says.
async fn back_off(attempt: usize) {
    tokio::time::sleep(Duration::from_millis(backoff_millis(attempt, rand::random()))).await;
}

fn gave_up(table: &Table) -> Error {
    Error::Conflict(format!("{}: gave up after {MAX_ATTEMPTS} attempts", table.name()))
}

/// A change built on one version: the next version's metadata, its live files if known, and what
/// the caller needs afterwards.
pub(crate) type Next<T> = (TableMetadata, Option<Arc<Vec<LiveEntry>>>, T);

/// Publishes the change `build` makes of the latest version (no new snapshot: settings, refs,
/// expiry), building it again on the newer version each time another writer publishes first, with
/// the same limits as a commit. Returns the new version and what `build` passed on, or, when
/// `build` has nothing to change, the version it looked at.
pub(crate) async fn publish_retrying<T>(
    table: &Table,
    // A boxed future rather than an async closure, so the caller's future stays `Send`.
    mut build: impl for<'a> FnMut(&'a Table) -> BoxFuture<'a, Result<Option<Next<T>>>>,
) -> Result<(Table, Option<T>)> {
    let mut base = table.clone();
    for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
            back_off(attempt).await;
            base = base.refresh().await?;
        }
        let Some((metadata, files, then)) = build(&base).await? else { return Ok((base, None)) };
        if let Some(published) = publish(&base, metadata, files).await? {
            return Ok((published, Some(then)));
        }
    }
    Err(gave_up(table))
}

/// Refuses to write to a table of a newer format than this Timelake knows: it could break what
/// the newer format relies on.
pub(crate) fn writable(table: &Table) -> Result<()> {
    table.contract().check_writable().map_err(|e| Error::invalid(format!("{}: {e}", table.name())))
}

/// Idempotency: an append whose batch id is already in the table's history (the current snapshot
/// and its ancestors, so not snapshots a rollback left behind) is a no-op.
fn is_duplicate(base: &Table, change: &Change, props: &HashMap<String, String>) -> bool {
    let Some(id) = change.batch_id(props) else { return false };
    let metadata = base.metadata();
    let mut snapshot = metadata.current_snapshot();
    while let Some(s) = snapshot {
        if s.summary().additional_properties.get(summary::BATCH_ID).map(String::as_str) == Some(id) {
            return true;
        }
        snapshot = s.parent_snapshot_id().and_then(|p| metadata.snapshot_by_id(p));
    }
    false
}

/// Commits new table metadata as version N+1. `Ok(None)` if another writer got there first.
/// `files` are the new version's live files, if known without reading its manifests.
///
/// A conditional PUT that reports failure may have landed all the same: the version is then read
/// back and judged by [`read_back`]. If that can't tell, the error is [`Error::CommitUnknown`], and
/// callers must not delete what the metadata references.
pub(crate) async fn publish(
    base: &Table,
    metadata: TableMetadata,
    files: Option<Arc<Vec<LiveEntry>>>,
) -> Result<Option<Table>> {
    writable(base)?;
    let storage = base.lake().storage();
    let version = base.version() + 1;
    let uri = metadata_uri(base.location(), version);
    let bytes = Bytes::from(serde_json::to_vec(&metadata)?);
    let unknown = |e: &Error| Error::CommitUnknown(format!("{} v{version}: {e}", base.name()));
    // Built first: metadata this Timelake can't load is never committed.
    let mut table =
        Table::new(base.lake().clone(), base.name().clone(), base.location().to_string(), version, metadata)?;
    let ours = match storage.write_if_absent(&uri, bytes.clone()).await {
        Ok(true) => true,
        put => {
            let found = storage.read_opt(&uri).await;
            read_back(put.as_ref().err(), found.as_ref().map(|f| f.as_deref()), &bytes).map_err(unknown)?
        }
    };
    if !ours {
        return Ok(None);
    }
    if let Some(files) = files {
        table = table.with_files(files);
    }
    base.lake().remember(&table);
    update_hint(&table);
    head::update(&table);
    Ok(Some(table))
}

/// Points the version hint at `table`'s version, in the background. The commit is done: readers
/// probe past a stale hint, so a failed update is ignored rather than turning a committed snapshot
/// into an error (the caller would delete files the snapshot uses).
fn update_hint(table: &Table) {
    let (lake, name, version) = (table.lake().clone(), table.name().clone(), table.version());
    let uri = hint_uri(table.location());
    background::spawn(async move {
        // A newer commit from this process writes its own hint.
        if lake.has_newer(&name, version) {
            return;
        }
        let _ = lake.storage().write(&uri, Bytes::from(version.to_string())).await;
    });
}

enum Attempt {
    Committed(Box<Table>),
    /// Another writer committed this version first.
    Lost,
}

/// One commit attempt: writes the files of a new snapshot on top of `base` and publishes it.
/// Every file is recorded before it is written, so [`SnapshotWriter::discard`] can remove what
/// an attempt that didn't commit left behind.
struct SnapshotWriter<'a> {
    base: &'a Table,
    change: &'a Change,
    snapshot_id: i64,
    sequence_number: i64,
    written: Mutex<Vec<String>>,
    /// Files of this attempt, encoded and waiting to be sent.
    uploads: Uploads,
}

/// Manifests of the new snapshot.
struct Manifests {
    /// Written by this attempt, listed first.
    new: Vec<ManifestFile>,
    /// Unchanged from the base snapshot.
    carried: Vec<ManifestFile>,
    /// Entries of each manifest written, for the cache.
    entries: Vec<(String, Vec<LiveEntry>)>,
}

/// The last-value index of the new snapshot.
struct LastValueIndex {
    /// Location to record in the summary.
    location: Option<String>,
    /// The index, if this attempt wrote a new one (at `location`).
    written: Option<RecordBatch>,
}

impl<'a> SnapshotWriter<'a> {
    fn new(base: &'a Table, change: &'a Change) -> Self {
        let metadata = base.metadata();
        let snapshot_id = loop {
            let id = rand::random_range(1..i64::MAX);
            if metadata.snapshot_by_id(id).is_none() {
                break id;
            }
        };
        let sequence_number = metadata.next_sequence_number();
        Self { base, change, snapshot_id, sequence_number, written: Mutex::default(), uploads: Uploads::default() }
    }

    async fn commit(&self, props: &HashMap<String, String>, data: &Uploads) -> Result<Attempt> {
        let base_summary =
            self.base.current_snapshot().map(|s| s.summary().additional_properties.clone()).unwrap_or_default();
        // Manifests and the last-value index don't depend on each other: prepare them concurrently.
        let (manifests, index) = futures::join!(self.write_manifests(), self.write_index(&base_summary));
        let (manifests, index) = (manifests?, index?);

        let list: Vec<ManifestFile> = manifests.new.into_iter().chain(manifests.carried).collect();
        let list_uri = self.write_manifest_list(&list).await?;
        // One round of uploads: the data files (on the first attempt) and this attempt's files.
        // Both run to their end, so neither is dropped halfway with uploads open.
        let (data_sent, sent) = futures::join!(data.send(), self.uploads.send());
        data_sent.and(sent)?;
        let (added, removed) = (self.change.added(), self.change.removed());
        let added = FileTotals::of(added.iter().map(|f| (f.record_count(), f.file_size_in_bytes())));
        let removed =
            FileTotals::of(removed.iter().map(|e| (e.data_file.record_count(), e.data_file.file_size_in_bytes())));
        let summary = snapshot_summary(props, &base_summary, index.location.as_deref(), added, removed);
        let metadata = self.base.metadata();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(metadata.current_snapshot_id())
            .with_sequence_number(self.sequence_number)
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .with_manifest_list(list_uri.clone())
            .with_summary(Summary { operation: operation(self.change), additional_properties: summary })
            .with_schema_id(metadata.current_schema_id())
            .build();
        let new_metadata = self.base.next_metadata().set_branch_snapshot(snapshot, "main")?.build()?.metadata;
        let files = self.next_files();
        let Some(table) = publish(self.base, new_metadata, files).await? else { return Ok(Attempt::Lost) };

        // Prime the caches with what this commit wrote, so the next commit and inline maintenance
        // don't read it back from object storage.
        let caches = self.base.lake().caches();
        for (path, entries) in manifests.entries {
            caches.manifests.insert(path, Arc::new(entries));
        }
        caches.manifest_lists.insert(list_uri, Arc::new(self.with_sequence_numbers(list)));
        if let (Some(location), Some(batch)) = (index.location, index.written) {
            caches.last_value_indexes.insert(location, batch);
        }
        Ok(Attempt::Committed(Box::new(table)))
    }

    /// Deletes everything this attempt wrote. Leftovers are unreferenced, so a failed cleanup
    /// only wastes space; it is ignored rather than hiding the attempt's own outcome.
    async fn discard(self) {
        self.uploads.abort().await;
        let written = self.written.into_inner().unwrap_or_else(PoisonError::into_inner);
        let _ = self.base.lake().storage().delete_many(written).await;
    }

    /// Records `uri` as written by this attempt (call before writing it).
    fn track(&self, uri: &str) {
        self.written.lock().unwrap_or_else(PoisonError::into_inner).push(uri.to_string());
    }

    /// The `i`th file this commit adds, as its manifest entry.
    fn added_entry(&self, i: usize, file: &DataFile) -> LiveEntry {
        LiveEntry {
            status: ManifestStatus::Added,
            data_file: Arc::new(file.clone()),
            snapshot_id: self.snapshot_id,
            sequence_number: self.change.data_sequence(i).unwrap_or(self.sequence_number),
            file_sequence_number: self.sequence_number,
        }
    }

    /// The new snapshot's live files, when the base's are known without reading manifests.
    fn next_files(&self) -> Option<Arc<Vec<LiveEntry>>> {
        let base = self.base.known_files()?;
        let removed: HashSet<&str> = self.change.removed().iter().map(|e| e.data_file.file_path()).collect();
        let mut files: Vec<LiveEntry> =
            self.change.added().iter().enumerate().map(|(i, f)| self.added_entry(i, f)).collect();
        files.extend(base.iter().filter(|e| !removed.contains(e.data_file.file_path())).cloned());
        Some(Arc::new(files))
    }

    fn manifest_writer(&self, kind: &str) -> (String, ManifestWriter) {
        let uri = manifest_uri(self.base.location(), uuid::Uuid::new_v4(), kind);
        self.track(&uri);
        let metadata = self.base.metadata();
        let writer = ManifestWriterBuilder::new(
            self.base.lake().storage().deferred_output_file(&uri, &self.uploads),
            Some(self.snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        (uri, writer)
    }

    /// Writes a manifest of the added files, then carries the base snapshot's manifests over or
    /// rewrites them, as [`manifest_plan`] says.
    async fn write_manifests(&self) -> Result<Manifests> {
        let mut out = Manifests { new: vec![], carried: vec![], entries: vec![] };
        let added = self.change.added();
        if !added.is_empty() {
            let (uri, mut w) = self.manifest_writer("added");
            let mut entries = Vec::with_capacity(added.len());
            for (i, f) in added.iter().enumerate() {
                w.add_file(f.clone(), self.change.data_sequence(i).unwrap_or(UNASSIGNED_SEQUENCE_NUMBER))?;
                entries.push(self.added_entry(i, f));
            }
            out.new.push(w.write_manifest_file().await?);
            out.entries.push((uri, entries));
        }

        let current = self.base.manifests().await?;
        let removed: HashSet<&str> = self.change.removed().iter().map(ManifestEntry::path).collect();
        let ManifestPlan::Rewrite { merge } = manifest_plan(removed.len(), current.len(), out.new.len()) else {
            out.carried = current.to_vec();
            return Ok(out);
        };
        let lake = self.base.lake();
        let loaded = futures::future::try_join_all(current.iter().map(|m| lake.manifest(m))).await?;
        let rewritten = rewrite_manifests(loaded.iter().map(|m| m.as_slice()), &removed, merge)
            .map_err(|e| Error::Conflict(format!("{}: {e}", self.base.name())))?;
        out.carried = rewritten.carried.iter().map(|&i| current[i].clone()).collect();
        if !rewritten.kept.is_empty() {
            let kept: Vec<LiveEntry> =
                rewritten.kept.iter().map(|e| LiveEntry { status: ManifestStatus::Existing, ..(*e).clone() }).collect();
            let (uri, mut w) = self.manifest_writer("merged");
            for e in &kept {
                let file = e.data_file.as_ref().clone();
                w.add_existing_file(file, e.snapshot_id, e.sequence_number, Some(e.file_sequence_number))?;
            }
            out.new.push(w.write_manifest_file().await?);
            out.entries.push((uri, kept));
        }
        if !rewritten.removed.is_empty() {
            let deleted: Vec<LiveEntry> = rewritten
                .removed
                .iter()
                .map(|e| LiveEntry { status: ManifestStatus::Deleted, snapshot_id: self.snapshot_id, ..(*e).clone() })
                .collect();
            let (uri, mut w) = self.manifest_writer("deleted");
            for e in &deleted {
                w.add_delete_file(e.data_file.as_ref().clone(), e.sequence_number, Some(e.file_sequence_number))?;
            }
            out.new.push(w.write_manifest_file().await?);
            out.entries.push((uri, deleted));
        }
        Ok(out)
    }

    /// The last-value index for the new snapshot, as [`Change::index_action`] says.
    async fn write_index(&self, base_summary: &HashMap<String, String>) -> Result<LastValueIndex> {
        let base = self.base;
        let time_column = &base.contract().time_column;
        let unchanged = || LastValueIndex { location: base.last_value_index(), written: None };
        let new = match self.change.index_action(base_summary, time_column.is_some()) {
            IndexAction::Keep => return Ok(unchanged()),
            IndexAction::Drop => None,
            IndexAction::Extend(latest) => lvi::merged(base, latest).await?,
            IndexAction::Trim(cutoff) => match (base.last_value_index(), time_column) {
                (Some(location), Some(time)) => {
                    Some(timelake_format::lvi::retain_since(&lvi::read(base, &location).await?, time, cutoff)?)
                }
                _ => return Ok(unchanged()),
            },
            IndexAction::Set(latest) => Some(latest.clone()),
        };
        let Some(index) = new else { return Ok(LastValueIndex { location: None, written: None }) };
        let location = lvi::new_uri(base, self.snapshot_id);
        self.track(&location);
        let bytes = Bytes::from(timelake_format::lvi::encode(&index)?);
        self.uploads.put(base.lake().storage(), &location, bytes)?;
        Ok(LastValueIndex { location: Some(location), written: Some(index) })
    }

    async fn write_manifest_list(&self, manifests: &[ManifestFile]) -> Result<String> {
        let base = self.base;
        let uri = manifest_list_uri(base.location(), self.snapshot_id, uuid::Uuid::new_v4());
        self.track(&uri);
        let output = base.lake().storage().deferred_output_file(&uri, &self.uploads).writer().await?;
        let parent = base.metadata().current_snapshot_id();
        let mut w = ManifestListWriter::v2(output, self.snapshot_id, parent, self.sequence_number);
        w.add_manifests(manifests.iter().cloned())?;
        w.close().await?;
        Ok(uri)
    }

    /// The manifest list as readers will see it: sequence numbers this commit assigned filled in.
    fn with_sequence_numbers(&self, mut list: Vec<ManifestFile>) -> Vec<ManifestFile> {
        for mf in &mut list {
            if mf.sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
                mf.sequence_number = self.sequence_number;
            }
            if mf.min_sequence_number == UNASSIGNED_SEQUENCE_NUMBER {
                mf.min_sequence_number = self.sequence_number;
            }
        }
        list
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use timelake_format::layout::{hint_uri, metadata_uri};

    use crate::testing::{Op, T0, batch, faulty_lake, opts, reopen, schema};
    use crate::{AppendOptions, Error};

    fn no_maintenance() -> AppendOptions {
        AppendOptions { maintain: Some(false), ..Default::default() }
    }

    /// Manifests and the last-value index are written concurrently; if one fails, the files the
    /// other wrote are removed too.
    #[tokio::test]
    async fn failed_index_write_leaves_no_files() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        let before = store.paths().await;

        store.fail(Op::Put, "/lvi-");
        t.append(&[batch(&[(T0 + 1, 1, 2.0)])], no_maintenance()).await.unwrap_err();
        assert_eq!(store.paths().await, before);
    }

    #[tokio::test]
    async fn failed_manifest_write_leaves_no_files() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        let before = store.paths().await;

        store.fail(Op::Put, "-m.avro");
        t.append(&[batch(&[(T0 + 1, 1, 2.0)])], no_maintenance()).await.unwrap_err();
        assert_eq!(store.paths().await, before);
    }

    /// A table of a newer patch format is read and written; of a newer minor (before 1.0), neither.
    #[tokio::test]
    async fn checks_the_table_format() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.new", &schema(), opts()).await.unwrap();
        assert_eq!(t.contract().format_version, timelake_format::FORMAT_VERSION);

        let newer = |patch, minor| {
            let v = timelake_format::FORMAT_VERSION;
            timelake_format::FormatVersion::new(v.major, v.minor + minor, v.patch + patch).to_string()
        };
        let set_format = |t, v| async move {
            crate::alter::write_properties(&t, &[(crate::property::FORMAT_VERSION.into(), v)], &[]).await
        };
        let t = lake.create_table("iot.patch", &schema(), opts()).await.unwrap();
        let t = set_format(t, newer(1, 0)).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();

        // This Timelake doesn't commit a version it can't read.
        let err = set_format(t.clone(), newer(0, 1)).await.map(|_| ()).unwrap_err();
        assert!(matches!(err, Error::Invalid(ref m) if m.contains("upgrade Timelake")), "{err}");
        assert_eq!(lake.table("iot.patch").await.unwrap().version(), t.version());
        // A newer one does.
        let props = [(crate::property::FORMAT_VERSION.to_string(), newer(0, 1))].into();
        let previous = Some(metadata_uri(t.location(), t.version()));
        let metadata = iceberg::spec::TableMetadataBuilder::new_from_metadata(t.metadata().clone(), previous)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let uri = metadata_uri(t.location(), t.version() + 1);
        lake.storage().write(&uri, serde_json::to_vec(&metadata).unwrap().into()).await.unwrap();
        let err = lake.table("iot.patch").await.map(|_| ()).unwrap_err();
        assert!(matches!(err, Error::Invalid(ref m) if m.contains("newer than this Timelake reads")), "{err}");
    }

    /// The version hint is only a shortcut for readers: failing to update it doesn't fail the commit.
    #[tokio::test]
    async fn commit_survives_failed_hint_update() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        store.fail(Op::Put, "version-hint");
        let (t, r) = t.append(&[batch(&[(T0, 1, 1.0), (T0, 2, 2.0)])], no_maintenance()).await.unwrap();
        assert_eq!((t.version(), r.rows), (2, 2));
        store.heal();
        let reloaded = lake.table("iot.r").await.unwrap();
        assert_eq!(reloaded.version(), 2);
        assert_eq!(reloaded.files().await.unwrap().len(), 1);
    }

    /// The commit doesn't wait for the version hint; it is written in the background.
    #[tokio::test]
    async fn hint_is_updated_in_the_background() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let hint = lake.storage().read(&hint_uri(t.location())).await.unwrap();
        assert_eq!(hint.as_ref(), b"2");
        assert!(store.paths().await.iter().any(|p| p.ends_with("v2.metadata.json")));
    }

    /// A writer that keeps losing the commit race gives up with a conflict and leaves nothing behind.
    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        crate::flush().await;
        let before = store.paths().await;

        store.fail(Op::LoseCreate, ".metadata.json");
        let err = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err}");
        assert_eq!(store.paths().await, before);
    }

    /// A commit whose metadata PUT landed but reported a failure (a retry's 412, or a timeout)
    /// is the commit: the batch is in the table once, and nothing it references is deleted.
    #[tokio::test]
    async fn a_metadata_put_that_landed_is_the_commit() {
        for fault in [Op::LandThenLose, Op::LandThenFail] {
            let (lake, store) = faulty_lake();
            let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
            store.fail(fault, "v2.metadata.json");
            let o = AppendOptions { batch_id: Some("b1".into()), ..no_maintenance() };
            let (t, r) = t.append(&[batch(&[(T0, 1, 1.0), (T0, 2, 2.0)])], o.clone()).await.unwrap();
            assert_eq!((t.version(), r.commit.attempts, r.commit.duplicate), (2, 1, false), "{fault:?}");
            store.heal();
            crate::flush().await;
            let (t, r) =
                reopen(&store).table("iot.r").await.unwrap().append(&[batch(&[(T0, 1, 1.0)])], o).await.unwrap();
            assert!(r.commit.duplicate, "{fault:?}");
            let files = t.files().await.unwrap();
            assert_eq!(files.iter().map(|f| f.data_file.record_count()).sum::<u64>(), 2, "{fault:?}");
            let paths = store.paths().await;
            for f in &files {
                assert!(paths.iter().any(|p| f.data_file.file_path().ends_with(p.as_str())), "{fault:?}");
            }
        }
    }

    /// When it can't be told whether the metadata PUT landed, the commit fails without deleting
    /// anything it wrote: the PUT may still land, and then the version needs its files.
    #[tokio::test]
    async fn an_unknown_commit_outcome_deletes_nothing() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        crate::flush().await;
        let before = store.paths().await;
        store.fail(Op::Put, "v2.metadata.json");
        let err = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap_err();
        assert!(matches!(err, Error::CommitUnknown(_)), "{err}");
        let after = store.paths().await;
        let new: Vec<_> = after.iter().filter(|p| !before.contains(p)).collect();
        assert!(
            new.iter().any(|p| p.contains("/data/")) && new.iter().any(|p| p.contains("/metadata/snap-")),
            "{new:?}"
        );
    }

    /// Round trips an append waits for, one after another, on a store where each request takes
    /// 100 ms of (paused, simulated) time.
    async fn append_round_trips(maintain: bool) -> u128 {
        const RTT: Duration = Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let o = AppendOptions { maintain: Some(maintain), ..Default::default() };
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], o.clone()).await.unwrap();
        store.set_latency(RTT);
        let started = tokio::time::Instant::now();
        t.append(&[batch(&[(T0 + 1, 1, 2.0), (T0 + 1, 2, 3.0)])], o).await.unwrap();
        started.elapsed().as_millis() / RTT.as_millis()
    }

    #[tokio::test(start_paused = true)]
    async fn append_takes_two_round_trips() {
        assert_eq!(append_round_trips(false).await, 2);
        assert_eq!(append_round_trips(true).await, 2);
    }

    /// Snapshot summaries carry Iceberg's running totals.
    #[tokio::test]
    async fn summary_totals() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0), (T0, 2, 2.0)])], no_maintenance()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0 + 1, 1, 3.0)])], no_maintenance()).await.unwrap();
        assert_eq!(t.summary_value(crate::summary::TOTAL_RECORDS), Some("3"));
        assert_eq!(t.summary_value(crate::summary::TOTAL_DATA_FILES), Some("2"));
        assert_eq!(t.summary_value(crate::summary::ADDED_RECORDS), Some("1"));
        assert_eq!(t.summary_value(crate::summary::DELETED_RECORDS), None);
    }
}
