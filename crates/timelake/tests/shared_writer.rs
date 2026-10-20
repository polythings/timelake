//! The shared write path (`timelake_format::{append, commit}`, which the WASM writer uses) and the
//! engine write the same table in turn: each reads what the other committed, the engine queries
//! rows the shared writer appended (with dedup, the last-value index and the head), and compacts
//! them.

#![allow(clippy::unwrap_used, reason = "test code")]

mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use common::{DAY, T0, batch, opts, rows, scalar_i64, schema, temp_lake};
use timelake::storage::Storage;
use timelake::{AppendOptions, Lake, MaintainOptions, Session};
use timelake_format::append::{AppendOptions as SharedAppend, append};
use timelake_format::error::{WriteError, WriteResult};
use timelake_format::host::{Host, Listed, Upload};
use timelake_format::table::TableState;

/// The shared writer's host over the engine's storage.
struct EngineHost {
    storage: Storage,
    seed: AtomicU64,
    faults: Arc<Faults>,
    row_group_bytes: Option<usize>,
}

/// Failures to inject, and the multipart uploads still open.
#[derive(Default)]
struct Faults {
    /// The write of data (a PUT or an upload's part) that fails, counting from 1; 0 for none.
    fail_data_write: AtomicUsize,
    data_writes: AtomicUsize,
    open_uploads: AtomicI64,
    /// How the next metadata PUT fails (once): one of the `METADATA_*` constants.
    metadata_put: AtomicUsize,
}

/// The metadata PUT lands, then reports "already exists" (a retry after a try that landed).
const METADATA_LANDS_THEN_LOSES: usize = 1;
/// The metadata PUT lands, then fails (a timeout).
const METADATA_LANDS_THEN_FAILS: usize = 2;
/// The metadata PUT fails without landing.
const METADATA_FAILS: usize = 3;

impl Faults {
    fn data_write(&self, uri: &str) -> WriteResult<()> {
        let n = if uri.contains("/data/") { self.data_writes.fetch_add(1, Ordering::Relaxed) + 1 } else { 0 };
        if n > 0 && n == self.fail_data_write.load(Ordering::Relaxed) {
            return Err(WriteError::Store(format!("injected failure writing {uri}")));
        }
        Ok(())
    }
}

fn err(e: &timelake::Error) -> WriteError {
    WriteError::Store(e.to_string())
}

struct BufferedUpload<'a> {
    storage: &'a Storage,
    uri: String,
    bytes: Vec<u8>,
    faults: Arc<Faults>,
}

impl Upload for BufferedUpload<'_> {
    fn put_part(&mut self, bytes: Vec<u8>) -> impl Future<Output = WriteResult<()>> {
        self.bytes.extend(bytes);
        std::future::ready(self.faults.data_write(&self.uri))
    }

    async fn complete(self) -> WriteResult<()> {
        self.faults.open_uploads.fetch_sub(1, Ordering::Relaxed);
        self.storage.write(&self.uri, Bytes::from(self.bytes)).await.map_err(|e| err(&e))
    }

    async fn abort(self) {
        self.faults.open_uploads.fetch_sub(1, Ordering::Relaxed);
    }
}

impl EngineHost {
    fn new(lake: &Lake) -> Self {
        Self { storage: lake.storage().clone(), seed: AtomicU64::new(7), faults: Arc::default(), row_group_bytes: None }
    }
}

impl Host for &EngineHost {
    type Upload = BufferedUpload<'static>;

    async fn get(&self, uri: &str) -> WriteResult<Option<Vec<u8>>> {
        Ok(self.storage.read_opt(uri).await.map_err(|e| err(&e))?.map(|b| b.to_vec()))
    }

    async fn get_range(&self, uri: &str, start: u64, len: u64) -> WriteResult<Vec<u8>> {
        let bytes = self.storage.read(uri).await.map_err(|e| err(&e))?;
        let start = usize::try_from(start).unwrap().min(bytes.len());
        let end = start.saturating_add(usize::try_from(len).unwrap()).min(bytes.len());
        Ok(bytes[start..end].to_vec())
    }

    async fn exists(&self, uri: &str) -> WriteResult<bool> {
        self.storage.exists(uri).await.map_err(|e| err(&e))
    }

    async fn put(&self, uri: &str, bytes: Vec<u8>) -> WriteResult<()> {
        self.faults.data_write(uri)?;
        self.storage.write(uri, Bytes::from(bytes)).await.map_err(|e| err(&e))
    }

    async fn put_if_absent(&self, uri: &str, bytes: Vec<u8>) -> WriteResult<bool> {
        let fault =
            if uri.ends_with(".metadata.json") { self.faults.metadata_put.swap(0, Ordering::Relaxed) } else { 0 };
        if fault == METADATA_FAILS {
            return Err(WriteError::Store(format!("injected failure writing {uri}")));
        }
        let written = self.storage.write_if_absent(uri, Bytes::from(bytes)).await.map_err(|e| err(&e))?;
        match fault {
            METADATA_LANDS_THEN_LOSES => Ok(false),
            METADATA_LANDS_THEN_FAILS => Err(WriteError::Store(format!("injected timeout writing {uri}"))),
            _ => Ok(written),
        }
    }

    fn upload(&self, uri: &str) -> impl Future<Output = WriteResult<BufferedUpload<'static>>> {
        let storage: &'static Storage = Box::leak(Box::new(self.storage.clone()));
        self.faults.open_uploads.fetch_add(1, Ordering::Relaxed);
        let faults = self.faults.clone();
        std::future::ready(Ok(BufferedUpload { storage, uri: uri.to_string(), bytes: vec![], faults }))
    }

    async fn list(&self, prefix: &str) -> WriteResult<Vec<Listed>> {
        let dir = &prefix[..prefix.rfind('/').unwrap()];
        let listed = self.storage.list(dir).await.map_err(|e| err(&e))?;
        Ok(listed
            .into_iter()
            .filter(|o| o.uri.starts_with(prefix))
            .map(|o| Listed { uri: o.uri, size: o.size })
            .collect())
    }

    async fn delete(&self, uris: &[String]) -> WriteResult<()> {
        self.storage.delete_many(uris.to_vec()).await.map(drop).map_err(|e| err(&e))
    }

    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    fn random(&self) -> u64 {
        let s = self.seed.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
        s ^ (s >> 31) ^ chrono::Utc::now().timestamp_nanos_opt().unwrap().unsigned_abs()
    }

    async fn sleep(&self, ms: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }

    fn max_row_group_bytes(&self) -> Option<usize> {
        self.row_group_bytes
    }
}

fn no_maintenance() -> AppendOptions {
    AppendOptions { maintain: Some(false), ..AppendOptions::default() }
}

fn shared_no_maintenance() -> SharedAppend {
    SharedAppend { maintain: Some(false), ..SharedAppend::default() }
}

/// A fresh lake on the same directory: what another process sees.
fn reopen(dir: &tempfile::TempDir) -> Lake {
    Lake::open(dir.path().to_str().unwrap(), std::collections::HashMap::new()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_and_shared_writer_take_turns() {
    let (dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let location = t.location().to_string();
    // The engine appends one day of 10 devices.
    let (t, _) =
        t.append(&[batch(T0, 24, 3_600_000_000, 10, |d, i| f64::from(d) + i as f64)], no_maintenance()).await.unwrap();
    let host = EngineHost::new(&lake);
    timelake::flush().await;

    // The shared writer loads that version from the head, and appends the next day plus a rewrite
    // of the first hour of device 0 (a newer write of keys the engine wrote).
    let state = TableState::load(&&host, &location).await.unwrap();
    assert_eq!((state.version, state.files.as_ref().map(Vec::len)), (t.version(), Some(1)));
    assert_eq!(state.schema, t.schema(), "the same Arrow schema as iceberg-rust gives");
    let rewrite = batch(T0, 1, 3_600_000_000, 1, |_, _| -1.0);
    let next = batch(T0 + DAY, 24, 3_600_000_000, 10, |d, i| 100.0 + f64::from(d) + i as f64);
    let (_, report) = append(&&host, state, &[rewrite, next], &shared_no_maintenance()).await.unwrap();
    assert_eq!((report.rows, report.files, report.commit.version), (241, 2, t.version() + 1));

    // A new process reads it through the head the shared writer wrote, then through the chain.
    for lake in [reopen(&dir), reopen(&dir)] {
        let s = Session::new(lake);
        assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r").await, 480);
        let v = rows(&s, "SELECT value FROM iot.r WHERE device = 0 AND time = TIMESTAMP '2026-01-01T00:00:00Z'").await;
        assert_eq!(common::f64_values(&v), [-1.0], "the newer write wins");
        let latest = s.latest("iot.r").await.unwrap().collect().await.unwrap();
        assert_eq!(latest.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>(), 10);
        assert!(common::f64_values(&latest).iter().all(|v| *v >= 123.0), "latest rows are the second day's");
    }

    // The engine appends on top of the shared writer's commit, the shared writer again (eight more
    // times, so it merges manifests), and the engine compacts everything.
    let t = reopen(&dir).table("iot.r").await.unwrap();
    let (t, _) = t.append(&[batch(T0 + 2 * DAY, 1, 1, 10, |_, _| 7.0)], no_maintenance()).await.unwrap();
    timelake::flush().await;
    let mut state = TableState::load(&&host, &location).await.unwrap();
    assert_eq!(state.version, t.version());
    for i in 0..8 {
        let b = batch(T0 + 2 * DAY + 1 + i, 1, 1, 10, |_, _| 8.0);
        let opts = SharedAppend { batch_id: Some(format!("b{i}")), ..shared_no_maintenance() };
        state = append(&&host, state, &[b], &opts).await.unwrap().0;
    }
    // A batch id already committed is skipped.
    let again = SharedAppend { batch_id: Some("b3".into()), ..shared_no_maintenance() };
    let (state, report) = append(&&host, state, &[batch(T0, 1, 1, 1, |_, _| 0.0)], &again).await.unwrap();
    assert!(report.commit.duplicate);
    // 11 appends: the 9th merged the 8 manifests into one, next to its own; two more since.
    assert_eq!(state.manifests(&&host).await.unwrap().len(), 4, "manifests were merged");

    let t = reopen(&dir).table("iot.r").await.unwrap();
    assert_eq!(t.version(), state.version);
    let s = Session::new(reopen(&dir));
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r").await, 480 + 90);
    let (t, report) = t.maintain(MaintainOptions { expire: false, ..MaintainOptions::default() }).await.unwrap();
    assert_eq!(report.duplicates_removed, 1, "the engine's row the shared writer rewrote");
    assert_eq!(t.files().await.unwrap().len(), 3, "one file per day");
    assert_eq!(scalar_i64(&Session::new(reopen(&dir)), "SELECT count(*) FROM iot.r").await, 570);
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_maintenance_compacts_and_expires() {
    use timelake_format::maintain::{MaintainOptions as SharedMaintain, maintain};

    let (dir, lake) = temp_lake();
    let mut o = opts();
    o.properties.insert(timelake::property::COMPACT_MIN_FILES.into(), "4".into());
    o.properties.insert(timelake::property::SNAPSHOT_RETENTION.into(), "0s".into());
    o.properties.insert(timelake::property::MIN_SNAPSHOTS.into(), "1".into());
    let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
    let location = t.location().to_string();
    let host = EngineHost::new(&lake);
    let mut state = TableState::load(&&host, &location).await.unwrap();
    // Six small appends to one day with inline maintenance: the size tier merges at four files.
    let mut merged = 0;
    for i in 0..6 {
        let b = batch(T0 + i * 3_600_000_000, 6, 600_000_000, 5, |d, j| f64::from(d) + j as f64);
        let (next, report) = append(&&host, state, &[b], &SharedAppend::default()).await.unwrap();
        state = next;
        if let Some(Ok(m)) = report.maintenance {
            merged += m.files_rewritten;
        }
    }
    assert_eq!(merged, 4, "one tier of four files merged");
    assert_eq!(reopen(&dir).table("iot.r").await.unwrap().files().await.unwrap().len(), 3);

    // A newer write of a reading, then a full compaction: one file, the newer value kept.
    let rewrite = batch(T0, 1, 1, 1, |_, _| -5.0);
    let (state, _) = append(&&host, state, &[rewrite], &shared_no_maintenance()).await.unwrap();
    let full = SharedMaintain { expire: false, ..SharedMaintain::default() };
    let (state, report) = maintain(&&host, state, &full).await.unwrap();
    assert_eq!((report.files_rewritten, report.files_written, report.duplicates_removed), (4, 1, 1));
    let s = Session::new(reopen(&dir));
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r").await, 180);
    let v = rows(&s, "SELECT value FROM iot.r WHERE device = 0 AND time = TIMESTAMP '2026-01-01T00:00:00Z'").await;
    assert_eq!(common::f64_values(&v), [-5.0]);
    let latest = s.latest("iot.r").await.unwrap().collect().await.unwrap();
    assert_eq!(latest.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>(), 5);

    // Expiry keeps the current snapshot and deletes what only older ones used; the engine still
    // reads every row, and only the compacted data file is left.
    let expire_only = SharedMaintain { compact: false, retention: false, ..SharedMaintain::default() };
    let (_, report) = maintain(&&host, state, &expire_only).await.unwrap();
    assert!(report.snapshots_expired >= 7 && report.objects_deleted > 0, "{report:?}");
    let data = walk(dir.path()).into_iter().filter(|p| p.to_string_lossy().contains("/data/")).count();
    assert_eq!(data, 1, "only the compacted data file is left");
    let s = Session::new(reopen(&dir));
    assert_eq!(scalar_i64(&s, "SELECT count(*) FROM iot.r").await, 180);
    // The engine appends and maintains on top of what the shared writer left.
    let t = reopen(&dir).table("iot.r").await.unwrap();
    let (t, _) = t.append(&[batch(T0 + DAY, 1, 1, 5, |_, _| 1.0)], AppendOptions::default()).await.unwrap();
    t.maintain(MaintainOptions::default()).await.unwrap();
    assert_eq!(scalar_i64(&Session::new(reopen(&dir)), "SELECT count(*) FROM iot.r").await, 185);
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = vec![];
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

/// Compaction reads its inputs a window of pages at a time: on a table without dedup (where a row
/// read twice would show), files large enough for many windows merge to exactly their rows.
#[tokio::test(flavor = "multi_thread")]
async fn windowed_compaction_reads_every_row_once() {
    use timelake_format::maintain::{MaintainOptions as SharedMaintain, maintain};

    let (dir, lake) = temp_lake();
    let mut o = opts();
    o.dedup = Some(false);
    let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
    let location = t.location().to_string();
    let host = EngineHost::new(&lake);
    let mut state = TableState::load(&&host, &location).await.unwrap();
    // Noisy values, so 60,000-row files are a few MB and span several windows.
    for i in 0..3 {
        let b =
            batch(T0 + i, 6_000, 3_000_000, 10, |d, j| ((j * 7919 + i64::from(d) * 104_729) % 100_003) as f64 / 7.0);
        state = append(&&host, state, &[b], &shared_no_maintenance()).await.unwrap().0;
    }
    let s = Session::new(reopen(&dir));
    let before =
        rows(&s, "SELECT count(*) AS n, sum(device) AS d, sum(CAST(round(value * 7) AS BIGINT)) AS v FROM iot.r").await;
    let (_, report) =
        maintain(&&host, state, &SharedMaintain { expire: false, ..SharedMaintain::default() }).await.unwrap();
    assert_eq!((report.files_rewritten, report.files_written, report.rows_rewritten), (3, 1, 180_000));
    let s = Session::new(reopen(&dir));
    assert_eq!(
        rows(&s, "SELECT count(*) AS n, sum(device) AS d, sum(CAST(round(value * 7) AS BIGINT)) AS v FROM iot.r").await,
        before
    );
}

/// `batches`, each in the table's schema and order, as `append` gives them to `write_files`: all
/// in one partition.
fn aligned(
    state: &TableState,
    batches: Vec<arrow::array::RecordBatch>,
) -> (Option<i32>, Vec<arrow::array::RecordBatch>) {
    let mut out = (None, vec![]);
    for b in batches {
        for (partition, rows) in
            timelake_format::rows::sort_and_split(&state.schema, &state.contract, &[b], T0).unwrap()
        {
            out = (partition, [out.1, rows].concat());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_write_deletes_the_files_it_finished() {
    use timelake_format::append::write_files;

    let (_dir, lake) = temp_lake();
    let mut o = opts();
    o.properties.insert(timelake::property::TARGET_FILE_SIZE.into(), "1".into());
    let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
    let host = EngineHost::new(&lake);
    let state = TableState::load(&&host, t.location()).await.unwrap();
    let (day, batches) = aligned(&state, (0..3).map(|i| batch(T0 + i, 1, 1, 1, |_, _| 0.0)).collect());
    assert_eq!(write_files(&&host, &state, day, &batches).await.unwrap().len(), 3, "one file per batch");

    host.faults.fail_data_write.store(host.faults.data_writes.load(Ordering::Relaxed) + 3, Ordering::Relaxed);
    let before = (&&host).list(&format!("{}/data/", t.location())).await.unwrap();
    assert!(write_files(&&host, &state, day, &batches).await.is_err());
    assert_eq!(
        (&&host).list(&format!("{}/data/", t.location())).await.unwrap(),
        before,
        "the two finished files are gone"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_write_aborts_its_multipart_upload() {
    use timelake_format::append::write_files;

    let (_dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let mut host = EngineHost::new(&lake);
    host.row_group_bytes = Some(1 << 20);
    host.faults.fail_data_write.store(2, Ordering::Relaxed);
    let state = TableState::load(&&host, t.location()).await.unwrap();
    // Values that don't compress, so the file outgrows one 8 MiB part.
    #[expect(clippy::cast_precision_loss, reason = "any values will do")]
    let rows = batch(T0, 150_000, 500_000, 10, |d, i| ((i64::from(d) * 7919 + i * 104_729) as f64).sin());
    let (day, rows) = aligned(&state, vec![rows]);
    assert!(write_files(&&host, &state, day, &rows).await.is_err());
    assert!(host.faults.data_writes.load(Ordering::Relaxed) >= 2, "an upload was started");
    assert_eq!(host.faults.open_uploads.load(Ordering::Relaxed), 0, "the upload was aborted");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shared_commit_whose_metadata_landed_is_the_commit() {
    for fault in [METADATA_LANDS_THEN_LOSES, METADATA_LANDS_THEN_FAILS] {
        let (dir, lake) = temp_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let host = EngineHost::new(&lake);
        let state = TableState::load(&&host, t.location()).await.unwrap();
        host.faults.metadata_put.store(fault, Ordering::Relaxed);
        let o = SharedAppend { batch_id: Some("b1".into()), ..shared_no_maintenance() };
        let (_, report) = append(&&host, state, &[batch(T0, 2, 1, 2, |_, _| 1.0)], &o).await.unwrap();
        let c = report.commit;
        assert_eq!((c.version, c.attempts, c.duplicate), (t.version() + 1, 1, false), "fault {fault}");
        assert_eq!(scalar_i64(&Session::new(reopen(&dir)), "SELECT count(*) FROM iot.r").await, 4, "fault {fault}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_shared_commit_outcome_deletes_nothing() {
    let (_dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let host = EngineHost::new(&lake);
    let state = TableState::load(&&host, t.location()).await.unwrap();
    host.faults.metadata_put.store(METADATA_FAILS, Ordering::Relaxed);
    let err = append(&&host, state, &[batch(T0, 2, 1, 2, |_, _| 1.0)], &shared_no_maintenance()).await.unwrap_err();
    assert!(matches!(err, WriteError::CommitUnknown(_)), "{err}");
    let data = (&&host).list(&format!("{}/data/", t.location())).await.unwrap();
    let lists = (&&host).list(&format!("{}/metadata/snap-", t.location())).await.unwrap();
    assert_eq!((data.len(), lists.len()), (1, 1), "the data file and the manifest list stay");
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_expiry_deletes_nothing_when_a_kept_snapshot_is_broken() {
    use timelake_format::maintain::{MaintainOptions as SharedMaintain, maintain};

    let (dir, lake) = temp_lake();
    let mut o = opts();
    o.properties.insert(timelake::property::SNAPSHOT_RETENTION.into(), "0s".into());
    o.properties.insert(timelake::property::MIN_SNAPSHOTS.into(), "1".into());
    let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
    let host = EngineHost::new(&lake);
    let mut state = TableState::load(&&host, t.location()).await.unwrap();
    for i in 0..3 {
        state = append(&&host, state, &[batch(T0 + i, 1, 1, 2, |_, _| 1.0)], &shared_no_maintenance()).await.unwrap().0;
    }
    let data = || async { (&&host).list(&format!("{}/data/", t.location())).await.unwrap().len() };
    assert_eq!(data().await, 3);
    let current = reopen(&dir).table("iot.r").await.unwrap().metadata().current_snapshot().unwrap().clone();
    (&&host).delete(&[current.manifest_list().to_string()]).await.unwrap();

    let expire_only = SharedMaintain { compact: false, retention: false, ..SharedMaintain::default() };
    let err = maintain(&&host, state, &expire_only).await.unwrap_err().to_string();
    assert!(err.contains(&current.snapshot_id().to_string()), "names the broken snapshot: {err}");
    assert_eq!(data().await, 3, "every data file is still there");
    assert_eq!(reopen(&dir).table("iot.r").await.unwrap().metadata().snapshots().count(), 3, "nothing expired");
}
