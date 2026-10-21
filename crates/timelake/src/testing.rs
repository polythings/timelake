//! Test support: an in-memory object store that fails chosen operations or adds latency, and table
//! fixtures.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arrow::array::{Float64Array, Int32Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};

use iceberg::spec::DataFileBuilder;

use crate::commit::{Change, commit};
use crate::storage::{Storage, Uploads};
use crate::write::{sort_and_split, write_partition, writer_properties};
use crate::{Lake, Table, TableOptions};

/// Which operation a [`Fault`] breaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    /// Reads (`get`, ranged reads, `head`).
    Get,
    /// Writes of any kind.
    Put,
    /// Put-if-absent reports "already exists", as if another writer won the commit race.
    LoseCreate,
    /// Put-if-absent writes the object, then reports "already exists": a retry after a first try
    /// that landed but returned an error.
    LandThenLose,
    /// Put-if-absent writes the object, then fails: a timeout after the write landed.
    LandThenFail,
    /// A part of a multipart upload.
    PutPart,
    /// Completing a multipart upload, which then stays open (as on S3).
    Complete,
}

/// Fails `op` on every path containing `pattern`.
#[derive(Clone, Debug)]
pub(crate) struct Fault {
    pub op: Op,
    pub pattern: String,
}

/// An in-memory store with switchable faults and latency, which counts the multipart uploads left
/// open.
#[derive(Debug, Default)]
pub(crate) struct FaultyStore {
    inner: InMemory,
    faults: Arc<Mutex<Vec<Fault>>>,
    latency: Arc<Mutex<Duration>>,
    open_uploads: Arc<AtomicUsize>,
}

impl FaultyStore {
    /// Multipart uploads started and neither completed nor aborted.
    pub(crate) fn open_uploads(&self) -> usize {
        self.open_uploads.load(Ordering::SeqCst)
    }

    /// Delays every request by `latency`, like a round trip to a remote store. With Tokio's clock
    /// paused, a test's elapsed time divided by `latency` is the number of requests made one after
    /// another.
    pub(crate) fn set_latency(&self, latency: Duration) {
        *self.latency.lock().unwrap_or_else(PoisonError::into_inner) = latency;
    }

    async fn round_trip(&self) {
        round_trip(&self.latency).await;
    }

    pub(crate) fn fail(&self, op: Op, pattern: &str) {
        self.faults().push(Fault { op, pattern: pattern.to_string() });
    }

    pub(crate) fn heal(&self) {
        self.faults().clear();
    }

    /// Every object path in the store.
    pub(crate) async fn paths(&self) -> Vec<String> {
        let mut out: Vec<String> = self.inner.list(None).map(|m| m.unwrap().location.to_string()).collect().await;
        out.sort();
        out
    }

    fn faults(&self) -> std::sync::MutexGuard<'_, Vec<Fault>> {
        self.faults.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn check(&self, op: Op, path: &Path) -> object_store::Result<()> {
        check(&self.faults, op, path)
    }
}

async fn round_trip(latency: &Mutex<Duration>) {
    let latency = *latency.lock().unwrap_or_else(PoisonError::into_inner);
    if !latency.is_zero() {
        tokio::time::sleep(latency).await;
    }
}

/// The error `faults` inject for `op` on `path`, if any.
fn check(faults: &Mutex<Vec<Fault>>, op: Op, path: &Path) -> object_store::Result<()> {
    let faults = faults.lock().unwrap_or_else(PoisonError::into_inner);
    if !faults.iter().any(|f| f.op == op && path.as_ref().contains(&f.pattern)) {
        return Ok(());
    }
    let path = path.to_string();
    Err(match op {
        Op::LoseCreate | Op::LandThenLose => object_store::Error::AlreadyExists { path, source: "injected".into() },
        Op::Get | Op::Put | Op::LandThenFail | Op::PutPart | Op::Complete => {
            object_store::Error::Generic { store: "faulty", source: format!("{op:?} {path}").into() }
        }
    })
}

/// A multipart upload of a [`FaultyStore`].
#[derive(Debug)]
struct FaultyUpload {
    inner: Box<dyn MultipartUpload>,
    path: Path,
    faults: Arc<Mutex<Vec<Fault>>>,
    latency: Arc<Mutex<Duration>>,
    open_uploads: Arc<AtomicUsize>,
    open: bool,
}

impl FaultyUpload {
    fn close(&mut self) {
        if std::mem::take(&mut self.open) {
            self.open_uploads.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl MultipartUpload for FaultyUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        match check(&self.faults, Op::PutPart, &self.path) {
            Ok(()) => self.inner.put_part(data),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        round_trip(&self.latency).await;
        check(&self.faults, Op::Complete, &self.path)?;
        let result = self.inner.complete().await?;
        self.close();
        Ok(result)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await?;
        self.close();
        Ok(())
    }
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FaultyStore")
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.round_trip().await;
        self.check(Op::Put, location)?;
        if opts.mode == PutMode::Create {
            self.check(Op::LoseCreate, location)?;
            let landed = [Op::LandThenLose, Op::LandThenFail].map(|op| self.check(op, location));
            if let Some(Err(e)) = landed.into_iter().find(Result::is_err) {
                self.inner.put_opts(location, payload, opts).await?;
                return Err(e);
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.round_trip().await;
        self.check(Op::Put, location)?;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        self.open_uploads.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FaultyUpload {
            inner,
            path: location.clone(),
            faults: Arc::clone(&self.faults),
            latency: Arc::clone(&self.latency),
            open_uploads: Arc::clone(&self.open_uploads),
            open: true,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.round_trip().await;
        self.check(Op::Get, location)?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
        self.round_trip().await;
        self.check(Op::Get, location)?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let latency = *self.latency.lock().unwrap_or_else(PoisonError::into_inner);
        let delay = futures::stream::once(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
        });
        delay.filter_map(|()| async { None }).chain(self.inner.list(prefix)).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.round_trip().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Appends rows the way another Iceberg writer would: without a last-value index.
pub(crate) async fn append_without_index(table: &Table, batches: &[RecordBatch]) -> Table {
    let props = writer_properties(table).unwrap();
    let uploads = Uploads::default();
    let mut files = vec![];
    for (partition, batches) in sort_and_split(table, batches).unwrap() {
        files.extend(write_partition(table, partition, batches, &props, &uploads).await.unwrap());
    }
    commit(table, Change::Append { files, latest: None }, HashMap::new(), uploads).await.unwrap().0
}

/// Appends rows as another Iceberg engine might: in the given order (unsorted, keys may repeat),
/// to one partition, without a sort order id or a last-value index.
pub(crate) async fn append_unsorted(table: &Table, partition: Option<i32>, batch: RecordBatch) -> Table {
    let props = writer_properties(table).unwrap();
    let uploads = Uploads::default();
    let spec_id = table.metadata().default_partition_spec().spec_id();
    let batch = match &table.contract().ingest_time_column {
        Some(c) => crate::write::with_ingest_time(&batch, c, 0).unwrap(),
        None => batch,
    };
    let batch = crate::schema::align_batch(&batch, &table.schema()).unwrap();
    let files = write_partition(table, partition, [batch], &props, &uploads).await.unwrap().into_iter().map(|f| {
        DataFileBuilder::default()
            .content(f.content_type())
            .file_path(f.file_path().to_string())
            .file_format(f.file_format())
            .partition(f.partition().clone())
            .partition_spec_id(spec_id)
            .record_count(f.record_count())
            .file_size_in_bytes(f.file_size_in_bytes())
            .column_sizes(f.column_sizes().clone())
            .value_counts(f.value_counts().clone())
            .null_value_counts(f.null_value_counts().clone())
            .lower_bounds(f.lower_bounds().clone())
            .upper_bounds(f.upper_bounds().clone())
            .build()
            .unwrap()
    });
    commit(table, Change::Append { files: files.collect(), latest: None }, HashMap::new(), uploads).await.unwrap().0
}

/// A lake on a [`FaultyStore`].
pub(crate) fn faulty_lake() -> (Lake, Arc<FaultyStore>) {
    let store = Arc::new(FaultyStore::default());
    let lake = Lake::with_storage(Storage::from_store(store.clone(), "memory://test"), "memory://test/wh");
    (lake, store)
}

/// A lake on a [`FaultyStore`] that uploads files of more than 1 KiB in parts.
pub(crate) fn faulty_lake_multipart() -> (Lake, Arc<FaultyStore>) {
    let store = Arc::new(FaultyStore::default());
    let storage = Storage::from_store(store.clone(), "memory://test").upload_buffer(1 << 10);
    (Lake::with_storage(storage, "memory://test/wh"), store)
}

/// Another lake on the same store, with cold caches: what a new process sees.
pub(crate) fn reopen(store: &Arc<FaultyStore>) -> Lake {
    Lake::with_storage(Storage::from_store(store.clone(), "memory://test"), "memory://test/wh")
}

/// Like [`reopen`], with `timelake_version_lookup=hint`.
pub(crate) fn reopen_hint_lookup(store: &Arc<FaultyStore>) -> Lake {
    Lake::with_storage(Storage::from_store(store.clone(), "memory://test").hint_lookup(), "memory://test/wh")
}

pub(crate) const T0: i64 = 1_767_225_600_000_000; // 2026-01-01T00:00:00Z

pub(crate) fn schema() -> Schema {
    Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("device", DataType::Int32, true),
        Field::new("value", DataType::Float64, true),
    ])
}

/// A time-series table keyed by device.
pub(crate) fn opts() -> TableOptions {
    TableOptions { time_column: Some("time".into()), series_key: vec!["device".into()], ..Default::default() }
}

/// `(time µs, device, value)` rows.
pub(crate) fn batch(rows: &[(i64, i32, f64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema()),
        vec![
            Arc::new(TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.0)).with_timezone("UTC")),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}
