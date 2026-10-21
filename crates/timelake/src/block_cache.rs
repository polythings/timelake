//! A cache of object blocks under the object store (spec 22, P5). Every read of an immutable
//! object (data files, manifests, manifest lists, last-value indexes: `*.parquet` and `*.avro`,
//! whose names Timelake never reuses) goes through it, in aligned blocks: in memory, and optionally
//! on local disk, shared by every process on the machine. Everything else (the version hint, the
//! head, listings, `vN.metadata.json`) goes to the store as before, so freshness doesn't change.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::future::{BoxFuture, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use serde::{Deserialize, Serialize};

/// What the cache holds and where.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CacheConfig {
    /// Block size in bytes.
    pub block: u64,
    /// The largest GET in bytes (rounded down to whole blocks, at least one); 0: no limit.
    pub request: u64,
    /// Memory budget in bytes.
    pub memory: u64,
    /// The disk cache: its directory and budget in bytes.
    pub disk: Option<(PathBuf, u64)>,
}

/// Counts of the cache's work since the lake was opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Blocks read from memory.
    pub memory_hits: u64,
    /// Blocks read from the disk cache.
    pub disk_hits: u64,
    /// Blocks fetched from the store.
    pub misses: u64,
    /// Bytes fetched from the store for the cache.
    pub bytes_fetched: u64,
}

#[derive(Debug, Default)]
struct Counters {
    memory_hits: AtomicU64,
    disk_hits: AtomicU64,
    misses: AtomicU64,
    bytes_fetched: AtomicU64,
}

/// What the cache knows of an object: enough to answer reads of it without the store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ObjectInfo {
    /// `<store>/<path>`: checked on disk, where directories are named by its hash.
    key: String,
    size: u64,
    last_modified: DateTime<Utc>,
    e_tag: Option<String>,
}

/// Objects written at most this fraction of the memory budget go into the cache when written, so a
/// writer reads its own appends without requests; compaction's large outputs don't evict the rest.
const WRITE_THROUGH_SHARE: u64 = 8;
/// How many objects' sizes are kept in memory.
const KNOWN_OBJECTS: usize = 1 << 16;
/// Disk blocks used again get a new modification time (their age for eviction) at most this often.
const TOUCH_AFTER: Duration = Duration::from_secs(60);
/// The disk cache is swept for its budget after about this share of it has been written.
const SWEEP_SHARE: u64 = 10;

/// An object store whose reads of immutable objects go through a block cache.
pub(crate) struct CachedStore {
    inner: Arc<dyn ObjectStore>,
    /// Names the store in disk keys: two stores never share a key.
    scope: String,
    config: CacheConfig,
    memory: Mutex<MemoryBlocks>,
    objects: crate::cache::BoundedCache<Path, ObjectInfo>,
    disk: Option<DiskBlocks>,
    inflight: Mutex<HashMap<(Path, u64), Fetch>>,
    counters: Counters,
}

/// A block on its way from the store, shared by every read waiting for it.
type Fetch = Shared<BoxFuture<'static, Result<Bytes, Arc<object_store::Error>>>>;

impl CachedStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, scope: String, config: CacheConfig) -> Self {
        let disk = config.disk.as_ref().map(|(dir, budget)| DiskBlocks::new(dir.join("blocks-v1"), *budget));
        Self {
            inner,
            scope,
            memory: Mutex::new(MemoryBlocks::new(config.memory)),
            objects: crate::cache::BoundedCache::new(KNOWN_OBJECTS),
            disk,
            config,
            inflight: Mutex::default(),
            counters: Counters::default(),
        }
    }

    pub(crate) fn stats(&self) -> CacheStats {
        let c = &self.counters;
        CacheStats {
            memory_hits: c.memory_hits.load(Ordering::Relaxed),
            disk_hits: c.disk_hits.load(Ordering::Relaxed),
            misses: c.misses.load(Ordering::Relaxed),
            bytes_fetched: c.bytes_fetched.load(Ordering::Relaxed),
        }
    }

    fn key(&self, location: &Path) -> String {
        format!("{}/{location}", self.scope)
    }

    /// The object's size and metadata, if the cache knows them (memory, then disk).
    async fn info(&self, location: &Path) -> Option<ObjectInfo> {
        if let Some(info) = self.objects.get(location) {
            return Some(info);
        }
        let info = self.disk.as_ref()?.info(&self.key(location)).await?;
        self.objects.insert(location.clone(), info.clone());
        Some(info)
    }

    fn learn(&self, location: &Path, meta: &ObjectMeta) -> ObjectInfo {
        let info = ObjectInfo {
            key: self.key(location),
            size: meta.size,
            last_modified: meta.last_modified,
            e_tag: meta.e_tag.clone(),
        };
        self.objects.insert(location.clone(), info.clone());
        info
    }

    /// The expected length of block `index` of an object of `size` bytes.
    fn block_len(&self, index: u64, size: u64) -> u64 {
        size.saturating_sub(index * self.config.block).min(self.config.block)
    }

    /// Stores `bytes`, which start at block `first` of the object, as blocks: whole ones, and the
    /// last one when it ends the object.
    fn store_blocks(&self, location: &Path, info: &ObjectInfo, first: u64, bytes: &Bytes) {
        let block = usize::try_from(self.config.block).unwrap_or(usize::MAX);
        for (i, chunk) in bytes.chunks(block).enumerate() {
            let index = first + i as u64;
            if chunk.len() as u64 != self.block_len(index, info.size) {
                continue;
            }
            let data = bytes.slice_ref(chunk);
            self.memory().insert((location.clone(), index), data.clone());
            if let Some(disk) = &self.disk {
                disk.write(info.clone(), index, data);
            }
        }
    }

    fn memory(&self) -> MutexGuard<'_, MemoryBlocks> {
        self.memory.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A cached block: from memory, or from disk (then also kept in memory).
    async fn cached(&self, location: &Path, index: u64, info: Option<&ObjectInfo>) -> Option<Bytes> {
        if let Some(b) = self.memory().get(&(location.clone(), index)) {
            self.counters.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Some(b);
        }
        let (disk, info) = (self.disk.as_ref()?, info?);
        let b = disk.read(&info.key, index, self.block_len(index, info.size)).await?;
        self.counters.disk_hits.fetch_add(1, Ordering::Relaxed);
        self.memory().insert((location.clone(), index), b.clone());
        Some(b)
    }

    /// The bytes of `ranges` of an immutable object, from cached blocks and one ranged GET per run
    /// of missing blocks. Ranges past the end of the object stop at its end, as in the store. The
    /// object's size is unknown only when every block came from memory after the size was
    /// forgotten; the ranges are then not clamped.
    async fn read(
        self: &Arc<Self>,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<(Option<ObjectInfo>, Vec<Bytes>)> {
        let block = self.config.block;
        let mut info = self.info(location).await;
        let mut indices: Vec<u64> =
            ranges.iter().filter(|r| r.end > r.start).flat_map(|r| r.start / block..=(r.end - 1) / block).collect();
        indices.sort_unstable();
        indices.dedup();
        if let Some(i) = &info {
            let last = i.size.div_ceil(block);
            indices.retain(|&b| b < last);
        }
        let mut blocks: BTreeMap<u64, Bytes> = BTreeMap::new();
        let mut missing = vec![];
        for &b in &indices {
            match self.cached(location, b, info.as_ref()).await {
                Some(bytes) => {
                    blocks.insert(b, bytes);
                }
                None => missing.push(b),
            }
        }
        // Blocks another read is already fetching are awaited, not fetched again.
        let mut waits: Vec<(u64, Fetch)> = vec![];
        {
            let mut inflight = self.inflight.lock().unwrap_or_else(PoisonError::into_inner);
            missing.retain(|&b| match inflight.get(&(location.clone(), b)) {
                Some(f) => {
                    waits.push((b, f.clone()));
                    false
                }
                None => true,
            });
            let max_blocks = if self.config.request == 0 { u64::MAX } else { (self.config.request / block).max(1) };
            for run in runs(&missing, max_blocks) {
                // A task of its own, so the GET runs to its end and its blocks are stored even if
                // every read waiting for it is dropped (a LIMIT, a timeout).
                let task = tokio::spawn(self.clone().fetch_run(location.clone(), run.clone()));
                let fetch = async move {
                    task.await.unwrap_or_else(|e| {
                        Err(Arc::new(object_store::Error::Generic { store: "cache", source: Box::new(e) }))
                    })
                }
                .boxed()
                .shared();
                for (i, b) in run.clone().enumerate() {
                    let one: Fetch = fetch
                        .clone()
                        .map(move |r| r.map(|v: Arc<Vec<Bytes>>| v.get(i).cloned().unwrap_or_default()))
                        .boxed()
                        .shared();
                    inflight.insert((location.clone(), b), one.clone());
                    waits.push((b, one));
                }
            }
        }
        // The runs are fetched at the same time, not one after another.
        let fetched = futures::future::try_join_all(waits.into_iter().map(|(b, f)| f.map(move |r| r.map(|v| (b, v)))))
            .await
            .map_err(unshared)?;
        blocks.extend(fetched);
        if info.is_none() {
            info = self.objects.get(location);
        }
        let end = |r: &Range<u64>| info.as_ref().map_or(r.end, |i| r.end.min(i.size));
        let out = ranges.iter().map(|r| slice(&blocks, block, r.start..end(r))).collect();
        Ok((info, out))
    }

    /// Fetches blocks `run` in one ranged GET and caches them; one result per block (empty past the
    /// end of the object).
    async fn fetch_run(
        self: Arc<Self>,
        location: Path,
        run: Range<u64>,
    ) -> Result<Arc<Vec<Bytes>>, Arc<object_store::Error>> {
        let fetched = self.fetch_and_store(&location, run.clone()).await;
        // Only now, with the blocks stored: a read that no longer finds the fetch finds its blocks.
        let mut inflight = self.inflight.lock().unwrap_or_else(PoisonError::into_inner);
        for b in run {
            inflight.remove(&(location.clone(), b));
        }
        fetched.map_err(Arc::new)
    }

    async fn fetch_and_store(&self, location: &Path, run: Range<u64>) -> object_store::Result<Arc<Vec<Bytes>>> {
        let block = self.config.block;
        let options =
            GetOptions { range: Some(GetRange::Bounded(run.start * block..run.end * block)), ..Default::default() };
        let result = self.inner.get_opts(location, options).await?;
        let (meta, start) = (result.meta.clone(), result.range.start);
        let bytes = result.bytes().await?;
        self.counters.misses.fetch_add(run.end - run.start, Ordering::Relaxed);
        self.counters.bytes_fetched.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let info = self.learn(location, &meta);
        let first = start / block;
        self.store_blocks(location, &info, first, &bytes);
        let size = usize::try_from(block).unwrap_or(usize::MAX);
        let per_block = (run.start..run.end)
            .map(|b| {
                let from = usize::try_from(b.saturating_sub(first)).unwrap_or(usize::MAX).saturating_mul(size);
                bytes.slice(from.min(bytes.len())..from.saturating_add(size).min(bytes.len()))
            })
            .collect();
        Ok(Arc::new(per_block))
    }

    /// Answers a whole-object, offset or suffix read of an object whose size the cache doesn't know:
    /// from the store, caching what it returns.
    async fn read_unknown_size(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        let (meta, range, attributes) = (result.meta.clone(), result.range.clone(), result.attributes.clone());
        let bytes = result.bytes().await?;
        self.counters.bytes_fetched.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let info = self.learn(location, &meta);
        // Cache the blocks the answer covers whole.
        let block = self.config.block;
        let first = range.start.div_ceil(block);
        let skip = usize::try_from(first * block - range.start).unwrap_or(usize::MAX).min(bytes.len());
        self.store_blocks(location, &info, first, &bytes.slice(skip..));
        Ok(answer(location, &info, range, bytes, attributes))
    }

    /// Caches an object Timelake just wrote.
    fn write_through(&self, location: &Path, payload: &PutPayload, result: &PutResult) {
        let size = payload.content_length() as u64;
        if size == 0 || size > self.config.memory / WRITE_THROUGH_SHARE {
            return;
        }
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size,
            e_tag: result.e_tag.clone(),
            version: result.version.clone(),
        };
        let info = self.learn(location, &meta);
        self.store_blocks(location, &info, 0, &Bytes::from(payload.clone()));
    }

    fn forget(&self, location: &Path) {
        self.objects.remove(location);
        self.memory().remove_object(location);
        if let Some(disk) = &self.disk {
            disk.remove(&self.key(location));
        }
    }
}

/// Runs of consecutive block numbers, each at most `max_blocks` long.
fn runs(sorted: &[u64], max_blocks: u64) -> Vec<Range<u64>> {
    let mut out: Vec<Range<u64>> = vec![];
    for &b in sorted {
        match out.last_mut() {
            Some(r) if r.end == b && r.end - r.start < max_blocks => r.end += 1,
            _ => out.push(b..b + 1),
        }
    }
    out
}

/// `range` of an object, assembled from its blocks.
fn slice(blocks: &BTreeMap<u64, Bytes>, block: u64, range: Range<u64>) -> Bytes {
    if range.end <= range.start {
        return Bytes::new();
    }
    let (first, last) = (range.start / block, (range.end - 1) / block);
    let offset = |b: u64, at: u64| usize::try_from(at.saturating_sub(b * block)).unwrap_or(usize::MAX);
    if first == last {
        let b = blocks.get(&first).cloned().unwrap_or_default();
        let (from, to) = (offset(first, range.start).min(b.len()), offset(first, range.end).min(b.len()));
        return b.slice(from..to);
    }
    let mut out = BytesMut::with_capacity(usize::try_from(range.end - range.start).unwrap_or(0));
    for b in first..=last {
        let bytes = blocks.get(&b).cloned().unwrap_or_default();
        let from = if b == first { offset(b, range.start) } else { 0 };
        let to = if b == last { offset(b, range.end) } else { bytes.len() };
        out.extend_from_slice(&bytes[from.min(bytes.len())..to.min(bytes.len())]);
    }
    out.freeze()
}

/// A `GetResult` with `bytes` as its body.
fn answer(location: &Path, info: &ObjectInfo, range: Range<u64>, bytes: Bytes, attributes: Attributes) -> GetResult {
    let meta = ObjectMeta {
        location: location.clone(),
        last_modified: info.last_modified,
        size: info.size,
        e_tag: info.e_tag.clone(),
        version: None,
    };
    let stream = futures::stream::once(async move { Ok(bytes) }).boxed();
    GetResult { payload: GetResultPayload::Stream(stream), meta, range, attributes }
}

/// Whether reads of `location` with `options` go through the cache.
fn cacheable(location: &Path, options: &GetOptions) -> bool {
    let plain = options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && options.version.is_none()
        && !options.head;
    plain && is_immutable(location)
}

/// Objects Timelake never rewrites: data files, manifests, manifest lists and last-value indexes,
/// named with a UUID (as Iceberg files are by every engine).
fn is_immutable(location: &Path) -> bool {
    matches!(location.extension(), Some("parquet" | "avro"))
}

/// A shared fetch's error for one of the reads waiting for it: a missing object stays
/// `NotFound`, which callers act on.
fn unshared(e: Arc<object_store::Error>) -> object_store::Error {
    match e.as_ref() {
        object_store::Error::NotFound { path, .. } => {
            object_store::Error::NotFound { path: path.clone(), source: Box::new(ArcError(e)) }
        }
        _ => object_store::Error::Generic { store: "cache", source: Box::new(ArcError(e)) },
    }
}

#[derive(Debug)]
struct ArcError(Arc<object_store::Error>);

impl fmt::Display for ArcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ArcError {}

impl fmt::Debug for CachedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedStore").field("inner", &self.inner).field("config", &self.config).finish_non_exhaustive()
    }
}

impl fmt::Display for CachedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cached({})", self.inner)
    }
}

/// The wrapper DataFusion and Timelake hold: [`CachedStore`] behind an `Arc`, so a fetch can
/// outlive the read that started it while others wait for it.
#[derive(Debug)]
pub(crate) struct Cached(pub(crate) Arc<CachedStore>);

impl fmt::Display for Cached {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[async_trait]
impl ObjectStore for Cached {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let cache = is_immutable(location).then(|| payload.clone());
        let result = self.0.inner.put_opts(location, payload, opts).await?;
        if let Some(payload) = cache {
            self.0.write_through(location, &payload, &result);
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if !cacheable(location, &options) {
            return self.0.inner.get_opts(location, options).await;
        }
        let size = self.0.info(location).await.map(|i| i.size);
        let range = match (&options.range, size) {
            (Some(GetRange::Bounded(r)), _) if r.end > r.start => Some(r.clone()),
            (None, Some(s)) => Some(0..s),
            (Some(GetRange::Offset(o)), Some(s)) if *o < s => Some(*o..s),
            (Some(GetRange::Suffix(n)), Some(s)) => Some(s.saturating_sub(*n)..s),
            _ => None,
        };
        let Some(range) = range else { return self.0.read_unknown_size(location, options).await };
        if size.is_some_and(|s| range.start >= s && s > 0) {
            // Out of range: let the store answer with its error.
            return self.0.inner.get_opts(location, options).await;
        }
        let (info, mut bytes) = self.0.read(location, std::slice::from_ref(&range)).await?;
        let Some(info) = info else { return self.0.inner.get_opts(location, options).await };
        let bytes = bytes.pop().unwrap_or_default();
        let range = range.start..range.start + bytes.len() as u64;
        Ok(answer(location, &info, range, bytes, Attributes::default()))
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
        if !is_immutable(location) || ranges.is_empty() {
            return self.0.inner.get_ranges(location, ranges).await;
        }
        Ok(self.0.read(location, ranges).await?.1)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let cache = self.0.clone();
        self.0
            .inner
            .delete_stream(locations)
            .inspect(move |r| {
                if let Ok(location) = r {
                    cache.forget(location);
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.0.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.0.forget(to);
        self.0.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.0.forget(from);
        self.0.forget(to);
        self.0.inner.rename_opts(from, to, options).await
    }
}

/// Blocks in memory, the least recently used evicted past the budget.
#[derive(Debug)]
struct MemoryBlocks {
    budget: u64,
    used: u64,
    tick: u64,
    blocks: HashMap<(Path, u64), (Bytes, u64)>,
    order: BTreeMap<u64, (Path, u64)>,
}

impl MemoryBlocks {
    fn new(budget: u64) -> Self {
        Self { budget, used: 0, tick: 0, blocks: HashMap::new(), order: BTreeMap::new() }
    }

    fn get(&mut self, key: &(Path, u64)) -> Option<Bytes> {
        let (bytes, tick) = self.blocks.get_mut(key)?;
        self.order.remove(tick);
        self.tick += 1;
        *tick = self.tick;
        self.order.insert(self.tick, key.clone());
        Some(bytes.clone())
    }

    fn insert(&mut self, key: (Path, u64), bytes: Bytes) {
        let len = bytes.len() as u64;
        if len > self.budget {
            return;
        }
        self.remove(&key);
        while self.used + len > self.budget
            && let Some((_, oldest)) = self.order.pop_first()
        {
            if let Some((b, _)) = self.blocks.remove(&oldest) {
                self.used -= b.len() as u64;
            }
        }
        self.tick += 1;
        self.order.insert(self.tick, key.clone());
        self.blocks.insert(key, (bytes, self.tick));
        self.used += len;
    }

    fn remove(&mut self, key: &(Path, u64)) {
        if let Some((b, tick)) = self.blocks.remove(key) {
            self.order.remove(&tick);
            self.used -= b.len() as u64;
        }
    }

    fn remove_object(&mut self, location: &Path) {
        let keys: Vec<(Path, u64)> = self.blocks.keys().filter(|(p, _)| p == location).cloned().collect();
        for key in keys {
            self.remove(&key);
        }
    }
}

/// Blocks on local disk: `<dir>/<hash prefix>/<hash>/` holds `meta.json` and one file per block.
/// Shared by processes: files are renamed into place, and any process may evict.
#[derive(Debug)]
struct DiskBlocks {
    dir: PathBuf,
    budget: u64,
    /// Bytes written since the last sweep for the budget.
    written: AtomicU64,
    sweeping: Arc<AtomicBool>,
}

impl DiskBlocks {
    fn new(dir: PathBuf, budget: u64) -> Self {
        // The first write sweeps: another process may have left the directory over budget.
        Self { dir, budget, written: AtomicU64::new(budget), sweeping: Arc::default() }
    }

    fn object_dir(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes()).to_hex();
        self.dir.join(&hash[..2]).join(&hash[..32])
    }

    async fn info(&self, key: &str) -> Option<ObjectInfo> {
        let bytes = tokio::fs::read(self.object_dir(key).join("meta.json")).await.ok()?;
        let info: ObjectInfo = serde_json::from_slice(&bytes).ok()?;
        (info.key == key).then_some(info)
    }

    /// Block `index` if it is on disk with the expected length.
    async fn read(&self, key: &str, index: u64, len: u64) -> Option<Bytes> {
        let path = self.object_dir(key).join(index.to_string());
        let bytes = tokio::fs::read(&path).await.ok()?;
        if bytes.len() as u64 != len || len == 0 {
            let _ = tokio::fs::remove_file(&path).await;
            return None;
        }
        tokio::task::spawn_blocking(move || touch(&path));
        Some(Bytes::from(bytes))
    }

    /// Writes a block in the background; a failure only costs a later miss.
    fn write(&self, info: ObjectInfo, index: u64, bytes: Bytes) {
        let dir = self.object_dir(&info.key);
        let len = bytes.len() as u64;
        tokio::task::spawn_blocking(move || {
            let _ = write_block(&dir, &info, index, &bytes);
        });
        let written = self.written.fetch_add(len, Ordering::Relaxed) + len;
        if written > self.budget / SWEEP_SHARE {
            self.written.store(0, Ordering::Relaxed);
            self.sweep();
        }
    }

    fn remove(&self, key: &str) {
        let dir = self.object_dir(key);
        tokio::task::spawn_blocking(move || {
            let _ = std::fs::remove_dir_all(dir);
        });
    }

    /// Deletes the least recently used blocks until the cache is within 90% of its budget, in the
    /// background, one sweep at a time per process.
    fn sweep(&self) {
        if self.sweeping.swap(true, Ordering::AcqRel) {
            return;
        }
        let (dir, budget, sweeping) = (self.dir.clone(), self.budget, self.sweeping.clone());
        let task = move || {
            let _ = sweep(&dir, budget);
            sweeping.store(false, Ordering::Release);
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => drop(handle.spawn_blocking(task)),
            Err(_) => task(),
        }
    }
}

fn write_block(dir: &std::path::Path, info: &ObjectInfo, index: u64, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let meta = dir.join("meta.json");
    if !meta.exists() {
        put_file(&meta, &serde_json::to_vec(info).map_err(std::io::Error::other)?)?;
    }
    put_file(&dir.join(index.to_string()), bytes)
}

/// Writes a file whole or not at all: to a temporary name, then renamed into place.
fn put_file(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

/// Marks a block as used now, unless it was within [`TOUCH_AFTER`].
fn touch(path: &std::path::Path) {
    let Ok(file) = std::fs::File::options().append(true).open(path) else { return };
    let recent =
        file.metadata().and_then(|m| m.modified()).is_ok_and(|t| t.elapsed().is_ok_and(|age| age < TOUCH_AFTER));
    if !recent {
        let _ = file.set_modified(SystemTime::now());
    }
}

fn sweep(dir: &std::path::Path, budget: u64) -> std::io::Result<()> {
    // Entries that can't be listed (a stray file such as `.DS_Store`, an object directory another
    // sweep just removed) are passed over: one of them must not stop every sweep.
    let entries = |dir: PathBuf| std::fs::read_dir(dir).into_iter().flatten().flatten();
    let mut blocks: Vec<(SystemTime, u64, PathBuf)> = vec![];
    for prefix in std::fs::read_dir(dir)?.flatten() {
        for object in entries(prefix.path()) {
            for file in entries(object.path()) {
                let name = file.file_name();
                if name == "meta.json" || name.to_string_lossy().contains(".tmp-") {
                    continue;
                }
                if let Ok(m) = file.metadata() {
                    blocks.push((m.modified().unwrap_or(SystemTime::UNIX_EPOCH), m.len(), file.path()));
                }
            }
        }
    }
    let mut used: u64 = blocks.iter().map(|b| b.1).sum();
    if used <= budget {
        return Ok(());
    }
    blocks.sort_unstable();
    let target = budget / 10 * 9;
    for (_, len, path) in blocks {
        if used <= target {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            used = used.saturating_sub(len);
        }
        // An object directory left with only its metadata goes too.
        if let Some(parent) = path.parent()
            && std::fs::read_dir(parent).is_ok_and(|mut d| d.all(|f| f.is_ok_and(|f| f.file_name() == "meta.json")))
        {
            let _ = std::fs::remove_dir_all(parent);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;

    /// An in-memory store that counts its reads.
    #[derive(Debug, Default)]
    struct Counting {
        inner: InMemory,
        gets: AtomicU64,
    }

    impl fmt::Display for Counting {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Counting")
        }
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            // A little latency, so concurrent reads overlap.
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(
            &self,
            l: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
    }

    const BLOCK: u64 = 4096;

    fn data(len: usize) -> Bytes {
        (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect::<Vec<u8>>().into()
    }

    /// A cache over `store`, with blocks of 4 KiB, and a disk cache in `dir` if given.
    fn cached(store: &Arc<Counting>, dir: Option<&std::path::Path>) -> Cached {
        let config =
            CacheConfig { block: BLOCK, request: 0, memory: 1 << 20, disk: dir.map(|d| (d.to_path_buf(), 1 << 20)) };
        Cached(Arc::new(CachedStore::new(store.clone(), "memory://".into(), config)))
    }

    async fn gets_for<F: Future<Output = ()>>(store: &Counting, f: F) -> u64 {
        let before = store.gets.load(Ordering::SeqCst);
        f.await;
        store.gets.load(Ordering::SeqCst) - before
    }

    #[tokio::test]
    async fn repeat_reads_come_from_memory() {
        let store = Arc::new(Counting::default());
        let path = Path::from("t/data/a.parquet");
        let bytes = data(10_000); // 2 whole blocks and a part
        store.inner.put(&path, bytes.clone().into()).await.unwrap();
        let c = cached(&store, None);
        let ranges = [100..200, 4000..9000, 9990..10_000];
        let n = gets_for(&store, async {
            let got = c.get_ranges(&path, &ranges).await.unwrap();
            for (r, g) in ranges.iter().zip(&got) {
                assert_eq!(g, &bytes.slice(usize::try_from(r.start).unwrap()..usize::try_from(r.end).unwrap()));
            }
        })
        .await;
        assert_eq!(n, 1, "the three blocks are next to each other: one GET");
        let n = gets_for(&store, async {
            assert_eq!(c.get(&path).await.unwrap().bytes().await.unwrap(), bytes);
            assert_eq!(c.get_range(&path, 9000..20_000).await.unwrap(), bytes.slice(9000..), "clamped at the end");
            let suffix = GetOptions { range: Some(GetRange::Suffix(8)), ..Default::default() };
            assert_eq!(c.get_opts(&path, suffix).await.unwrap().bytes().await.unwrap(), bytes.slice(9992..));
        })
        .await;
        assert_eq!(n, 0);
        let stats = c.0.stats();
        assert_eq!((stats.misses, stats.bytes_fetched), (3, 10_000));
    }

    /// Runs of missing blocks apart from each other are fetched at the same time: three runs take
    /// one request's latency (5 ms in `Counting`), not three.
    #[tokio::test(start_paused = true)]
    async fn separate_runs_are_fetched_at_once() {
        let store = Arc::new(Counting::default());
        let path = Path::from("t/data/b.parquet");
        store.inner.put(&path, data(40_000).into()).await.unwrap();
        let c = cached(&store, None);
        let started = tokio::time::Instant::now();
        let n = gets_for(&store, async {
            c.get_ranges(&path, &[0..100, 16_000..16_100, 32_000..32_100]).await.unwrap();
        })
        .await;
        assert_eq!(n, 3);
        assert_eq!(started.elapsed(), Duration::from_millis(5));
    }

    #[tokio::test]
    async fn only_immutable_objects_are_cached() {
        let store = Arc::new(Counting::default());
        let hint = Path::from("t/metadata/version-hint.text");
        store.inner.put(&hint, Bytes::from("1").into()).await.unwrap();
        let c = cached(&store, None);
        let n = gets_for(&store, async {
            for _ in 0..3 {
                assert_eq!(c.get(&hint).await.unwrap().bytes().await.unwrap(), "1");
            }
        })
        .await;
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn writes_and_deletes_update_the_cache() {
        let store = Arc::new(Counting::default());
        let c = cached(&store, None);
        let path = Path::from("t/metadata/snap-1.avro");
        c.put(&path, data(5000).into()).await.unwrap();
        let n = gets_for(&store, async {
            assert_eq!(c.get(&path).await.unwrap().bytes().await.unwrap(), data(5000));
        })
        .await;
        assert_eq!(n, 0, "written through");
        c.delete(&path).await.unwrap();
        assert!(c.get(&path).await.is_err(), "gone from the store, and not served from the cache");
    }

    #[tokio::test]
    async fn concurrent_reads_share_one_fetch() {
        let store = Arc::new(Counting::default());
        let path = Path::from("t/data/b.parquet");
        store.inner.put(&path, data(3 * 4096).into()).await.unwrap();
        let c = Arc::new(cached(&store, None));
        let n = gets_for(&store, async {
            let reads = (0..8).map(|_| {
                let (c, path) = (c.clone(), path.clone());
                tokio::spawn(async move { c.get_range(&path, 0..3 * 4096).await.unwrap() })
            });
            for r in futures::future::join_all(reads).await {
                assert_eq!(r.unwrap(), data(3 * 4096));
            }
        })
        .await;
        assert_eq!(n, 1);
    }

    /// A read dropped while its GET is out (a LIMIT, a timeout) neither keeps the cache alive nor
    /// leaves the GET for a later read to resume: it finishes on its own and its blocks are kept.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_read_leaves_nothing_behind() {
        let store = Arc::new(Counting::default());
        let path = Path::from("t/data/d.parquet");
        store.inner.put(&path, data(5000).into()).await.unwrap();
        let c = cached(&store, None);
        let read = c.get_range(&path, 0..100);
        assert!(tokio::time::timeout(Duration::from_millis(1), read).await.is_err(), "dropped before its GET returns");
        tokio::time::sleep(Duration::from_millis(10)).await;
        let n = gets_for(&store, async { assert_eq!(c.get_range(&path, 0..100).await.unwrap(), data(100)) }).await;
        assert_eq!(n, 0, "the dropped read's GET finished and its block is cached");

        let weak = Arc::downgrade(&c.0);
        let read = c.get_range(&path, 4096..4196);
        assert!(tokio::time::timeout(Duration::from_millis(1), read).await.is_err());
        drop(c);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(weak.upgrade().is_none(), "nothing holds the cache once its fetches are done");
    }

    /// A missing object is `NotFound`, as from the store: callers tell it from other errors.
    #[tokio::test]
    async fn a_missing_object_is_not_found() {
        let store = Arc::new(Counting::default());
        let c = cached(&store, None);
        let err = c.get_range(&Path::from("t/data/gone.parquet"), 0..100).await.unwrap_err();
        assert!(matches!(err, object_store::Error::NotFound { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn the_disk_cache_serves_another_process() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Counting::default());
        let path = Path::from("t/data/c.parquet");
        store.inner.put(&path, data(9000).into()).await.unwrap();
        let first = cached(&store, Some(dir.path()));
        first.get(&path).await.unwrap().bytes().await.unwrap();
        // Disk writes run in the background: wait for the three blocks (a slow runner took over 200 ms).
        let blocks = first.0.disk.as_ref().unwrap().object_dir(&first.0.key(&path));
        for _ in 0..200 {
            let written = std::fs::read_dir(&blocks).into_iter().flatten().flatten();
            if written.filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok()).count() >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let other = cached(&store, Some(dir.path()));
        let n = gets_for(&store, async {
            assert_eq!(other.get(&path).await.unwrap().bytes().await.unwrap(), data(9000));
        })
        .await;
        assert_eq!(n, 0);
        assert_eq!(other.0.stats().disk_hits, 3);

        // A block of the wrong length is fetched again.
        let key = other.0.key(&path);
        let block = other.0.disk.as_ref().unwrap().object_dir(&key).join("1");
        std::fs::write(&block, b"short").unwrap();
        let third = cached(&store, Some(dir.path()));
        let n = gets_for(&store, async {
            assert_eq!(third.get(&path).await.unwrap().bytes().await.unwrap(), data(9000));
        })
        .await;
        assert_eq!(n, 1);
    }

    #[test]
    fn memory_evicts_the_least_recently_used() {
        let mut m = MemoryBlocks::new(10);
        let key = |i| (Path::from("a.parquet"), i);
        m.insert(key(0), Bytes::from_static(b"aaaa"));
        m.insert(key(1), Bytes::from_static(b"bbbb"));
        assert!(m.get(&key(0)).is_some());
        m.insert(key(2), Bytes::from_static(b"cccc"));
        assert!(m.get(&key(1)).is_none() && m.get(&key(0)).is_some() && m.get(&key(2)).is_some());
        assert_eq!(m.used, 8);
        m.remove_object(&Path::from("a.parquet"));
        assert_eq!(m.used, 0);
    }

    #[test]
    fn sweeping_keeps_the_disk_cache_within_budget() {
        let dir = tempfile::tempdir().unwrap();
        let info = |k: &str| ObjectInfo { key: k.into(), size: 1000, last_modified: Utc::now(), e_tag: None };
        let disk = DiskBlocks::new(dir.path().to_path_buf(), 2500);
        for k in ["x", "y", "z"] {
            write_block(&disk.object_dir(k), &info(k), 0, &[0; 1000]).unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }
        // Files that aren't the cache's, where directories are expected, are passed over.
        std::fs::write(dir.path().join(".DS_Store"), b"").unwrap();
        let prefix = disk.object_dir("z").parent().unwrap().to_path_buf();
        std::fs::write(prefix.join(".DS_Store"), b"").unwrap();
        sweep(dir.path(), 2500).unwrap();
        let left: Vec<bool> = ["x", "y", "z"].iter().map(|k| disk.object_dir(k).join("0").exists()).collect();
        assert_eq!(left, [false, true, true], "the oldest block goes, and its directory with it");
        assert!(!disk.object_dir("x").exists());
    }

    #[test]
    fn runs_and_slices() {
        assert_eq!(runs(&[1, 2, 3, 5, 7, 8], u64::MAX), vec![1..4, 5..6, 7..9]);
        assert_eq!(runs(&[1, 2, 3, 4, 5, 7], 2), vec![1..3, 3..5, 5..6, 7..8], "split at two blocks");
        let blocks: BTreeMap<u64, Bytes> =
            [(0, Bytes::from_static(b"abcd")), (1, Bytes::from_static(b"efgh")), (2, Bytes::from_static(b"ij"))].into();
        assert_eq!(slice(&blocks, 4, 2..9), "cdefghi");
        assert_eq!(slice(&blocks, 4, 4..8), "efgh");
        assert_eq!(slice(&blocks, 4, 3..3), "");
    }
}
