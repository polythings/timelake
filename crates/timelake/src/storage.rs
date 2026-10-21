//! Object storage access. One `object_store` client serves everything: Iceberg metadata (through an
//! adapter that implements iceberg-rust's `Storage` trait), Parquet data written by Timelake, and
//! DataFusion scans. Any `object_store` backend works: S3 and S3-compatible stores (R2, `RustFS`,
//! `MinIO`), GCS, Azure, and local disk.

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, StorageConfig, StorageFactory,
};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::buffered::BufWriter;
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::http::HttpBuilder;
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use url::Url;

pub use crate::block_cache::CacheStats;
use crate::block_cache::{CacheConfig, Cached, CachedStore};
use crate::error::{Error, Result};

/// A store rooted at one URL authority (`s3://bucket`, `file://`, ...). Paths handed to Timelake and
/// written into Iceberg metadata are absolute URIs; [`Storage::path`] maps them to store paths.
#[derive(Clone, Debug)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    /// `scheme://authority` with no trailing slash, e.g. `s3://bucket` or `file://`.
    base: String,
    /// `base` with a trailing slash, as DataFusion registers object stores.
    url: Url,
    listable: bool,
    /// Lists by raw key prefix (S3, GCS, Azure).
    lister: Option<Lister>,
    check: FreshnessCheck,
    /// Connections opened ahead of a cold table load (P3); 0 for local stores.
    warm_connections: usize,
    /// How a cold table load finds the newest version.
    version_lookup: VersionLookup,
    /// The block cache under `store`, if on.
    cache: Option<Arc<CachedStore>>,
    /// Bytes a file's upload buffers before it becomes a multipart upload.
    upload_buffer: usize,
}

/// What a file's upload buffers before it becomes a multipart upload: `object_store`'s default.
const UPLOAD_BUFFER: usize = 10 << 20;

/// A store's raw-prefix listing.
#[derive(Clone)]
struct Lister(Arc<dyn PaginatedListStore>);

impl std::fmt::Debug for Lister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Lister")
    }
}

/// Storage option mapping a warehouse URL onto a local directory (see [`Storage::open`]).
pub const LOCAL_ROOT_OPTION: &str = "timelake_local_root";

/// Storage option choosing how a reader checks whether a newer table version exists:
/// `head` or `list` (see [`Storage::open`]).
pub const FRESHNESS_CHECK_OPTION: &str = "timelake_freshness_check";

/// Storage option choosing how a table's first load in a process finds the newest version:
/// `list` (the default on stores that list by prefix: one LIST of `metadata/v`, sent together with
/// the head GET, so a current head is confirmed in the same round trip) or `hint` (GET the version
/// hint with the head, then check for the next version: no LIST, one more round trip).
pub const VERSION_LOOKUP_OPTION: &str = "timelake_version_lookup";

/// Storage option setting how many connections a cold table load opens ahead of the data reads,
/// with small HEAD requests (P3). Off by default: measured on S3 and Tigris it saved at most
/// about 20 ms on cold scans and nothing on point queries.
pub const WARM_CONNECTIONS_OPTION: &str = "timelake_warm_connections";

/// Storage option choosing the block cache (spec 22): `off`, `memory` (the default for object
/// stores and HTTPS) or `disk` (memory and a directory shared by every process on the machine).
pub const CACHE_OPTION: &str = "timelake_cache";
/// Storage option: the disk cache's directory (default: `timelake` in the OS cache directory).
pub const CACHE_DIR_OPTION: &str = "timelake_cache_dir";
/// Storage option: the memory cache's budget in bytes (default 256 MiB).
pub const CACHE_MEMORY_BYTES_OPTION: &str = "timelake_cache_memory_bytes";
/// Storage option: the disk cache's budget in bytes (default 4 GiB).
pub const CACHE_DISK_BYTES_OPTION: &str = "timelake_cache_disk_bytes";
/// Storage option: the cache's block size in bytes (default 1 MiB, at least 4 KiB).
pub const CACHE_BLOCK_BYTES_OPTION: &str = "timelake_cache_block_bytes";
/// Storage option: the cache's largest GET in bytes; longer runs of missing blocks are split into
/// parts fetched at once (default 1 MiB; 0: no limit).
pub const CACHE_REQUEST_BYTES_OPTION: &str = "timelake_cache_request_bytes";

const DEFAULT_CACHE_MEMORY_BYTES: u64 = 256 << 20;
const DEFAULT_CACHE_DISK_BYTES: u64 = 4 << 30;
const DEFAULT_CACHE_BLOCK_BYTES: u64 = 1 << 20;
const MIN_CACHE_BLOCK_BYTES: u64 = 4 << 10;
const DEFAULT_CACHE_REQUEST_BYTES: u64 = 1 << 20;

/// How [`Storage::check_exists`] asks whether an object exists. A HEAD request is the cheapest
/// on most stores; on some (Tigris) a HEAD of a missing key is slow while a LIST of its exact
/// name is fast (spec 44).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FreshnessCheck {
    Head,
    List,
}

impl FromStr for FreshnessCheck {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "head" => Ok(Self::Head),
            "list" => Ok(Self::List),
            _ => Err(Error::config(format!("{FRESHNESS_CHECK_OPTION} must be head or list, got {s:?}"))),
        }
    }
}

/// How a table's first load in a process finds the newest version (see [`VERSION_LOOKUP_OPTION`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VersionLookup {
    /// LIST the metadata files, together with the head GET.
    List,
    /// GET the version hint with the head, then check for the next version.
    Hint,
}

impl FromStr for VersionLookup {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "list" => Ok(Self::List),
            "hint" => Ok(Self::Hint),
            _ => Err(Error::config(format!("{VERSION_LOOKUP_OPTION} must be list or hint, got {s:?}"))),
        }
    }
}

/// The block cache (see [`CACHE_OPTION`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CacheMode {
    Off,
    Memory,
    /// Memory and a directory shared by every process on the machine.
    Disk,
}

impl FromStr for CacheMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "memory" => Ok(Self::Memory),
            "disk" => Ok(Self::Disk),
            _ => Err(Error::config(format!("{CACHE_OPTION} must be off, memory or disk, got {s:?}"))),
        }
    }
}

/// Removes option `key` from `options` and parses it.
fn take_option<T: FromStr<Err = Error>>(options: &mut HashMap<String, String>, key: &str) -> Result<Option<T>> {
    options.remove(key).map(|v| v.parse()).transpose()
}

/// An object store as [`Storage::open`] builds it for a URL.
struct Backend {
    store: Arc<dyn ObjectStore>,
    /// Its listing by raw key prefix, and whether a LIST of an exact name is its faster check.
    lister: Option<(Arc<dyn PaginatedListStore>, bool)>,
    /// The endpoint requests go to, where the store has one besides its URL.
    endpoint: Option<String>,
}

impl Backend {
    /// The store for `url` (`base` is its `scheme://authority`), configured by `options`.
    fn open(url: &Url, base: &str, options: &HashMap<String, String>) -> Result<Self> {
        let bucket = url.host_str().unwrap_or_default();
        let plain = |store: Arc<dyn ObjectStore>| Self { store, lister: None, endpoint: None };
        match url.scheme() {
            "file" => Ok(plain(Arc::new(LocalFileSystem::new()))),
            // Plain HTTPS (public buckets, Vercel Blob, CDNs): read-only, and it cannot list,
            // so table discovery uses the warehouse's `timelake-tables.json` index.
            "https" => Ok(plain(Arc::new(HttpBuilder::new().with_url(base).build()?))),
            "http" => {
                let client = object_store::ClientOptions::new().with_allow_http(true);
                Ok(plain(Arc::new(HttpBuilder::new().with_url(base).with_client_options(client).build()?)))
            }
            "memory" => Ok(plain(Arc::new(InMemory::new()))),
            "s3" | "s3a" => Self::s3(bucket, options),
            "gs" => {
                let b = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
                let gcs =
                    Arc::new(configured(b, options, "GCS", |b, k: GoogleConfigKey, v| b.with_config(k, v))?.build()?);
                Ok(Self { store: gcs.clone(), lister: Some((gcs, false)), endpoint: None })
            }
            "az" | "abfs" | "abfss" => {
                let b = MicrosoftAzureBuilder::from_env().with_url(url.as_str());
                let azure =
                    Arc::new(configured(b, options, "Azure", |b, k: AzureConfigKey, v| b.with_config(k, v))?.build()?);
                Ok(Self { store: azure.clone(), lister: Some((azure, false)), endpoint: None })
            }
            other => Err(Error::config(format!("unsupported storage scheme {other:?}"))),
        }
    }

    fn s3(bucket: &str, options: &HashMap<String, String>) -> Result<Self> {
        let b = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            // Atomic commits rely on put-if-absent (`If-None-Match: *`), supported by AWS S3 since
            // 2024 and by most S3-compatible stores.
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        let b = configured(b, options, "S3", |b, k: AmazonS3ConfigKey, v| b.with_config(k, v))?;
        // The endpoint requests go to: object_store prefers the S3-specific one
        // (`AWS_ENDPOINT_URL_S3`, which Fly.io and the Tigris CLI set) over the generic one.
        let endpoint = b
            .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
            .or_else(|| b.get_config_value(&AmazonS3ConfigKey::Endpoint))
            .filter(|e| !e.is_empty());
        let tigris = endpoint.as_ref().is_some_and(|e| ["tigris", "storage.dev"].iter().any(|t| e.contains(t)));
        let s3 = Arc::new(b.build()?);
        Ok(Self { store: s3.clone(), lister: Some((s3, tigris)), endpoint })
    }
}

/// `builder` with each of `options` set as one of the `backend`'s config keys `K`.
fn configured<B, K: FromStr>(
    builder: B,
    options: &HashMap<String, String>,
    backend: &str,
    set: impl Fn(B, K, &str) -> B,
) -> Result<B> {
    options.iter().try_fold(builder, |b, (k, v)| {
        let key = K::from_str(k).map_err(|_| Error::config(format!("unknown {backend} option {k:?}")))?;
        Ok(set(b, key, v))
    })
}

/// What [`Storage::write_versioned`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VersionedWrite {
    Written,
    /// The object changed (or appeared) since it was read: read it again.
    Changed,
}

impl Storage {
    /// Opens the store for `url`. `options` are backend config keys (e.g. `aws_endpoint`,
    /// `aws_access_key_id`, `google_service_account`); for S3, GCS and Azure the usual environment
    /// variables are read as well.
    ///
    /// `timelake_local_root=<dir>` maps the URL onto a local directory: objects are written under
    /// `<dir>` while every path recorded in metadata keeps the URL. That builds a warehouse on disk
    /// that is ready to upload to its final location, e.g. a public Vercel Blob or CDN URL that is
    /// then read over HTTPS.
    ///
    /// `timelake_freshness_check=head|list` sets how readers check for a newer table version: a
    /// HEAD request (the default) or a LIST of the exact name (the default on Tigris, where a HEAD
    /// of a missing key is slow). `list` needs S3, GCS or Azure.
    pub fn open(url: &Url, mut options: HashMap<String, String>) -> Result<Self> {
        let local_root = options.remove(LOCAL_ROOT_OPTION);
        let read = ReadOptions::take(&mut options)?;
        let cache = cache_config(&mut options, url.scheme())?;
        let scheme = url.scheme();
        let authority = url.host_str().unwrap_or_default();
        let base = match scheme {
            // Keep the port for HTTP origins (e.g. http://127.0.0.1:4000).
            "http" | "https" => url.origin().ascii_serialization(),
            _ => format!("{scheme}://{authority}"),
        };
        if let Some(dir) = local_root {
            std::fs::create_dir_all(&dir)?;
            let store = LocalFileSystem::new_with_prefix(&dir)?;
            return Self::new(Arc::new(store), base, true);
        }
        let Backend { store, lister, endpoint } = Backend::open(url, &base, &options)?;
        let listable = !matches!(scheme, "http" | "https");
        let endpoint = endpoint.or_else(|| options.get("endpoint").cloned());
        let (store, cached) = with_cache(store, &base, endpoint.as_deref(), cache);
        let mut storage = Self::new(store, base, listable)?;
        storage.cache = cached;
        // Unset, the check is the faster one for the store.
        storage.check = match (&lister, read.freshness_check) {
            (Some(_), Some(FreshnessCheck::List)) | (Some((_, true)), None) => FreshnessCheck::List,
            (None, Some(FreshnessCheck::List)) => {
                return Err(Error::config(format!("{FRESHNESS_CHECK_OPTION}=list needs S3, GCS or Azure")));
            }
            _ => FreshnessCheck::Head,
        };
        storage.lister = lister.map(|(l, _)| Lister(l));
        storage.version_lookup = read.version_lookup;
        if scheme != "file" && scheme != "memory" {
            storage.warm_connections = read.warm_connections;
        }
        Ok(storage)
    }

    fn new(store: Arc<dyn ObjectStore>, base: String, listable: bool) -> Result<Self> {
        let url = Url::parse(&format!("{base}/")).map_err(|e| Error::config(format!("bad store URL {base}: {e}")))?;
        Ok(Self {
            store,
            base,
            url,
            listable,
            lister: None,
            check: FreshnessCheck::Head,
            warm_connections: 0,
            version_lookup: VersionLookup::List,
            cache: None,
            upload_buffer: UPLOAD_BUFFER,
        })
    }

    /// What the block cache did since the store was opened (`None` with the cache off).
    pub fn cache_stats(&self) -> Option<CacheStats> {
        self.cache.as_ref().map(|c| c.stats())
    }

    /// A listable store addressed as `base` (`scheme://authority`).
    #[cfg(test)]
    pub(crate) fn from_store(store: Arc<dyn ObjectStore>, base: &str) -> Self {
        Self::new(store, base.to_string(), true).unwrap()
    }

    /// This store with files uploaded in parts of `bytes`, so tests see multipart uploads.
    #[cfg(test)]
    pub(crate) fn upload_buffer(mut self, bytes: usize) -> Self {
        self.upload_buffer = bytes;
        self
    }

    /// A buffered upload of `path`.
    fn buf_writer(&self, path: Path) -> BufWriter {
        BufWriter::with_capacity(self.store.clone(), path, self.upload_buffer)
    }

    /// This store as `timelake_version_lookup=hint` would open it.
    #[cfg(test)]
    pub(crate) fn hint_lookup(mut self) -> Self {
        self.version_lookup = VersionLookup::Hint;
        self
    }

    /// Whether a cold table load lists the metadata files (see [`VERSION_LOOKUP_OPTION`]).
    pub(crate) fn list_on_load(&self) -> bool {
        self.version_lookup == VersionLookup::List
    }

    /// Whether the store is object storage, not local files or memory: each request is a round trip.
    pub fn is_remote(&self) -> bool {
        !matches!(self.url.scheme(), "file" | "memory")
    }

    /// Whether the store supports listing (plain HTTPS does not).
    pub fn can_list(&self) -> bool {
        self.listable
    }

    /// The underlying `object_store` client.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// `scheme://authority`, the prefix of every URI in this store.
    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// `scheme://authority/`, the URL DataFusion registers this store under.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Store path for an absolute URI under this store.
    pub fn path(&self, uri: &str) -> Result<Path> {
        let rest =
            uri.strip_prefix(&self.base).ok_or_else(|| Error::config(format!("{uri} is not under {}", self.base)))?;
        Ok(Path::parse(rest.trim_start_matches('/'))?)
    }

    /// Reads a whole object.
    pub async fn read(&self, uri: &str) -> Result<Bytes> {
        Ok(self.store.get(&self.path(uri)?).await?.bytes().await?)
    }

    /// Like [`Self::read`], but `Ok(None)` when the object doesn't exist.
    pub async fn read_opt(&self, uri: &str) -> Result<Option<Bytes>> {
        match self.store.get(&self.path(uri)?).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Whether an object exists.
    pub async fn exists(&self, uri: &str) -> Result<bool> {
        match self.store.head(&self.path(uri)?).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Whether an object exists, asked the way this store answers fastest (see
    /// [`FRESHNESS_CHECK_OPTION`]). Readers use it to check for a newer table version.
    pub async fn check_exists(&self, uri: &str) -> Result<bool> {
        let (FreshnessCheck::List, Some(Lister(lister))) = (self.check, &self.lister) else {
            return self.exists(uri).await;
        };
        let path = self.path(uri)?;
        // The exact name sorts before any longer name it prefixes, so one key answers.
        let opts = PaginatedListOptions { max_keys: Some(1), ..Default::default() };
        let listed = lister.list_paginated(Some(path.as_ref()), opts).await?;
        Ok(listed.result.objects.iter().any(|o| o.location == path))
    }

    /// URIs of the objects whose URI starts with `prefix` (which need not end at a `/`), in one
    /// request where the store lists by raw prefix. `None` if the store can't list, or the result
    /// doesn't fit one page.
    pub(crate) async fn list_prefix(&self, prefix: &str) -> Result<Option<Vec<String>>> {
        self.list_prefix_pages(prefix, Some(1)).await
    }

    /// URIs of every object whose URI starts with `prefix`, however many pages that takes. An
    /// error if the store can't list.
    pub(crate) async fn list_prefix_all(&self, prefix: &str) -> Result<Vec<String>> {
        self.list_prefix_pages(prefix, None)
            .await?
            .ok_or_else(|| Error::config(format!("{prefix}: this store can't list objects")))
    }

    /// Objects whose URI starts with `prefix`, in at most `max_pages` requests where the store
    /// lists by raw prefix (`None` if more are needed, or the store can't list).
    async fn list_prefix_pages(&self, prefix: &str, max_pages: Option<usize>) -> Result<Option<Vec<String>>> {
        let rest = prefix
            .strip_prefix(&self.base)
            .ok_or_else(|| Error::config(format!("{prefix} is not under {}", self.base)))?;
        let key_prefix = rest.trim_start_matches('/');
        let uri = |location: &Path| format!("{}/{location}", self.base);
        if let Some(Lister(lister)) = &self.lister {
            let (mut out, mut page_token, mut pages) = (vec![], None, 0);
            loop {
                if max_pages.is_some_and(|max| pages >= max) {
                    return Ok(None);
                }
                let opts = PaginatedListOptions { page_token, ..Default::default() };
                let listed = lister.list_paginated(Some(key_prefix), opts).await?;
                out.extend(listed.result.objects.iter().map(|o| uri(&o.location)));
                pages += 1;
                match listed.page_token {
                    Some(token) => page_token = Some(token),
                    None => return Ok(Some(out)),
                }
            }
        }
        if !self.listable {
            return Ok(None);
        }
        // Local stores list by directory: list the prefix's directory and filter.
        let dir = key_prefix.rsplit_once('/').map_or("", |(dir, _)| dir);
        let listed: Vec<_> = self.store.list(Some(&Path::parse(dir)?)).try_collect().await?;
        Ok(Some(
            listed.iter().filter(|o| o.location.as_ref().starts_with(key_prefix)).map(|o| uri(&o.location)).collect(),
        ))
    }

    /// Opens connections ahead of the requests that will follow, by sending small HEAD requests
    /// for `uri` in the background (spec 7, P3). Does nothing on local stores.
    pub(crate) fn warm_up(&self, uri: &str) {
        let Ok(path) = self.path(uri) else { return };
        for _ in 0..self.warm_connections {
            let (store, path) = (self.store.clone(), path.clone());
            tokio::spawn(async move {
                let _ = store.head(&path).await;
            });
        }
    }

    /// The first `len` bytes of an object (all of it if shorter) and its version, for a later
    /// [`Self::write_versioned`]; `None` if it doesn't exist.
    pub(crate) async fn read_prefix_versioned(&self, uri: &str, len: u64) -> Result<Option<(Bytes, UpdateVersion)>> {
        let opts = GetOptions { range: Some(GetRange::Bounded(0..len)), ..Default::default() };
        match self.store.get_opts(&self.path(uri)?, opts).await {
            Ok(r) => {
                let version = UpdateVersion { e_tag: r.meta.e_tag.clone(), version: r.meta.version.clone() };
                Ok(Some((r.bytes().await?, version)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Writes an object only if it is still at `version` (`None`: still absent). Stores without
    /// conditional updates (local disk) overwrite.
    pub(crate) async fn write_versioned(
        &self,
        uri: &str,
        bytes: Bytes,
        version: Option<UpdateVersion>,
    ) -> Result<VersionedWrite> {
        let path = self.path(uri)?;
        let mode = version.map_or(PutMode::Create, PutMode::Update);
        let opts = PutOptions { mode, ..Default::default() };
        match self.store.put_opts(&path, PutPayload::from_bytes(bytes.clone()), opts).await {
            Ok(_) => Ok(VersionedWrite::Written),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                Ok(VersionedWrite::Changed)
            }
            Err(object_store::Error::NotImplemented { .. }) => {
                self.store.put(&path, PutPayload::from_bytes(bytes)).await?;
                Ok(VersionedWrite::Written)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Writes (or overwrites) an object.
    pub async fn write(&self, uri: &str, bytes: Bytes) -> Result<()> {
        self.store.put(&self.path(uri)?, PutPayload::from_bytes(bytes)).await?;
        Ok(())
    }

    /// Writes only if the object doesn't exist yet. Returns `false` if it already existed.
    /// This is the commit primitive of the object-store catalog.
    pub async fn write_if_absent(&self, uri: &str, bytes: Bytes) -> Result<bool> {
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        match self.store.put_opts(&self.path(uri)?, PutPayload::from_bytes(bytes), opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes an object; a missing object is not an error.
    pub async fn delete(&self, uri: &str) -> Result<()> {
        match self.store.delete(&self.path(uri)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes many objects, using batch deletes where the backend supports them. Returns how
    /// many were requested; missing objects are not an error.
    pub async fn delete_many(&self, uris: Vec<String>) -> Result<usize> {
        let paths = uris.iter().map(|u| self.path(u)).collect::<Result<Vec<_>>>()?;
        let n = paths.len();
        let stream = futures::stream::iter(paths.into_iter().map(Ok)).boxed();
        let mut results = self.store.delete_stream(stream);
        while let Some(r) = results.next().await {
            match r {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(n)
    }

    /// Immediate child "directories" of `uri` (a prefix ending without `/`).
    pub async fn list_dirs(&self, uri: &str) -> Result<Vec<String>> {
        let prefix = self.path(uri)?;
        let res = self.store.list_with_delimiter(Some(&prefix)).await?;
        Ok(res.common_prefixes.iter().filter_map(|p| p.filename().map(str::to_string)).collect())
    }

    /// All objects under `uri`.
    pub async fn list(&self, uri: &str) -> Result<Vec<ListedObject>> {
        let prefix = self.path(uri)?;
        self.store
            .list(Some(&prefix))
            .map(|m| {
                let m = m?;
                Ok(ListedObject {
                    uri: format!("{}/{}", self.base, m.location),
                    size: m.size,
                    last_modified: m.last_modified,
                })
            })
            .try_collect()
            .await
    }

    /// An iceberg-rust `FileIO` backed by this store.
    pub fn file_io(&self) -> FileIO {
        FileIOBuilder::new(Arc::new(IcebergStorageFactory { inner: Some(self.clone()) })).build()
    }

    /// This store as an iceberg-rust `Storage`.
    pub fn iceberg_storage(&self) -> Arc<dyn iceberg::io::Storage> {
        Arc::new(IcebergStorage { inner: Some(self.clone()), uploads: None })
    }

    /// An iceberg-rust output file at `uri`.
    pub fn output_file(&self, uri: &str) -> OutputFile {
        OutputFile::new(self.iceberg_storage(), uri.to_string())
    }

    /// An output file whose upload completes in [`Uploads::send`] rather than when it is closed.
    /// A multipart upload of `uri` whose completion is queued in `uploads`, like
    /// [`Storage::deferred_output_file`], for writers that produce bytes themselves.
    pub(crate) fn deferred_writer(&self, uri: &str, uploads: &Uploads) -> Result<DeferredWriter> {
        Ok(DeferredWriter { inner: self.buf_writer(self.path(uri)?), uploads: uploads.clone() })
    }

    pub(crate) fn deferred_output_file(&self, uri: &str, uploads: &Uploads) -> OutputFile {
        let storage = IcebergStorage { inner: Some(self.clone()), uploads: Some(uploads.clone()) };
        OutputFile::new(Arc::new(storage), uri.to_string())
    }
}

/// An object found by [`Storage::list`].
#[derive(Clone, Debug)]
pub struct ListedObject {
    /// Absolute URI.
    pub uri: String,
    /// Size in bytes.
    pub size: u64,
    /// Last modification time.
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

/// Uploads prepared now and sent later, all at once. A commit encodes its data files, manifests,
/// manifest list and index first, then waits for one round of concurrent uploads instead of one
/// round per stage (spec 2, P4). A large file's earlier parts are uploaded while it is written;
/// only its last part waits here. Cheap to clone; clones share the queue.
#[derive(Clone, Default)]
pub(crate) struct Uploads(Arc<Mutex<Vec<Pending>>>);

enum Pending {
    Put {
        store: Arc<dyn ObjectStore>,
        path: Path,
        bytes: Bytes,
    },
    /// A file written through [`Storage::deferred_output_file`]: closing it completes the upload.
    Writer(BufWriter),
}

/// How many queued uploads are sent at once.
const UPLOAD_CONCURRENCY: usize = 32;

impl Uploads {
    /// Queues writing `bytes` to `uri`.
    pub(crate) fn put(&self, storage: &Storage, uri: &str, bytes: Bytes) -> Result<()> {
        self.push(Pending::Put { store: storage.store.clone(), path: storage.path(uri)?, bytes });
        Ok(())
    }

    fn push(&self, pending: Pending) {
        self.queue().push(pending);
    }

    fn queue(&self) -> std::sync::MutexGuard<'_, Vec<Pending>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends everything queued so far, concurrently. Every upload runs to its end even when one
    /// fails: one dropped halfway would leave its multipart upload open.
    pub(crate) async fn send(&self) -> Result<()> {
        let pending = std::mem::take(&mut *self.queue());
        let results: Vec<Result<()>> = futures::stream::iter(pending)
            .map(|p| async move {
                match p {
                    Pending::Put { store, path, bytes } => {
                        store.put(&path, PutPayload::from_bytes(bytes)).await?;
                    }
                    Pending::Writer(mut w) => w.shutdown().await?,
                }
                Ok(())
            })
            .buffer_unordered(UPLOAD_CONCURRENCY)
            .collect()
            .await;
        results.into_iter().collect()
    }

    /// Drops everything queued, aborting multipart uploads already started, concurrently.
    pub(crate) async fn abort(&self) {
        let pending = std::mem::take(&mut *self.queue());
        let writers = pending.into_iter().filter_map(|p| match p {
            Pending::Writer(w) => Some(w),
            Pending::Put { .. } => None,
        });
        futures::stream::iter(writers)
            .for_each_concurrent(UPLOAD_CONCURRENCY, |mut w| async move {
                // An upload that can't be aborted is left to the bucket's lifecycle rules.
                let _ = w.abort().await;
            })
            .await;
    }
}

impl std::fmt::Debug for Uploads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Uploads({} queued)", self.queue().len())
    }
}

// --- iceberg-rust adapter -----------------------------------------------------------------------
//
// iceberg-rust storage is pluggable (`Storage` + `StorageFactory`). Both traits require serde via
// typetag so storages can cross process boundaries; Timelake never serializes them, so the store
// handle is skipped and a deserialized instance is unusable by design.

fn ice_err(e: impl std::error::Error + Send + Sync + 'static) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, "Timelake storage").with_source(e)
}

fn detached() -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, "detached Timelake storage")
}

#[derive(Debug, Serialize, Deserialize)]
struct IcebergStorageFactory {
    #[serde(skip)]
    inner: Option<Storage>,
}

#[typetag::serde]
impl StorageFactory for IcebergStorageFactory {
    fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn iceberg::io::Storage>> {
        let inner = self.inner.clone().ok_or_else(detached)?;
        Ok(Arc::new(IcebergStorage { inner: Some(inner), uploads: None }))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct IcebergStorage {
    #[serde(skip)]
    inner: Option<Storage>,
    /// Set for deferred output files: writes are queued here instead of sent.
    #[serde(skip)]
    uploads: Option<Uploads>,
}

impl IcebergStorage {
    fn s(&self) -> iceberg::Result<&Storage> {
        self.inner.as_ref().ok_or_else(detached)
    }
    fn p(&self, path: &str) -> iceberg::Result<Path> {
        self.s()?.path(path).map_err(ice_err)
    }
}

#[async_trait]
#[typetag::serde]
impl iceberg::io::Storage for IcebergStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.s()?.exists(path).await.map_err(ice_err)
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let m = self.s()?.store.head(&self.p(path)?).await.map_err(ice_err)?;
        Ok(FileMetadata { size: m.size })
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        self.s()?.read(path).await.map_err(ice_err)
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        Ok(Box::new(ObjectRead { store: self.s()?.store.clone(), path: self.p(path)? }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> iceberg::Result<()> {
        match &self.uploads {
            Some(uploads) => uploads.put(self.s()?, path, bs).map_err(ice_err),
            None => self.s()?.write(path, bs).await.map_err(ice_err),
        }
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        let inner = Some(self.s()?.buf_writer(self.p(path)?));
        Ok(Box::new(ObjectWrite { inner, uploads: self.uploads.clone() }))
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        self.s()?.delete(path).await.map_err(ice_err)
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        let s = self.s()?;
        let uris = s.list(path).await.map_err(ice_err)?.into_iter().map(|o| o.uri).collect();
        s.delete_many(uris).await.map_err(ice_err)?;
        Ok(())
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        self.s()?.delete_many(paths.collect().await).await.map_err(ice_err)?;
        Ok(())
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        let storage = IcebergStorage { inner: self.inner.clone(), uploads: None };
        Ok(InputFile::new(Arc::new(storage), path.to_string()))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        let storage = IcebergStorage { inner: self.inner.clone(), uploads: self.uploads.clone() };
        Ok(OutputFile::new(Arc::new(storage), path.to_string()))
    }
}

#[derive(Debug)]
struct ObjectRead {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

#[async_trait]
impl FileRead for ObjectRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        self.store.get_range(&self.path, range).await.map_err(ice_err)
    }
}

#[derive(Debug)]
struct ObjectWrite {
    inner: Option<BufWriter>,
    uploads: Option<Uploads>,
}

#[async_trait]
impl FileWrite for ObjectWrite {
    async fn write(&mut self, bs: Bytes) -> iceberg::Result<()> {
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| iceberg::Error::new(iceberg::ErrorKind::Unexpected, "write after close"))?;
        w.put(bs).await.map_err(ice_err)
    }

    async fn close(&mut self) -> iceberg::Result<()> {
        let mut w = self
            .inner
            .take()
            .ok_or_else(|| iceberg::Error::new(iceberg::ErrorKind::Unexpected, "file already closed"))?;
        match &self.uploads {
            Some(uploads) => {
                uploads.push(Pending::Writer(w));
                Ok(())
            }
            None => w.shutdown().await.map_err(ice_err),
        }
    }
}

/// See [`Storage::deferred_writer`].
pub(crate) struct DeferredWriter {
    inner: BufWriter,
    uploads: Uploads,
}

impl DeferredWriter {
    /// Sends `bytes` (in parts, as they fill).
    pub(crate) async fn put(&mut self, bytes: Vec<u8>) -> Result<()> {
        if !bytes.is_empty() {
            self.inner.put(Bytes::from(bytes)).await?;
        }
        Ok(())
    }

    /// Queues the end of the upload, to be sent with the commit.
    pub(crate) fn close(self) {
        self.uploads.push(Pending::Writer(self.inner));
    }

    /// Aborts the upload, if it started one.
    pub(crate) async fn abort(mut self) {
        // An upload that can't be aborted is left to the bucket's lifecycle rules.
        let _ = self.inner.abort().await;
    }
}

/// The reader's storage options.
struct ReadOptions {
    /// `None`: the store's faster check.
    freshness_check: Option<FreshnessCheck>,
    version_lookup: VersionLookup,
    /// Connections to open ahead of a cold load.
    warm_connections: usize,
}

impl ReadOptions {
    /// The reader's options among `options`, which loses them.
    fn take(options: &mut HashMap<String, String>) -> Result<Self> {
        let warm_connections = match options.remove(WARM_CONNECTIONS_OPTION) {
            Some(n) => n.trim().parse().map_err(|_| {
                Error::config(format!("{WARM_CONNECTIONS_OPTION} must be a non-negative integer, got {n:?}"))
            })?,
            None => 0,
        };
        Ok(Self {
            freshness_check: take_option(options, FRESHNESS_CHECK_OPTION)?,
            version_lookup: take_option(options, VERSION_LOOKUP_OPTION)?.unwrap_or(VersionLookup::List),
            warm_connections,
        })
    }
}

/// `store` under the block cache, if on (and the cache, for its statistics).
fn with_cache(
    store: Arc<dyn ObjectStore>,
    base: &str,
    endpoint: Option<&str>,
    config: Option<CacheConfig>,
) -> (Arc<dyn ObjectStore>, Option<Arc<CachedStore>>) {
    let Some(config) = config else { return (store, None) };
    // Two stores never share disk blocks: the scope names the endpoint as well as the bucket.
    let scope = endpoint.map_or_else(|| base.to_string(), |e| format!("{base}@{e}"));
    let cache = Arc::new(CachedStore::new(store, scope, config));
    (Arc::new(Cached(cache.clone())), Some(cache))
}

/// The block cache's settings from the storage options (removed from `options`); `None` when off.
fn cache_config(options: &mut HashMap<String, String>, scheme: &str) -> Result<Option<CacheConfig>> {
    let bytes = |options: &mut HashMap<String, String>, key: &str, default: u64| -> Result<u64> {
        options.remove(key).map_or(Ok(default), |v| {
            v.trim().parse().map_err(|_| Error::config(format!("{key} must be a number of bytes, got {v:?}")))
        })
    };
    let memory = bytes(options, CACHE_MEMORY_BYTES_OPTION, DEFAULT_CACHE_MEMORY_BYTES)?;
    let disk_bytes = bytes(options, CACHE_DISK_BYTES_OPTION, DEFAULT_CACHE_DISK_BYTES)?;
    let block = bytes(options, CACHE_BLOCK_BYTES_OPTION, DEFAULT_CACHE_BLOCK_BYTES)?;
    if block < MIN_CACHE_BLOCK_BYTES {
        return Err(Error::config(format!("{CACHE_BLOCK_BYTES_OPTION} must be at least {MIN_CACHE_BLOCK_BYTES}")));
    }
    let request = bytes(options, CACHE_REQUEST_BYTES_OPTION, DEFAULT_CACHE_REQUEST_BYTES)?;
    let dir = options.remove(CACHE_DIR_OPTION).map(PathBuf::from);
    // On by default where each request is a round trip.
    let remote = !matches!(scheme, "file" | "memory");
    let default = if remote { CacheMode::Memory } else { CacheMode::Off };
    let disk = match take_option(options, CACHE_OPTION)?.unwrap_or(default) {
        CacheMode::Off => return Ok(None),
        CacheMode::Memory => false,
        CacheMode::Disk => true,
    };
    let disk = if disk {
        let dir = dir.or_else(default_cache_dir).ok_or_else(|| {
            Error::config(format!("{CACHE_OPTION}=disk: no cache directory found; set {CACHE_DIR_OPTION}"))
        })?;
        Some((dir, disk_bytes))
    } else {
        None
    };
    Ok(Some(CacheConfig { block, request, memory, disk }))
}

/// `timelake` in the OS cache directory: `~/Library/Caches` on macOS, `%LOCALAPPDATA%` on Windows,
/// else `$XDG_CACHE_HOME` or `~/.cache`.
fn default_cache_dir() -> Option<PathBuf> {
    let env = |k: &str| std::env::var_os(k).filter(|v| !v.is_empty()).map(PathBuf::from);
    let base = if cfg!(target_os = "macos") {
        env("HOME").map(|h| h.join("Library").join("Caches"))
    } else if cfg!(windows) {
        env("LOCALAPPDATA")
    } else {
        env("XDG_CACHE_HOME").or_else(|| env("HOME").map(|h| h.join(".cache")))
    };
    base.map(|b| b.join("timelake"))
}

/// Normalizes a warehouse location: absolute and relative local paths become `file://` URLs.
pub fn warehouse_url(location: &str) -> Result<Url> {
    if location.contains("://") {
        let s = location.trim_end_matches('/');
        return Url::parse(s).map_err(|e| Error::config(format!("bad warehouse URL {location}: {e}")));
    }
    let p = std::path::Path::new(location);
    std::fs::create_dir_all(p)?;
    let abs = p.canonicalize()?;
    let url = Url::from_directory_path(&abs).map_err(|()| Error::config(format!("bad local path {location}")))?;
    Url::parse(url.as_str().trim_end_matches('/')).map_err(|e| Error::config(format!("bad local path {location}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Larger than `BufWriter`'s buffer, so the file is uploaded in parts.
    const LARGE: usize = 11 << 20;

    fn memory() -> (Storage, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        (Storage::from_store(store.clone(), "memory://test"), store)
    }

    async fn write_deferred(storage: &Storage, uri: &str, uploads: &Uploads, len: usize) {
        let mut w = storage.deferred_output_file(uri, uploads).writer().await.unwrap();
        w.write(Bytes::from(vec![7u8; len])).await.unwrap();
        w.close().await.unwrap();
    }

    /// Deferred files, small and multipart, appear only when the queue is sent.
    #[tokio::test]
    async fn deferred_files_are_written_on_send() {
        let (storage, _) = memory();
        let uploads = Uploads::default();
        write_deferred(&storage, "memory://test/small", &uploads, 10).await;
        write_deferred(&storage, "memory://test/large", &uploads, LARGE).await;
        uploads.put(&storage, "memory://test/put", Bytes::from_static(b"x")).unwrap();
        for uri in ["memory://test/small", "memory://test/large", "memory://test/put"] {
            assert!(!storage.exists(uri).await.unwrap(), "{uri} written before send");
        }

        uploads.send().await.unwrap();
        assert_eq!(storage.read("memory://test/small").await.unwrap().len(), 10);
        assert_eq!(storage.read("memory://test/large").await.unwrap().len(), LARGE);
        assert_eq!(storage.read("memory://test/put").await.unwrap().as_ref(), b"x");
    }

    #[test]
    fn freshness_check_option() {
        let open = |url: &str, check: &str| {
            let options = HashMap::from([(FRESHNESS_CHECK_OPTION.to_string(), check.to_string())]);
            Storage::open(&Url::parse(url).unwrap(), options).map(|s| format!("{:?}", s.check))
        };
        let s3 = |key: &str, endpoint: &str| {
            let options = HashMap::from([
                (key.to_string(), endpoint.to_string()),
                ("aws_region".to_string(), "auto".to_string()),
            ]);
            format!("{:?}", Storage::open(&Url::parse("s3://b").unwrap(), options).unwrap().check)
        };
        assert_eq!(s3("aws_endpoint", "https://s3.us-east-2.amazonaws.com"), "Head");
        // Each way of naming the endpoint finds Tigris (#102).
        for key in ["aws_endpoint", "aws_endpoint_url", "aws_endpoint_url_s3"] {
            assert_eq!(s3(key, "https://t3.storage.dev"), "List", "{key}");
        }
        assert_eq!(open("s3://b", "LIST").unwrap(), "List");
        assert_eq!(open("memory://", "head").unwrap(), "Head");
        assert!(open("memory://", "list").is_err(), "memory can't list a name");
        assert!(open("s3://b", "maybe").is_err());

        let tigris_hint = HashMap::from([
            ("aws_endpoint".to_string(), "https://t3.storage.dev".to_string()),
            ("aws_region".to_string(), "auto".to_string()),
            (VERSION_LOOKUP_OPTION.to_string(), "hint".to_string()),
        ]);
        let s = Storage::open(&Url::parse("s3://b").unwrap(), tigris_hint).unwrap();
        assert_eq!((s.check, s.version_lookup), (FreshnessCheck::List, VersionLookup::Hint));
        let fast = Storage::open(&Url::parse("s3://b").unwrap(), HashMap::from([("aws_region".into(), "x".into())]));
        let fast = fast.unwrap();
        assert_eq!((fast.warm_connections, fast.version_lookup), (0, VersionLookup::List));
        let lookup = HashMap::from([(VERSION_LOOKUP_OPTION.to_string(), "sometimes".to_string())]);
        let err = Storage::open(&Url::parse("memory://").unwrap(), lookup).unwrap_err().to_string();
        assert_eq!(err, "timelake_version_lookup must be list or hint, got \"sometimes\"");
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path().to_string_lossy().to_string();
        let cache = HashMap::from([(CACHE_OPTION.to_string(), "Disk".to_string()), (CACHE_DIR_OPTION.into(), dir)]);
        assert!(Storage::open(&Url::parse("memory://").unwrap(), cache).unwrap().cache.is_some());
        let cache = HashMap::from([(CACHE_OPTION.to_string(), "ssd".to_string())]);
        assert!(Storage::open(&Url::parse("memory://").unwrap(), cache).is_err());
    }

    /// The disk cache's keys name the endpoint requests go to, however it was given (here as
    /// `endpoint_url`, which the S3 builder reads like `aws_endpoint`).
    #[test]
    fn the_cache_scope_names_the_endpoint() {
        let scope = |key: &str| {
            let options = HashMap::from([
                (key.to_string(), "https://a.example".to_string()),
                ("aws_region".to_string(), "auto".to_string()),
            ]);
            let s = Storage::open(&Url::parse("s3://b").unwrap(), options).unwrap();
            s.cache.unwrap().scope().to_string()
        };
        for key in ["aws_endpoint", "endpoint_url", "aws_endpoint_url_s3"] {
            assert_eq!(scope(key), "s3://b@https://a.example", "{key}");
        }
    }

    #[tokio::test]
    async fn aborted_files_are_never_written() {
        let (storage, store) = memory();
        let uploads = Uploads::default();
        write_deferred(&storage, "memory://test/large", &uploads, LARGE).await;
        uploads.put(&storage, "memory://test/put", Bytes::from_static(b"x")).unwrap();
        uploads.abort().await;
        uploads.send().await.unwrap();
        assert_eq!(store.list(None).count().await, 0);
    }
}
