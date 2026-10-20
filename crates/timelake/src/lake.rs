//! [`Lake`]: a warehouse location, and the object-store-only catalog of its tables.
//!
//! Catalog layout (Hadoop-catalog compatible, so `DuckDB` `iceberg_scan('<table dir>')` and Spark's
//! `HadoopCatalog` can read it):
//!
//! ```text
//! <warehouse>/<namespace>/<table>/metadata/v<N>.metadata.json   committed with put-if-absent
//! <warehouse>/<namespace>/<table>/metadata/version-hint.text    "<N>", best-effort pointer
//! <warehouse>/<namespace>/<table>/metadata/timelake-head        state of version N for readers
//! <warehouse>/<namespace>/<table>/data/<partition>/<file>.parquet
//! ```
//!
//! A commit is "create `v<N+1>.metadata.json` if it doesn't exist". Exactly one writer wins; the
//! others get [`Error::Conflict`] and rebase. No database or catalog server is involved. The
//! version hint and the head (see [`crate::head`]) are written after the commit, as shortcuts.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryStreamExt};
use iceberg::TableCreation;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{
    FormatVersion, Manifest, ManifestFile, ManifestList, ManifestStatus, NullOrder, Schema, SortDirection, SortField,
    SortOrder, TableMetadata, TableMetadataBuilder, Transform, UnboundPartitionSpec,
};

use crate::alter::property_key;
use crate::cache::Caches;
use crate::contract::{Chunk, Contract, TableOptions, format_duration};
use crate::error::{Error, Result};
use crate::head::{self, Codecs, HeadCodec};
use crate::name::TableName;
use crate::property;
use crate::schema::{UTC, normalize_schema};
use crate::settings::{self, Level};
use crate::storage::{Storage, warehouse_url};
use crate::table::{FreshnessCheck, LiveEntry, Table};
use timelake_format::INGEST_TIME_COLUMN;
use timelake_format::iceberg::avro_name;

/// Where a cold load starts.
enum Start {
    /// The head, known to be the newest version.
    Current(Box<Table>),
    /// The head, not yet checked for a newer version.
    Unchecked(Box<Table>),
    /// The metadata of this version (or a newer one).
    Version(u64),
}

/// The newest version as a cold load finds it.
enum Newest {
    /// From a listing of the metadata files: authoritative. `None`: there are none.
    Listed(Option<u64>),
    /// From the version hint, which may lag. `None`: there is no hint.
    Hint(Option<u64>),
}

/// How many object-store requests listing runs at once.
const LIST_CONCURRENCY: usize = 16;

/// A warehouse: a root location holding namespaces and tables. Cheap to clone; clones share
/// the storage client and caches.
#[derive(Clone)]
pub struct Lake {
    inner: Arc<LakeInner>,
}

struct LakeInner {
    storage: Storage,
    root: String,
    caches: Caches,
    head_codecs: Codecs,
}

impl Lake {
    /// Opens (or initializes) a warehouse. `location` is a URL (`s3://bucket/prefix`, `gs://…`,
    /// `file:///…`) or a local path; `options` are storage options (see [`Storage::open`]).
    pub fn open(location: &str, options: HashMap<String, String>) -> Result<Self> {
        let url = warehouse_url(location)?;
        let storage = Storage::open(&url, options)?;
        Ok(Self::with_storage(storage, url.as_str().trim_end_matches('/')))
    }

    /// A lake rooted at `root` on an already opened store.
    pub(crate) fn with_storage(storage: Storage, root: &str) -> Self {
        Self {
            inner: Arc::new(LakeInner {
                storage,
                root: root.to_string(),
                caches: Caches::default(),
                head_codecs: Codecs::default(),
            }),
        }
    }

    /// The object store holding the warehouse.
    pub fn storage(&self) -> &Storage {
        &self.inner.storage
    }

    /// The warehouse root URI.
    pub fn location(&self) -> &str {
        &self.inner.root
    }

    pub(crate) fn caches(&self) -> &Caches {
        &self.inner.caches
    }

    /// Adds a codec for head objects (see [`crate::head`]), or replaces the one with the same name.
    /// Tables select it with the `timelake.head-format` property; heads it wrote can then be read.
    pub fn register_head_codec(&self, codec: Arc<dyn HeadCodec>) {
        self.inner.head_codecs.register(codec);
    }

    pub(crate) fn head_codecs(&self) -> &Codecs {
        &self.inner.head_codecs
    }

    /// Where a table lives: `<warehouse>/<namespace>/<table>`.
    pub fn table_location(&self, name: &TableName) -> String {
        format!("{}/{}/{}", self.inner.root, name.namespace(), name.name())
    }

    /// Namespaces in the warehouse, sorted.
    pub async fn namespaces(&self) -> Result<Vec<String>> {
        let mut v = if self.storage().can_list() {
            self.storage().list_dirs(&self.inner.root).await?
        } else {
            self.indexed_tables().await?.into_iter().map(|t| t.namespace().to_string()).collect()
        };
        v.sort();
        v.dedup();
        Ok(v)
    }

    /// Tables in a namespace (directories that contain `metadata/version-hint.text`), sorted.
    pub async fn tables(&self, namespace: &str) -> Result<Vec<TableName>> {
        if !self.storage().can_list() {
            return Ok(self.indexed_tables().await?.into_iter().filter(|t| t.namespace() == namespace).collect());
        }
        let dirs = self.storage().list_dirs(&format!("{}/{namespace}", self.inner.root)).await?;
        let checks: Vec<_> = dirs
            .iter()
            // Directories with names a table can't have are not tables.
            .filter_map(|d| TableName::new(namespace, d).ok())
            .map(|name| async move {
                let exists = self.storage().exists(&hint_uri(&self.table_location(&name))).await?;
                Ok::<_, Error>(exists.then_some(name))
            })
            .collect();
        let found: Vec<Option<TableName>> =
            futures::stream::iter(checks).buffered(LIST_CONCURRENCY).try_collect().await?;
        let mut tables: Vec<TableName> = found.into_iter().flatten().collect();
        tables.sort();
        Ok(tables)
    }

    /// Every table in the warehouse, sorted by namespace and name.
    pub async fn all_tables(&self) -> Result<Vec<TableName>> {
        let namespaces = self.namespaces().await?;
        let tables = futures::future::try_join_all(namespaces.iter().map(|ns| self.tables(ns))).await?;
        Ok(tables.into_iter().flatten().collect())
    }

    /// Tables recorded in the warehouse's `timelake-tables.json` index.
    async fn indexed_tables(&self) -> Result<Vec<TableName>> {
        let Some(bytes) = self.storage().read_opt(&self.tables_index_uri()).await? else { return Ok(vec![]) };
        let names: Vec<String> = serde_json::from_slice(&bytes)?;
        names.iter().map(|n| n.parse()).collect()
    }

    fn tables_index_uri(&self) -> String {
        format!("{}/{}", self.inner.root, timelake_format::TABLES_INDEX)
    }

    /// Rewrites `timelake-tables.json`, the table index used by readers that cannot list (HTTPS,
    /// browsers). Called on create and drop; run it explicitly after copying tables around.
    pub async fn write_tables_index(&self) -> Result<Vec<String>> {
        let names: Vec<String> = self.all_tables().await?.iter().map(TableName::to_string).collect();
        self.storage().write(&self.tables_index_uri(), Bytes::from(serde_json::to_vec_pretty(&names)?)).await?;
        Ok(names)
    }

    /// Creates a table from an Arrow schema. Arrow types are normalized to what Iceberg supports
    /// (e.g. any timestamp → microseconds, small ints → int, dictionaries → their value type).
    pub async fn create_table<N>(&self, name: N, schema: &ArrowSchema, opts: TableOptions) -> Result<Table>
    where
        N: TryInto<TableName>,
        Error: From<N::Error>,
    {
        self.create_table_with(&name.try_into()?, schema, opts, HashMap::new()).await
    }

    /// Creates a table with properties Timelake manages itself (`managed`, unchecked), such as a
    /// rollup's definition.
    pub(crate) async fn create_table_with(
        &self,
        name: &TableName,
        schema: &ArrowSchema,
        opts: TableOptions,
        managed: HashMap<String, String>,
    ) -> Result<Table> {
        let name = name.clone();
        let location = self.table_location(&name);
        let storage = self.storage();
        if storage.exists(&hint_uri(&location)).await? {
            return Err(Error::AlreadyExists(name.to_string()));
        }
        let metadata = new_table_metadata(&name, &location, schema, &opts, managed)?;
        self.check_head_format(&Contract::from_properties(metadata.properties())?.head_format)?;
        if !storage.write_if_absent(&metadata_uri(&location, 1), Bytes::from(serde_json::to_vec(&metadata)?)).await? {
            return Err(Error::AlreadyExists(name.to_string()));
        }
        storage.write(&hint_uri(&location), Bytes::from("1")).await?;
        let table = Table::new(self.clone(), name, location, 1, metadata)?.with_files(Arc::default());
        head::update(&table);
        if storage.can_list() {
            // The index only serves readers that can't list; a stale one is repaired by the next
            // create, drop or `write_tables_index`, so it must not fail the create.
            let _ = self.write_tables_index().await;
        }
        Ok(table)
    }

    /// Errors unless `format` is `none` or a head codec this lake has.
    pub(crate) fn check_head_format(&self, format: &str) -> Result<()> {
        if format == timelake_format::head::NO_HEAD || self.head_codecs().get(format).is_some() {
            return Ok(());
        }
        Err(Error::invalid(format!("unknown head format {format:?} (built in: arrow, json, none)")))
    }

    /// Loads the current version of a table.
    pub async fn table<N>(&self, name: N) -> Result<Table>
    where
        N: TryInto<TableName>,
        Error: From<N::Error>,
    {
        self.load(&name.try_into()?).await
    }

    /// Loads the current version. A version this process already has is confirmed with one
    /// request; otherwise the table comes from its head when that is current (see
    /// [`Lake::start_load`]), or through the metadata chain.
    pub(crate) async fn load(&self, name: &TableName) -> Result<Table> {
        if let Some((_, table)) = self.caches().tables.get(name)
            && !self.storage().check_exists(&next_metadata_uri(&table)).await?
        {
            return Ok(self.reuse(table));
        }
        match self.start_load(name).await? {
            Start::Current(table) => Ok(self.reuse(*table)),
            Start::Unchecked(table) if !self.storage().check_exists(&next_metadata_uri(&table)).await? => {
                Ok(self.reuse(*table))
            }
            Start::Unchecked(table) => self.load_from(name, table.version() + 1).await,
            Start::Version(version) => self.load_from(name, version).await,
        }
    }

    /// Like [`Lake::load`], but a version that still needs the check for a newer one is returned
    /// at once: the check runs while the query is planned, and the scan waits for it (and reloads
    /// if the version was outdated).
    async fn load_for_query(&self, name: &TableName) -> Result<Table> {
        if let Some((_, table)) = self.caches().tables.get(name) {
            return Ok(self.reuse(self.with_freshness_check(table)));
        }
        match self.start_load(name).await? {
            Start::Current(table) => Ok(self.reuse(*table)),
            Start::Unchecked(table) => Ok(self.reuse(self.with_freshness_check(*table))),
            Start::Version(version) => self.load_from(name, version).await,
        }
    }

    /// Records `table` as seen now, and returns it.
    fn reuse(&self, table: Table) -> Table {
        self.remember(&table);
        table
    }

    /// `table` with a check for a newer version, started now.
    fn with_freshness_check(&self, table: Table) -> Table {
        let (storage, next) = (self.storage().clone(), next_metadata_uri(&table));
        let check: FreshnessCheck = tokio::spawn(async move { storage.check_exists(&next).await })
            .map(|r| match r {
                Ok(r) => r.map_err(Arc::new),
                Err(e) => Err(Arc::new(Error::Io(std::io::Error::other(e)))),
            })
            .boxed()
            .shared();
        table.with_check(check)
    }

    /// The first step of a cold load. The head and the newest version are read at once: a LIST of
    /// `metadata/v` on stores that list by prefix (which also says whether the head is current),
    /// otherwise the version hint. A table without a head (written by another engine or an older
    /// Timelake) loads as fast as before heads existed. Connections for the reads that follow are
    /// opened at the same time (P3).
    async fn start_load(&self, name: &TableName) -> Result<Start> {
        let location = self.table_location(name);
        let hint_uri = hint_uri(&location);
        self.storage().warm_up(&hint_uri);
        let (head, newest) = tokio::join!(head::read(self, name, &location), self.newest_version(&location, &hint_uri));
        match (head?, newest?) {
            (Some(table), Newest::Listed(Some(newest))) if table.version() >= newest => {
                Ok(Start::Current(Box::new(table)))
            }
            // The hint is ahead: the head is outdated.
            (Some(table), Newest::Hint(Some(hint))) if hint > table.version() => Ok(Start::Version(hint)),
            (Some(table), Newest::Hint(_)) => Ok(Start::Unchecked(Box::new(table))),
            (_, Newest::Listed(Some(version)) | Newest::Hint(Some(version))) => Ok(Start::Version(version)),
            (_, Newest::Listed(None) | Newest::Hint(None)) => Err(Error::NotFound(name.to_string())),
        }
    }

    /// The newest committed version, listed, or else (on stores that can't list, or with
    /// `timelake_version_lookup=hint`) the version hint.
    async fn newest_version(&self, location: &str, hint_uri: &str) -> Result<Newest> {
        if self.storage().list_on_load()
            && let Some(listed) = self.storage().list_prefix(&format!("{location}/metadata/v")).await?
        {
            return Ok(Newest::Listed(listed.iter().filter_map(|uri| metadata_version(uri)).max()));
        }
        let Some(hint) = self.storage().read_opt(hint_uri).await? else { return Ok(Newest::Hint(None)) };
        let version = std::str::from_utf8(&hint)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .ok_or_else(|| Error::invalid(format!("bad version hint at {hint_uri}")))?;
        Ok(Newest::Hint(Some(version)))
    }

    /// Loads `version`, or a newer one if it exists.
    async fn load_from(&self, name: &TableName, mut version: u64) -> Result<Table> {
        let location = self.table_location(name);
        let storage = self.storage();
        // The hint and the head are updated after the commit, so they may lag: probe forward while fetching.
        loop {
            let (current, next) = (metadata_uri(&location, version), metadata_uri(&location, version + 1));
            let (current, next) = tokio::join!(storage.read_opt(&current), storage.check_exists(&next));
            if next? {
                version += 1;
                continue;
            }
            let bytes = current?.ok_or_else(|| Error::NotFound(format!("{name} metadata v{version}")))?;
            let metadata: TableMetadata = serde_json::from_slice(&bytes)?;
            let table = Table::new(self.clone(), name.clone(), location, version, metadata)?;
            self.remember(&table);
            return Ok(table);
        }
    }

    /// Like [`Lake::table`], but for the Timelake reader: reuses a version this process loaded,
    /// checked or committed within the table's `version-check-interval` (or `interval`, a
    /// session's value), and at least within `at_least`. Query planning passes a short `at_least`
    /// after its first lookup, so one query checks each table once. A reused version may still
    /// be checked for freshness (see `Lake::load_for_query`).
    pub(crate) async fn table_recent(
        &self,
        name: &TableName,
        interval: Option<Duration>,
        at_least: Duration,
    ) -> Result<Table> {
        match self.caches().tables.get(name) {
            Some((at, table))
                if at.elapsed() <= interval.unwrap_or(table.contract().version_check_interval).max(at_least) =>
            {
                Ok(table)
            }
            _ => self.load_for_query(name).await,
        }
    }

    /// Whether this process has seen a version of `name` newer than `version`.
    pub(crate) fn has_newer(&self, name: &TableName, version: u64) -> bool {
        self.caches().tables.get(name).is_some_and(|(_, t)| t.version() > version)
    }

    /// Records a table version as recently seen, unless a newer one is already recorded.
    pub(crate) fn remember(&self, table: &Table) {
        let entry = (Instant::now(), table.clone());
        self.caches().tables.insert_unless(table.name().clone(), entry, |(_, old)| old.version() > table.version());
    }

    /// Deletes a table and all its files.
    pub async fn drop_table<N>(&self, name: N) -> Result<()>
    where
        N: TryInto<TableName>,
        Error: From<N::Error>,
    {
        let name = name.try_into()?;
        self.caches().tables.remove(&name);
        let uris = self.storage().list(&self.table_location(&name)).await?.into_iter().map(|o| o.uri).collect();
        self.storage().delete_many(uris).await?;
        // The table is gone; a stale index entry only makes index-based readers see "not found".
        let _ = self.write_tables_index().await;
        Ok(())
    }

    pub(crate) async fn manifest_list(&self, uri: &str, version: FormatVersion) -> Result<Arc<Vec<ManifestFile>>> {
        if let Some(list) = self.caches().manifest_lists.get(uri) {
            return Ok(list);
        }
        let bytes = self.storage().read(uri).await?;
        let list: Arc<Vec<ManifestFile>> =
            Arc::new(ManifestList::parse_with_version(&bytes, version)?.consume_entries().into_iter().collect());
        self.caches().manifest_lists.insert(uri.to_string(), list.clone());
        Ok(list)
    }

    /// Entries of one manifest with inherited snapshot ids and sequence numbers filled in.
    pub(crate) async fn manifest(&self, mf: &ManifestFile) -> Result<Arc<Vec<LiveEntry>>> {
        if let Some(entries) = self.caches().manifests.get(&mf.manifest_path) {
            return Ok(entries);
        }
        let bytes = self.storage().read(&mf.manifest_path).await?;
        let (entries, _) = Manifest::parse_avro(&bytes)?.into_parts();
        let entries: Arc<Vec<LiveEntry>> = Arc::new(
            entries
                .iter()
                .map(|e| {
                    let added = e.status == ManifestStatus::Added;
                    let inherit = |v: Option<i64>| v.or(added.then_some(mf.sequence_number)).unwrap_or(0);
                    LiveEntry {
                        status: e.status,
                        data_file: Arc::new(e.data_file.clone()),
                        snapshot_id: e.snapshot_id.unwrap_or(mf.added_snapshot_id),
                        sequence_number: inherit(e.sequence_number),
                        file_sequence_number: inherit(e.file_sequence_number),
                    }
                })
                .collect(),
        );
        self.caches().manifests.insert(mf.manifest_path.clone(), entries.clone());
        Ok(entries)
    }
}

impl fmt::Debug for Lake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lake({})", self.location())
    }
}

pub(crate) fn metadata_uri(location: &str, version: u64) -> String {
    format!("{location}/metadata/v{version}.metadata.json")
}

fn next_metadata_uri(table: &Table) -> String {
    metadata_uri(table.location(), table.version() + 1)
}

/// The version of a `…/v<N>.metadata.json` URI.
fn metadata_version(uri: &str) -> Option<u64> {
    uri.rsplit('/').next()?.strip_prefix('v')?.strip_suffix(".metadata.json")?.parse().ok()
}

pub(crate) fn hint_uri(location: &str) -> String {
    format!("{location}/metadata/version-hint.text")
}

/// Metadata of a new table: schema, partitioning and sort order from the options, and the
/// contract as properties.
fn new_table_metadata(
    name: &TableName,
    location: &str,
    schema: &ArrowSchema,
    opts: &TableOptions,
    managed: HashMap<String, String>,
) -> Result<TableMetadata> {
    let mut normalized = normalize_schema(schema, opts.time_column.as_deref())?;
    let ingest_time = opts.time_column.is_some() && opts.ingest_time.unwrap_or(true);
    if ingest_time {
        normalized = with_ingest_time_column(&normalized)?;
    }
    let ice_schema = arrow_schema_to_schema_auto_assign_ids(&normalized)?;
    let mut creation = TableCreation::builder()
        .name(name.name().to_string())
        .location(location.to_string())
        .schema(ice_schema.clone())
        .format_version(FormatVersion::V2)
        .build();
    let mut props: HashMap<String, String> = HashMap::from([
        (property::FORMAT_VERSION.into(), timelake_format::FORMAT_VERSION.to_string()),
        ("write.format.default".into(), "parquet".into()),
        ("write.parquet.compression-codec".into(), "zstd".into()),
        // Keep metadata.json small: it is read by every query.
        ("write.metadata.previous-versions-max".into(), "20".into()),
    ]);
    match &opts.time_column {
        Some(time_column) => {
            time_series_layout(&mut creation, &mut props, &ice_schema, &normalized, time_column, opts)?;
        }
        None if !opts.series_key.is_empty() => return Err(Error::invalid("series_key requires a time_column")),
        None => {}
    }
    if ingest_time {
        props.insert(property::INGEST_TIME_COLUMN.into(), INGEST_TIME_COLUMN.into());
    }
    if let Some(retention) = opts.retention {
        props.insert(property::RETENTION.into(), format_duration(retention));
    }
    // Settings in any spelling, as `alter` takes them (`retention`, `timelake.read_dedup`).
    let named =
        opts.properties.iter().map(|(k, v)| Ok((property_key(k)?, v.clone()))).collect::<Result<HashMap<_, _>>>()?;
    props.extend(settings::check_properties(&named, Level::Table)?);
    props.extend(managed);
    Contract::from_properties(&props)?; // validate
    creation.properties = props;
    Ok(TableMetadataBuilder::from_table_creation(creation)?.build()?.metadata)
}

/// `schema` with the insertion-time column (`_ingest_ts`, a timestamp, never null) at the end, or
/// in its place if the schema has it already (an import keeping arrival times).
fn with_ingest_time_column(schema: &ArrowSchema) -> Result<ArrowSchema> {
    let ingest = Field::new(INGEST_TIME_COLUMN, DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())), false);
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    match fields.iter_mut().find(|f| f.name() == INGEST_TIME_COLUMN) {
        Some(f) if matches!(f.data_type(), DataType::Timestamp(_, _)) => *f = ingest,
        Some(f) => {
            return Err(Error::invalid(format!("{INGEST_TIME_COLUMN} must be a timestamp, got {}", f.data_type())));
        }
        None => fields.push(ingest),
    }
    Ok(ArrowSchema::new(fields))
}

/// Partitions a time-series table by time, sorts it by `(series_key…, time)`, and records the
/// contract.
fn time_series_layout(
    creation: &mut TableCreation,
    props: &mut HashMap<String, String>,
    schema: &Schema,
    normalized: &ArrowSchema,
    time_column: &str,
    opts: &TableOptions,
) -> Result<()> {
    let field_id = |column: &str, role: &str| {
        schema
            .field_by_name(column)
            .map(|f| f.id)
            .ok_or_else(|| Error::invalid(format!("{role} column {column:?} is not in the schema")))
    };
    let time_id = field_id(time_column, "time")?;
    match normalized.field_with_name(time_column)?.data_type() {
        DataType::Timestamp(_, _) => {}
        other => return Err(Error::invalid(format!("time column {time_column:?} must be a timestamp, got {other}"))),
    }
    let chunk = opts.chunk.unwrap_or(Chunk::Day);
    let transform = match chunk {
        Chunk::Hour => Transform::Hour,
        Chunk::Day => Transform::Day,
        Chunk::Month => Transform::Month,
    };
    creation.partition_spec = Some(
        UnboundPartitionSpec::builder()
            // A valid Avro name: manifests record it as an Avro field, which readers reject otherwise.
            .add_partition_field(time_id, avro_name(&format!("{time_column}_{chunk}")), transform)?
            .build(),
    );
    let mut order = SortOrder::builder();
    order.with_order_id(1);
    for column in &opts.series_key {
        order.with_sort_field(ascending(field_id(column, "series key")?));
    }
    order.with_sort_field(ascending(time_id));
    creation.sort_order = Some(order.build(schema)?);

    props.insert(property::TIME_COLUMN.into(), time_column.to_string());
    props.insert(property::CHUNK.into(), chunk.to_string());
    if !opts.series_key.is_empty() {
        props.insert(property::SERIES_KEY.into(), opts.series_key.join(","));
    }
    // Dedup defaults on when rows have an identity (series key + time).
    props.insert(property::DEDUP.into(), opts.dedup.unwrap_or(!opts.series_key.is_empty()).to_string());
    Ok(())
}

fn ascending(source_id: i32) -> SortField {
    SortField {
        source_id,
        transform: Transform::Identity,
        direction: SortDirection::Ascending,
        null_order: NullOrder::First,
    }
}
