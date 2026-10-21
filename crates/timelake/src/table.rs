use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use futures::future::{BoxFuture, Shared};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, ManifestContentType, ManifestFile,
    ManifestStatus, PrimitiveLiteral, SnapshotRef, TableMetadata, TableMetadataBuilder,
};
use timelake_format::iceberg as shared;

use crate::TableName;
use crate::contract::Contract;
use crate::error::{Error, Result};
use crate::lake::Lake;
use crate::summary;
use crate::write::{file_partition, partition_struct};

/// A table at one committed version. Cheap to clone; call [`Table::refresh`] to see newer commits.
///
/// Operations that commit ([`Table::append`], [`Table::maintain`]) return the new version and
/// leave `self` unchanged.
#[derive(Clone)]
pub struct Table {
    lake: Lake,
    name: TableName,
    location: String,
    version: u64,
    metadata: Arc<TableMetadata>,
    contract: Contract,
    schema: SchemaRef,
    /// The live files, when known without reading manifests (from the head, or derived by a commit).
    files: Option<Arc<Vec<LiveEntry>>>,
    /// For a version loaded from the head for a query: whether a newer version exists.
    check: Option<FreshnessCheck>,
}

/// Resolves to whether a newer version than the table's exists. Started when the table is loaded
/// and awaited before its data is read, so the request overlaps planning (spec 7).
pub(crate) type FreshnessCheck = Shared<BoxFuture<'static, Result<bool, Arc<Error>>>>;

/// A manifest entry of a data file, with inherited snapshot id and sequence numbers resolved.
#[derive(Clone, Debug)]
pub struct LiveEntry {
    /// `Added` or `Existing` for live files; `Deleted` entries only record a removal.
    pub status: ManifestStatus,
    /// The data file (shared: entries are cloned on every scan plan).
    pub data_file: Arc<DataFile>,
    /// The snapshot that added the file.
    pub snapshot_id: i64,
    /// Data sequence number: orders the file's rows against other files' rows.
    pub sequence_number: i64,
    /// Sequence number of the commit that added the file.
    pub file_sequence_number: i64,
}

impl LiveEntry {
    /// The file's lowest and highest value of the time column, field `time_id`, as its manifest
    /// entry records them; `None` where it doesn't.
    pub(crate) fn time_bounds(&self, time_id: i32) -> (Option<i64>, Option<i64>) {
        let bound = |bounds: &HashMap<i32, Datum>| match bounds.get(&time_id).map(Datum::literal) {
            Some(PrimitiveLiteral::Long(t)) => Some(*t),
            _ => None,
        };
        (bound(self.data_file.lower_bounds()), bound(self.data_file.upper_bounds()))
    }

    /// The entry as `timelake_format` records it (bounds in Iceberg's single-value encoding), for
    /// the head.
    pub(crate) fn to_shared(&self) -> Result<shared::Entry> {
        let f = &self.data_file;
        let bytes = |m: &HashMap<i32, Datum>| -> Result<BTreeMap<i32, Vec<u8>>> {
            m.iter().map(|(id, d)| Ok((*id, d.to_bytes()?.to_vec()))).collect()
        };
        let counts = |m: &HashMap<i32, u64>| m.iter().map(|(k, v)| (*k, *v)).collect();
        let file = shared::DataFile {
            content: match f.content_type() {
                DataContentType::Data => shared::Content::Data,
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => shared::Content::Deletes,
            },
            path: f.file_path().to_string(),
            partition: file_partition(f),
            record_count: f.record_count(),
            file_size: f.file_size_in_bytes(),
            column_sizes: counts(f.column_sizes()),
            value_counts: counts(f.value_counts()),
            null_counts: counts(f.null_value_counts()),
            nan_counts: counts(f.nan_value_counts()),
            lower_bounds: bytes(f.lower_bounds())?,
            upper_bounds: bytes(f.upper_bounds())?,
            split_offsets: f.split_offsets().map(<[i64]>::to_vec),
            sort_order_id: f.sort_order_id(),
        };
        Ok(shared::Entry {
            status: if self.status == ManifestStatus::Added { shared::Status::Added } else { shared::Status::Existing },
            snapshot_id: self.snapshot_id,
            sequence_number: self.sequence_number,
            file_sequence_number: self.file_sequence_number,
            file: Arc::new(file),
        })
    }

    /// The entry of a live file `timelake_format` recorded, in a table with `metadata`. Bounds of
    /// columns no longer in the schema are left out: iceberg-rust needs their type.
    pub(crate) fn from_shared(entry: &shared::Entry, metadata: &TableMetadata) -> Result<Self> {
        let f = &entry.file;
        let schema = metadata.current_schema();
        let typed = |m: &BTreeMap<i32, Vec<u8>>| -> Result<HashMap<i32, Datum>> {
            let mut out = HashMap::new();
            for (id, bytes) in m {
                if let Some(ty) = schema.field_by_id(*id).and_then(|f| f.field_type.as_primitive_type()) {
                    out.insert(*id, Datum::try_from_bytes(bytes, ty.clone())?);
                }
            }
            Ok(out)
        };
        let counts = |m: &BTreeMap<i32, u64>| m.iter().map(|(k, v)| (*k, *v)).collect::<HashMap<_, _>>();
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path(f.path.clone())
            .file_format(DataFileFormat::Parquet)
            .partition(partition_struct(f.partition))
            .partition_spec_id(metadata.default_partition_spec_id())
            .record_count(f.record_count)
            .file_size_in_bytes(f.file_size)
            .lower_bounds(typed(&f.lower_bounds)?)
            .upper_bounds(typed(&f.upper_bounds)?)
            .value_counts(counts(&f.value_counts))
            .null_value_counts(counts(&f.null_counts))
            .nan_value_counts(counts(&f.nan_counts))
            .column_sizes(counts(&f.column_sizes));
        if let Some(id) = f.sort_order_id {
            b.sort_order_id(id);
        }
        if let Some(offsets) = &f.split_offsets {
            b.split_offsets(Some(offsets.clone()));
        }
        let data_file = b.build().map_err(|e| {
            Error::from(iceberg::Error::new(iceberg::ErrorKind::Unexpected, "building data file").with_source(e))
        })?;
        Ok(Self {
            status: if entry.status == shared::Status::Added {
                ManifestStatus::Added
            } else {
                ManifestStatus::Existing
            },
            data_file: Arc::new(data_file),
            snapshot_id: entry.snapshot_id,
            sequence_number: entry.sequence_number,
            file_sequence_number: entry.file_sequence_number,
        })
    }
}

impl Table {
    pub(crate) fn new(
        lake: Lake,
        name: TableName,
        location: String,
        version: u64,
        metadata: TableMetadata,
    ) -> Result<Self> {
        let contract = Contract::from_properties(metadata.properties())?;
        contract.check_readable().map_err(|e| Error::invalid(format!("{name}: {e}")))?;
        let schema = Arc::new(schema_to_arrow_schema(metadata.current_schema())?);
        Ok(Self {
            lake,
            name,
            location,
            version,
            metadata: Arc::new(metadata),
            contract,
            schema,
            files: None,
            check: None,
        })
    }

    pub(crate) fn with_files(mut self, files: Arc<Vec<LiveEntry>>) -> Self {
        self.files = Some(files);
        self
    }

    /// A builder of the next version's metadata: this version's, with this version's file as the
    /// previous one.
    pub(crate) fn next_metadata(&self) -> TableMetadataBuilder {
        let previous = Some(timelake_format::layout::metadata_uri(self.location(), self.version()));
        TableMetadataBuilder::new_from_metadata(self.metadata().clone(), previous)
    }

    /// The live files if they are known without reading manifests.
    pub(crate) fn known_files(&self) -> Option<&Arc<Vec<LiveEntry>>> {
        self.files.as_ref()
    }

    pub(crate) fn with_check(mut self, check: FreshnessCheck) -> Self {
        self.check = Some(check);
        self
    }

    /// This version without its pending check for a newer one, so a scan reads exactly it.
    pub(crate) fn without_check(mut self) -> Self {
        self.check = None;
        self
    }

    /// Whether a newer version existed when this one was loaded from the head without checking.
    pub(crate) async fn is_stale(&self) -> Result<bool, Arc<Error>> {
        match &self.check {
            Some(check) => check.clone().await,
            None => Ok(false),
        }
    }

    /// The lake this table belongs to.
    pub fn lake(&self) -> &Lake {
        &self.lake
    }

    /// The table's name.
    pub fn name(&self) -> &TableName {
        &self.name
    }

    /// The table's root URI (`<warehouse>/<namespace>/<table>`).
    pub fn location(&self) -> &str {
        &self.location
    }

    /// The metadata version (`v<N>.metadata.json`) this handle reads.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The Iceberg metadata of this version.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// The Timelake contract (time column, series key, layout and maintenance policy).
    pub fn contract(&self) -> &Contract {
        &self.contract
    }

    /// The field id of the time column, if the table has one.
    pub(crate) fn time_field_id(&self) -> Option<i32> {
        let time = self.contract.time_column.as_ref()?;
        Some(self.metadata.current_schema().field_by_name(time)?.id)
    }

    /// Arrow schema of the table (fields carry Iceberg field ids as `PARQUET:field_id` metadata).
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Loads the latest committed version.
    pub async fn refresh(&self) -> Result<Table> {
        self.lake.load(&self.name).await
    }

    /// The current snapshot, if anything was ever committed.
    pub fn current_snapshot(&self) -> Option<&SnapshotRef> {
        self.metadata.current_snapshot()
    }

    /// A value from the current snapshot's summary.
    pub(crate) fn summary_value(&self, key: &str) -> Option<&str> {
        self.current_snapshot()?.summary().additional_properties.get(key).map(String::as_str)
    }

    /// Location of the last-value index for the current snapshot, if the table has one.
    pub fn last_value_index(&self) -> Option<String> {
        self.summary_value(summary::LAST_VALUE_INDEX).map(str::to_string)
    }

    /// Manifests of the current snapshot. A table with delete files (row-level deletes another
    /// engine wrote) is an error: Timelake doesn't apply them, so it would read the deleted rows
    /// back, and carry the delete files over as data when it writes.
    pub async fn manifests(&self) -> Result<Arc<Vec<ManifestFile>>> {
        let Some(snapshot) = self.current_snapshot() else { return Ok(Arc::default()) };
        let manifests = self.lake.manifest_list(snapshot.manifest_list(), self.metadata.format_version()).await?;
        match manifests.iter().find(|m| m.content != ManifestContentType::Data) {
            Some(m) => Err(self.has_deletes(&m.manifest_path)),
            None => Ok(manifests),
        }
    }

    /// All live data files of the current snapshot (an error with delete files, as [`Self::manifests`]).
    pub async fn files(&self) -> Result<Vec<LiveEntry>> {
        if let Some(files) = &self.files {
            return Ok(files.as_ref().clone());
        }
        let manifests = self.manifests().await?;
        let loaded = futures::future::try_join_all(manifests.iter().map(|m| self.lake.manifest(m))).await?;
        let live: Vec<LiveEntry> =
            loaded.iter().flat_map(|es| es.iter().filter(|e| e.status != ManifestStatus::Deleted).cloned()).collect();
        match live.iter().find(|e| e.data_file.content_type() != DataContentType::Data) {
            Some(e) => Err(self.has_deletes(e.data_file.file_path())),
            None => Ok(live),
        }
    }

    fn has_deletes(&self, path: &str) -> Error {
        Error::invalid(format!(
            "{}: the table has delete files ({path}), which Timelake doesn't apply; compact them away with the \
             engine that wrote them first",
            self.name
        ))
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").field("name", &self.name).field("version", &self.version).finish_non_exhaustive()
    }
}
