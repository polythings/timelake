//! What the reader needs from a table's Iceberg metadata (`vN.metadata.json`) and manifests
//! (Avro): the contract, the current schema's columns, the snapshots, and the live data files
//! with their bounds by column name. Read with `timelake_format`'s readers, as the shared write
//! path reads them.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use parquet::basic::TimeUnit;
use serde::Serialize;
use timelake_format::Contract;
use timelake_format::bounds::{Bound, decode};
use timelake_format::iceberg::{Content, Entry, ManifestFile, Status, read_manifest, read_manifest_list};
use timelake_format::metadata::{Metadata, SnapshotRef};

use crate::error::{Error, Result};
use crate::prune::scale_to_micros;

/// How many snapshots [`TableMeta::snapshots`] keeps, newest first.
pub const MAX_SNAPSHOTS: usize = 50;

/// A top-level column of the current schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub id: i32,
    pub name: String,
    /// Iceberg type name for primitives (`long`, `timestamptz`, …); nested types are `"nested"`.
    #[serde(rename = "type")]
    pub ty: String,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotInfo {
    /// The snapshot id as a string: it does not fit a JavaScript number.
    pub id: String,
    pub timestamp_ms: i64,
    pub operation: String,
    pub summary: HashMap<String, String>,
}

impl From<&SnapshotRef> for SnapshotInfo {
    fn from(s: &SnapshotRef) -> Self {
        Self {
            id: s.id.to_string(),
            timestamp_ms: s.timestamp_ms,
            operation: s.summary.get("operation").cloned().unwrap_or_default(),
            summary: s.summary.clone(),
        }
    }
}

/// What a reader needs from the table metadata.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMeta {
    pub location: String,
    pub format_version: u64,
    pub schema: Vec<Field>,
    pub properties: HashMap<String, String>,
    pub time_column: Option<String>,
    pub series_key: Vec<String>,
    pub chunk: Option<String>,
    pub dedup: bool,
    pub current_snapshot: Option<SnapshotInfo>,
    pub manifest_list: Option<String>,
    pub last_value_index: Option<String>,
    pub snapshot_count: usize,
    /// Newest first, at most [`MAX_SNAPSHOTS`].
    pub snapshots: Vec<SnapshotInfo>,
    pub last_updated_ms: i64,
}

impl TableMeta {
    /// The current schema's top-level columns by field id.
    pub(crate) fn fields_by_id(&self) -> HashMap<i32, &Field> {
        by_id(&self.schema)
    }

    /// Parses `vN.metadata.json`, including the Timelake contract in its properties.
    pub fn parse(json: &str) -> Result<Self> {
        let metadata = Metadata::parse(json.as_bytes())?;
        let properties = metadata.properties();
        let contract = Contract::from_properties(&properties)?;
        contract.check_readable().map_err(Error::Unreadable)?;
        let schema = metadata
            .columns()?
            .into_iter()
            .map(|c| Field {
                id: c.id,
                name: c.name,
                ty: c.iceberg_type.unwrap_or_else(|| "nested".into()),
                required: c.required,
            })
            .collect();
        let all = metadata.snapshot_list();
        let current = metadata.current_snapshot_id().and_then(|id| all.iter().find(|s| s.id == id));
        let mut snapshots: Vec<SnapshotInfo> = all.iter().map(SnapshotInfo::from).collect();
        snapshots.sort_by_key(|s| Reverse(s.timestamp_ms));
        snapshots.truncate(MAX_SNAPSHOTS);
        Ok(Self {
            location: metadata.location().to_string(),
            format_version: metadata.format_version(),
            schema,
            time_column: contract.time_column,
            series_key: contract.series_key,
            chunk: contract.chunk.map(|c| c.as_str().to_string()),
            dedup: contract.dedup,
            properties,
            current_snapshot: current.map(SnapshotInfo::from),
            manifest_list: current.map(|s| s.manifest_list.clone()),
            last_value_index: current.and_then(|s| s.last_value_index.clone()),
            snapshot_count: all.len(),
            snapshots,
            last_updated_ms: metadata.last_updated_ms(),
        })
    }
}

fn by_id(schema: &[Field]) -> HashMap<i32, &Field> {
    schema.iter().map(|f| (f.id, f)).collect()
}

// --- manifests ----------------------------------------------------------------------------------

/// The data manifests of a manifest list. Delete manifests (row-level deletes another engine
/// wrote) are an error: this reader doesn't apply them, and rows they delete would come back.
pub fn data_manifests(bytes: &[u8]) -> Result<Vec<ManifestFile>> {
    let manifests = read_manifest_list(bytes)?;
    if let Some(m) = manifests.iter().find(|m| m.content == Content::Deletes) {
        return Err(deletes_unsupported(&m.path));
    }
    Ok(manifests)
}

fn deletes_unsupported(path: &str) -> Error {
    Error::invalid(format!(
        "the table has delete files ({path}), which this reader doesn't apply; read it with an engine that does"
    ))
}

/// The live data files of `manifest`, read from its `bytes`. Entries without sequence numbers
/// inherit the manifest's, as Iceberg says.
pub fn parse_manifest(manifest: &ManifestFile, bytes: &[u8], schema: &[Field]) -> Result<Vec<DataFile>> {
    let by_id = by_id(schema);
    read_manifest(manifest, bytes)?
        .iter()
        .filter(|e| e.status != Status::Deleted)
        .map(|e| match e.file.content {
            Content::Data => Ok(DataFile::from_entry(e, &by_id)),
            Content::Deletes => Err(deletes_unsupported(&e.file.path)),
        })
        .collect()
}

/// A live data file with its column bounds, keyed by column name.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    pub path: String,
    pub size: u64,
    pub records: u64,
    /// The time partition value (hours, days or months since the epoch, by the table's chunk).
    pub partition: Option<i64>,
    /// The data sequence number: rows of a file with a higher one were written later.
    pub sequence_number: i64,
    pub lower: HashMap<String, Bound>,
    pub upper: HashMap<String, Bound>,
}

impl DataFile {
    /// A live file as the reader plans with it, bounds decoded by column name ([`decode_bound`]).
    /// `by_id` holds the current schema's columns; bounds of others are left out.
    pub(crate) fn from_entry(entry: &Entry, by_id: &HashMap<i32, &Field>) -> Self {
        let f = &entry.file;
        let bounds = |m: &BTreeMap<i32, Vec<u8>>, upper: bool| {
            m.iter()
                .filter_map(|(id, bytes)| {
                    let field = by_id.get(id)?;
                    Some((field.name.clone(), decode_bound(field, bytes, upper)?))
                })
                .collect()
        };
        Self {
            path: f.path.clone(),
            size: f.file_size,
            records: f.record_count,
            partition: f.partition.map(i64::from),
            sequence_number: entry.sequence_number,
            lower: bounds(&f.lower_bounds, false),
            upper: bounds(&f.upper_bounds, true),
        }
    }
}

/// A column's Iceberg bound (`bytes` in Iceberg's single-value encoding) as queries compare it:
/// nanosecond timestamps in microseconds, as queries give times (rounded outward, so the bounds
/// still contain every row).
pub(crate) fn decode_bound(f: &Field, bytes: &[u8], upper: bool) -> Option<Bound> {
    Some(match (f.ty.as_str(), decode(&f.ty, bytes)?) {
        ("timestamp_ns" | "timestamptz_ns", Bound::Int(x)) => Bound::Int(scale_to_micros(x, TimeUnit::NANOS, upper)),
        (_, bound) => bound,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use timelake_format::iceberg::{
        self as shared, ManifestContext, ManifestWrite, write_manifest, write_manifest_list,
    };

    use super::*;

    fn entry(file: shared::DataFile) -> Entry {
        Entry {
            status: Status::Added,
            snapshot_id: 1,
            sequence_number: 1,
            file_sequence_number: 1,
            file: Arc::new(file),
        }
    }

    #[test]
    fn nanosecond_bounds_are_microseconds() {
        let fields = [
            Field { id: 1, name: "ns".into(), ty: "timestamptz_ns".into(), required: true },
            Field { id: 2, name: "us".into(), ty: "timestamptz".into(), required: true },
        ];
        let bound = |x: i64| x.to_le_bytes().to_vec();
        let bounds = BTreeMap::from([(1, bound(1_500_001)), (2, bound(1_500_001)), (3, bound(7))]);
        let file = shared::DataFile { lower_bounds: bounds.clone(), upper_bounds: bounds, ..Default::default() };
        let f = DataFile::from_entry(&entry(file), &by_id(&fields));
        assert_eq!((&f.lower["ns"], &f.upper["ns"]), (&Bound::Int(1500), &Bound::Int(1501)));
        assert_eq!((&f.lower["us"], &f.upper["us"]), (&Bound::Int(1_500_001), &Bound::Int(1_500_001)));
        assert_eq!(f.lower.len(), 2, "columns not in the schema are left out");
    }

    /// Delete manifests and delete files are errors, not skipped: their rows would come back.
    #[test]
    fn delete_files_are_errors() {
        let data = ManifestFile {
            path: "m.avro".into(),
            length: 1,
            content: Content::Data,
            partition_spec_id: 0,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files: 1,
            existing_files: 0,
            deleted_files: 0,
            added_rows: 1,
            existing_rows: 0,
            deleted_rows: 0,
            partitions: None,
        };
        let deletes = ManifestFile { path: "d.avro".into(), content: Content::Deletes, ..data.clone() };
        let list = write_manifest_list(std::slice::from_ref(&data), 1, None, 1).unwrap();
        assert_eq!(data_manifests(&list).unwrap().iter().map(|m| m.path.as_str()).collect::<Vec<_>>(), ["m.avro"]);
        let list = write_manifest_list(&[data.clone(), deletes], 1, None, 1).unwrap();
        assert!(data_manifests(&list).unwrap_err().to_string().contains("delete files (d.avro)"));

        let ctx = ManifestContext {
            schema_json: r#"{"schema-id":0,"type":"struct","fields":[]}"#,
            schema_id: 0,
            spec_fields_json: "[]",
            spec_id: 0,
            partition: None,
        };
        let file = shared::DataFile { content: Content::Deletes, path: "p".into(), ..Default::default() };
        let write = ManifestWrite {
            status: Status::Added,
            snapshot_id: Some(1),
            sequence_number: None,
            file_sequence_number: None,
            file: Arc::new(file),
        };
        let manifest = write_manifest(&ctx, &[write]).unwrap();
        assert!(parse_manifest(&data, &manifest, &[]).unwrap_err().to_string().contains("delete files (p)"));
    }

    fn meta(format: &str) -> Result<TableMeta> {
        let schema = r#"{"schema-id": 0, "fields": [{"id": 1, "name": "x", "type": "int", "required": false}]}"#;
        TableMeta::parse(&format!(
            r#"{{"format-version": 2, "location": "s3://b/t", "schemas": [{schema}],
                "properties": {{"timelake.format-version": "{format}"}}}}"#
        ))
    }

    #[test]
    fn reads_only_formats_it_knows() {
        assert!(meta("0.1.0").is_ok());
        assert!(meta("2").is_ok());
        assert!(meta("0.2.9").is_ok());
        assert!(meta("0.3.0").is_ok());
        let err = meta("0.4.0").map(|_| ()).unwrap_err().to_string();
        assert!(err.contains("upgrade Timelake"), "{err}");
    }
}
