//! Iceberg table metadata (`vN.metadata.json`), and manifests read by lakebed (`lakebed_iceberg`),
//! without iceberg-rust, so the reader stays small and compiles to WebAssembly.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use parquet::basic::TimeUnit;
use serde::{Deserialize, Serialize};
use timelake_format::bounds::{Bound, decode};
use timelake_format::{Contract, summary};

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

/// What a reader needs from the table metadata.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMeta {
    pub location: String,
    pub format_version: u8,
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

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawMeta {
    format_version: u8,
    location: String,
    #[serde(default)]
    current_schema_id: i32,
    #[serde(default)]
    schemas: Vec<RawSchema>,
    #[serde(default)]
    properties: HashMap<String, String>,
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<RawSnapshot>,
    #[serde(default)]
    last_updated_ms: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawSchema {
    #[serde(default)]
    schema_id: i32,
    fields: Vec<RawField>,
}

#[derive(Deserialize)]
struct RawField {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    ty: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawSnapshot {
    snapshot_id: i64,
    timestamp_ms: i64,
    manifest_list: String,
    #[serde(default)]
    summary: HashMap<String, String>,
}

impl RawSnapshot {
    fn info(&self) -> SnapshotInfo {
        SnapshotInfo {
            id: self.snapshot_id.to_string(),
            timestamp_ms: self.timestamp_ms,
            operation: self.summary.get("operation").cloned().unwrap_or_default(),
            summary: self.summary.clone(),
        }
    }
}

impl TableMeta {
    /// Parses `vN.metadata.json`, including the Timelake contract in its properties.
    pub fn parse(json: &str) -> Result<Self> {
        let raw: RawMeta = serde_json::from_str(json)?;
        let schema = raw
            .schemas
            .iter()
            .find(|s| s.schema_id == raw.current_schema_id)
            .or(raw.schemas.last())
            .ok_or_else(|| Error::invalid("table metadata has no schema"))?;
        let fields = schema
            .fields
            .iter()
            .map(|f| Field {
                id: f.id,
                name: f.name.clone(),
                ty: f.ty.as_str().unwrap_or("nested").to_string(),
                required: f.required,
            })
            .collect();
        let contract = Contract::from_properties(&raw.properties)?;
        contract.check_readable().map_err(|e| Error::invalid(e.0))?;
        let current = raw.current_snapshot_id.and_then(|id| raw.snapshots.iter().find(|s| s.snapshot_id == id));
        let mut snapshots: Vec<SnapshotInfo> = raw.snapshots.iter().map(RawSnapshot::info).collect();
        snapshots.sort_by_key(|s| Reverse(s.timestamp_ms));
        snapshots.truncate(MAX_SNAPSHOTS);
        Ok(Self {
            location: raw.location,
            format_version: raw.format_version,
            schema: fields,
            time_column: contract.time_column,
            series_key: contract.series_key,
            chunk: contract.chunk.map(|c| c.as_str().to_string()),
            dedup: contract.dedup,
            properties: raw.properties,
            current_snapshot: current.map(RawSnapshot::info),
            manifest_list: current.map(|s| s.manifest_list.clone()),
            last_value_index: current.and_then(|s| s.summary.get(summary::LAST_VALUE_INDEX).cloned()),
            snapshot_count: raw.snapshots.len(),
            snapshots,
            last_updated_ms: raw.last_updated_ms,
        })
    }
}

// --- manifests ----------------------------------------------------------------------------------

/// What a manifest tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestContent {
    Data,
    Deletes,
}

/// One manifest of the current snapshot.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestRef {
    pub path: String,
    pub length: i64,
    pub content: ManifestContent,
    pub sequence_number: i64,
    pub added_snapshot_id: i64,
}

/// Parses a manifest list (`snap-….avro`).
pub fn parse_manifest_list(bytes: &[u8]) -> Result<Vec<ManifestRef>> {
    Ok(lakebed_iceberg::read_manifest_list(bytes)?
        .into_iter()
        .map(|m| ManifestRef {
            content: match m.content {
                lakebed_iceberg::ManifestContent::Data => ManifestContent::Data,
                lakebed_iceberg::ManifestContent::Deletes => ManifestContent::Deletes,
            },
            path: m.path,
            length: m.length,
            sequence_number: m.sequence_number,
            added_snapshot_id: m.added_snapshot_id,
        })
        .collect())
}

/// A live data file with its column bounds, keyed by column name.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFile {
    pub path: String,
    pub size: u64,
    pub records: u64,
    /// The time partition value (hours, days or months since the epoch, by the table's chunk).
    pub partition: Option<i64>,
    pub lower: HashMap<String, Bound>,
    pub upper: HashMap<String, Bound>,
}

/// Column bounds by name; nanosecond timestamps in microseconds, as queries give times (rounded
/// outward, so the bounds still contain every row).
fn bounds(by_field_id: &BTreeMap<i32, Vec<u8>>, by_id: &HashMap<i32, &Field>, upper: bool) -> HashMap<String, Bound> {
    by_field_id
        .iter()
        .filter_map(|(id, b)| {
            let f = by_id.get(id)?;
            let bound = match (f.ty.as_str(), decode(&f.ty, b)?) {
                ("timestamp_ns" | "timestamptz_ns", Bound::Int(x)) => {
                    Bound::Int(scale_to_micros(x, TimeUnit::NANOS, upper))
                }
                (_, bound) => bound,
            };
            Some((f.name.clone(), bound))
        })
        .collect()
}

/// Parses a data manifest; returns its live (not deleted) data files.
pub fn parse_manifest(bytes: &[u8], schema: &[Field]) -> Result<Vec<DataFile>> {
    let by_id: HashMap<i32, &Field> = schema.iter().map(|f| (f.id, f)).collect();
    let entries = lakebed_iceberg::read_manifest(&lakebed_iceberg::ManifestFile::default(), bytes)?;
    Ok(entries
        .into_iter()
        // Timelake writes no delete files.
        .filter(|e| e.status != lakebed_iceberg::Status::Deleted && e.file.content == lakebed_iceberg::Content::Data)
        .map(|e| DataFile {
            path: e.file.path.clone(),
            size: e.file.file_size,
            records: e.file.record_count,
            partition: e.file.partition.first().and_then(|v| v.as_ref()?.as_i64()),
            lower: bounds(&e.file.lower_bounds, &by_id, false),
            upper: bounds(&e.file.upper_bounds, &by_id, true),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanosecond_bounds_are_microseconds() {
        let fields = [
            Field { id: 1, name: "ns".into(), ty: "timestamptz_ns".into(), required: true },
            Field { id: 2, name: "us".into(), ty: "timestamptz".into(), required: true },
        ];
        let by_id = fields.iter().map(|f| (f.id, f)).collect();
        let v = BTreeMap::from([(1, 1_500_001i64.to_le_bytes().to_vec()), (2, 1_500_001i64.to_le_bytes().to_vec())]);
        let lower = bounds(&v, &by_id, false);
        let upper = bounds(&v, &by_id, true);
        assert_eq!((&lower["ns"], &upper["ns"]), (&Bound::Int(1500), &Bound::Int(1501)));
        assert_eq!((&lower["us"], &upper["us"]), (&Bound::Int(1_500_001), &Bound::Int(1_500_001)));
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
