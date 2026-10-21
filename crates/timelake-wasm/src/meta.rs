//! Iceberg table metadata (`vN.metadata.json`) and manifests (Avro), read without iceberg-rust so
//! the reader stays small and compiles to WebAssembly.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use apache_avro::types::Value;
use parquet::basic::TimeUnit;
use serde::{Deserialize, Serialize};
use timelake_format::bounds::{Bound, decode};
use timelake_format::iceberg::Entry;
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
    /// The current schema's top-level columns by field id.
    pub(crate) fn fields_by_id(&self) -> HashMap<i32, &Field> {
        self.schema.iter().map(|f| (f.id, f)).collect()
    }

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

// --- Avro manifests -------------------------------------------------------------------------------

/// Iceberg's integer codes in manifests.
mod code {
    /// `manifest_file.content` and `data_file.content`: data rather than deletes.
    pub(super) const CONTENT_DATA: i64 = 0;
    /// `manifest_file.content`: delete files.
    pub(super) const CONTENT_DELETES: i64 = 1;
    /// `manifest_entry.status`: the file was removed in this snapshot.
    pub(super) const STATUS_DELETED: i64 = 2;
}

/// Optional Avro fields are unions with null; this returns the value inside.
fn strip_union(v: &Value) -> &Value {
    match v {
        Value::Union(_, inner) => strip_union(inner),
        other => other,
    }
}

fn field<'a>(record: &'a Value, name: &str) -> Option<&'a Value> {
    match strip_union(record) {
        Value::Record(fields) => fields.iter().find(|(n, _)| n == name).map(|(_, v)| strip_union(v)),
        _ => None,
    }
}

fn as_i64(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Long(x) => Some(*x),
        Value::Int(x) => Some(i64::from(*x)),
        _ => None,
    }
}

fn as_u64(v: Option<&Value>) -> u64 {
    as_i64(v).and_then(|x| u64::try_from(x).ok()).unwrap_or(0)
}

fn as_string(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn read_avro(bytes: &[u8]) -> Result<Vec<Value>> {
    Ok(apache_avro::Reader::new(bytes)?.collect::<Result<_, _>>()?)
}

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
    read_avro(bytes)?
        .iter()
        .map(|r| {
            let content = match as_i64(field(r, "content")).unwrap_or(code::CONTENT_DATA) {
                code::CONTENT_DATA => ManifestContent::Data,
                code::CONTENT_DELETES => ManifestContent::Deletes,
                other => return Err(Error::invalid(format!("unknown manifest content {other}"))),
            };
            Ok(ManifestRef {
                path: as_string(field(r, "manifest_path"))
                    .ok_or_else(|| Error::invalid("manifest list entry without path"))?,
                length: as_i64(field(r, "manifest_length")).unwrap_or(0),
                content,
                sequence_number: as_i64(field(r, "sequence_number")).unwrap_or(0),
                added_snapshot_id: as_i64(field(r, "added_snapshot_id")).unwrap_or(0),
            })
        })
        .collect()
}

/// The data manifests of a manifest list. Delete manifests (row-level deletes another engine
/// wrote) are an error: this reader doesn't apply them, and rows they delete would come back.
pub fn data_manifests(bytes: &[u8]) -> Result<Vec<ManifestRef>> {
    let manifests = parse_manifest_list(bytes)?;
    if let Some(m) = manifests.iter().find(|m| m.content == ManifestContent::Deletes) {
        return Err(deletes_unsupported(&m.path));
    }
    Ok(manifests)
}

fn deletes_unsupported(path: &str) -> Error {
    Error::invalid(format!(
        "the table has delete files ({path}), which this reader doesn't apply; read it with an engine that does"
    ))
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

/// Column bounds by name (see [`decode_bound`]).
fn bounds(v: Option<&Value>, by_id: &HashMap<i32, &Field>, upper: bool) -> HashMap<String, Bound> {
    let Some(Value::Array(entries)) = v else { return HashMap::new() };
    entries
        .iter()
        .filter_map(|e| {
            let id = i32::try_from(as_i64(field(e, "key"))?).ok()?;
            let Some(Value::Bytes(b)) = field(e, "value") else { return None };
            let f = by_id.get(&id)?;
            Some((f.name.clone(), decode_bound(f, b, upper)?))
        })
        .collect()
}

/// Parses a data manifest; returns its live (not deleted) data files. Entries without a sequence
/// number inherit `sequence_number`, the manifest's in the manifest list.
pub fn parse_manifest(bytes: &[u8], schema: &[Field], sequence_number: i64) -> Result<Vec<DataFile>> {
    let by_id: HashMap<i32, &Field> = schema.iter().map(|f| (f.id, f)).collect();
    let mut out = vec![];
    for entry in read_avro(bytes)? {
        if as_i64(field(&entry, "status")) == Some(code::STATUS_DELETED) {
            continue;
        }
        let Some(df) = field(&entry, "data_file") else { continue };
        if as_i64(field(df, "content")).unwrap_or(code::CONTENT_DATA) != code::CONTENT_DATA {
            return Err(deletes_unsupported(&as_string(field(df, "file_path")).unwrap_or_default()));
        }
        let partition = match field(df, "partition") {
            Some(Value::Record(fs)) => fs.first().and_then(|(_, v)| match strip_union(v) {
                // A day partition is an Avro `date`.
                Value::Date(days) => Some(i64::from(*days)),
                other => as_i64(Some(other)),
            }),
            _ => None,
        };
        out.push(DataFile {
            path: as_string(field(df, "file_path")).ok_or_else(|| Error::invalid("data file without path"))?,
            size: as_u64(field(df, "file_size_in_bytes")),
            records: as_u64(field(df, "record_count")),
            partition,
            sequence_number: as_i64(field(&entry, "sequence_number")).unwrap_or(sequence_number),
            lower: bounds(field(df, "lower_bounds"), &by_id, false),
            upper: bounds(field(df, "upper_bounds"), &by_id, true),
        });
    }
    Ok(out)
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
        let entry = |id: i32, x: i64| {
            Value::Record(vec![
                ("key".into(), Value::Int(id)),
                ("value".into(), Value::Bytes(x.to_le_bytes().to_vec())),
            ])
        };
        let v = Value::Array(vec![entry(1, 1_500_001), entry(2, 1_500_001)]);
        let lower = bounds(Some(&v), &by_id, false);
        let upper = bounds(Some(&v), &by_id, true);
        assert_eq!((&lower["ns"], &upper["ns"]), (&Bound::Int(1500), &Bound::Int(1501)));
        assert_eq!((&lower["us"], &upper["us"]), (&Bound::Int(1_500_001), &Bound::Int(1_500_001)));
    }

    /// Delete manifests and delete files are errors, not skipped: their rows would come back.
    #[test]
    fn delete_files_are_errors() {
        use timelake_format::iceberg::{Content, ManifestFile, write_manifest_list};

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
        let list = write_manifest_list(&[data, deletes], 1, None, 1).unwrap();
        assert!(data_manifests(&list).unwrap_err().to_string().contains("delete files (d.avro)"));

        let schema = apache_avro::Schema::parse_str(
            r#"{"type": "record", "name": "manifest_entry", "fields": [
                {"name": "status", "type": "int"},
                {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
                    {"name": "content", "type": "int"}, {"name": "file_path", "type": "string"}
                ]}}
            ]}"#,
        )
        .unwrap();
        let mut w = apache_avro::Writer::new(&schema, vec![]);
        let file =
            Value::Record(vec![("content".into(), Value::Int(1)), ("file_path".into(), Value::String("p".into()))]);
        w.append(Value::Record(vec![("status".into(), Value::Int(1)), ("data_file".into(), file)])).unwrap();
        let manifest = w.into_inner().unwrap();
        assert!(parse_manifest(&manifest, &[], 0).unwrap_err().to_string().contains("delete files (p)"));
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
