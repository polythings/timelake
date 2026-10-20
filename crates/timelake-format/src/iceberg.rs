//! Iceberg manifests and manifest lists, written and read without iceberg-rust (which doesn't build
//! for WebAssembly), in the format iceberg-rust writes: format v2, the same Avro schemas, field ids
//! and header metadata. The engine's tests check that iceberg-rust reads what this writes.

use std::collections::BTreeMap;
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::{Codec, Reader, Schema, Writer};

use crate::Chunk;
use crate::error::{WriteError, WriteResult};

/// Whether a manifest entry adds, keeps or removes its file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Carried over from an earlier snapshot.
    Existing = 0,
    /// Added by the manifest's snapshot.
    Added = 1,
    /// Removed by the manifest's snapshot.
    Deleted = 2,
}

/// A data file as Iceberg records it. Bounds are Iceberg's single-value binary encoding.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataFile {
    /// Absolute URI.
    pub path: String,
    /// The time partition (`None` for unpartitioned tables).
    pub partition: Option<i32>,
    /// Rows.
    pub record_count: u64,
    /// Bytes.
    pub file_size: u64,
    /// Compressed bytes per field id.
    pub column_sizes: BTreeMap<i32, u64>,
    /// Values per field id.
    pub value_counts: BTreeMap<i32, u64>,
    /// Nulls per field id.
    pub null_counts: BTreeMap<i32, u64>,
    /// NaNs per field id.
    pub nan_counts: BTreeMap<i32, u64>,
    /// Smallest value per field id.
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    /// Largest value per field id.
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
    /// Row group offsets.
    pub split_offsets: Option<Vec<i64>>,
    /// The table sort order the file follows.
    pub sort_order_id: Option<i32>,
}

/// A file in a snapshot, with the sequence numbers that order its rows against other files'.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Added, existing or deleted.
    pub status: Status,
    /// The snapshot that added (or deleted) the file.
    pub snapshot_id: i64,
    /// Data sequence number: orders the file's rows against other files' rows.
    pub sequence_number: i64,
    /// Sequence number of the commit that added the file.
    pub file_sequence_number: i64,
    /// The file.
    pub file: Arc<DataFile>,
}

/// What a manifest lists: data files, or delete files (row-level deletes, which Timelake doesn't
/// write but other engines may).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Content {
    /// Data files.
    Data = 0,
    /// Position or equality delete files.
    Deletes = 1,
}

/// An entry of a manifest list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestFile {
    /// Absolute URI of the manifest.
    pub path: String,
    /// Its size in bytes.
    pub length: i64,
    /// Data or delete files.
    pub content: Content,
    /// The partition spec its files use.
    pub partition_spec_id: i32,
    /// Sequence number of the snapshot that wrote it.
    pub sequence_number: i64,
    /// Smallest data sequence number of its files.
    pub min_sequence_number: i64,
    /// The snapshot that wrote it.
    pub added_snapshot_id: i64,
    /// Files added.
    pub added_files: i32,
    /// Files carried over.
    pub existing_files: i32,
    /// Files removed.
    pub deleted_files: i32,
    /// Rows in added files.
    pub added_rows: i64,
    /// Rows in existing files.
    pub existing_rows: i64,
    /// Rows in deleted files.
    pub deleted_rows: i64,
    /// Lowest and highest partition value, with whether any file is unpartitioned.
    pub partitions: Option<PartitionSummary>,
}

/// The range of partition values of a manifest's files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionSummary {
    /// Some file has no partition value.
    pub contains_null: bool,
    /// Lowest value.
    pub lower: Option<i32>,
    /// Highest value.
    pub upper: Option<i32>,
}

/// What a manifest's header records about the table.
#[derive(Clone, Debug)]
pub struct ManifestContext<'a> {
    /// The current schema as Iceberg JSON.
    pub schema_json: &'a str,
    /// Its id.
    pub schema_id: i32,
    /// The partition spec's fields as Iceberg JSON (`[{"source-id":…,"field-id":…,…}]`).
    pub spec_fields_json: &'a str,
    /// The spec's id.
    pub spec_id: i32,
    /// The partition field, if the table is partitioned.
    pub partition: Option<&'a PartitionField>,
}

/// A table's time partition field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionField {
    /// Its name in the partition spec.
    pub name: String,
    /// Its field id.
    pub field_id: i32,
    /// The time transform.
    pub chunk: Chunk,
}

/// `name` as a valid Avro name, by the rule Iceberg's Avro schemas use: a leading digit gets a
/// `_` before it, and any other character outside `[A-Za-z0-9_]` becomes `_x` and its code in
/// uppercase hex (`event-time` → `event_x2Dtime`). Valid names are unchanged.
pub fn avro_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (i, c) in name.chars().enumerate() {
        match c {
            'A'..='Z' | 'a'..='z' | '_' => out.push(c),
            '0'..='9' if i > 0 => out.push(c),
            '0'..='9' => {
                out.push('_');
                out.push(c);
            }
            _ => out.push_str(&format!("_x{:X}", u32::from(c))),
        }
    }
    out
}

/// The Avro schema of manifest entries for a table partitioned as `partition`.
fn manifest_schema(partition: Option<&PartitionField>) -> WriteResult<Schema> {
    let partition_fields = match partition {
        None => String::new(),
        Some(p) => {
            let ty = match p.chunk {
                Chunk::Day => serde_json::json!({"type": "int", "logicalType": "date"}),
                _ => serde_json::json!("int"),
            };
            let field = serde_json::json!({
                "name": avro_name(&p.name), "type": ["null", ty], "default": null, "field-id": p.field_id,
            });
            field.to_string()
        }
    };
    let map = |name: &str, key: i32, value: i32, ty: &str| {
        format!(
            r#"{{"name": "{name}", "type": ["null", {{"type": "array", "items": {{"type": "record", "name": "k{key}_v{value}", "fields": [{{"name": "key", "type": "int", "field-id": {key}}}, {{"name": "value", "type": "{ty}", "field-id": {value}}}]}}, "logicalType": "map"}}], "default": null"#
        )
    };
    let json = format!(
        r#"{{"type": "record", "name": "manifest_entry", "fields": [
            {{"name": "status", "type": "int", "field-id": 0}},
            {{"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1}},
            {{"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3}},
            {{"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4}},
            {{"name": "data_file", "type": {{"type": "record", "name": "r2", "fields": [
                {{"name": "content", "type": "int", "default": 0, "field-id": 134}},
                {{"name": "file_path", "type": "string", "field-id": 100}},
                {{"name": "file_format", "type": "string", "field-id": 101}},
                {{"name": "partition", "type": {{"type": "record", "name": "r102", "fields": [{partition_fields}]}}, "field-id": 102}},
                {{"name": "record_count", "type": "long", "field-id": 103}},
                {{"name": "file_size_in_bytes", "type": "long", "field-id": 104}},
                {}, "field-id": 108}},
                {}, "field-id": 109}},
                {}, "field-id": 110}},
                {}, "field-id": 137}},
                {}, "field-id": 125}},
                {}, "field-id": 128}},
                {{"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 131}},
                {{"name": "split_offsets", "type": ["null", {{"type": "array", "items": "long", "element-id": 133}}], "default": null, "field-id": 132}},
                {{"name": "equality_ids", "type": ["null", {{"type": "array", "items": "int", "element-id": 136}}], "default": null, "field-id": 135}},
                {{"name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140}},
                {{"name": "first_row_id", "type": ["null", "long"], "default": null, "field-id": 142}},
                {{"name": "referenced_data_file", "type": ["null", "string"], "default": null, "field-id": 143}},
                {{"name": "content_offset", "type": ["null", "long"], "default": null, "field-id": 144}},
                {{"name": "content_size_in_bytes", "type": ["null", "long"], "default": null, "field-id": 145}}
            ]}}, "field-id": 2}}
        ]}}"#,
        map("column_sizes", 117, 118, "long"),
        map("value_counts", 119, 120, "long"),
        map("null_value_counts", 121, 122, "long"),
        map("nan_value_counts", 138, 139, "long"),
        map("lower_bounds", 126, 127, "bytes"),
        map("upper_bounds", 129, 130, "bytes"),
    );
    Schema::parse_str(&json).map_err(avro)
}

const MANIFEST_LIST_SCHEMA: &str = r#"{"type": "record", "name": "manifest_file", "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517},
    {"name": "sequence_number", "type": "long", "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    {"name": "partitions", "type": ["null", {"type": "array", "items": {"type": "record", "name": "r508", "fields": [
        {"name": "contains_null", "type": "boolean", "field-id": 509},
        {"name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 518},
        {"name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 510},
        {"name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 511}
    ]}, "element-id": 508}], "default": null, "field-id": 507},
    {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 519}
]}"#;

fn avro(e: impl std::fmt::Display) -> WriteError {
    WriteError::Corrupt(format!("avro: {e}"))
}

fn some(v: Value) -> Value {
    Value::Union(1, Box::new(v))
}

fn null() -> Value {
    Value::Union(0, Box::new(Value::Null))
}

fn long_map(m: &BTreeMap<i32, u64>) -> Value {
    some(Value::Array(
        m.iter()
            .map(|(k, v)| {
                Value::Record(vec![
                    ("key".into(), Value::Int(*k)),
                    ("value".into(), Value::Long(i64::try_from(*v).unwrap_or(i64::MAX))),
                ])
            })
            .collect(),
    ))
}

fn bytes_map(m: &BTreeMap<i32, Vec<u8>>) -> Value {
    some(Value::Array(
        m.iter()
            .map(|(k, v)| {
                Value::Record(vec![("key".into(), Value::Int(*k)), ("value".into(), Value::Bytes(v.clone()))])
            })
            .collect(),
    ))
}

fn i64_of(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Writes a manifest of `entries` for the table `ctx` describes. Entries whose snapshot id or
/// sequence numbers are `None` inherit them from the manifest list (the commit's own); existing
/// and deleted entries keep theirs.
pub fn write_manifest(ctx: &ManifestContext<'_>, entries: &[ManifestWrite]) -> WriteResult<Vec<u8>> {
    let schema = manifest_schema(ctx.partition)?;
    let mut w = Writer::with_codec(&schema, vec![], Codec::Null);
    for (k, v) in [
        ("schema", ctx.schema_json.to_string()),
        ("schema-id", ctx.schema_id.to_string()),
        ("partition-spec", ctx.spec_fields_json.to_string()),
        ("partition-spec-id", ctx.spec_id.to_string()),
        ("format-version", "2".to_string()),
        ("content", "data".to_string()),
    ] {
        w.add_user_metadata(k.to_string(), v).map_err(avro)?;
    }
    for e in entries {
        let f = &e.file;
        let partition = match (ctx.partition, f.partition) {
            (None, _) => vec![],
            (Some(p), v) => {
                let value =
                    v.map_or_else(null, |v| some(if p.chunk == Chunk::Day { Value::Date(v) } else { Value::Int(v) }));
                vec![(avro_name(&p.name), value)]
            }
        };
        let data_file = Value::Record(vec![
            ("content".into(), Value::Int(0)),
            ("file_path".into(), Value::String(f.path.clone())),
            ("file_format".into(), Value::String("PARQUET".into())),
            ("partition".into(), Value::Record(partition)),
            ("record_count".into(), Value::Long(i64_of(f.record_count))),
            ("file_size_in_bytes".into(), Value::Long(i64_of(f.file_size))),
            ("column_sizes".into(), long_map(&f.column_sizes)),
            ("value_counts".into(), long_map(&f.value_counts)),
            ("null_value_counts".into(), long_map(&f.null_counts)),
            ("nan_value_counts".into(), long_map(&f.nan_counts)),
            ("lower_bounds".into(), bytes_map(&f.lower_bounds)),
            ("upper_bounds".into(), bytes_map(&f.upper_bounds)),
            ("key_metadata".into(), null()),
            (
                "split_offsets".into(),
                f.split_offsets
                    .as_ref()
                    .map_or_else(null, |o| some(Value::Array(o.iter().map(|v| Value::Long(*v)).collect()))),
            ),
            ("equality_ids".into(), null()),
            ("sort_order_id".into(), f.sort_order_id.map_or_else(null, |v| some(Value::Int(v)))),
            ("first_row_id".into(), null()),
            ("referenced_data_file".into(), null()),
            ("content_offset".into(), null()),
            ("content_size_in_bytes".into(), null()),
        ]);
        let opt_long = |v: Option<i64>| v.map_or_else(null, |v| some(Value::Long(v)));
        w.append(Value::Record(vec![
            ("status".into(), Value::Int(e.status as i32)),
            ("snapshot_id".into(), opt_long(e.snapshot_id)),
            ("sequence_number".into(), opt_long(e.sequence_number)),
            ("file_sequence_number".into(), opt_long(e.file_sequence_number)),
            ("data_file".into(), data_file),
        ]))
        .map_err(avro)?;
    }
    w.into_inner().map_err(avro)
}

/// An entry to write into a manifest: `None` fields are inherited from the manifest list.
#[derive(Clone, Debug)]
pub struct ManifestWrite {
    /// Added, existing or deleted.
    pub status: Status,
    /// The snapshot, or `None` to inherit the manifest's.
    pub snapshot_id: Option<i64>,
    /// Data sequence number, or `None` to inherit.
    pub sequence_number: Option<i64>,
    /// File sequence number, or `None` to inherit.
    pub file_sequence_number: Option<i64>,
    /// The file.
    pub file: Arc<DataFile>,
}

/// Writes a manifest list for snapshot `snapshot_id` (child of `parent`) with sequence number
/// `sequence_number`. Manifests with `sequence_number` -1 get the snapshot's (they were written
/// by it).
pub fn write_manifest_list(
    manifests: &[ManifestFile],
    snapshot_id: i64,
    parent: Option<i64>,
    sequence_number: i64,
) -> WriteResult<Vec<u8>> {
    let schema = Schema::parse_str(MANIFEST_LIST_SCHEMA).map_err(avro)?;
    let mut w = Writer::with_codec(&schema, vec![], Codec::Null);
    w.add_user_metadata("snapshot-id".into(), snapshot_id.to_string()).map_err(avro)?;
    w.add_user_metadata("parent-snapshot-id".into(), parent.map_or_else(|| "null".into(), |p| p.to_string()))
        .map_err(avro)?;
    w.add_user_metadata("sequence-number".into(), sequence_number.to_string()).map_err(avro)?;
    w.add_user_metadata("format-version".into(), "2").map_err(avro)?;
    for m in manifests {
        let assign = |v: i64| if v == UNASSIGNED { sequence_number } else { v };
        let partitions = m.partitions.as_ref().map_or_else(null, |p| {
            let bound = |v: Option<i32>| v.map_or_else(null, |v| some(Value::Bytes(v.to_le_bytes().to_vec())));
            some(Value::Array(vec![Value::Record(vec![
                ("contains_null".into(), Value::Boolean(p.contains_null)),
                ("contains_nan".into(), null()),
                ("lower_bound".into(), bound(p.lower)),
                ("upper_bound".into(), bound(p.upper)),
            ])]))
        });
        w.append(Value::Record(vec![
            ("manifest_path".into(), Value::String(m.path.clone())),
            ("manifest_length".into(), Value::Long(m.length)),
            ("partition_spec_id".into(), Value::Int(m.partition_spec_id)),
            ("content".into(), Value::Int(m.content as i32)),
            ("sequence_number".into(), Value::Long(assign(m.sequence_number))),
            ("min_sequence_number".into(), Value::Long(assign(m.min_sequence_number))),
            ("added_snapshot_id".into(), Value::Long(m.added_snapshot_id)),
            ("added_files_count".into(), Value::Int(m.added_files)),
            ("existing_files_count".into(), Value::Int(m.existing_files)),
            ("deleted_files_count".into(), Value::Int(m.deleted_files)),
            ("added_rows_count".into(), Value::Long(m.added_rows)),
            ("existing_rows_count".into(), Value::Long(m.existing_rows)),
            ("deleted_rows_count".into(), Value::Long(m.deleted_rows)),
            ("partitions".into(), partitions),
            ("key_metadata".into(), null()),
        ]))
        .map_err(avro)?;
    }
    w.into_inner().map_err(avro)
}

/// A sequence number the manifest list assigns when the snapshot commits.
pub const UNASSIGNED: i64 = -1;

/// The manifest-list entry of a manifest written as `bytes` at `path` with `entries`.
pub fn manifest_file(
    path: &str,
    bytes: &[u8],
    spec_id: i32,
    snapshot_id: i64,
    entries: &[ManifestWrite],
    partitioned: bool,
) -> ManifestFile {
    let count = |s: Status| i32::try_from(entries.iter().filter(|e| e.status == s).count()).unwrap_or(i32::MAX);
    let rows = |s: Status| i64_of(entries.iter().filter(|e| e.status == s).map(|e| e.file.record_count).sum());
    // Entries that inherit the commit's sequence number sort after every explicit one.
    let min_sequence_number = entries.iter().filter_map(|e| e.sequence_number).min().unwrap_or(UNASSIGNED);
    let partitions = partitioned.then(|| {
        let values: Vec<Option<i32>> = entries.iter().map(|e| e.file.partition).collect();
        PartitionSummary {
            contains_null: values.iter().any(Option::is_none),
            lower: values.iter().flatten().min().copied(),
            upper: values.iter().flatten().max().copied(),
        }
    });
    ManifestFile {
        path: path.to_string(),
        length: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        content: Content::Data,
        partition_spec_id: spec_id,
        sequence_number: UNASSIGNED,
        min_sequence_number,
        added_snapshot_id: snapshot_id,
        added_files: count(Status::Added),
        existing_files: count(Status::Existing),
        deleted_files: count(Status::Deleted),
        added_rows: rows(Status::Added),
        existing_rows: rows(Status::Existing),
        deleted_rows: rows(Status::Deleted),
        partitions,
    }
}

// --- reading ---------------------------------------------------------------------------------

fn records(bytes: &[u8]) -> WriteResult<Vec<Value>> {
    let reader = Reader::new(bytes).map_err(avro)?;
    reader.map(|v| v.map_err(avro)).collect()
}

fn field<'a>(record: &'a Value, name: &str) -> Option<&'a Value> {
    let Value::Record(fields) = record else { return None };
    fields.iter().find(|(n, _)| n == name).map(|(_, v)| match v {
        Value::Union(_, inner) => inner.as_ref(),
        v => v,
    })
}

fn long(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Long(n) => Some(*n),
        Value::Int(n) | Value::Date(n) => Some(i64::from(*n)),
        _ => None,
    }
}

fn int(v: Option<&Value>) -> Option<i32> {
    match v? {
        Value::Int(n) | Value::Date(n) => Some(*n),
        Value::Long(n) => i32::try_from(*n).ok(),
        _ => None,
    }
}

fn required<T>(v: Option<T>, what: &str) -> WriteResult<T> {
    v.ok_or_else(|| WriteError::Corrupt(format!("manifest without {what}")))
}

/// The manifests of a manifest list.
pub fn read_manifest_list(bytes: &[u8]) -> WriteResult<Vec<ManifestFile>> {
    records(bytes)?
        .iter()
        .map(|r| {
            let partitions = match field(r, "partitions") {
                Some(Value::Array(items)) => items.first().map(|p| PartitionSummary {
                    contains_null: matches!(field(p, "contains_null"), Some(Value::Boolean(true))),
                    lower: bound_i32(field(p, "lower_bound")),
                    upper: bound_i32(field(p, "upper_bound")),
                }),
                _ => None,
            };
            let content = match int(field(r, "content")).unwrap_or(0) {
                0 => Content::Data,
                1 => Content::Deletes,
                other => return Err(WriteError::Corrupt(format!("manifest content {other}"))),
            };
            Ok(ManifestFile {
                path: match field(r, "manifest_path") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(WriteError::Corrupt("manifest list entry without a path".into())),
                },
                length: required(long(field(r, "manifest_length")), "length")?,
                content,
                partition_spec_id: int(field(r, "partition_spec_id")).unwrap_or(0),
                sequence_number: long(field(r, "sequence_number")).unwrap_or(0),
                min_sequence_number: long(field(r, "min_sequence_number")).unwrap_or(0),
                added_snapshot_id: long(field(r, "added_snapshot_id")).unwrap_or(0),
                added_files: int(field(r, "added_files_count")).unwrap_or(0),
                existing_files: int(field(r, "existing_files_count")).unwrap_or(0),
                deleted_files: int(field(r, "deleted_files_count")).unwrap_or(0),
                added_rows: long(field(r, "added_rows_count")).unwrap_or(0),
                existing_rows: long(field(r, "existing_rows_count")).unwrap_or(0),
                deleted_rows: long(field(r, "deleted_rows_count")).unwrap_or(0),
                partitions,
            })
        })
        .collect()
}

fn bound_i32(v: Option<&Value>) -> Option<i32> {
    match v? {
        Value::Bytes(b) => b.as_slice().try_into().ok().map(i32::from_le_bytes),
        _ => None,
    }
}

fn long_map_of(v: Option<&Value>) -> BTreeMap<i32, u64> {
    let Some(Value::Array(items)) = v else { return BTreeMap::new() };
    items
        .iter()
        .filter_map(|kv| Some((int(field(kv, "key"))?, u64::try_from(long(field(kv, "value"))?).ok()?)))
        .collect()
}

fn bytes_map_of(v: Option<&Value>) -> BTreeMap<i32, Vec<u8>> {
    let Some(Value::Array(items)) = v else { return BTreeMap::new() };
    items
        .iter()
        .filter_map(|kv| match field(kv, "value") {
            Some(Value::Bytes(b)) => Some((int(field(kv, "key"))?, b.clone())),
            _ => None,
        })
        .collect()
}

/// The entries of `manifest`, with snapshot ids and sequence numbers inherited from it filled in.
pub fn read_manifest(manifest: &ManifestFile, bytes: &[u8]) -> WriteResult<Vec<Entry>> {
    records(bytes)?
        .iter()
        .map(|r| {
            let status = match int(field(r, "status")) {
                Some(0) => Status::Existing,
                Some(1) => Status::Added,
                Some(2) => Status::Deleted,
                other => return Err(WriteError::Corrupt(format!("manifest entry status {other:?}"))),
            };
            let df = required(field(r, "data_file"), "data_file")?;
            let partition = match field(df, "partition") {
                Some(Value::Record(fields)) => fields.first().and_then(|(_, v)| match v {
                    Value::Union(_, inner) => int(Some(inner)),
                    v => int(Some(v)),
                }),
                _ => None,
            };
            let file = DataFile {
                path: match field(df, "file_path") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return Err(WriteError::Corrupt("data file without a path".into())),
                },
                partition,
                record_count: u64::try_from(required(long(field(df, "record_count")), "record_count")?).unwrap_or(0),
                file_size: u64::try_from(required(long(field(df, "file_size_in_bytes")), "file size")?).unwrap_or(0),
                column_sizes: long_map_of(field(df, "column_sizes")),
                value_counts: long_map_of(field(df, "value_counts")),
                null_counts: long_map_of(field(df, "null_value_counts")),
                nan_counts: long_map_of(field(df, "nan_value_counts")),
                lower_bounds: bytes_map_of(field(df, "lower_bounds")),
                upper_bounds: bytes_map_of(field(df, "upper_bounds")),
                split_offsets: match field(df, "split_offsets") {
                    Some(Value::Array(v)) => Some(v.iter().filter_map(|x| long(Some(x))).collect()),
                    _ => None,
                },
                sort_order_id: int(field(df, "sort_order_id")),
            };
            // Added entries without sequence numbers inherit the manifest's (Iceberg's rule).
            let inherit = |v: Option<i64>| v.unwrap_or(manifest.sequence_number);
            Ok(Entry {
                status,
                snapshot_id: long(field(r, "snapshot_id")).unwrap_or(manifest.added_snapshot_id),
                sequence_number: inherit(long(field(r, "sequence_number"))),
                file_sequence_number: inherit(long(field(r, "file_sequence_number"))),
                file: Arc::new(file),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, partition: Option<i32>) -> Arc<DataFile> {
        Arc::new(DataFile {
            path: path.into(),
            partition,
            record_count: 10,
            file_size: 100,
            column_sizes: BTreeMap::from([(1, 40)]),
            value_counts: BTreeMap::from([(1, 10)]),
            lower_bounds: BTreeMap::from([(1, 5i64.to_le_bytes().to_vec())]),
            upper_bounds: BTreeMap::from([(1, 9i64.to_le_bytes().to_vec())]),
            split_offsets: Some(vec![4]),
            sort_order_id: Some(1),
            ..DataFile::default()
        })
    }

    #[test]
    fn manifests_round_trip() {
        let ctx = ManifestContext {
            schema_json: r#"{"schema-id":0,"type":"struct","fields":[]}"#,
            schema_id: 0,
            spec_fields_json: "[]",
            spec_id: 0,
            partition: Some(&PartitionField { name: "time_day".into(), field_id: 1000, chunk: Chunk::Day }),
        };
        let entries = vec![
            ManifestWrite {
                status: Status::Added,
                snapshot_id: Some(7),
                sequence_number: None,
                file_sequence_number: None,
                file: file("a", Some(20_000)),
            },
            ManifestWrite {
                status: Status::Existing,
                snapshot_id: Some(3),
                sequence_number: Some(2),
                file_sequence_number: Some(2),
                file: file("b", Some(19_999)),
            },
        ];
        let bytes = write_manifest(&ctx, &entries).unwrap();
        let mut mf = manifest_file("m.avro", &bytes, 0, 7, &entries, true);
        assert_eq!((mf.added_files, mf.existing_files, mf.added_rows), (1, 1, 10));
        assert_eq!(
            mf.partitions,
            Some(PartitionSummary { contains_null: false, lower: Some(19_999), upper: Some(20_000) })
        );

        let list = write_manifest_list(std::slice::from_ref(&mf), 7, Some(3), 5).unwrap();
        let read = read_manifest_list(&list).unwrap();
        // The list assigns the snapshot's sequence number; the minimum is the explicit 2.
        mf.sequence_number = 5;
        assert_eq!(read, [mf.clone()]);

        // Another engine's delete manifest stays one when the list is written again.
        let deletes = ManifestFile { path: "d.avro".into(), content: Content::Deletes, ..mf.clone() };
        let list = write_manifest_list(&[mf.clone(), deletes], 8, Some(7), 6).unwrap();
        let contents: Vec<Content> = read_manifest_list(&list).unwrap().iter().map(|m| m.content).collect();
        assert_eq!(contents, [Content::Data, Content::Deletes]);

        let read = read_manifest(&mf, &bytes).unwrap();
        assert_eq!((read[0].sequence_number, read[0].snapshot_id, read[0].status), (5, 7, Status::Added));
        assert_eq!((read[1].sequence_number, read[1].snapshot_id, read[1].status), (2, 3, Status::Existing));
        assert_eq!(read[0].file, entries[0].file);

        // A partition field named outside Avro's names is written under its Avro name.
        let odd = PartitionField { name: "event-time \"day\"".into(), field_id: 1001, chunk: Chunk::Hour };
        let ctx = ManifestContext { partition: Some(&odd), ..ctx };
        let bytes = write_manifest(&ctx, &entries).unwrap();
        assert_eq!(read_manifest(&mf, &bytes).unwrap()[0].file.partition, Some(20_000));
    }

    #[test]
    fn avro_names() {
        assert_eq!(avro_name("time_day"), "time_day");
        assert_eq!(avro_name("event-time_day"), "event_x2Dtime_day");
        assert_eq!(avro_name("1st"), "_1st");
        assert_eq!(avro_name("a b\"é"), "a_x20b_x22_xE9");
    }
}
