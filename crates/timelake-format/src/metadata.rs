//! Iceberg table metadata (`vN.metadata.json`) for the shared write path: read as JSON, so fields
//! Timelake doesn't know are kept, and updated the way iceberg-rust's metadata builder updates it
//! when a snapshot is added.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use serde_json::{Map, Value, json};

use crate::error::{WriteError, WriteResult};
use crate::iceberg::PartitionField;
use crate::rows::UTC;
use crate::{Chunk, Contract, IcebergType, property, summary};

/// The Arrow metadata key carrying an Iceberg field id, as Parquet and iceberg-rust use it.
pub const FIELD_ID_KEY: &str = "PARQUET:field_id";
/// How many earlier metadata files the metadata log keeps by default.
const PREVIOUS_VERSIONS_MAX: usize = 100;

/// A table's metadata at one version.
#[derive(Clone, Debug, PartialEq)]
pub struct Metadata(Value);

fn corrupt(what: &str) -> WriteError {
    WriteError::Corrupt(format!("table metadata: {what}"))
}

impl Metadata {
    /// Parses `vN.metadata.json`.
    pub fn parse(bytes: &[u8]) -> WriteResult<Self> {
        let value: Value = serde_json::from_slice(bytes).map_err(|e| corrupt(&e.to_string()))?;
        Ok(Self(value))
    }

    /// The Iceberg format version (1, 2, …).
    pub fn format_version(&self) -> u64 {
        self.0.get("format-version").and_then(Value::as_u64).unwrap_or(1)
    }

    /// The metadata as JSON text.
    pub fn to_json(&self) -> String {
        self.0.to_string()
    }

    /// The table's root location.
    pub fn location(&self) -> &str {
        self.0.get("location").and_then(Value::as_str).unwrap_or_default()
    }

    /// The table properties.
    pub fn properties(&self) -> HashMap<String, String> {
        string_map(self.0.get("properties"))
    }

    /// The Timelake contract in the properties.
    pub fn contract(&self) -> WriteResult<Contract> {
        Ok(Contract::from_properties(&self.properties())?)
    }

    /// The current snapshot's id.
    pub fn current_snapshot_id(&self) -> Option<i64> {
        self.0.get("current-snapshot-id").and_then(Value::as_i64).filter(|&id| id >= 0)
    }

    fn snapshots(&self) -> &[Value] {
        self.0.get("snapshots").and_then(Value::as_array).map_or(&[], Vec::as_slice)
    }

    fn snapshot(&self, id: i64) -> Option<&Value> {
        self.snapshots().iter().find(|s| s.get("snapshot-id").and_then(Value::as_i64) == Some(id))
    }

    /// Whether a snapshot with this id exists.
    pub fn has_snapshot(&self, id: i64) -> bool {
        self.snapshot(id).is_some()
    }

    /// The current snapshot's summary.
    pub fn summary(&self) -> HashMap<String, String> {
        string_map(self.current_snapshot_id().and_then(|id| self.snapshot(id)?.get("summary")))
    }

    /// Whether the current snapshot or one of its ancestors committed batch `batch_id`
    /// (`timelake.batch-id`).
    pub fn history_has_batch(&self, batch_id: &str) -> bool {
        let mut id = self.current_snapshot_id();
        while let Some(s) = id.and_then(|id| self.snapshot(id)) {
            if s.get("summary").and_then(|m| m.get(summary::BATCH_ID)).and_then(Value::as_str) == Some(batch_id) {
                return true;
            }
            id = s.get("parent-snapshot-id").and_then(Value::as_i64);
        }
        false
    }

    /// The current snapshot's manifest list.
    pub fn manifest_list(&self) -> Option<&str> {
        self.current_snapshot_id().and_then(|id| self.snapshot(id)?.get("manifest-list")?.as_str())
    }

    /// The sequence number the next snapshot gets.
    pub fn next_sequence_number(&self) -> i64 {
        self.0.get("last-sequence-number").and_then(Value::as_i64).unwrap_or(0) + 1
    }

    /// The current schema's id.
    pub fn schema_id(&self) -> i32 {
        self.0.get("current-schema-id").and_then(Value::as_i64).and_then(|v| i32::try_from(v).ok()).unwrap_or(0)
    }

    /// The current schema as Iceberg JSON.
    pub fn schema_json(&self) -> WriteResult<&Value> {
        let id = i64::from(self.schema_id());
        self.0
            .get("schemas")
            .and_then(Value::as_array)
            .and_then(|s| s.iter().find(|s| s.get("schema-id").and_then(Value::as_i64) == Some(id)))
            .ok_or_else(|| corrupt("no current schema"))
    }

    /// The current schema's top-level columns.
    pub fn columns(&self) -> WriteResult<Vec<Column>> {
        let fields = self.schema_json()?.get("fields").and_then(Value::as_array);
        let fields = fields.ok_or_else(|| corrupt("schema without fields"))?;
        fields
            .iter()
            .map(|f| {
                Ok(Column {
                    id: f
                        .get("id")
                        .and_then(Value::as_i64)
                        .and_then(|id| i32::try_from(id).ok())
                        .ok_or_else(|| corrupt("field id"))?,
                    name: f
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| corrupt("field without a name"))?
                        .to_string(),
                    iceberg_type: f.get("type").and_then(Value::as_str).map_or(IcebergType::Nested, IcebergType::named),
                    required: f.get("required").and_then(Value::as_bool).unwrap_or(false),
                })
            })
            .collect()
    }

    /// The current schema as Arrow, fields carrying their Iceberg field ids (as iceberg-rust's
    /// `schema_to_arrow_schema` makes it).
    pub fn arrow_schema(&self) -> WriteResult<SchemaRef> {
        let fields = struct_fields(self.schema_json()?)?;
        Ok(Arc::new(Schema::new(fields)))
    }

    /// The default partition spec's id and fields (as JSON).
    pub fn partition_spec(&self) -> WriteResult<(i32, &Value)> {
        let id = self.0.get("default-spec-id").and_then(Value::as_i64).unwrap_or(0);
        let spec = self
            .0
            .get("partition-specs")
            .and_then(Value::as_array)
            .and_then(|s| s.iter().find(|s| s.get("spec-id").and_then(Value::as_i64) == Some(id)))
            .ok_or_else(|| corrupt("no default partition spec"))?;
        let fields = spec.get("fields").ok_or_else(|| corrupt("partition spec without fields"))?;
        Ok((i32::try_from(id).map_err(|_| corrupt("spec id"))?, fields))
    }

    /// The partition field, for tables partitioned by time. A spec of more than one field is an
    /// error: Timelake partitions by time alone.
    pub fn partition_field(&self) -> WriteResult<Option<PartitionField>> {
        let (_, fields) = self.partition_spec()?;
        let fields = fields.as_array().map_or(&[][..], Vec::as_slice);
        let field = match fields {
            [] => return Ok(None),
            [field] => field,
            _ => {
                return Err(WriteError::invalid(format!(
                    "{} partition fields: only one, by time, is supported",
                    fields.len()
                )));
            }
        };
        let transform = field.get("transform").and_then(Value::as_str);
        let Some(chunk) = transform.and_then(Chunk::from_transform) else {
            return Err(WriteError::invalid(format!("unsupported partition transform {transform:?}")));
        };
        let name = field.get("name").and_then(Value::as_str).ok_or_else(|| corrupt("partition field name"))?;
        let field_id = field
            .get("field-id")
            .and_then(Value::as_i64)
            .and_then(|id| i32::try_from(id).ok())
            .ok_or_else(|| corrupt("partition field id"))?;
        Ok(Some(PartitionField { name: name.to_string(), field_id, chunk }))
    }

    /// The default sort order's id.
    pub fn sort_order_id(&self) -> i32 {
        self.0.get("default-sort-order-id").and_then(Value::as_i64).and_then(|v| i32::try_from(v).ok()).unwrap_or(0)
    }

    /// When the metadata was last updated (ms since the epoch).
    pub fn last_updated_ms(&self) -> i64 {
        self.0.get("last-updated-ms").and_then(Value::as_i64).unwrap_or(0)
    }

    /// Every snapshot with a manifest list.
    pub fn snapshot_list(&self) -> Vec<SnapshotRef> {
        self.snapshots()
            .iter()
            .filter_map(|s| {
                Some(SnapshotRef {
                    id: s.get("snapshot-id")?.as_i64()?,
                    timestamp_ms: s.get("timestamp-ms").and_then(Value::as_i64).unwrap_or(0),
                    manifest_list: s.get("manifest-list")?.as_str()?.to_string(),
                    last_value_index: s
                        .get("summary")
                        .and_then(|m| m.get(summary::LAST_VALUE_INDEX))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    summary: string_map(s.get("summary")),
                })
            })
            .collect()
    }

    /// The snapshots a branch or tag points at (`main` among them).
    pub fn referenced_snapshots(&self) -> HashSet<i64> {
        let Some(Value::Object(refs)) = self.0.get("refs") else { return HashSet::new() };
        refs.values().filter_map(|r| r.get("snapshot-id")?.as_i64()).collect()
    }

    /// How many earlier metadata files the metadata log keeps (Iceberg's
    /// `write.metadata.previous-versions-max`), at least one.
    fn previous_versions_max(&self) -> usize {
        self.properties()
            .get(property::METADATA_PREVIOUS_VERSIONS_MAX)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(PREVIOUS_VERSIONS_MAX)
            .max(1)
    }

    /// Entries in the metadata log.
    pub fn metadata_log_len(&self) -> usize {
        self.0.get("metadata-log").and_then(Value::as_array).map_or(0, Vec::len)
    }

    /// This metadata without the snapshots `ids`, as iceberg-rust's builder removes them: refs to
    /// them go, and the snapshot log keeps only what follows the last entry of a removed snapshot
    /// (so it has no gaps). `previous` is recorded in the metadata log.
    pub fn without_snapshots(&self, ids: &[i64], previous: &str, now_ms: i64) -> WriteResult<Self> {
        let mut m = self.0.clone();
        let obj = m.as_object_mut().ok_or_else(|| corrupt("not an object"))?;
        let removed = |v: &Value| v.get("snapshot-id").and_then(Value::as_i64).is_some_and(|id| ids.contains(&id));
        if let Some(Value::Array(snapshots)) = obj.get_mut("snapshots") {
            snapshots.retain(|s| !removed(s));
        }
        if let Some(Value::Object(refs)) = obj.get_mut("refs") {
            refs.retain(|_, r| !removed(r));
        }
        if let Some(Value::Array(log)) = obj.get_mut("snapshot-log") {
            let mut kept = vec![];
            for entry in log.drain(..) {
                if removed(&entry) {
                    kept.clear();
                } else {
                    kept.push(entry);
                }
            }
            *log = kept;
        }
        push(obj, "metadata-log", json!({"metadata-file": previous, "timestamp-ms": self.last_updated_ms()}));
        let keep = self.previous_versions_max();
        if let Some(Value::Array(log)) = obj.get_mut("metadata-log")
            && log.len() > keep
        {
            log.drain(..log.len() - keep);
        }
        obj.insert("last-updated-ms".into(), json!(now_ms));
        Ok(Self(m))
    }

    /// This metadata with `snapshot` added as the current snapshot of `main`, `previous` (this
    /// version's file) recorded in the metadata log, updated at `now_ms`.
    pub fn with_snapshot(&self, snapshot: &NewSnapshot, previous: &str, now_ms: i64) -> WriteResult<Self> {
        let mut m = self.0.clone();
        let obj = m.as_object_mut().ok_or_else(|| corrupt("not an object"))?;
        let mut s = json!({
            "snapshot-id": snapshot.snapshot_id,
            "sequence-number": snapshot.sequence_number,
            "timestamp-ms": snapshot.timestamp_ms,
            "manifest-list": snapshot.manifest_list,
            "summary": snapshot.summary_json(),
            "schema-id": self.schema_id(),
        });
        if let (Some(parent), Some(s)) = (self.current_snapshot_id(), s.as_object_mut()) {
            s.insert("parent-snapshot-id".into(), json!(parent));
        }
        push(obj, "snapshots", s);
        push(obj, "snapshot-log", json!({"snapshot-id": snapshot.snapshot_id, "timestamp-ms": snapshot.timestamp_ms}));
        push(obj, "metadata-log", json!({"metadata-file": previous, "timestamp-ms": self.last_updated_ms()}));
        let keep = self.previous_versions_max();
        if let Some(Value::Array(log)) = obj.get_mut("metadata-log")
            && log.len() > keep
        {
            log.drain(..log.len() - keep);
        }
        obj.insert("current-snapshot-id".into(), json!(snapshot.snapshot_id));
        obj.insert("last-sequence-number".into(), json!(snapshot.sequence_number));
        obj.insert("last-updated-ms".into(), json!(now_ms));
        let refs = obj.entry("refs").or_insert_with(|| Value::Object(Map::new()));
        if let Some(refs) = refs.as_object_mut() {
            refs.insert("main".into(), json!({"snapshot-id": snapshot.snapshot_id, "type": "branch"}));
        }
        Ok(Self(m))
    }
}

/// The string values of a JSON object.
fn string_map(object: Option<&Value>) -> HashMap<String, String> {
    let Some(Value::Object(m)) = object else { return HashMap::new() };
    m.iter().filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string()))).collect()
}

fn push(obj: &mut Map<String, Value>, key: &str, value: Value) {
    match obj.get_mut(key) {
        Some(Value::Array(a)) => a.push(value),
        _ => {
            obj.insert(key.into(), Value::Array(vec![value]));
        }
    }
}

/// A top-level column of a schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// Its field id.
    pub id: i32,
    /// Its name.
    pub name: String,
    /// Its Iceberg type ([`IcebergType::Nested`] for structs, lists and maps).
    pub iceberg_type: IcebergType,
    /// Whether it has a value in every row.
    pub required: bool,
}

/// A snapshot in the metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef {
    /// Its id.
    pub id: i64,
    /// When it was made (ms since the epoch).
    pub timestamp_ms: i64,
    /// Its manifest list.
    pub manifest_list: String,
    /// Its last-value index.
    pub last_value_index: Option<String>,
    /// Its summary (`operation` among it).
    pub summary: HashMap<String, String>,
}

/// Iceberg's snapshot operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    /// Only adds files.
    Append,
    /// Replaces files with files holding the same rows (or changes no data).
    Replace,
    /// Only removes files.
    Delete,
}

impl Operation {
    /// The name in the snapshot summary.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Replace => "replace",
            Self::Delete => "delete",
        }
    }
}

/// A snapshot to add (see [`Metadata::with_snapshot`]).
#[derive(Clone, Debug)]
pub struct NewSnapshot {
    /// Its id.
    pub snapshot_id: i64,
    /// Its sequence number.
    pub sequence_number: i64,
    /// When it was made (ms since the epoch).
    pub timestamp_ms: i64,
    /// Its manifest list.
    pub manifest_list: String,
    /// What it does.
    pub operation: Operation,
    /// Summary properties besides the operation.
    pub summary: HashMap<String, String>,
}

impl NewSnapshot {
    fn summary_json(&self) -> Value {
        let mut s: Map<String, Value> = self.summary.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
        s.insert(summary::OPERATION.into(), json!(self.operation.as_str()));
        Value::Object(s)
    }
}

fn struct_fields(schema: &Value) -> WriteResult<Vec<Field>> {
    let fields = schema.get("fields").and_then(Value::as_array).ok_or_else(|| corrupt("schema without fields"))?;
    fields.iter().map(field).collect()
}

fn field(f: &Value) -> WriteResult<Field> {
    let name = f.get("name").and_then(Value::as_str).ok_or_else(|| corrupt("field without a name"))?;
    let id = f.get("id").and_then(Value::as_i64).ok_or_else(|| corrupt("field without an id"))?;
    let required = f.get("required").and_then(Value::as_bool).unwrap_or(false);
    let ty = data_type(f.get("type").ok_or_else(|| corrupt("field without a type"))?)?;
    Ok(Field::new(name, ty, !required).with_metadata(HashMap::from([(FIELD_ID_KEY.into(), id.to_string())])))
}

fn data_type(t: &Value) -> WriteResult<DataType> {
    if let Some(name) = t.as_str() {
        return primitive(&IcebergType::named(name));
    }
    match t.get("type").and_then(Value::as_str) {
        Some("struct") => Ok(DataType::Struct(Fields::from(struct_fields(t)?))),
        Some("list") => {
            let id = t.get("element-id").and_then(Value::as_i64).ok_or_else(|| corrupt("list without element-id"))?;
            let required = t.get("element-required").and_then(Value::as_bool).unwrap_or(false);
            let element = data_type(t.get("element").ok_or_else(|| corrupt("list without element"))?)?;
            let field = Field::new("element", element, !required)
                .with_metadata(HashMap::from([(FIELD_ID_KEY.into(), id.to_string())]));
            Ok(DataType::List(Arc::new(field)))
        }
        Some("map") => {
            let id = |k: &str| t.get(k).and_then(Value::as_i64).ok_or_else(|| corrupt("map without ids"));
            let meta = |id: i64| HashMap::from([(FIELD_ID_KEY.to_string(), id.to_string())]);
            let key = data_type(t.get("key").ok_or_else(|| corrupt("map without key"))?)?;
            let value = data_type(t.get("value").ok_or_else(|| corrupt("map without value"))?)?;
            let value_required = t.get("value-required").and_then(Value::as_bool).unwrap_or(false);
            let entries = Field::new(
                "key_value",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", key, false).with_metadata(meta(id("key-id")?)),
                    Field::new("value", value, !value_required).with_metadata(meta(id("value-id")?)),
                ])),
                false,
            );
            Ok(DataType::Map(Arc::new(entries), false))
        }
        _ => Err(corrupt("unknown type")),
    }
}

fn primitive(t: &IcebergType) -> WriteResult<DataType> {
    use IcebergType as T;
    Ok(match t {
        T::Boolean => DataType::Boolean,
        T::Int => DataType::Int32,
        T::Long => DataType::Int64,
        T::Float => DataType::Float32,
        T::Double => DataType::Float64,
        T::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        T::Date => DataType::Date32,
        T::Time => DataType::Time64(TimeUnit::Microsecond),
        T::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        T::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        T::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        T::TimestamptzNs => DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC.into())),
        T::String => DataType::Utf8,
        T::Uuid => DataType::FixedSizeBinary(16),
        T::Fixed(length) => DataType::FixedSizeBinary(i32::try_from(*length).map_err(|_| corrupt("fixed length"))?),
        T::Binary => DataType::LargeBinary,
        T::Nested | T::Other(_) => return Err(corrupt(&format!("unknown type {t}"))),
    })
}
