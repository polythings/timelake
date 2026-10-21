//! The head object's container (spec 7): a fixed header naming the table version and the
//! codec, then a format-neutral [`HeadDoc`] (string properties plus named Arrow tables) encoded by
//! that codec. What the sections hold is [`content`]'s (feature `write`). Shared by the engine and
//! the WebAssembly reader and writer.
//!
//! Built-in codecs: `arrow` (Arrow IPC streams, zstd-compressed; the default) and `json` (readable,
//! for debugging and tools without Arrow). Any format with an Arrow bridge (Avro, Vortex, BSON,
//! Parquet, …) fits the [`HeadCodec`] trait.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_ipc::CompressionType;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef, TimeUnit};
use arrow_select::concat::concat_batches;
use serde::{Deserialize, Serialize};

use crate::FormatError;

#[cfg(feature = "write")]
pub mod content;

/// `Result` of head encoding and decoding.
pub type Result<T, E = FormatError> = std::result::Result<T, E>;

/// `timelake.head-format` value that turns the head off.
pub const NO_HEAD: &str = "none";
const MAGIC: &[u8; 4] = b"TLHD";
const HEADER_VERSION: u8 = 1;
/// The longest header: magic, header version, table version, codec name length, codec name.
pub const MAX_HEADER: u64 = 4 + 1 + 8 + 1 + 255;
/// Property naming the layout of the sections; readers skip heads with a layout they don't know.
pub const LAYOUT_KEY: &str = "layout";
/// The layout this version writes.
pub const LAYOUT: &str = "1";

/// Section names.
pub mod section {
    /// One row, column `json`: the table's `vN.metadata.json`.
    pub const METADATA: &str = "metadata";
    /// One row per live data file.
    pub const FILES: &str = "files";
    /// One row per file and column with statistics.
    pub const COLUMNS: &str = "columns";
    /// The last-value index, when it fits the inline budget.
    pub const LAST_VALUES: &str = "last_values";
    /// Parquet footers of the newest files, in what is left of the budget.
    pub const FOOTERS: &str = "footers";
}

/// The fixed part of a head: which table version it describes and which codec wrote the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header<'a> {
    /// The table version (`vN.metadata.json`) the head describes.
    pub version: u64,
    /// Name of the codec that encoded `body`.
    pub codec: &'a str,
    /// The encoded [`HeadDoc`].
    pub body: &'a [u8],
}

impl<'a> Header<'a> {
    /// Parses the header; `None` if `bytes` don't start with one (or the prefix is too short).
    pub fn parse(bytes: &'a [u8]) -> Option<Self> {
        let rest = bytes.strip_prefix(MAGIC)?;
        let (&header_version, rest) = rest.split_first()?;
        if header_version != HEADER_VERSION {
            return None;
        }
        let (version, rest) = rest.split_first_chunk::<8>()?;
        let (&len, rest) = rest.split_first()?;
        let (codec, body) = rest.split_at_checked(len.into())?;
        Some(Self { version: u64::from_le_bytes(*version), codec: std::str::from_utf8(codec).ok()?, body })
    }
}

/// A head for table `version`: the header, then `doc` encoded by `codec`.
pub fn encode(version: u64, codec: &dyn HeadCodec, doc: &HeadDoc) -> Result<Vec<u8>> {
    let name = codec.name();
    let len = u8::try_from(name.len()).map_err(|_| FormatError::new(format!("head codec name too long: {name}")))?;
    let mut out = Vec::from(*MAGIC);
    out.push(HEADER_VERSION);
    out.extend_from_slice(&version.to_le_bytes());
    out.push(len);
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(&codec.encode(doc)?);
    Ok(out)
}

/// The built-in codec called `name`.
pub fn built_in(name: &str) -> Option<&'static dyn HeadCodec> {
    match name {
        "arrow" => Some(&ArrowCodec),
        "json" => Some(&JsonCodec),
        _ => None,
    }
}

impl From<ArrowError> for FormatError {
    fn from(e: ArrowError) -> Self {
        Self::new(format!("head: {e}"))
    }
}

impl From<serde_json::Error> for FormatError {
    fn from(e: serde_json::Error) -> Self {
        Self::new(format!("head: {e}"))
    }
}

/// The content of a head, independent of its encoding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeadDoc {
    /// Scalar fields.
    pub properties: BTreeMap<String, String>,
    /// Named tables.
    pub sections: BTreeMap<String, RecordBatch>,
}

/// Encodes a [`HeadDoc`] to bytes and back.
pub trait HeadCodec: Send + Sync + Debug {
    /// The name stored in every head this codec writes, and the `timelake.head-format` value that
    /// selects it: lowercase ASCII, at most 255 bytes.
    fn name(&self) -> &str;

    /// Encodes a document.
    fn encode(&self, doc: &HeadDoc) -> Result<Vec<u8>>;

    /// Decodes what [`HeadCodec::encode`] wrote.
    fn decode(&self, bytes: &[u8]) -> Result<HeadDoc>;
}

/// Schema metadata key holding a section's name in the `arrow` codec.
const SECTION_KEY: &str = "timelake.section";

/// Arrow IPC: one stream for the properties (as schema metadata), then one stream per section,
/// each prefixed with its length. Buffers are zstd-compressed.
#[derive(Clone, Copy, Debug, Default)]
pub struct ArrowCodec;

impl HeadCodec for ArrowCodec {
    fn name(&self) -> &'static str {
        "arrow"
    }

    fn encode(&self, doc: &HeadDoc) -> Result<Vec<u8>> {
        let options =
            IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD)).map_err(FormatError::from)?;
        let mut out = vec![];
        let properties = Schema::empty().with_metadata(doc.properties.clone().into_iter().collect());
        write_stream(&mut out, &properties, None, &options)?;
        for (name, batch) in &doc.sections {
            let mut metadata = batch.schema().metadata().clone();
            metadata.insert(SECTION_KEY.into(), name.clone());
            let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
            write_stream(&mut out, &schema, Some(&with_schema(batch, &schema)?), &options)?;
        }
        Ok(out)
    }

    fn decode(&self, mut bytes: &[u8]) -> Result<HeadDoc> {
        let mut doc = HeadDoc::default();
        let mut first = true;
        while !bytes.is_empty() {
            let (stream, rest) = next_stream(bytes)?;
            bytes = rest;
            let reader = StreamReader::try_new(Cursor::new(stream), None).map_err(FormatError::from)?;
            let mut schema = reader.schema().as_ref().clone();
            if first {
                doc.properties = schema.metadata.into_iter().collect();
                first = false;
                continue;
            }
            let name =
                schema.metadata.remove(SECTION_KEY).ok_or_else(|| FormatError::new("head section without name"))?;
            let schema = Arc::new(schema);
            let batches =
                reader.map(|b| with_schema(&b.map_err(FormatError::from)?, &schema)).collect::<Result<Vec<_>>>()?;
            doc.sections.insert(name, concat_batches(&schema, &batches).map_err(FormatError::from)?);
        }
        Ok(doc)
    }
}

/// `batch` with `schema`, which differs from its own only in metadata.
fn with_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), batch.columns().to_vec(), &options).map_err(FormatError::from)
}

fn write_stream(
    out: &mut Vec<u8>,
    schema: &Schema,
    batch: Option<&RecordBatch>,
    options: &IpcWriteOptions,
) -> Result<()> {
    let mut buf = vec![];
    let mut w = StreamWriter::try_new_with_options(&mut buf, schema, options.clone()).map_err(FormatError::from)?;
    if let Some(batch) = batch {
        w.write(batch).map_err(FormatError::from)?;
    }
    w.finish().map_err(FormatError::from)?;
    drop(w);
    out.extend_from_slice(&(buf.len() as u64).to_le_bytes());
    out.extend_from_slice(&buf);
    Ok(())
}

/// Splits off the next length-prefixed stream.
fn next_stream(bytes: &[u8]) -> Result<(&[u8], &[u8])> {
    let truncated = || FormatError::new("truncated head");
    let (len, rest) = bytes.split_first_chunk::<8>().ok_or_else(truncated)?;
    let len = usize::try_from(u64::from_le_bytes(*len)).map_err(|_| truncated())?;
    if rest.len() < len {
        return Err(truncated());
    }
    Ok(rest.split_at(len))
}

/// JSON: `{"properties": {…}, "sections": [{"name", "schema", "rows": [{…}]}]}`. Rows are Arrow's
/// JSON form (binary as hex) with timestamps as integers in their unit; schemas are
/// `arrow-schema`'s serde form.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

#[derive(Serialize, Deserialize)]
struct JsonDoc {
    properties: BTreeMap<String, String>,
    sections: Vec<JsonSection>,
}

#[derive(Serialize, Deserialize)]
struct JsonSection {
    name: String,
    schema: Schema,
    rows: Vec<serde_json::Value>,
}

impl HeadCodec for JsonCodec {
    fn name(&self) -> &'static str {
        "json"
    }

    fn encode(&self, doc: &HeadDoc) -> Result<Vec<u8>> {
        let sections = doc
            .sections
            .iter()
            .map(|(name, batch)| {
                let mut w = arrow_json::WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, arrow_json::writer::JsonArray>(vec![]);
                w.write(&timestamps_as_integers(batch)?).map_err(FormatError::from)?;
                w.finish().map_err(FormatError::from)?;
                let rows = serde_json::from_slice(&w.into_inner()).map_err(FormatError::from)?;
                Ok(JsonSection { name: name.clone(), schema: batch.schema().as_ref().clone(), rows })
            })
            .collect::<Result<_>>()?;
        serde_json::to_vec(&JsonDoc { properties: doc.properties.clone(), sections }).map_err(FormatError::from)
    }

    fn decode(&self, bytes: &[u8]) -> Result<HeadDoc> {
        let json: JsonDoc = serde_json::from_slice(bytes).map_err(FormatError::from)?;
        let mut doc = HeadDoc { properties: json.properties, sections: BTreeMap::new() };
        for section in json.sections {
            let schema = Arc::new(section.schema);
            let read_as = Arc::new(Schema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .map(|f| match f.data_type() {
                        DataType::Timestamp(_, _) => Arc::new(f.as_ref().clone().with_data_type(DataType::Int64)),
                        _ => f.clone(),
                    })
                    .collect::<Vec<_>>(),
                schema.metadata().clone(),
            ));
            let mut decoder = arrow_json::ReaderBuilder::new(read_as.clone())
                .with_batch_size(section.rows.len().max(1))
                .build_decoder()
                .map_err(FormatError::from)?;
            decoder.serialize(&section.rows).map_err(FormatError::from)?;
            let batch = decoder.flush().map_err(FormatError::from)?.unwrap_or_else(|| RecordBatch::new_empty(read_as));
            let batch = integers_as_timestamps(&batch, &schema)?;
            doc.sections.insert(section.name, batch);
        }
        Ok(doc)
    }
}

/// Timestamp columns as their integer values since the epoch, so JSON keeps them exact and
/// reading them back needs no timezone database.
fn timestamps_as_integers(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let (mut fields, mut columns) = (vec![], vec![]);
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let column = match field.data_type() {
            DataType::Timestamp(unit, _) => as_integers(column, *unit),
            _ => column.clone(),
        };
        fields.push(Arc::new(field.as_ref().clone().with_data_type(column.data_type().clone())));
        columns.push(column);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(FormatError::from)
}

/// The inverse of [`timestamps_as_integers`], given the original schema.
fn integers_as_timestamps(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| match field.data_type() {
            DataType::Timestamp(unit, tz) => as_timestamps(column, *unit, tz.clone()),
            _ => column.clone(),
        })
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &options).map_err(FormatError::from)
}

macro_rules! by_unit {
    ($unit:expr, $f:ident!($($arg:tt)*)) => {
        match $unit {
            TimeUnit::Second => $f!(TimestampSecondType, $($arg)*),
            TimeUnit::Millisecond => $f!(TimestampMillisecondType, $($arg)*),
            TimeUnit::Microsecond => $f!(TimestampMicrosecondType, $($arg)*),
            TimeUnit::Nanosecond => $f!(TimestampNanosecondType, $($arg)*),
        }
    };
}

/// A timestamp column of `unit` as `Int64`.
fn as_integers(column: &ArrayRef, unit: TimeUnit) -> ArrayRef {
    macro_rules! cast {
        ($t:ty,) => {
            Arc::new(column.as_primitive::<$t>().reinterpret_cast::<Int64Type>())
        };
    }
    by_unit!(unit, cast!())
}

/// An `Int64` column as a timestamp of `unit` in `tz`.
fn as_timestamps(column: &ArrayRef, unit: TimeUnit, tz: Option<Arc<str>>) -> ArrayRef {
    macro_rules! cast {
        ($t:ty,) => {
            Arc::new(column.as_primitive::<Int64Type>().reinterpret_cast::<$t>().with_timezone_opt(tz))
        };
    }
    by_unit!(unit, cast!())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::{BinaryArray, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::Field;

    use super::*;

    fn doc() -> HeadDoc {
        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("n", DataType::Int64, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)
                .with_metadata(HashMap::from([("PARQUET:field_id".into(), "4".into())])),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(BinaryArray::from(vec![Some(b"\x00\xff".as_slice()), None])),
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2]).with_timezone("UTC")),
            ],
        )
        .unwrap();
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)])));
        HeadDoc {
            properties: BTreeMap::from([("k".into(), "v".into())]),
            sections: BTreeMap::from([("files".into(), batch), ("empty".into(), empty)]),
        }
    }

    #[test]
    fn built_in_codecs_round_trip() {
        for codec in [&ArrowCodec as &dyn HeadCodec, &JsonCodec] {
            let doc = doc();
            assert_eq!(codec.decode(&codec.encode(&doc).unwrap()).unwrap(), doc, "{}", codec.name());
        }
    }

    #[test]
    fn header_round_trips() {
        let bytes = encode(7, &JsonCodec, &doc()).unwrap();
        let h = Header::parse(&bytes).unwrap();
        assert_eq!((h.version, h.codec), (7, "json"));
        assert_eq!(JsonCodec.decode(h.body).unwrap(), doc());
        assert!(Header::parse(&bytes[..10]).is_none());
        assert!(Header::parse(b"PAR1").is_none());
    }

    #[test]
    fn truncated_input_is_an_error() {
        let bytes = ArrowCodec.encode(&doc()).unwrap();
        assert!(ArrowCodec.decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(JsonCodec.decode(b"{").is_err());
    }
}
