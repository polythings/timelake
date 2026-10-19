//! A table's head object (`metadata/timelake-head`, spec 7): its metadata, live files with
//! column bounds, and when they fit, the last-value index and the footers of the newest files. One
//! request instead of the version hint, metadata, manifest list and manifests.

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Int32Type, UInt32Type, UInt64Type};
use arrow_array::{Array, BinaryArray, PrimitiveArray, RecordBatch, StringArray};
use bytes::Bytes;
use timelake_format::bounds::decode;
use timelake_format::head::section::{COLUMNS, FILES, FOOTERS, LAST_VALUES, METADATA};
use timelake_format::head::{HeadDoc, Header, LAYOUT, LAYOUT_KEY, built_in};

use crate::error::{Error, Result};
use crate::meta::{DataFile, Field, TableMeta};

/// A table version as its head describes it.
#[derive(Debug)]
pub struct Head {
    /// The table version (`vN.metadata.json`).
    pub version: u64,
    pub meta: TableMeta,
    pub files: Vec<DataFile>,
    /// The last-value index, if it was inline.
    pub last_values: Option<RecordBatch>,
    /// Footers of data files by path.
    pub footers: HashMap<String, Bytes>,
}

impl Head {
    /// Parses a head written with a built-in codec (`arrow` or `json`).
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let header = Header::parse(bytes).ok_or_else(|| Error::invalid("not a Timelake head"))?;
        let codec =
            built_in(header.codec).ok_or_else(|| Error::invalid(format!("unknown head format {}", header.codec)))?;
        let mut doc = codec.decode(header.body)?;
        if doc.properties.get(LAYOUT_KEY).map(String::as_str) != Some(LAYOUT) {
            return Err(Error::invalid("unknown head layout"));
        }
        let metadata = section(&doc, METADATA)?;
        let json = string_column(metadata, "json")?;
        if json.len() != 1 {
            return Err(Error::invalid("head metadata must be one row"));
        }
        let meta = TableMeta::parse(json.value(0))?;
        let files = files(&doc, &meta.schema)?;
        let footers = match doc.sections.get(FOOTERS) {
            Some(f) => {
                let (paths, bytes) = (string_column(f, "path")?, binary(f, "footer")?);
                (0..f.num_rows())
                    .map(|i| (paths.value(i).to_string(), Bytes::copy_from_slice(bytes.value(i))))
                    .collect()
            }
            None => HashMap::new(),
        };
        Ok(Self { version: header.version, meta, files, last_values: doc.sections.remove(LAST_VALUES), footers })
    }
}

fn files(doc: &HeadDoc, schema: &[Field]) -> Result<Vec<DataFile>> {
    let files = section(doc, FILES)?;
    let (paths, sizes) = (string_column(files, "path")?, primitive::<UInt64Type>(files, "size")?);
    let records = primitive::<UInt64Type>(files, "records")?;
    let partitions = primitive::<Int32Type>(files, "partition")?;
    let mut out: Vec<DataFile> = (0..files.num_rows())
        .map(|i| DataFile {
            path: paths.value(i).to_string(),
            size: sizes.value(i),
            records: records.value(i),
            partition: partitions.is_valid(i).then(|| i64::from(partitions.value(i))),
            lower: HashMap::new(),
            upper: HashMap::new(),
        })
        .collect();

    let by_id: HashMap<i32, &Field> = schema.iter().map(|f| (f.id, f)).collect();
    let columns = section(doc, COLUMNS)?;
    let file = primitive::<UInt32Type>(columns, "file")?;
    let field_id = primitive::<Int32Type>(columns, "field_id")?;
    let (lower, upper) = (binary(columns, "lower")?, binary(columns, "upper")?);
    for row in 0..columns.num_rows() {
        let Some(f) = by_id.get(&field_id.value(row)) else { continue };
        let df = usize::try_from(file.value(row))
            .ok()
            .and_then(|i| out.get_mut(i))
            .ok_or_else(|| Error::invalid("head statistics for a missing file"))?;
        if let Some(b) = lower.is_valid(row).then(|| decode(&f.ty, lower.value(row))).flatten() {
            df.lower.insert(f.name.clone(), b);
        }
        if let Some(b) = upper.is_valid(row).then(|| decode(&f.ty, upper.value(row))).flatten() {
            df.upper.insert(f.name.clone(), b);
        }
    }
    Ok(out)
}

fn section<'a>(doc: &'a HeadDoc, name: &str) -> Result<&'a RecordBatch> {
    doc.sections.get(name).ok_or_else(|| Error::invalid(format!("head has no {name} section")))
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array> {
    batch.column_by_name(name).map(AsRef::as_ref).ok_or_else(|| Error::invalid(format!("head has no column {name}")))
}

fn wrong_type(name: &str) -> Error {
    Error::invalid(format!("head column {name} has the wrong type"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    column(batch, name)?.as_string_opt::<i32>().ok_or_else(|| wrong_type(name))
}

fn binary<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BinaryArray> {
    column(batch, name)?.as_binary_opt::<i32>().ok_or_else(|| wrong_type(name))
}

fn primitive<'a, T: ArrowPrimitiveType>(batch: &'a RecordBatch, name: &str) -> Result<&'a PrimitiveArray<T>> {
    column(batch, name)?.as_primitive_opt::<T>().ok_or_else(|| wrong_type(name))
}
