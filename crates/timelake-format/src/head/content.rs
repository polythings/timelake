//! What a head holds, whatever its codec (spec 7): the table metadata, the live data files with
//! their column statistics, and while they fit the table's `head-inline-bytes`, the last-value
//! index and the footers of the newest files. The engine and the shared write path write heads
//! with [`HeadBuilder`]; the engine, the shared write path and the WASM reader read them with
//! [`Contents::decode`].
//!
//! Sections ([`super::section`]):
//! - `metadata`: one row, column `json`: the table's `vN.metadata.json`.
//! - `files`: one row per live data file.
//! - `columns`: one row per file and column with statistics: bounds (Iceberg's single-value
//!   encoding) and counts.
//! - `last_values`: the last-value index.
//! - `footers`: Parquet footers (`crate::footer`), by file path.

use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, BinaryBuilder, StringBuilder};
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::section::{COLUMNS, FILES, FOOTERS, LAST_VALUES, METADATA};
use super::{HeadCodec, HeadDoc, Header, LAYOUT, LAYOUT_KEY, Result, encode};
use crate::FormatError;
use crate::iceberg::{DataFile, Entry, Status};

fn files_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("records", DataType::UInt64, false),
        Field::new("partition", DataType::Int32, true),
        Field::new("sort_order_id", DataType::Int32, true),
        Field::new("split_offsets", DataType::List(Arc::new(Field::new("item", DataType::Int64, true))), true),
        // 1 if the file was added by the snapshot of `snapshot_id`'s manifest, else 0.
        Field::new("added", DataType::UInt8, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("sequence_number", DataType::Int64, false),
        Field::new("file_sequence_number", DataType::Int64, false),
    ]))
}

fn columns_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // Row of the file in `files`.
        Field::new("file", DataType::UInt32, false),
        Field::new("field_id", DataType::Int32, false),
        Field::new("lower", DataType::Binary, true),
        Field::new("upper", DataType::Binary, true),
        Field::new("values", DataType::UInt64, true),
        Field::new("nulls", DataType::UInt64, true),
        Field::new("nans", DataType::UInt64, true),
        Field::new("size", DataType::UInt64, true),
    ]))
}

fn footers_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("footer", DataType::Binary, false),
    ]))
}

/// Builds a head: the required sections, then the optional ones while they fit the budget, the
/// last-value index first and then footers, newest partitions first ([`footer_order`]).
#[derive(Debug)]
pub struct HeadBuilder<'a> {
    version: u64,
    codec: &'a dyn HeadCodec,
    doc: HeadDoc,
    /// The head as encoded so far.
    bytes: Vec<u8>,
    /// Bytes left for optional sections.
    budget: u64,
    footer_paths: StringBuilder,
    footers: BinaryBuilder,
}

impl<'a> HeadBuilder<'a> {
    /// A head of table `version` (its `vN.metadata.json` is `metadata_json`) whose live files are
    /// `files`, encoded by `codec`, with `budget` bytes for the optional sections.
    pub fn new(
        version: u64,
        codec: &'a dyn HeadCodec,
        metadata_json: String,
        files: &[Entry],
        budget: u64,
    ) -> Result<Self> {
        let mut doc = HeadDoc::default();
        doc.properties.insert(LAYOUT_KEY.into(), LAYOUT.into());
        let json = Schema::new(vec![Field::new("json", DataType::Utf8, false)]);
        let json = RecordBatch::try_new(Arc::new(json), vec![Arc::new(StringArray::from(vec![metadata_json]))])?;
        doc.sections.insert(METADATA.into(), json);
        doc.sections.insert(FILES.into(), encode_files(files)?);
        doc.sections.insert(COLUMNS.into(), encode_columns(files)?);
        let bytes = encode(version, codec, &doc)?;
        Ok(Self {
            version,
            codec,
            doc,
            bytes,
            budget,
            footer_paths: StringBuilder::new(),
            footers: BinaryBuilder::new(),
        })
    }

    /// Adds the last-value index if it fits the budget; whether it did.
    pub fn last_values(&mut self, index: &RecordBatch) -> Result<bool> {
        self.doc.sections.insert(LAST_VALUES.into(), index.clone());
        let with_index = encode(self.version, self.codec, &self.doc)?;
        let cost = with_index.len().saturating_sub(self.bytes.len()) as u64;
        if cost > self.budget {
            self.doc.sections.remove(LAST_VALUES);
            return Ok(false);
        }
        self.bytes = with_index;
        self.budget -= cost;
        Ok(true)
    }

    /// Adds the footer of the file at `path` if it fits what is left of the budget. Once one
    /// doesn't (`false`), add no more: footers go in [`footer_order`].
    pub fn footer(&mut self, path: &str, footer: &[u8]) -> bool {
        let Some(rest) = self.budget.checked_sub(footer.len() as u64) else { return false };
        self.budget = rest;
        self.footer_paths.append_value(path);
        self.footers.append_value(footer);
        true
    }

    /// The encoded head.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        if self.footers.is_empty() {
            return Ok(self.bytes);
        }
        let columns: Vec<ArrayRef> = vec![Arc::new(self.footer_paths.finish()), Arc::new(self.footers.finish())];
        self.doc.sections.insert(FOOTERS.into(), RecordBatch::try_new(footers_schema(), columns)?);
        encode(self.version, self.codec, &self.doc)
    }
}

/// The order footers go into a head: files of the newest partitions first.
pub fn footer_order(files: &[Entry]) -> Vec<&Entry> {
    let mut files: Vec<&Entry> = files.iter().collect();
    files.sort_by_key(|e| std::cmp::Reverse((e.file.partition, e.file.path.as_str())));
    files
}

fn encode_files(files: &[Entry]) -> Result<RecordBatch> {
    let df = || files.iter().map(|e| e.file.as_ref());
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(df().map(|f| f.path.as_str()))),
        Arc::new(UInt64Array::from_iter_values(df().map(|f| f.file_size))),
        Arc::new(UInt64Array::from_iter_values(df().map(|f| f.record_count))),
        Arc::new(df().map(|f| f.partition).collect::<Int32Array>()),
        Arc::new(df().map(|f| f.sort_order_id).collect::<Int32Array>()),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            df().map(|f| f.split_offsets.as_ref().map(|o| o.iter().copied().map(Some).collect::<Vec<_>>())),
        )),
        Arc::new(UInt8Array::from_iter_values(files.iter().map(|e| u8::from(e.status == Status::Added)))),
        Arc::new(Int64Array::from_iter_values(files.iter().map(|e| e.snapshot_id))),
        Arc::new(Int64Array::from_iter_values(files.iter().map(|e| e.sequence_number))),
        Arc::new(Int64Array::from_iter_values(files.iter().map(|e| e.file_sequence_number))),
    ];
    Ok(RecordBatch::try_new(files_schema(), columns)?)
}

fn encode_columns(files: &[Entry]) -> Result<RecordBatch> {
    let (mut file, mut field_id, mut lower, mut upper) = (vec![], vec![], vec![], vec![]);
    let (mut values, mut nulls, mut nans, mut size) = (vec![], vec![], vec![], vec![]);
    for (i, e) in files.iter().enumerate() {
        let f = &e.file;
        let ids: BTreeSet<i32> = (f.lower_bounds.keys())
            .chain(f.upper_bounds.keys())
            .chain(f.value_counts.keys())
            .chain(f.null_counts.keys())
            .chain(f.nan_counts.keys())
            .chain(f.column_sizes.keys())
            .copied()
            .collect();
        let i = u32::try_from(i).map_err(|_| FormatError::new("too many files for a head"))?;
        for id in ids {
            file.push(i);
            field_id.push(id);
            lower.push(f.lower_bounds.get(&id).map(Vec::as_slice));
            upper.push(f.upper_bounds.get(&id).map(Vec::as_slice));
            values.push(f.value_counts.get(&id).copied());
            nulls.push(f.null_counts.get(&id).copied());
            nans.push(f.nan_counts.get(&id).copied());
            size.push(f.column_sizes.get(&id).copied());
        }
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(file)),
        Arc::new(Int32Array::from(field_id)),
        Arc::new(lower.into_iter().collect::<BinaryArray>()),
        Arc::new(upper.into_iter().collect::<BinaryArray>()),
        Arc::new(UInt64Array::from(values)),
        Arc::new(UInt64Array::from(nulls)),
        Arc::new(UInt64Array::from(nans)),
        Arc::new(UInt64Array::from(size)),
    ];
    Ok(RecordBatch::try_new(columns_schema(), columns)?)
}

/// What a head holds.
#[derive(Clone, Debug)]
pub struct Contents {
    /// The table version (`vN.metadata.json`) the head describes.
    pub version: u64,
    /// That version's `vN.metadata.json`.
    pub metadata_json: String,
    /// The live data files, bounds in Iceberg's single-value encoding.
    pub files: Vec<Entry>,
    /// The last-value index, if it was inline, as the head stores it (align it to the schema).
    pub last_values: Option<RecordBatch>,
    /// Footers of data files, by path.
    pub footers: Vec<(String, Vec<u8>)>,
}

impl Contents {
    /// Decodes a head. `codec` gives the codec a head names (`None`: the reader doesn't have it,
    /// an error like a head of a layout it doesn't know).
    pub fn decode<C: Deref<Target = dyn HeadCodec>>(
        bytes: &[u8],
        codec: impl FnOnce(&str) -> Option<C>,
    ) -> Result<Self> {
        let header = Header::parse(bytes).ok_or_else(|| FormatError::new("not a Timelake head"))?;
        let codec =
            codec(header.codec).ok_or_else(|| FormatError::new(format!("unknown head codec {}", header.codec)))?;
        let mut doc = codec.decode(header.body)?;
        if doc.properties.get(LAYOUT_KEY).map(String::as_str) != Some(LAYOUT) {
            return Err(FormatError::new("unknown head layout"));
        }
        let json = column::<StringArray>(section(&doc, METADATA)?, "json")?;
        if json.len() != 1 {
            return Err(FormatError::new("head metadata must be one row"));
        }
        let footers = match doc.sections.get(FOOTERS) {
            Some(f) => {
                let (paths, bytes) = (column::<StringArray>(f, "path")?, column::<BinaryArray>(f, "footer")?);
                paths.iter().zip(bytes.iter()).filter_map(|(p, b)| Some((p?.to_string(), b?.to_vec()))).collect()
            }
            None => vec![],
        };
        Ok(Self {
            version: header.version,
            metadata_json: json.value(0).to_string(),
            files: decode_files(&doc)?,
            last_values: doc.sections.remove(LAST_VALUES),
            footers,
        })
    }
}

fn decode_files(doc: &HeadDoc) -> Result<Vec<Entry>> {
    let files = section(doc, FILES)?;
    let mut stats: Vec<DataFile> = (0..files.num_rows()).map(|_| DataFile::default()).collect();
    let columns = section(doc, COLUMNS)?;
    let (file, field_id) = (column::<UInt32Array>(columns, "file")?, column::<Int32Array>(columns, "field_id")?);
    let (lower, upper) = (column::<BinaryArray>(columns, "lower")?, column::<BinaryArray>(columns, "upper")?);
    let counts = [
        column::<UInt64Array>(columns, "values")?,
        column::<UInt64Array>(columns, "nulls")?,
        column::<UInt64Array>(columns, "nans")?,
        column::<UInt64Array>(columns, "size")?,
    ];
    for row in 0..columns.num_rows() {
        let f = usize::try_from(file.value(row))
            .ok()
            .and_then(|i| stats.get_mut(i))
            .ok_or_else(|| FormatError::new("head statistics for a missing file"))?;
        let id = field_id.value(row);
        if lower.is_valid(row) {
            f.lower_bounds.insert(id, lower.value(row).to_vec());
        }
        if upper.is_valid(row) {
            f.upper_bounds.insert(id, upper.value(row).to_vec());
        }
        let maps = [&mut f.value_counts, &mut f.null_counts, &mut f.nan_counts, &mut f.column_sizes];
        for (array, map) in counts.iter().zip(maps) {
            if array.is_valid(row) {
                map.insert(id, array.value(row));
            }
        }
    }
    let path = column::<StringArray>(files, "path")?;
    let size = column::<UInt64Array>(files, "size")?;
    let records = column::<UInt64Array>(files, "records")?;
    let partition = column::<Int32Array>(files, "partition")?;
    let sort_order_id = column::<Int32Array>(files, "sort_order_id")?;
    // Absent in heads written before it was added.
    let split_offsets =
        files.column_by_name("split_offsets").map(|_| column::<ListArray>(files, "split_offsets")).transpose()?;
    let added = column::<UInt8Array>(files, "added")?;
    let snapshot_id = column::<Int64Array>(files, "snapshot_id")?;
    let sequence_number = column::<Int64Array>(files, "sequence_number")?;
    let file_sequence_number = column::<Int64Array>(files, "file_sequence_number")?;
    stats
        .into_iter()
        .enumerate()
        .map(|(i, mut f)| {
            f.path = path.value(i).to_string();
            f.file_size = size.value(i);
            f.record_count = records.value(i);
            f.partition = partition.is_valid(i).then(|| partition.value(i));
            f.sort_order_id = sort_order_id.is_valid(i).then(|| sort_order_id.value(i));
            if let Some(offsets) = split_offsets.filter(|s| s.is_valid(i)) {
                let offsets = offsets.value(i);
                let offsets = offsets
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| FormatError::new("head split offsets"))?;
                f.split_offsets = Some(offsets.values().to_vec());
            }
            Ok(Entry {
                status: if added.value(i) == 1 { Status::Added } else { Status::Existing },
                snapshot_id: snapshot_id.value(i),
                sequence_number: sequence_number.value(i),
                file_sequence_number: file_sequence_number.value(i),
                file: Arc::new(f),
            })
        })
        .collect()
}

fn section<'a>(doc: &'a HeadDoc, name: &str) -> Result<&'a RecordBatch> {
    doc.sections.get(name).ok_or_else(|| FormatError::new(format!("head has no {name} section")))
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| FormatError::new(format!("head column {name} is missing or has the wrong type")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::head::{JsonCodec, built_in};

    fn entry(path: &str, partition: Option<i32>) -> Entry {
        Entry {
            status: Status::Added,
            snapshot_id: 7,
            sequence_number: 3,
            file_sequence_number: 4,
            file: Arc::new(DataFile {
                path: path.into(),
                partition,
                record_count: 10,
                file_size: 100,
                column_sizes: BTreeMap::from([(1, 40)]),
                value_counts: BTreeMap::from([(1, 10), (2, 10)]),
                null_counts: BTreeMap::from([(2, 1)]),
                lower_bounds: BTreeMap::from([(1, 5i64.to_le_bytes().to_vec())]),
                upper_bounds: BTreeMap::from([(1, 9i64.to_le_bytes().to_vec())]),
                split_offsets: Some(vec![4]),
                sort_order_id: Some(1),
                ..DataFile::default()
            }),
        }
    }

    /// Files, statistics, the index and footers come back as they went in, with every codec.
    #[test]
    fn heads_round_trip() {
        let files = vec![entry("a", Some(1)), Entry { status: Status::Existing, ..entry("b", None) }];
        let index = RecordBatch::try_from_iter([("v", Arc::new(Int64Array::from(vec![1])) as ArrayRef)]).unwrap();
        for codec in [built_in("arrow").unwrap(), &JsonCodec] {
            let mut head = HeadBuilder::new(5, codec, "{}".into(), &files, 1 << 20).unwrap();
            assert!(head.last_values(&index).unwrap());
            assert!(head.footer("a", b"footer"));
            let bytes = head.finish().unwrap();
            let read = Contents::decode(&bytes, built_in).unwrap();
            assert_eq!((read.version, read.metadata_json.as_str()), (5, "{}"));
            assert_eq!(read.files, files);
            assert_eq!(read.last_values, Some(index.clone()));
            assert_eq!(read.footers, [("a".to_string(), b"footer".to_vec())]);
        }
    }

    /// The index takes what it encodes to from the budget, and footers what they are.
    #[test]
    fn optional_sections_fit_the_budget() {
        let files = [entry("a", Some(1)), entry("b", Some(2))];
        let codec = built_in("arrow").unwrap();
        let index = RecordBatch::try_from_iter([("v", Arc::new(Int64Array::from(vec![1])) as ArrayRef)]).unwrap();
        let mut head = HeadBuilder::new(1, codec, "{}".into(), &files, 0).unwrap();
        assert!(!head.last_values(&index).unwrap());
        assert!(!head.footer("b", b"x"));
        let read = Contents::decode(&head.finish().unwrap(), built_in).unwrap();
        assert!(read.last_values.is_none() && read.footers.is_empty());

        let order: Vec<&str> = footer_order(&files).iter().map(|e| e.file.path.as_str()).collect();
        assert_eq!(order, ["b", "a"], "the newest partition first");
        let mut head = HeadBuilder::new(1, codec, "{}".into(), &files, 6).unwrap();
        assert!(head.footer("b", b"four"));
        assert!(!head.footer("a", b"four"));
        assert_eq!(Contents::decode(&head.finish().unwrap(), built_in).unwrap().footers.len(), 1);
    }

    #[test]
    fn unknown_codecs_and_layouts_are_errors() {
        let bytes = HeadBuilder::new(1, &JsonCodec, "{}".into(), &[], 0).unwrap().finish().unwrap();
        let none = |_: &str| None::<&'static dyn HeadCodec>;
        assert!(Contents::decode(&bytes, none).unwrap_err().to_string().contains("unknown head codec json"));
        let other = encode(1, &JsonCodec, &HeadDoc::default()).unwrap();
        assert!(Contents::decode(&other, built_in).unwrap_err().to_string().contains("layout"));
        assert!(Contents::decode(b"PAR1", built_in).is_err());
    }
}
