//! A table at one version, as the shared write path sees it: its metadata, schema and contract, and
//! (loaded on demand) its live files and last-value index. Loaded from the head when the head is
//! current, otherwise from the metadata chain; heads written here have the engine's layout, so
//! either reads the other's.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::error::{WriteError, WriteResult};
use crate::head::section::{COLUMNS, FILES, FOOTERS, LAST_VALUES, METADATA};
use crate::head::{FILE_NAME, HeadDoc, Header, LAYOUT, LAYOUT_KEY, MAX_HEADER, built_in, encode};
use crate::host::Host;
use crate::iceberg::{Content, DataFile, Entry, ManifestFile, Status, read_manifest, read_manifest_list};
use crate::metadata::Metadata;
use crate::rows::align_batch;
use crate::{Contract, summary};

/// A table at one committed version.
#[derive(Clone, Debug)]
pub struct TableState {
    /// The table's root URI.
    pub location: String,
    /// The metadata version (`vN.metadata.json`).
    pub version: u64,
    /// The metadata.
    pub metadata: Metadata,
    /// The current schema as Arrow, with field ids.
    pub schema: SchemaRef,
    /// The Timelake contract.
    pub contract: Contract,
    /// The live files, when known (from the head, or derived by a commit).
    pub files: Option<Vec<Entry>>,
    /// The last-value index, when known (inline in the head, or written by a commit).
    pub last_values: Option<RecordBatch>,
    /// Footers of data files (path → footer bytes), carried from head to head.
    pub footers: BTreeMap<String, Vec<u8>>,
}

/// `<location>/metadata/v<version>.metadata.json`.
pub fn metadata_uri(location: &str, version: u64) -> String {
    format!("{location}/metadata/v{version}.metadata.json")
}

/// `<location>/metadata/version-hint.text`.
pub fn hint_uri(location: &str) -> String {
    format!("{location}/metadata/version-hint.text")
}

/// `<location>/metadata/timelake-head`.
pub fn head_uri(location: &str) -> String {
    format!("{location}/metadata/{FILE_NAME}")
}

fn metadata_version(uri: &str) -> Option<u64> {
    uri.rsplit('/').next()?.strip_prefix('v')?.strip_suffix(".metadata.json")?.parse().ok()
}

impl TableState {
    /// The current version of the table at `location` (its root URI, without a trailing slash).
    pub async fn load<H: Host>(host: &H, location: &str) -> WriteResult<Self> {
        let prefix = format!("{location}/metadata/v");
        let (head, listed) = futures_util::future::join(host.get(&head_uri(location)), host.list(&prefix)).await;
        let newest = listed?.iter().filter_map(|o| metadata_version(&o.uri)).max();
        let Some(newest) = newest else { return Err(WriteError::NotFound(location.to_string())) };
        if let Some(state) = head?.and_then(|bytes| decode_head(location, &bytes).ok())
            && state.version >= newest
        {
            return Ok(state);
        }
        Self::load_version(host, location, newest).await
    }

    /// Version `version` from its metadata file (without the head).
    pub async fn load_version<H: Host>(host: &H, location: &str, version: u64) -> WriteResult<Self> {
        let uri = metadata_uri(location, version);
        let bytes = host.get(&uri).await?.ok_or_else(|| WriteError::NotFound(uri.clone()))?;
        Self::from_metadata(location, version, Metadata::parse(&bytes)?)
    }

    fn from_metadata(location: &str, version: u64, metadata: Metadata) -> WriteResult<Self> {
        let contract = metadata.contract()?;
        contract.check_writable().map_err(|e| WriteError::invalid(format!("{location}: {e}")))?;
        Ok(Self {
            location: location.to_string(),
            version,
            schema: metadata.arrow_schema()?,
            metadata,
            contract,
            files: None,
            last_values: None,
            footers: BTreeMap::new(),
        })
    }

    /// The current snapshot's manifests. A table with delete files (row-level deletes another
    /// engine wrote) is an error: the shared write path would carry them over as data, or
    /// compact deleted rows back in.
    pub async fn manifests<H: Host>(&self, host: &H) -> WriteResult<Vec<ManifestFile>> {
        let Some(list) = self.metadata.manifest_list() else { return Ok(vec![]) };
        let bytes = host.get(list).await?.ok_or_else(|| WriteError::NotFound(list.to_string()))?;
        let manifests = read_manifest_list(&bytes)?;
        if let Some(m) = manifests.iter().find(|m| m.content == Content::Deletes) {
            return Err(WriteError::invalid(format!(
                "{}: the table has delete files ({}), which Timelake doesn't apply; compact them away with the engine \
                 that wrote them first",
                self.location, m.path
            )));
        }
        Ok(manifests)
    }

    /// The entries of `manifest`.
    pub async fn manifest<H: Host>(host: &H, manifest: &ManifestFile) -> WriteResult<Vec<Entry>> {
        let bytes = host.get(&manifest.path).await?.ok_or_else(|| WriteError::NotFound(manifest.path.clone()))?;
        read_manifest(manifest, &bytes)
    }

    /// The live files, read from the manifests unless known.
    pub async fn live_files<H: Host>(&mut self, host: &H) -> WriteResult<&[Entry]> {
        if self.files.is_none() {
            let mut files = vec![];
            for m in self.manifests(host).await? {
                let entries = Self::manifest(host, &m).await?;
                files.extend(entries.into_iter().filter(|e| e.status != Status::Deleted));
            }
            self.files = Some(files);
        }
        Ok(self.files.as_deref().unwrap_or_default())
    }

    /// The last-value index location recorded in the current snapshot.
    pub fn last_value_index(&self) -> Option<String> {
        self.metadata.summary().get(summary::LAST_VALUE_INDEX).cloned()
    }

    /// The last-value index, read unless known.
    pub async fn last_values<H: Host>(&mut self, host: &H) -> WriteResult<Option<RecordBatch>> {
        if self.last_values.is_none()
            && let Some(uri) = self.last_value_index()
        {
            let bytes = host.get(&uri).await?.ok_or_else(|| WriteError::NotFound(uri.clone()))?;
            self.last_values = Some(decode_index(&bytes, &self.schema)?);
        }
        Ok(self.last_values.clone())
    }

    /// Writes the head of this version unless the stored one is as new. Two writers racing can
    /// leave the older head; readers notice it is behind the listing and skip it.
    pub async fn write_head<H: Host>(&self, host: &H) -> WriteResult<()> {
        let uri = head_uri(&self.location);
        let Some(codec) = built_in(&self.contract.head_format) else {
            // Turned off, or a codec the shared writer doesn't have: leave no stale head behind.
            return host.delete(&[uri]).await;
        };
        if let Ok(prefix) = host.get_range(&uri, 0, MAX_HEADER).await
            && Header::parse(&prefix).is_some_and(|h| h.version >= self.version)
        {
            return Ok(());
        }
        let Some(files) = &self.files else { return Ok(()) };
        let mut doc = head_doc(&self.metadata, files)?;
        let mut bytes = encode(self.version, codec, &doc)?;
        let mut budget = self.contract.head_inline_bytes;
        if let Some(index) = &self.last_values {
            doc.sections.insert(LAST_VALUES.into(), index.clone());
            let with_index = encode(self.version, codec, &doc)?;
            let cost = (with_index.len() - bytes.len()) as u64;
            if cost <= budget {
                bytes = with_index;
                budget -= cost;
            } else {
                doc.sections.remove(LAST_VALUES);
            }
        }
        let footers = footers_within(files, &self.footers, budget)?;
        if footers.num_rows() > 0 {
            doc.sections.insert(FOOTERS.into(), footers);
            bytes = encode(self.version, codec, &doc)?;
        }
        host.put(&uri, bytes).await
    }
}

/// The last-value index as Parquet (ZSTD), as the engine writes it.
pub fn encode_index(batch: &RecordBatch) -> WriteResult<Vec<u8>> {
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?)).build();
    let mut buf = vec![];
    let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    w.write(batch)?;
    w.close()?;
    Ok(buf)
}

/// A last-value index file, aligned to `schema`.
pub fn decode_index(bytes: &[u8], schema: &SchemaRef) -> WriteResult<RecordBatch> {
    let batches = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?
        .build()?
        .map(|b| align_batch(&b?, schema))
        .collect::<WriteResult<Vec<_>>>()?;
    Ok(concat_batches(schema, &batches)?)
}

// --- the head --------------------------------------------------------------------------------

fn files_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("records", DataType::UInt64, false),
        Field::new("partition", DataType::Int32, true),
        Field::new("sort_order_id", DataType::Int32, true),
        Field::new("split_offsets", DataType::List(Arc::new(Field::new("item", DataType::Int64, true))), true),
        Field::new("added", DataType::UInt8, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("sequence_number", DataType::Int64, false),
        Field::new("file_sequence_number", DataType::Int64, false),
    ]))
}

fn columns_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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

fn head_doc(metadata: &Metadata, files: &[Entry]) -> WriteResult<HeadDoc> {
    let mut doc = HeadDoc::default();
    doc.properties.insert(LAYOUT_KEY.into(), LAYOUT.into());
    let json = Schema::new(vec![Field::new("json", DataType::Utf8, false)]);
    doc.sections.insert(
        METADATA.into(),
        RecordBatch::try_new(Arc::new(json), vec![Arc::new(StringArray::from(vec![metadata.to_json()]))])?,
    );
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
    doc.sections.insert(FILES.into(), RecordBatch::try_new(files_schema(), columns)?);
    doc.sections.insert(COLUMNS.into(), encode_columns(files)?);
    Ok(doc)
}

fn encode_columns(files: &[Entry]) -> WriteResult<RecordBatch> {
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
        let i = u32::try_from(i).map_err(|_| WriteError::invalid("too many files for a head"))?;
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

/// Footers of `files` this writer has, newest partitions first, while they fit in `budget` bytes.
fn footers_within(files: &[Entry], known: &BTreeMap<String, Vec<u8>>, mut budget: u64) -> WriteResult<RecordBatch> {
    let mut files: Vec<&Entry> = files.iter().filter(|e| known.contains_key(&e.file.path)).collect();
    files.sort_by_key(|e| std::cmp::Reverse((e.file.partition, e.file.path.as_str())));
    let (mut paths, mut footers) = (vec![], vec![]);
    for e in files {
        let bytes = &known[&e.file.path];
        let Some(rest) = budget.checked_sub(bytes.len() as u64) else { break };
        budget = rest;
        paths.push(e.file.path.as_str());
        footers.push(bytes.as_slice());
    }
    let columns: Vec<ArrayRef> =
        vec![Arc::new(StringArray::from(paths)), Arc::new(footers.into_iter().map(Some).collect::<BinaryArray>())];
    Ok(RecordBatch::try_new(footers_schema(), columns)?)
}

fn decode_head(location: &str, bytes: &[u8]) -> WriteResult<TableState> {
    let header = Header::parse(bytes).ok_or_else(|| WriteError::Corrupt("not a Timelake head".into()))?;
    let codec = built_in(header.codec).ok_or_else(|| WriteError::Corrupt(format!("head codec {}", header.codec)))?;
    let doc = codec.decode(header.body)?;
    if doc.properties.get(LAYOUT_KEY).map(String::as_str) != Some(LAYOUT) {
        return Err(WriteError::Corrupt("unknown head layout".into()));
    }
    let json = column::<StringArray>(section(&doc, METADATA)?, "json")?;
    if json.len() != 1 {
        return Err(WriteError::Corrupt("head metadata must be one row".into()));
    }
    let mut state = TableState::from_metadata(location, header.version, Metadata::parse(json.value(0).as_bytes())?)?;
    state.files = Some(decode_files(&doc)?);
    if let (Some(_), Some(index)) = (state.last_value_index(), doc.sections.get(LAST_VALUES)) {
        state.last_values = Some(align_batch(index, &state.schema)?);
    }
    if let Some(footers) = doc.sections.get(FOOTERS) {
        let (paths, bytes) = (column::<StringArray>(footers, "path")?, column::<BinaryArray>(footers, "footer")?);
        for (path, bytes) in paths.iter().zip(bytes.iter()) {
            if let (Some(path), Some(bytes)) = (path, bytes) {
                state.footers.insert(path.to_string(), bytes.to_vec());
            }
        }
    }
    Ok(state)
}

fn decode_files(doc: &HeadDoc) -> WriteResult<Vec<Entry>> {
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
            .ok_or_else(|| WriteError::Corrupt("head statistics for a missing file".into()))?;
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
                    .ok_or_else(|| WriteError::Corrupt("head split offsets".into()))?;
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

fn section<'a>(doc: &'a HeadDoc, name: &str) -> WriteResult<&'a RecordBatch> {
    doc.sections.get(name).ok_or_else(|| WriteError::Corrupt(format!("head has no {name} section")))
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> WriteResult<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| WriteError::Corrupt(format!("head column {name} is missing or has the wrong type")))
}
