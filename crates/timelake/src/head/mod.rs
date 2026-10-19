//! The head object (spec 7, P1): `<table>/metadata/timelake-head`, one object holding what a
//! reader needs to plan a query at the current version: the table metadata, the live data files
//! with their column statistics, and the last-value index when it fits. A cold query reads it
//! instead of following the version hint, metadata, manifest list and manifests one by one.
//!
//! The head is a cache of a commit, not part of it. `vN.metadata.json` stays the commit point, so
//! other Iceberg engines see no change. The head is written in the background after each commit
//! and never replaces a newer one; readers check that `vN+1.metadata.json` doesn't exist before
//! they rely on it, and load the table the slow way when it does.
//!
//! Layout: a header (`TLHD`, header version, table version, codec name) followed by the codec's
//! encoding of a [`HeadDoc`] with these sections:
//! - `metadata`: one row, column `json`: the table's `vN.metadata.json`.
//! - `files`: one row per live data file.
//! - `columns`: one row per file and column with statistics: bounds (Iceberg single-value
//!   encoding) and counts.
//! - `last_values`: the last-value index, while it fits in `timelake.head-inline-bytes`.
//! - `footers`: Parquet footers of the newest files, in what is left of that budget (P2).

mod codec;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use bytes::Bytes;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Datum, ManifestStatus, TableMetadata};

pub(crate) use self::codec::Codecs;
use crate::background;
use crate::error::{Error, Result};
use crate::lake::Lake;
use crate::name::TableName;
use crate::schema::align_batch;
use crate::storage::{Storage, VersionedWrite};
use crate::table::{LiveEntry, Table};
use crate::write::{file_partition, partition_struct};
use crate::{footer, lvi};
pub use timelake_format::FormatError;
use timelake_format::head::section::{COLUMNS, FILES, FOOTERS, LAST_VALUES, METADATA};
pub use timelake_format::head::{ArrowCodec, HeadCodec, HeadDoc, JsonCodec};
use timelake_format::head::{FILE_NAME, Header, LAYOUT, LAYOUT_KEY, MAX_HEADER, encode};

/// Attempts to replace a head that other writers keep changing; they write newer ones anyway.
const WRITE_ATTEMPTS: usize = 5;
/// Footers read from the store at once while writing a head.
const FOOTER_READS: usize = 16;

/// Location of a table's head.
pub(crate) fn uri(location: &str) -> String {
    format!("{location}/metadata/{FILE_NAME}")
}

/// Writes `table`'s head in the background. Failures are ignored: readers fall back to the
/// metadata chain, and the next commit writes a new head.
pub(crate) fn update(table: &Table) {
    let table = table.clone();
    background::spawn(async move {
        let _ = write(&table).await;
    });
}

/// The table version the stored head describes, if there is a head this process can parse.
pub(crate) async fn stored_version(table: &Table) -> Result<Option<u64>> {
    let prefix = table.lake().storage().read_prefix_versioned(&uri(table.location()), MAX_HEADER).await?;
    Ok(prefix.and_then(|(bytes, _)| Header::parse(&bytes).map(|h| h.version)))
}

/// Writes `table`'s head now (see [`update`] for the background version).
pub(crate) async fn write(table: &Table) -> Result<()> {
    let lake = table.lake();
    if lake.has_newer(table.name(), table.version()) {
        return Ok(());
    }
    let storage = lake.storage();
    let uri = uri(table.location());
    let contract = table.contract();
    let Some(codec) = lake.head_codecs().get(&contract.head_format) else {
        // Turned off, or a codec this process doesn't have: leave no head behind that could go stale.
        return storage.delete(&uri).await;
    };
    let files = table.files().await?;
    let mut doc = to_doc(table, &files)?;
    let mut bytes = encode(table.version(), codec.as_ref(), &doc)?;
    // Optional sections share one budget: the last-value index first, then footers.
    let mut budget = contract.head_inline_bytes;
    if let Some(index) = table.last_value_index() {
        doc.sections.insert(LAST_VALUES.into(), lvi::read(table, &index).await?);
        let with_index = encode(table.version(), codec.as_ref(), &doc)?;
        let cost = (with_index.len() - bytes.len()) as u64;
        if cost <= budget {
            bytes = with_index;
            budget -= cost;
        } else {
            doc.sections.remove(LAST_VALUES);
        }
    }
    let footers = footers_within(lake, &files, budget).await?;
    if footers.num_rows() > 0 {
        doc.sections.insert(FOOTERS.into(), footers);
        bytes = encode(table.version(), codec.as_ref(), &doc)?;
    }
    write_if_newer(storage, &uri, table.version(), Bytes::from(bytes)).await
}

fn footers_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("footer", DataType::Binary, false),
    ]))
}

/// Footers of `files`, newest partitions first, while their encoded size fits in `budget` bytes.
/// Footers this process doesn't have yet are read from the store (by this background task).
async fn footers_within(lake: &Lake, files: &[LiveEntry], mut budget: u64) -> Result<RecordBatch> {
    let mut files: Vec<&LiveEntry> = files.iter().collect();
    files.sort_by_key(|e| std::cmp::Reverse((file_partition(&e.data_file), e.data_file.file_path())));
    let (mut paths, mut footers) = (vec![], vec![]);
    'chunks: for chunk in files.chunks(FOOTER_READS) {
        let reads = chunk.iter().map(|e| footer::read(lake, e.data_file.file_path(), e.data_file.file_size_in_bytes()));
        for (e, bytes) in chunk.iter().zip(futures::future::try_join_all(reads).await?) {
            let Some(rest) = budget.checked_sub(bytes.len() as u64) else { break 'chunks };
            budget = rest;
            paths.push(e.data_file.file_path());
            footers.push(bytes);
        }
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(paths)),
        Arc::new(footers.iter().map(|b| Some(b.as_ref())).collect::<BinaryArray>()),
    ];
    Ok(RecordBatch::try_new(footers_schema(), columns)?)
}

/// Writes a head unless the stored one is for `version` or newer. The write is conditional on the
/// stored head being unchanged since it was read, so two writers can't move it backwards.
async fn write_if_newer(storage: &Storage, uri: &str, version: u64, bytes: Bytes) -> Result<()> {
    for _ in 0..WRITE_ATTEMPTS {
        let expected = match storage.read_prefix_versioned(uri, MAX_HEADER).await? {
            Some((prefix, _)) if Header::parse(&prefix).is_some_and(|h| h.version >= version) => return Ok(()),
            Some((_, stored)) => Some(stored),
            None => None,
        };
        if storage.write_versioned(uri, bytes.clone(), expected).await? == VersionedWrite::Written {
            break;
        }
    }
    Ok(())
}

/// Loads a table from its head, without checking for newer versions. `None` if the table has no
/// head, or one this process can't decode (an unknown codec or layout).
pub(crate) async fn read(lake: &Lake, name: &TableName, location: &str) -> Result<Option<Table>> {
    let Some(bytes) = lake.storage().read_opt(&uri(location)).await? else { return Ok(None) };
    Ok(decode(lake, name, location, &bytes).ok())
}

fn decode(lake: &Lake, name: &TableName, location: &str, bytes: &[u8]) -> Result<Table> {
    let header = Header::parse(bytes).ok_or_else(|| Error::invalid("not a Timelake head"))?;
    let codec = lake
        .head_codecs()
        .get(header.codec)
        .ok_or_else(|| Error::invalid(format!("no head codec {}", header.codec)))?;
    let doc = codec.decode(header.body)?;
    if doc.properties.get(LAYOUT_KEY).map(String::as_str) != Some(LAYOUT) {
        return Err(Error::invalid("unknown head layout"));
    }
    let json = column::<StringArray>(section(&doc, METADATA)?, "json")?;
    if json.len() != 1 {
        return Err(Error::invalid("head metadata must be one row"));
    }
    let metadata: TableMetadata = serde_json::from_str(json.value(0))?;
    let table = Table::new(lake.clone(), name.clone(), location.to_string(), header.version, metadata)?;
    let files = decode_files(&doc, table.metadata())?;
    if let (Some(uri), Some(index)) = (table.last_value_index(), doc.sections.get(LAST_VALUES)) {
        lake.caches().last_value_indexes.insert(uri, align_batch(index, &table.schema())?);
    }
    if let Some(footers) = doc.sections.get(FOOTERS) {
        let (paths, bytes) = (column::<StringArray>(footers, "path")?, column::<BinaryArray>(footers, "footer")?);
        for (path, bytes) in paths.iter().zip(bytes.iter()) {
            if let (Some(path), Some(bytes)) = (path, bytes) {
                lake.caches().footers.insert(path.to_string(), Bytes::copy_from_slice(bytes));
            }
        }
    }
    Ok(table.with_files(Arc::new(files)))
}

// --- sections ------------------------------------------------------------------------------------

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

/// The head of `table` with `files` as its live files (without the optional sections).
fn to_doc(table: &Table, files: &[LiveEntry]) -> Result<HeadDoc> {
    let mut doc = HeadDoc::default();
    doc.properties.insert(LAYOUT_KEY.into(), LAYOUT.into());
    let json = serde_json::to_string(table.metadata())?;
    let metadata = Schema::new(vec![Field::new("json", DataType::Utf8, false)]);
    doc.sections.insert(
        METADATA.into(),
        RecordBatch::try_new(Arc::new(metadata), vec![Arc::new(StringArray::from(vec![json]))])?,
    );
    doc.sections.insert(FILES.into(), encode_files(files)?);
    doc.sections.insert(COLUMNS.into(), encode_columns(files)?);
    Ok(doc)
}

fn encode_files(files: &[LiveEntry]) -> Result<RecordBatch> {
    let df = || files.iter().map(|e| &e.data_file);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(df().map(|f| f.file_path()))),
        Arc::new(UInt64Array::from_iter_values(df().map(|f| f.file_size_in_bytes()))),
        Arc::new(UInt64Array::from_iter_values(df().map(|f| f.record_count()))),
        Arc::new(df().map(|f| file_partition(f)).collect::<Int32Array>()),
        Arc::new(df().map(|f| f.sort_order_id()).collect::<Int32Array>()),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            df().map(|f| f.split_offsets().map(|o| o.iter().copied().map(Some).collect::<Vec<_>>())),
        )),
        Arc::new(UInt8Array::from_iter_values(files.iter().map(|e| u8::from(e.status == ManifestStatus::Added)))),
        Arc::new(Int64Array::from_iter_values(files.iter().map(|e| e.snapshot_id))),
        Arc::new(Int64Array::from_iter_values(files.iter().map(|e| e.sequence_number))),
        Arc::new(Int64Array::from_iter_values(files.iter().map(|e| e.file_sequence_number))),
    ];
    Ok(RecordBatch::try_new(files_schema(), columns)?)
}

fn encode_columns(files: &[LiveEntry]) -> Result<RecordBatch> {
    let (mut file, mut field_id, mut lower, mut upper) = (vec![], vec![], vec![], vec![]);
    let (mut values, mut nulls, mut nans, mut size) = (vec![], vec![], vec![], vec![]);
    let bytes = |d: Option<&Datum>| d.map(|d| d.to_bytes().map(|b| b.to_vec())).transpose();
    for (i, e) in files.iter().enumerate() {
        let f = &e.data_file;
        let ids: BTreeSet<i32> = (f.lower_bounds().keys())
            .chain(f.upper_bounds().keys())
            .chain(f.value_counts().keys())
            .chain(f.null_value_counts().keys())
            .chain(f.nan_value_counts().keys())
            .chain(f.column_sizes().keys())
            .copied()
            .collect();
        let i = u32::try_from(i).map_err(|_| Error::invalid("too many files for a head"))?;
        for id in ids {
            file.push(i);
            field_id.push(id);
            lower.push(bytes(f.lower_bounds().get(&id))?);
            upper.push(bytes(f.upper_bounds().get(&id))?);
            values.push(f.value_counts().get(&id).copied());
            nulls.push(f.null_value_counts().get(&id).copied());
            nans.push(f.nan_value_counts().get(&id).copied());
            size.push(f.column_sizes().get(&id).copied());
        }
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(file)),
        Arc::new(Int32Array::from(field_id)),
        Arc::new(lower.iter().map(Option::as_deref).collect::<BinaryArray>()),
        Arc::new(upper.iter().map(Option::as_deref).collect::<BinaryArray>()),
        Arc::new(UInt64Array::from(values)),
        Arc::new(UInt64Array::from(nulls)),
        Arc::new(UInt64Array::from(nans)),
        Arc::new(UInt64Array::from(size)),
    ];
    Ok(RecordBatch::try_new(columns_schema(), columns)?)
}

/// Statistics of one file, as Iceberg's `DataFile` keeps them.
#[derive(Default)]
struct FileStats {
    lower: HashMap<i32, Datum>,
    upper: HashMap<i32, Datum>,
    values: HashMap<i32, u64>,
    nulls: HashMap<i32, u64>,
    nans: HashMap<i32, u64>,
    size: HashMap<i32, u64>,
}

fn decode_files(doc: &HeadDoc, metadata: &TableMetadata) -> Result<Vec<LiveEntry>> {
    let files = section(doc, FILES)?;
    let mut stats: Vec<FileStats> = (0..files.num_rows()).map(|_| FileStats::default()).collect();
    let columns = section(doc, COLUMNS)?;
    let (file, field_id) = (column::<UInt32Array>(columns, "file")?, column::<Int32Array>(columns, "field_id")?);
    let (lower, upper) = (column::<BinaryArray>(columns, "lower")?, column::<BinaryArray>(columns, "upper")?);
    let values = column::<UInt64Array>(columns, "values")?;
    let nulls = column::<UInt64Array>(columns, "nulls")?;
    let nans = column::<UInt64Array>(columns, "nans")?;
    let size = column::<UInt64Array>(columns, "size")?;
    let schema = metadata.current_schema();
    for row in 0..columns.num_rows() {
        let s = usize::try_from(file.value(row))
            .ok()
            .and_then(|i| stats.get_mut(i))
            .ok_or_else(|| Error::invalid("head statistics for a missing file"))?;
        let id = field_id.value(row);
        // Bounds of columns no longer in the schema can't be decoded, and aren't needed.
        if let Some(ty) = schema.field_by_id(id).and_then(|f| f.field_type.as_primitive_type()) {
            let datum = |a: &BinaryArray| a.is_valid(row).then(|| Datum::try_from_bytes(a.value(row), ty.clone()));
            if let Some(d) = datum(lower).transpose()? {
                s.lower.insert(id, d);
            }
            if let Some(d) = datum(upper).transpose()? {
                s.upper.insert(id, d);
            }
        }
        for (array, map) in [(values, &mut s.values), (nulls, &mut s.nulls), (nans, &mut s.nans), (size, &mut s.size)] {
            if array.is_valid(row) {
                map.insert(id, array.value(row));
            }
        }
    }

    let path = column::<StringArray>(files, "path")?;
    let bytes = column::<UInt64Array>(files, "size")?;
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
    let spec_id = metadata.default_partition_spec_id();
    stats
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            let mut b = DataFileBuilder::default();
            b.content(DataContentType::Data)
                .file_path(path.value(i).to_string())
                .file_format(DataFileFormat::Parquet)
                .partition(partition_struct(partition.is_valid(i).then(|| partition.value(i))))
                .partition_spec_id(spec_id)
                .record_count(records.value(i))
                .file_size_in_bytes(bytes.value(i))
                .lower_bounds(s.lower)
                .upper_bounds(s.upper)
                .value_counts(s.values)
                .null_value_counts(s.nulls)
                .nan_value_counts(s.nans)
                .column_sizes(s.size);
            if sort_order_id.is_valid(i) {
                b.sort_order_id(sort_order_id.value(i));
            }
            if let Some(split_offsets) = split_offsets.filter(|s| s.is_valid(i)) {
                let offsets = split_offsets.value(i);
                let offsets =
                    offsets.as_any().downcast_ref::<Int64Array>().ok_or_else(|| Error::invalid("bad split offsets"))?;
                b.split_offsets(Some(offsets.values().to_vec()));
            }
            let data_file = b.build().map_err(|e| {
                Error::from(iceberg::Error::new(iceberg::ErrorKind::Unexpected, "building data file").with_source(e))
            })?;
            Ok(LiveEntry {
                status: if added.value(i) == 1 { ManifestStatus::Added } else { ManifestStatus::Existing },
                data_file: Arc::new(data_file),
                snapshot_id: snapshot_id.value(i),
                sequence_number: sequence_number.value(i),
                file_sequence_number: file_sequence_number.value(i),
            })
        })
        .collect()
}

fn section<'a>(doc: &'a HeadDoc, name: &str) -> Result<&'a RecordBatch> {
    doc.sections.get(name).ok_or_else(|| Error::invalid(format!("head has no {name} section")))
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| Error::invalid(format!("head column {name} is missing or has the wrong type")))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::{FaultyStore, Op, T0, batch, faulty_lake, opts, reopen, reopen_hint_lookup, schema};
    use crate::{AppendOptions, Session, TableOptions, property};

    fn no_maintenance() -> AppendOptions {
        AppendOptions { maintain: Some(false), ..Default::default() }
    }

    fn with_properties(kv: &[(&str, &str)]) -> TableOptions {
        let mut o = opts();
        o.properties = kv.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
        o
    }

    async fn stored_header(lake: &Lake, table: &Table) -> Option<(u64, String)> {
        let bytes = lake.storage().read_opt(&uri(table.location())).await.unwrap()?;
        let h = Header::parse(&bytes).unwrap();
        Some((h.version, h.codec.to_string()))
    }

    fn paths(files: &[LiveEntry]) -> Vec<String> {
        let mut v: Vec<String> = files.iter().map(|e| e.data_file.file_path().to_string()).collect();
        v.sort();
        v
    }

    /// A table loaded from its head has the same version, files, statistics and last-value index
    /// as one loaded through the manifests.
    #[tokio::test]
    async fn head_describes_the_table() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0), (T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0 + 86_400_000_000, 1, 3.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, Some((3, "arrow".into())));

        let cold = reopen(&store);
        let from_head = cold.table("iot.r").await.unwrap();
        assert_eq!(from_head.version(), 3);
        let files = from_head.known_files().expect("files from the head").to_vec();
        // The same version read through its manifests.
        let metadata = t.metadata().clone();
        let manifests = Table::new(lake.clone(), t.name().clone(), t.location().to_string(), 3, metadata).unwrap();
        let mut expected = manifests.files().await.unwrap();
        expected.sort_by(|a, b| a.data_file.file_path().cmp(b.data_file.file_path()));
        let mut got = files.clone();
        got.sort_by(|a, b| a.data_file.file_path().cmp(b.data_file.file_path()));
        assert_eq!(paths(&got), paths(&expected));
        for (g, e) in got.iter().zip(&expected) {
            assert_eq!(g.data_file.as_ref(), e.data_file.as_ref());
            assert_eq!((g.snapshot_id, g.sequence_number), (e.snapshot_id, e.sequence_number));
        }
        let index = from_head.last_value_index().unwrap();
        assert!(cold.caches().last_value_indexes.get(&index).is_some(), "index inline");
    }

    /// Round trips a cold query waits for, one after another, on a store where each request
    /// takes 100 ms of (paused) time: `None` runs `latest`, `Some(sql)` a query. The memory store
    /// stands in for object storage, so filter pushdown is off as `auto` would have it there.
    async fn cold_round_trips(head_format: &str, sql: Option<&str>) -> u128 {
        cold_round_trips_with(&[(property::HEAD_FORMAT, head_format)], sql, reopen).await
    }

    async fn cold_round_trips_with(
        properties: &[(&str, &str)],
        sql: Option<&str>,
        open: fn(&Arc<FaultyStore>) -> Lake,
    ) -> u128 {
        const RTT: Duration = Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let o = with_properties(&[&[(property::FILTER_PUSHDOWN, "false")], properties].concat());
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        t.append(&[batch(&[(T0, 1, 1.0), (T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;

        store.set_latency(RTT);
        let session = Session::new(open(&store));
        let started = tokio::time::Instant::now();
        let df = match sql {
            None => session.latest("iot.r").await.unwrap(),
            Some(sql) => session.sql(sql).await.unwrap(),
        };
        assert_ne!(df.collect().await.unwrap().len(), 0);
        started.elapsed().as_millis() / RTT.as_millis()
    }

    /// With the head, a cold `latest` reads the head and checks for a newer version; a filtered
    /// scan also reads footers (alongside the check) and data. Without it, the version hint,
    /// metadata, manifest list and manifests come first.
    #[tokio::test(start_paused = true)]
    async fn cold_queries_read_the_head_first() {
        let scan = Some("SELECT value FROM iot.r WHERE device = 2");
        assert_eq!(cold_round_trips("arrow", None).await, 1);
        assert_eq!(cold_round_trips("arrow", scan).await, 2);
        // `timelake_version_lookup=hint`: the version hint instead of a listing, then the check.
        assert_eq!(cold_round_trips_with(&[], None, reopen_hint_lookup).await, 2);
        assert_eq!(cold_round_trips_with(&[], scan, reopen_hint_lookup).await, 3);
        assert_eq!(cold_round_trips("none", None).await, 3);
        assert_eq!(cold_round_trips("none", scan).await, 6);
        // Filter pushdown reads the filter column, then the others.
        let pushdown = [(property::FILTER_PUSHDOWN, "true")];
        assert_eq!(cold_round_trips_with(&pushdown, scan, reopen).await, 3);
    }

    /// A head that is behind (its write failed, and so did the version hint's) is noticed, and
    /// the newest version is read.
    #[tokio::test]
    async fn readers_notice_a_stale_head() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        store.fail(Op::Put, "timelake-head");
        store.fail(Op::Put, "version-hint");
        let (t, _) = t.append(&[batch(&[(T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await.unwrap().0, 2);

        assert_eq!(reopen(&store).table("iot.r").await.unwrap().version(), 3);
        let session = Session::new(reopen(&store));
        // Rows only in version 3: one in total, with device 2.
        for (sql, expected) in [("SELECT count(*) FROM iot.r", 2), ("SELECT count(*) FROM iot.r WHERE device = 2", 1)] {
            let rows = session.sql(sql).await.unwrap().collect().await.unwrap();
            let n = rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
            assert_eq!(n, expected, "{sql}");
        }
        let latest = session.latest("iot.r").await.unwrap().collect().await.unwrap();
        assert_eq!(latest.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }

    /// A reader that already has a version confirms it with one request (no head download), and
    /// sees a commit made by another process.
    #[tokio::test(start_paused = true)]
    async fn warm_queries_check_for_newer_versions() {
        const RTT: Duration = Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let session = Session::new(reopen(&store));
        let count = || async {
            let rows = session.sql("SELECT count(*) FROM iot.r").await.unwrap().collect().await.unwrap();
            rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
        };
        assert_eq!(count().await, 1);

        // Past the planning cache's second (wall-clock time).
        std::thread::sleep(Duration::from_millis(1100));
        store.set_latency(RTT);
        let started = tokio::time::Instant::now();
        session.latest("iot.r").await.unwrap().collect().await.unwrap();
        assert_eq!(started.elapsed().as_millis() / RTT.as_millis(), 1);

        store.set_latency(Duration::ZERO);
        t.append(&[batch(&[(T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(count().await, 2);
    }

    /// Within `version-check-interval` a reader uses its version without a request, and doesn't
    /// see other processes' commits; a session's value overrides the table's.
    #[tokio::test(start_paused = true)]
    async fn version_check_interval_skips_the_check() {
        const RTT: Duration = Duration::from_millis(100);
        let (lake, store) = faulty_lake();
        let o = with_properties(&[(property::VERSION_CHECK_INTERVAL, "1h")]);
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let session_with = |interval: Option<&str>| {
            let mut config = Session::config();
            if let Some(v) = interval {
                config.options_mut().set("timelake.version_check_interval", v).unwrap();
            }
            Session::with_config(reopen(&store), config)
        };
        let count = |session: &Session| {
            let session = session.clone();
            async move {
                let rows = session.sql("SELECT count(*) FROM iot.r").await.unwrap().collect().await.unwrap();
                rows[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
            }
        };
        let (trusting, checking) = (session_with(None), session_with(Some("0s")));
        assert_eq!((count(&trusting).await, count(&checking).await), (1, 1));

        // Past the default second (wall-clock time): only the session with `0s` checks.
        std::thread::sleep(Duration::from_millis(1100));
        store.set_latency(RTT);
        let round_trips = |session: Session| async move {
            let started = tokio::time::Instant::now();
            session.latest("iot.r").await.unwrap().collect().await.unwrap();
            started.elapsed().as_millis() / RTT.as_millis()
        };
        assert_eq!(round_trips(trusting.clone()).await, 0);
        assert_eq!(round_trips(checking.clone()).await, 1);

        store.set_latency(Duration::ZERO);
        t.append(&[batch(&[(T0 + 1, 2, 2.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!((count(&trusting).await, count(&checking).await), (1, 2));
    }

    /// Writers that finish out of order never move the head back.
    #[tokio::test]
    async fn head_never_moves_back() {
        let (lake, _) = faulty_lake();
        let storage = lake.storage();
        let uri = "memory://test/wh/h";
        let head = |v: u64| Bytes::from(encode(v, &JsonCodec, &HeadDoc::default()).unwrap());
        write_if_newer(storage, uri, 5, head(5)).await.unwrap();
        write_if_newer(storage, uri, 4, head(4)).await.unwrap();
        assert_eq!(Header::parse(&storage.read(uri).await.unwrap()).unwrap().version, 5);
        write_if_newer(storage, uri, 6, head(6)).await.unwrap();
        assert_eq!(Header::parse(&storage.read(uri).await.unwrap()).unwrap().version, 6);
    }

    #[tokio::test]
    async fn format_is_a_table_property() {
        let (lake, store) = faulty_lake();
        let t =
            lake.create_table("iot.j", &schema(), with_properties(&[(property::HEAD_FORMAT, "json")])).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, Some((2, "json".into())));
        let cold = reopen(&store).table("iot.j").await.unwrap();
        assert_eq!((cold.version(), cold.known_files().map(|f| f.len())), (2, Some(1)));

        let off = with_properties(&[(property::HEAD_FORMAT, "none")]);
        let t = lake.create_table("iot.off", &schema(), off).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, None);
        assert_eq!(reopen(&store).table("iot.off").await.unwrap().version(), 2);

        let err = lake.create_table("iot.x", &schema(), with_properties(&[(property::HEAD_FORMAT, "bson")])).await;
        assert!(matches!(err, Err(Error::Invalid(_))), "{err:?}");
    }

    /// A codec registered by the application: here JSON, stored reversed.
    #[derive(Debug)]
    struct Reversed;

    impl HeadCodec for Reversed {
        fn name(&self) -> &'static str {
            "reversed"
        }
        fn encode(&self, doc: &HeadDoc) -> Result<Vec<u8>, FormatError> {
            Ok(JsonCodec.encode(doc)?.into_iter().rev().collect())
        }
        fn decode(&self, bytes: &[u8]) -> Result<HeadDoc, FormatError> {
            JsonCodec.decode(&bytes.iter().rev().copied().collect::<Vec<_>>())
        }
    }

    /// Custom codecs work like the built-in ones; a reader without the codec loads the table
    /// through the metadata chain instead.
    #[tokio::test]
    async fn custom_codecs_can_be_registered() {
        let (lake, store) = faulty_lake();
        lake.register_head_codec(Arc::new(Reversed));
        let o = with_properties(&[(property::HEAD_FORMAT, "reversed")]);
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        assert_eq!(stored_header(&lake, &t).await, Some((2, "reversed".into())));

        let with_codec = reopen(&store);
        with_codec.register_head_codec(Arc::new(Reversed));
        assert!(with_codec.table("iot.r").await.unwrap().known_files().is_some());
        let without = reopen(&store).table("iot.r").await.unwrap();
        assert_eq!((without.version(), without.known_files().is_some()), (2, false));
    }

    /// The last-value index and footers are inline only while they fit the table's budget.
    #[tokio::test]
    async fn optional_sections_fit_the_budget() {
        let (lake, store) = faulty_lake();
        let o = with_properties(&[(property::HEAD_INLINE_BYTES, "0")]);
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let cold = reopen(&store);
        let table = cold.table("iot.r").await.unwrap();
        assert!(cold.caches().last_value_indexes.get(&table.last_value_index().unwrap()).is_none());
        let file = table.files().await.unwrap()[0].data_file.file_path().to_string();
        assert!(cold.caches().footers.get(&file).is_none());

        // With the default budget, both come with the head.
        let t = lake.create_table("iot.d", &schema(), opts()).await.unwrap();
        t.append(&[batch(&[(T0, 1, 1.0)])], no_maintenance()).await.unwrap();
        crate::flush().await;
        let cold = reopen(&store);
        let table = cold.table("iot.d").await.unwrap();
        assert!(cold.caches().last_value_indexes.get(&table.last_value_index().unwrap()).is_some());
        let file = table.files().await.unwrap()[0].data_file.file_path().to_string();
        assert!(cold.caches().footers.get(&file).is_some());
    }
}
