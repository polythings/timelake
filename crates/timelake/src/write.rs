//! The write path: align incoming Arrow data to the table schema, sort it by
//! `(series_key…, time)`, split it by time partition, and write Parquet laid out for time series.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, PrimitiveLiteral, Struct, Type,
};
use parquet::file::properties::WriterProperties;
use timelake_format::file::{FileStats, FileWriter};

use crate::error::{Error, Result};
use crate::storage::{DeferredWriter, Uploads};
use crate::table::Table;

pub(crate) use timelake_format::rows::PartitionRows;

/// Concatenates, sorts by `(partition, series…, time)` with input order as the tiebreak, splits by
/// partition and, when dedup is on, keeps the last of rows with the same `(series…, time)`: every
/// file is then free of duplicates on its own, which dedup on read relies on.
pub(crate) fn sort_and_split(table: &Table, batches: &[RecordBatch]) -> Result<Vec<PartitionRows>> {
    let now = chrono::Utc::now().timestamp_micros();
    Ok(timelake_format::rows::sort_and_split(&table.schema(), table.contract(), batches, now)?)
}

/// `batch` with its insertion-time `column` set to `now` where it is missing or null.
#[cfg(test)]
pub(crate) fn with_ingest_time(batch: &RecordBatch, column: &str, now: i64) -> Result<RecordBatch> {
    Ok(timelake_format::rows::with_ingest_time(batch, column, now)?)
}

/// Parquet settings for the table's data files (see `timelake_format::file::writer_properties`).
pub(crate) fn writer_properties(table: &Table) -> Result<WriterProperties> {
    let created_by = format!("timelake {}", env!("CARGO_PKG_VERSION"));
    let page_rows = timelake_format::file::DATA_PAGE_ROWS;
    Ok(timelake_format::file::writer_properties(&table.schema(), table.contract(), &created_by, page_rows)?)
}

/// Partition value of a data file written by Timelake (`None` if unpartitioned).
pub fn file_partition(df: &DataFile) -> Option<i32> {
    match df.partition().fields().first() {
        Some(Some(Literal::Primitive(PrimitiveLiteral::Int(v)))) => Some(*v),
        _ => None,
    }
}

/// Writes already-sorted batches of one partition, rolling to a new file at the target size. The
/// end of each file's upload is queued in `uploads`, to be sent with the commit's metadata.
pub(crate) async fn write_partition(
    table: &Table,
    partition: Option<i32>,
    batches: impl IntoIterator<Item = RecordBatch>,
    props: &WriterProperties,
    uploads: &Uploads,
) -> Result<Vec<DataFile>> {
    let mut writer = PartitionWriter::new(table, partition, props, uploads);
    for batch in batches {
        if let Err(e) = writer.write(&batch).await {
            writer.abort().await;
            return Err(e);
        }
    }
    writer.finish().await
}

/// Writes sorted batches of one partition as they come, rolling to a new file at the target size
/// (see [`write_partition`]). Files are encoded by the shared writer (`timelake_format::file`),
/// columns in parallel, and stream out as multipart uploads, so memory is the row group being
/// encoded.
pub(crate) struct PartitionWriter<'a> {
    table: &'a Table,
    partition: Option<i32>,
    props: WriterProperties,
    uploads: &'a Uploads,
    dir: String,
    target: u64,
    open: Option<(String, FileWriter, DeferredWriter)>,
    files: Vec<DataFile>,
}

impl<'a> PartitionWriter<'a> {
    pub(crate) fn new(
        table: &'a Table,
        partition: Option<i32>,
        props: &WriterProperties,
        uploads: &'a Uploads,
    ) -> Self {
        let contract = table.contract();
        let dir = match (partition, &contract.time_column, contract.chunk) {
            (Some(v), Some(time_column), Some(chunk)) => {
                format!("{}/data/{time_column}_{chunk}={}", table.location(), chunk.label(v))
            }
            _ => format!("{}/data", table.location()),
        };
        let props = props.clone();
        Self { table, partition, props, uploads, dir, target: contract.target_file_size, open: None, files: vec![] }
    }

    /// Writes rows that sort after every row written so far. After an error, call
    /// [`abort`](Self::abort).
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Kept in `open` while it is written, so `abort` finds its upload if anything fails.
        if self.open.is_none() {
            let uri = format!("{}/{}.parquet", self.dir, uuid::Uuid::now_v7());
            let writer = FileWriter::new(self.table.schema(), self.props.clone())?;
            let upload = self.table.lake().storage().deferred_writer(&uri, self.uploads)?;
            self.open = Some((uri, writer, upload));
        }
        let (_, writer, upload) = self.open.as_mut().expect("a file was opened above");
        writer.write(batch)?;
        upload.put(writer.take()).await?;
        if writer.size() >= self.target {
            self.close().await?;
        }
        Ok(())
    }

    /// Finishes the open file and queues the end of its upload.
    async fn close(&mut self) -> Result<()> {
        let Some((uri, writer, mut upload)) = self.open.take() else { return Ok(()) };
        let finished = match writer.finish() {
            Ok((rest, stats)) => upload.put(rest).await.map(|()| stats),
            Err(e) => Err(e.into()),
        };
        match finished {
            Ok(stats) => {
                upload.close();
                self.files.push(data_file(self.table, uri, self.partition, &stats)?);
                Ok(())
            }
            Err(e) => {
                upload.abort().await;
                Err(e)
            }
        }
    }

    /// Closes the last file; returns the files written.
    pub(crate) async fn finish(mut self) -> Result<Vec<DataFile>> {
        self.close().await?;
        Ok(self.files)
    }

    /// After a failed write: aborts the open file's upload. The files already closed are queued
    /// in the uploads, for their owner to send or abort.
    pub(crate) async fn abort(self) {
        if let Some((_, _, upload)) = self.open {
            upload.abort().await;
        }
    }
}

/// The Iceberg entry of a data file the shared writer wrote, with the statistics iceberg-rust's
/// writer would record.
fn data_file(table: &Table, uri: String, partition: Option<i32>, stats: &FileStats) -> Result<DataFile> {
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    let datums = |bounds: &std::collections::BTreeMap<i32, Vec<u8>>| -> Result<HashMap<i32, Datum>> {
        let mut out = HashMap::new();
        for (id, bytes) in bounds {
            if let Some(Type::Primitive(ty)) = schema.field_by_id(*id).map(|f| f.field_type.as_ref()) {
                out.insert(*id, Datum::try_from_bytes(bytes, ty.clone())?);
            }
        }
        Ok(out)
    };
    let counts = |m: &std::collections::BTreeMap<i32, u64>| m.iter().map(|(k, v)| (*k, *v)).collect::<HashMap<_, _>>();
    let mut b = DataFileBuilder::default();
    b.content(DataContentType::Data)
        .file_path(uri)
        .file_format(DataFileFormat::Parquet)
        .partition(partition_struct(partition))
        .partition_spec_id(metadata.default_partition_spec().spec_id())
        .record_count(stats.record_count)
        .file_size_in_bytes(stats.file_size)
        .column_sizes(counts(&stats.column_sizes))
        .value_counts(counts(&stats.value_counts))
        .null_value_counts(counts(&stats.null_counts))
        .nan_value_counts(counts(&stats.nan_counts))
        .lower_bounds(datums(&stats.lower_bounds)?)
        .upper_bounds(datums(&stats.upper_bounds)?)
        .split_offsets(Some(stats.split_offsets.clone()));
    if !table.contract().sort_columns().is_empty() {
        let id = i32::try_from(metadata.default_sort_order_id())
            .map_err(|_| Error::invalid("sort order id out of range"))?;
        b.sort_order_id(id);
    }
    b.build()
        .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, "building data file").with_source(e).into())
}

pub(crate) fn partition_struct(v: Option<i32>) -> Struct {
    match v {
        Some(v) => Struct::from_iter([Some(Literal::int(v))]),
        None => Struct::empty(),
    }
}

/// Splits a large batch into slices of `rows` rows (one Parquet row group each), so the writer
/// streams them.
pub(crate) fn row_groups(batch: RecordBatch, rows: usize) -> impl Iterator<Item = RecordBatch> {
    let n = batch.num_rows();
    (0..n).step_by(rows.max(1)).map(move |i| batch.slice(i, rows.min(n - i)))
}

#[cfg(test)]
mod tests {
    use iceberg::writer::file_writer::{FileWriter as _, FileWriterBuilder, ParquetWriterBuilder};

    use super::*;
    use crate::testing::{T0, batch, faulty_lake, opts, schema};

    /// The shared writer records what iceberg-rust's writer records for the same rows, and writes
    /// the same bytes.
    #[tokio::test]
    async fn shared_writer_matches_iceberg_rust() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let rows: Vec<(i64, i32, f64)> = (0..50_000)
            .map(|i| (T0 + i64::from(i), i % 9, if i % 7 == 0 { f64::NAN } else { f64::from(i) / 3.0 }))
            .collect();
        let (partition, mut sorted) = sort_and_split(&t, &[batch(&rows)]).unwrap().remove(0);
        let sorted = sorted.remove(0);
        let props = writer_properties(&t).unwrap();

        let uploads = Uploads::default();
        let ours = write_partition(&t, partition, [sorted.clone()], &props, &uploads).await.unwrap().remove(0);
        uploads.send().await.unwrap();

        let builder = ParquetWriterBuilder::new(props, t.metadata().current_schema().clone());
        let uri = format!("{}/data/theirs.parquet", t.location());
        let mut writer = builder.build(lake.storage().deferred_output_file(&uri, &uploads)).await.unwrap();
        writer.write(&sorted).await.unwrap();
        let theirs = writer.close().await.unwrap().remove(0).partition(partition_struct(partition)).build().unwrap();
        uploads.send().await.unwrap();

        assert_eq!(ours.record_count(), theirs.record_count());
        assert_eq!(ours.file_size_in_bytes(), theirs.file_size_in_bytes());
        assert_eq!(ours.column_sizes(), theirs.column_sizes());
        assert_eq!(ours.value_counts(), theirs.value_counts());
        assert_eq!(ours.null_value_counts(), theirs.null_value_counts());
        assert_eq!(ours.nan_value_counts(), theirs.nan_value_counts());
        assert_eq!(ours.lower_bounds(), theirs.lower_bounds());
        assert_eq!(ours.upper_bounds(), theirs.upper_bounds());
        assert_eq!(ours.split_offsets(), theirs.split_offsets());
        let storage = lake.storage();
        assert_eq!(storage.read(ours.file_path()).await.unwrap(), storage.read(theirs.file_path()).await.unwrap());
    }
}

/// The shared Iceberg writer (`timelake_format::iceberg`, used by the WASM writer) and iceberg-rust
/// read each other's manifests and manifest lists.
#[cfg(test)]
mod shared_iceberg_tests {
    use std::sync::Arc;

    use iceberg::spec::{FormatVersion, Manifest, ManifestList, ManifestStatus};
    use timelake_format::iceberg::{self as shared, ManifestContext, ManifestWrite, Status};

    use super::file_partition;
    use crate::append::AppendOptions;
    use crate::testing::{T0, batch, faulty_lake, opts, schema};

    fn no_maintenance() -> AppendOptions {
        AppendOptions { maintain: Some(false), ..AppendOptions::default() }
    }

    fn shared_file(f: &iceberg::spec::DataFile) -> shared::DataFile {
        let bytes = |m: &std::collections::HashMap<i32, iceberg::spec::Datum>| {
            m.iter().map(|(k, d)| (*k, d.to_bytes().unwrap().to_vec())).collect()
        };
        let counts = |m: &std::collections::HashMap<i32, u64>| m.iter().map(|(k, v)| (*k, *v)).collect();
        shared::DataFile {
            path: f.file_path().to_string(),
            partition: file_partition(f),
            record_count: f.record_count(),
            file_size: f.file_size_in_bytes(),
            column_sizes: counts(f.column_sizes()),
            value_counts: counts(f.value_counts()),
            null_counts: counts(f.null_value_counts()),
            nan_counts: counts(f.nan_value_counts()),
            lower_bounds: bytes(f.lower_bounds()),
            upper_bounds: bytes(f.upper_bounds()),
            split_offsets: f.split_offsets().map(<[i64]>::to_vec),
            sort_order_id: f.sort_order_id(),
        }
    }

    #[tokio::test]
    async fn shared_manifests_match_iceberg_rust() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0), (T0 + 1, 2, f64::NAN)])], no_maintenance()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0 + 86_400_000_000, 1, 2.0)])], no_maintenance()).await.unwrap();
        let storage = lake.storage();

        // The shared reader reads what iceberg-rust wrote.
        let list_uri = t.metadata().current_snapshot().unwrap().manifest_list().to_string();
        let list = shared::read_manifest_list(&storage.read(&list_uri).await.unwrap()).unwrap();
        let mut ours = vec![];
        for m in &list {
            ours.extend(shared::read_manifest(m, &storage.read(&m.path).await.unwrap()).unwrap());
        }
        let mut theirs = t.files().await.unwrap();
        theirs.sort_by(|a, b| a.data_file.file_path().cmp(b.data_file.file_path()));
        ours.sort_by(|a, b| a.file.path.cmp(&b.file.path));
        assert_eq!(ours.len(), theirs.len());
        for (o, e) in ours.iter().zip(&theirs) {
            assert_eq!(*o.file, shared_file(&e.data_file));
            assert_eq!(
                (o.snapshot_id, o.sequence_number, o.file_sequence_number),
                (e.snapshot_id, e.sequence_number, e.file_sequence_number)
            );
        }

        // iceberg-rust reads what the shared writer writes.
        let metadata = t.metadata();
        let schema_json = serde_json::to_string(metadata.current_schema().as_ref()).unwrap();
        let spec = metadata.default_partition_spec();
        let spec_json = serde_json::to_string(spec.fields()).unwrap();
        let ctx = ManifestContext {
            schema_json: &schema_json,
            schema_id: metadata.current_schema_id(),
            spec_fields_json: &spec_json,
            spec_id: spec.spec_id(),
            partition: Some(&shared::PartitionField {
                name: "time_day".into(),
                field_id: spec.fields()[0].field_id,
                chunk: t.contract().chunk.unwrap(),
            }),
        };
        let writes: Vec<ManifestWrite> = ours
            .iter()
            .enumerate()
            .map(|(i, e)| ManifestWrite {
                status: if i == 0 { Status::Added } else { Status::Existing },
                snapshot_id: Some(42),
                sequence_number: (i > 0).then_some(e.sequence_number),
                file_sequence_number: (i > 0).then_some(e.file_sequence_number),
                file: Arc::clone(&e.file),
            })
            .collect();
        let bytes = shared::write_manifest(&ctx, &writes).unwrap();
        let (entries, _) = Manifest::parse_avro(&bytes).unwrap().into_parts();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].status, ManifestStatus::Added);
        assert_eq!(entries[1].status, ManifestStatus::Existing);
        for (entry, theirs) in entries.iter().zip(&theirs) {
            assert_eq!(entry.data_file, *theirs.data_file);
        }
        let mf = shared::manifest_file("memory://test/m.avro", &bytes, spec.spec_id(), 42, &writes, true);
        let list = shared::write_manifest_list(&[mf], 42, Some(7), 9).unwrap();
        let parsed = ManifestList::parse_with_version(&list, FormatVersion::V2).unwrap();
        let parsed = &parsed.entries()[0];
        assert_eq!((parsed.sequence_number, parsed.added_snapshot_id, parsed.added_files_count), (9, 42, Some(1)));
        assert_eq!(parsed.existing_rows_count, Some(theirs[1].data_file.record_count()));
    }
}

#[cfg(test)]
mod codec_tests {
    use parquet::basic::Compression;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use crate::append::AppendOptions;
    use crate::testing::{T0, batch, faulty_lake, opts, schema};

    /// Iceberg's `write.parquet.compression-codec` picks the codec; an unknown one fails the write.
    #[tokio::test]
    async fn the_iceberg_codec_property_picks_the_codec() {
        let (lake, _store) = faulty_lake();
        let mut o = opts();
        o.properties.insert(timelake_format::property::COMPRESSION_CODEC.into(), "LZ4".into());
        let t = lake.create_table("iot.lz4", &schema(), o).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], AppendOptions::default()).await.unwrap();
        let file = t.files().await.unwrap().remove(0);
        let bytes = lake.storage().read(file.data_file.file_path()).await.unwrap();
        let reader = SerializedFileReader::new(bytes).unwrap();
        assert_eq!(reader.metadata().row_group(0).column(0).compression(), Compression::LZ4_RAW);

        let mut o = opts();
        o.properties.insert(timelake_format::property::COMPRESSION_CODEC.into(), "zip".into());
        let t = lake.create_table("iot.zip", &schema(), o).await.unwrap();
        let err = t.append(&[batch(&[(T0, 1, 1.0)])], AppendOptions::default()).await.unwrap_err();
        assert!(err.to_string().contains("compression-codec \"zip\""), "{err}");
    }
}
