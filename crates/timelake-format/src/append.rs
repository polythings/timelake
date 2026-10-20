//! Appends for the shared write path (the WASM writer): rows are prepared as the engine prepares
//! them ([`crate::rows`]), written with the shared Parquet writer ([`crate::file`]) and committed
//! with the engine's protocol ([`crate::commit`]).

use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataWriter};

use crate::commit::{Change, CommitInfo, commit};
use crate::error::{WriteError, WriteResult};
use crate::file::{FileStats, FileWriter, writer_properties};
use crate::host::{Host, Upload, uuid_v7};
use crate::iceberg::DataFile;
use crate::maintain::{MaintainOptions, MaintainReport, maintain};
use crate::rows::{latest_of_sorted, sort_and_split};
use crate::summary;
use crate::table::TableState;

/// Bytes sent per part of a multipart upload: S3 and R2 take parts of 5 MiB or more, and R2 wants
/// every part but the last to be the same size.
pub const PART_BYTES: usize = 8 << 20;

/// Options for [`append`].
#[derive(Clone, Debug, Default)]
pub struct AppendOptions {
    /// Idempotency key: appending the same batch id twice commits once.
    pub batch_id: Option<String>,
    /// Extra snapshot summary properties.
    pub properties: HashMap<String, String>,
    /// Override the table's `auto-maintain`.
    pub maintain: Option<bool>,
    /// Cap on the bytes inline compaction reads after this append (see
    /// [`MaintainOptions::max_bytes`]).
    pub max_compact_bytes: Option<u64>,
}

/// What [`append`] did.
#[derive(Clone, Debug, Default)]
pub struct AppendReport {
    /// Rows committed (after in-append dedup); 0 for a duplicate batch.
    pub rows: usize,
    /// Data files committed.
    pub files: usize,
    /// Bytes of data files committed.
    pub bytes: u64,
    /// The commit.
    pub commit: CommitInfo,
    /// Inline maintenance after the commit: `None` if it didn't run or had nothing to do. An
    /// error here means the append is committed but maintenance failed: don't retry the append.
    pub maintenance: Option<Result<MaintainReport, String>>,
}

/// Appends `batches` to `table` as one commit; returns the new version.
pub async fn append<H: Host>(
    host: &H,
    table: TableState,
    batches: &[RecordBatch],
    opts: &AppendOptions,
) -> WriteResult<(TableState, AppendReport)> {
    let now_us = host.now_ms().saturating_mul(1000);
    let partitions = sort_and_split(&table.schema, &table.contract, batches, now_us)?;
    let rows = partitions.iter().flat_map(|(_, b)| b).map(RecordBatch::num_rows).sum();
    let touched: HashSet<Option<i32>> = partitions.iter().map(|(p, _)| *p).collect();
    let latest = latest_of_sorted(&table.schema, &table.contract, &partitions)?;
    let mut files = vec![];
    let mut footers = vec![];
    let result = async {
        for (partition, batches) in &partitions {
            for (file, footer) in write_files(host, &table, *partition, batches).await? {
                footers.push((file.path.clone(), footer));
                files.push(file);
            }
        }
        Ok::<_, crate::error::WriteError>(())
    }
    .await;
    let paths: Vec<String> = files.iter().map(|f: &DataFile| f.path.clone()).collect();
    if let Err(e) = result {
        let _ = host.delete(&paths).await;
        return Err(e);
    }
    if files.is_empty() {
        let info = CommitInfo { version: table.version, ..CommitInfo::default() };
        return Ok((table, AppendReport { commit: info, ..AppendReport::default() }));
    }
    let (n_files, bytes) = (files.len(), files.iter().map(|f| f.file_size).sum());
    let mut props = opts.properties.clone();
    if let Some(id) = &opts.batch_id {
        props.insert(summary::BATCH_ID.into(), id.clone());
    }
    let committed = commit(host, table, &Change::Append { files, latest }, &props).await;
    if !matches!(&committed, Ok((_, info)) if !info.duplicate)
        && !matches!(committed, Err(WriteError::CommitUnknown(_)))
    {
        // Nothing references the data files.
        let _ = host.delete(&paths).await;
    }
    let (mut table, info) = committed?;
    if info.duplicate {
        return Ok((table, AppendReport { commit: info, ..AppendReport::default() }));
    }
    table.footers.extend(footers);
    let _ = table.write_head(host).await;
    let mut report = AppendReport { rows, files: n_files, bytes, commit: info, maintenance: None };
    if !opts.maintain.unwrap_or(table.contract.auto_maintain) {
        return Ok((table, report));
    }
    let mut maintain_opts = MaintainOptions::inline(&table, touched, host.now_ms());
    maintain_opts.max_bytes = opts.max_compact_bytes;
    match maintain(host, table.clone(), &maintain_opts).await {
        Ok((maintained, done)) => {
            report.maintenance = (!done.is_noop()).then_some(Ok(done));
            Ok((maintained, report))
        }
        Err(e) => {
            report.maintenance = Some(Err(e.to_string()));
            Ok((table, report))
        }
    }
}

/// Writes the sorted rows of one partition as data files of the table's target size, each with its
/// encoded footer (for the head).
pub async fn write_files<H: Host>(
    host: &H,
    table: &TableState,
    partition: Option<i32>,
    batches: &[RecordBatch],
) -> WriteResult<Vec<(DataFile, Vec<u8>)>> {
    let mut writer = PartitionFiles::new(host, table, partition)?;
    for rows in batches {
        if let Err(e) = writer.write(rows).await {
            writer.abort().await;
            return Err(e);
        }
    }
    writer.finish().await
}

/// Writes sorted batches of one partition into files of the target size, uploading as they grow.
/// After a failed [`write`](Self::write), [`abort`](Self::abort) removes what was written.
pub struct PartitionFiles<'a, H: Host> {
    host: &'a H,
    table: &'a TableState,
    partition: Option<i32>,
    open: Option<OpenFile<H::Upload>>,
    done: Vec<(DataFile, Vec<u8>)>,
}

impl<H: Host> std::fmt::Debug for PartitionFiles<'_, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionFiles").field("partition", &self.partition).finish_non_exhaustive()
    }
}

struct OpenFile<U> {
    uri: String,
    writer: FileWriter,
    /// Encoded bytes not sent yet, as the writer's chunks.
    pending: Pending,
    upload: Option<U>,
}

/// Bytes waiting to be uploaded, kept as chunks and copied into parts of exactly `PART_BYTES`:
/// parts of one size reuse the memory the previous part freed.
#[derive(Default)]
struct Pending {
    chunks: std::collections::VecDeque<Vec<u8>>,
    len: usize,
}

impl Pending {
    fn extend(&mut self, chunks: Vec<Vec<u8>>) {
        for c in chunks {
            self.len += c.len();
            self.chunks.push_back(c);
        }
    }

    /// The first `n` bytes (or all, if fewer) as one buffer.
    fn take(&mut self, n: usize) -> Vec<u8> {
        let n = n.min(self.len);
        let mut part = Vec::with_capacity(n);
        while part.len() < n {
            let Some(mut chunk) = self.chunks.pop_front() else { break };
            let want = n - part.len();
            if chunk.len() > want {
                let rest = chunk.split_off(want);
                self.chunks.push_front(rest);
            }
            part.extend_from_slice(&chunk);
        }
        self.len -= part.len();
        part
    }
}

impl<'a, H: Host> PartitionFiles<'a, H> {
    /// A writer of `partition`'s files for `table`.
    pub fn new(host: &'a H, table: &'a TableState, partition: Option<i32>) -> WriteResult<Self> {
        Ok(Self { host, table, partition, open: None, done: vec![] })
    }

    fn dir(&self) -> String {
        let c = &self.table.contract;
        match (self.partition, &c.time_column, c.chunk) {
            (Some(v), Some(time_column), Some(chunk)) => {
                format!("{}/data/{time_column}_{chunk}={}", self.table.location, chunk.label(v))
            }
            _ => format!("{}/data", self.table.location),
        }
    }

    /// Writes rows that sort after every row written so far.
    pub async fn write(&mut self, batch: &RecordBatch) -> WriteResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let file = if let Some(f) = self.open.take() {
            f
        } else {
            let page_rows = self.host.data_page_rows();
            let props = writer_properties(&self.table.schema, &self.table.contract, &created_by(), page_rows)?;
            OpenFile {
                uri: format!("{}/{}.parquet", self.dir(), uuid_v7(self.host)),
                writer: FileWriter::new(self.table.schema.clone(), props)?
                    .with_max_row_group_bytes(self.host.max_row_group_bytes()),
                pending: Pending::default(),
                upload: None,
            }
        };
        // Kept in `open` until closed, so `abort` finds its upload if anything below fails.
        let file = self.open.insert(file);
        file.writer.write(batch)?;
        file.pending.extend(file.writer.take_chunks());
        // Parts of exactly PART_BYTES: R2 requires every part but the last to have the same size.
        while file.pending.len >= PART_BYTES {
            if file.upload.is_none() {
                file.upload = Some(self.host.upload(&file.uri).await?);
            }
            if let Some(upload) = &mut file.upload {
                upload.put_part(file.pending.take(PART_BYTES)).await?;
            }
        }
        if file.writer.size() >= self.table.contract.target_file_size
            && let Some(file) = self.open.take()
        {
            self.close(file).await?;
        }
        Ok(())
    }

    /// Writes out `file`. If that fails, nothing of it is left: its upload is aborted, or the
    /// object it may have written is deleted.
    async fn close(&mut self, file: OpenFile<H::Upload>) -> WriteResult<()> {
        let OpenFile { uri, writer, mut pending, upload } = file;
        let finished = writer.finish_chunks();
        let stats = if let Some(mut u) = upload {
            let sent = async {
                let (rest, stats) = finished?;
                pending.extend(rest);
                // Full parts first: R2 takes a last part no larger than the others.
                while pending.len > 0 {
                    u.put_part(pending.take(PART_BYTES)).await?;
                }
                Ok(stats)
            }
            .await;
            match sent {
                Ok(stats) => {
                    if let Err(e) = u.complete().await {
                        let _ = self.host.delete(std::slice::from_ref(&uri)).await;
                        return Err(e);
                    }
                    stats
                }
                Err(e) => {
                    u.abort().await;
                    return Err(e);
                }
            }
        } else {
            let (rest, stats) = finished?;
            pending.extend(rest);
            let all = pending.len;
            if let Err(e) = self.host.put(&uri, pending.take(all)).await {
                let _ = self.host.delete(std::slice::from_ref(&uri)).await;
                return Err(e);
            }
            stats
        };
        let footer = footer_bytes(&stats.metadata)?;
        self.done.push((data_file(self.table, uri, self.partition, &stats), footer));
        Ok(())
    }

    /// Closes the last file; returns the files written with their footers. If that fails, the
    /// files are deleted.
    pub async fn finish(mut self) -> WriteResult<Vec<(DataFile, Vec<u8>)>> {
        if let Some(file) = self.open.take()
            && let Err(e) = self.close(file).await
        {
            self.abort().await;
            return Err(e);
        }
        Ok(self.done)
    }

    /// After a failed write: aborts the open file's upload and deletes the files already written.
    /// Best effort: what can't be removed is left for orphan cleanup.
    pub async fn abort(self) {
        if let Some(OpenFile { upload: Some(u), .. }) = self.open {
            u.abort().await;
        }
        let paths: Vec<String> = self.done.into_iter().map(|(f, _)| f.path).collect();
        if !paths.is_empty() {
            let _ = self.host.delete(&paths).await;
        }
    }
}

/// What data files record as their writer: the same as the engine's, so files are identical.
fn created_by() -> String {
    format!("timelake {}", env!("CARGO_PKG_VERSION"))
}

/// The Iceberg entry of a written file.
pub fn data_file(table: &TableState, uri: String, partition: Option<i32>, stats: &FileStats) -> DataFile {
    DataFile {
        path: uri,
        partition,
        record_count: stats.record_count,
        file_size: stats.file_size,
        column_sizes: stats.column_sizes.clone(),
        value_counts: stats.value_counts.clone(),
        null_counts: stats.null_counts.clone(),
        nan_counts: stats.nan_counts.clone(),
        lower_bounds: stats.lower_bounds.clone(),
        upper_bounds: stats.upper_bounds.clone(),
        split_offsets: Some(stats.split_offsets.clone()),
        sort_order_id: (!table.contract.sort_columns().is_empty()).then(|| table.metadata.sort_order_id()),
    }
}

/// A footer as the head stores it (`ParquetMetaDataWriter`: page indexes, metadata, length, magic).
pub fn footer_bytes(metadata: &ParquetMetaData) -> WriteResult<Vec<u8>> {
    let mut buf = vec![];
    ParquetMetaDataWriter::new(&mut buf, metadata).finish()?;
    Ok(buf)
}
