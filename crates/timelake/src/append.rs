//! Appending data, with inline auto-maintenance: the writer keeps the table healthy as it goes
//! (spec 20), so no separate compaction job is needed.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use arrow::array::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use iceberg::spec::DataFile;

use crate::commit::{Change, CommitInfo, commit};
use crate::error::Result;
use crate::maintain::{MaintainOptions, MaintainReport};
use crate::rollup::RollupRefresh;
use crate::storage::Uploads;
use crate::summary;
use crate::table::Table;
use crate::write::{PartitionRows, row_groups, sort_and_split, write_partition, writer_properties};

/// How many partitions an append writes at once.
const CONCURRENT_PARTITION_WRITES: usize = 8;
/// Inline maintenance expires snapshots in batches of about this many, not after every commit.
const EXPIRE_BATCH: usize = 20;

/// Options for [`Table::append`].
#[derive(Clone, Debug, Default)]
pub struct AppendOptions {
    /// Idempotency key: appending the same batch id twice commits once.
    pub batch_id: Option<String>,
    /// Override the table's `timelake.auto-maintain`.
    pub maintain: Option<bool>,
    /// Extra snapshot summary properties.
    pub properties: HashMap<String, String>,
}

/// What [`Table::append`] did.
#[derive(Debug)]
pub struct AppendResult {
    /// Rows committed by this call, after removing rows with the same `(series…, time)` within the
    /// call when dedup is on: 0 if the batch id was already committed (`commit.duplicate`).
    pub rows: usize,
    /// Data files committed by this call.
    pub files: usize,
    /// Bytes of data files committed by this call.
    pub bytes: u64,
    /// The commit itself.
    pub commit: CommitInfo,
    /// Inline maintenance after the commit: `None` if it didn't run or had nothing to do. An error
    /// here means the append is committed but maintenance failed: don't retry the append (without
    /// a batch id it would write the rows twice); run [`Table::maintain`] later instead.
    pub maintenance: Option<Result<MaintainReport>>,
}

impl AppendResult {
    fn nothing(commit: CommitInfo) -> Self {
        Self { rows: 0, files: 0, bytes: 0, commit, maintenance: None }
    }
}

impl Table {
    /// Appends Arrow data as one commit. Rows are aligned to the table schema, sorted by
    /// `(series_key…, time)` (the last of equal keys kept when dedup is on), and written to one file
    /// per time partition. Returns the new table version.
    pub async fn append(&self, batches: &[RecordBatch], opts: AppendOptions) -> Result<(Table, AppendResult)> {
        let partitions = sort_and_split(self, batches)?;
        let rows = partitions.iter().flat_map(|(_, b)| b).map(RecordBatch::num_rows).sum();
        let touched: HashSet<Option<i32>> = partitions.iter().map(|(p, _)| *p).collect();
        // Each partition is sorted: its last row per series is its newest (no second sort).
        let latest = timelake_format::rows::latest_of_sorted(&self.schema(), self.contract(), &partitions)?;
        let uploads = Uploads::default();
        let files = self.write_partitions(partitions, &uploads).await?;
        if files.is_empty() {
            let commit = CommitInfo {
                snapshot_id: 0,
                version: self.version(),
                attempts: 0,
                duration: Duration::ZERO,
                duplicate: false,
            };
            return Ok((self.clone(), AppendResult::nothing(commit)));
        }
        let (n_files, bytes) = (files.len(), files.iter().map(DataFile::file_size_in_bytes).sum());
        let paths: Vec<String> = files.iter().map(|f| f.file_path().to_string()).collect();

        let mut props = opts.properties.clone();
        if let Some(id) = &opts.batch_id {
            props.insert(summary::BATCH_ID.into(), id.clone());
        }
        let committed = commit(self, Change::Append { files, latest }, props, uploads).await;
        if !matches!(&committed, Ok((_, info)) if !info.duplicate) {
            // Nothing references the data files: a failed cleanup only wastes space, and the
            // commit's own outcome is what the caller needs to see.
            let _ = self.lake().storage().delete_many(paths).await;
        }
        let (table, info) = committed?;
        if info.duplicate {
            return Ok((table, AppendResult::nothing(info)));
        }

        let mut result = AppendResult { rows, files: n_files, bytes, commit: info, maintenance: None };
        if !opts.maintain.unwrap_or(table.contract().auto_maintain) {
            return Ok((table, result));
        }
        match table.maintain(table.auto_maintain_options(touched)).await {
            Ok((maintained, report)) => {
                result.maintenance = (!report.is_noop()).then_some(Ok(report));
                Ok((maintained, result))
            }
            Err(e) => {
                result.maintenance = Some(Err(e));
                Ok((table, result))
            }
        }
    }

    /// Writes each partition's rows to Parquet, a few partitions at a time.
    async fn write_partitions(&self, partitions: Vec<PartitionRows>, uploads: &Uploads) -> Result<Vec<DataFile>> {
        let props = writer_properties(self)?;
        let rows_per_group = self.contract().row_group_rows;
        // The futures are created up front (not in a stream closure) so the returned future stays
        // `Send` and callers can spawn it.
        let writes: Vec<_> = partitions
            .into_iter()
            .map(|(partition, batches)| {
                let groups = batches.into_iter().flat_map(move |rows| row_groups(rows, rows_per_group));
                write_partition(self, partition, groups, &props, uploads)
            })
            .collect();
        let files: Vec<Vec<DataFile>> =
            futures::stream::iter(writes).buffered(CONCURRENT_PARTITION_WRITES).try_collect().await?;
        Ok(files.into_iter().flatten().collect())
    }

    /// Cheap checks after an append: compact size tiers of the touched partitions that crossed
    /// the threshold, apply retention, and expire snapshots in batches.
    fn auto_maintain_options(&self, touched: HashSet<Option<i32>>) -> MaintainOptions {
        let c = self.contract();
        let snapshots = self.metadata().snapshots().len();
        let oldest = self.metadata().snapshots().map(|s| s.timestamp_ms()).min().unwrap_or(i64::MAX);
        let retention_ms = i64::try_from(c.snapshot_retention.as_millis()).unwrap_or(i64::MAX);
        let cutoff = chrono::Utc::now().timestamp_millis().saturating_sub(retention_ms);
        MaintainOptions {
            compact: true,
            retention: c.retention.is_some(),
            expire: snapshots >= c.max_snapshots + EXPIRE_BATCH
                || (snapshots >= c.min_snapshots + EXPIRE_BATCH && oldest < cutoff),
            min_small_files: c.compact_min_files,
            partitions: Some(touched),
            tiered: true,
            cold_after: c.compact_cold_after,
            rollups: RollupRefresh::Due,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{Op, T0, batch, faulty_lake, opts, schema};

    /// A failure in inline maintenance after the append committed is reported, not returned as an
    /// error: a caller retrying the "failed" append would write the rows twice.
    #[tokio::test]
    async fn failed_maintenance_keeps_the_committed_append() {
        let (lake, store) = faulty_lake();
        let mut o = opts();
        o.properties.insert(crate::property::COMPACT_MIN_FILES.into(), "2".into());
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        let (t, r) = t.append(&[batch(&[(T0, 1, 1.0)])], AppendOptions::default()).await.unwrap();
        assert!(r.maintenance.is_none());

        // The second small file triggers compaction, which can't read the data files.
        store.fail(Op::Get, "/data/");
        let (t2, r) = t.append(&[batch(&[(T0 + 1, 1, 2.0)])], AppendOptions::default()).await.unwrap();
        assert!(matches!(r.maintenance, Some(Err(_))), "{:?}", r.maintenance);
        assert_eq!((t2.version(), r.rows), (t.version() + 1, 1));
        store.heal();
        assert_eq!(lake.table("iot.r").await.unwrap().files().await.unwrap().len(), 2);

        // Maintenance can simply run again later.
        let (t3, report) = t2.maintain(MaintainOptions::default()).await.unwrap();
        assert_eq!(report.files_rewritten, 2);
        assert_eq!(t3.files().await.unwrap().len(), 1);
    }

    /// A batch id that is already committed appends nothing, and the result says so.
    #[tokio::test]
    async fn duplicate_batch_reports_nothing_appended() {
        let (lake, store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let opts = AppendOptions { batch_id: Some("b1".into()), ..Default::default() };
        let rows = batch(&[(T0, 1, 1.0), (T0, 2, 2.0)]);
        let (t1, first) = t.append(std::slice::from_ref(&rows), opts.clone()).await.unwrap();
        assert_eq!((first.rows, first.files, first.commit.duplicate), (2, 1, false));
        let objects = store.paths().await;

        let (t2, second) = t1.append(&[rows], opts).await.unwrap();
        assert!(second.commit.duplicate);
        assert_eq!((second.rows, second.files, second.bytes), (0, 0, 0));
        assert!(second.maintenance.is_none());
        assert_eq!(t2.version(), t1.version());
        // The data files written before the duplicate was detected are gone again.
        assert_eq!(store.paths().await, objects);
    }

    /// Rows with the same `(series, time)` in one append keep only the last (dedup on), so every
    /// file is free of duplicates; with dedup off, all are kept.
    #[tokio::test]
    async fn one_append_keeps_the_last_of_equal_keys() {
        let (lake, _store) = faulty_lake();
        let rows = batch(&[(T0, 1, 1.0), (T0, 2, 2.0), (T0, 1, 3.0)]);
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (_, r) = t.append(std::slice::from_ref(&rows), AppendOptions::default()).await.unwrap();
        assert_eq!(r.rows, 2);
        let values = crate::Session::new(lake.clone())
            .sql("SELECT value FROM iot.r WHERE device = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let values = values[0].column(0).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(values.values().as_ref(), [3.0]);

        let keep_all = crate::TableOptions { dedup: Some(false), ..opts() };
        let t = lake.create_table("iot.all", &schema(), keep_all).await.unwrap();
        let (_, r) = t.append(&[rows], AppendOptions::default()).await.unwrap();
        assert_eq!(r.rows, 3);
    }

    /// `(time, device, value, _ingest_ts)` rows: an input that brings its own insertion times.
    fn with_ingest(rows: &[(i64, i32, f64, i64)]) -> RecordBatch {
        use std::sync::Arc;

        use arrow::array::{Float64Array, Int32Array, TimestampMicrosecondArray};
        let mut fields = schema().fields().to_vec();
        fields.push(Arc::new(arrow::datatypes::Field::new(
            "_ingest_ts",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )));
        let column = |f: fn(&(i64, i32, f64, i64)) -> i64| {
            TimestampMicrosecondArray::from_iter_values(rows.iter().map(f)).with_timezone("UTC")
        };
        RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            vec![
                Arc::new(column(|r| r.0)),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(column(|r| r.3)),
            ],
        )
        .unwrap()
    }

    async fn values(lake: &crate::Lake, sql: &str) -> Vec<f64> {
        let out = crate::Session::new(lake.clone()).sql(sql).await.unwrap().collect().await.unwrap();
        out.iter()
            .flat_map(|b| b.column(0).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().values().to_vec())
            .collect()
    }

    /// A time-series table gets an insertion-time column the writer fills; `ingest_time = false`
    /// leaves it out.
    #[tokio::test]
    async fn appends_record_their_insertion_time() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        assert_eq!(t.contract().ingest_time_column.as_deref(), Some("_ingest_ts"));
        let before = chrono::Utc::now().timestamp_micros();
        t.append(&[batch(&[(T0, 1, 1.0)])], AppendOptions::default()).await.unwrap();
        let sql = "SELECT CAST(to_unixtime(_ingest_ts) * 1000000 AS DOUBLE) FROM iot.r";
        let stamped = values(&lake, sql).await[0];
        assert!(stamped >= before as f64 - 1e6, "{stamped} vs {before}");

        let plain = crate::TableOptions { ingest_time: Some(false), ..opts() };
        let t = lake.create_table("iot.plain", &schema(), plain).await.unwrap();
        assert!(t.contract().ingest_time_column.is_none());
        assert_eq!(t.schema().fields().len(), 3);
    }

    /// The latest insertion wins over commit order: within one append, on read and in compaction.
    #[tokio::test]
    async fn the_latest_insertion_time_wins() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let no_maintenance = AppendOptions { maintain: Some(false), ..AppendOptions::default() };
        // One append: the row inserted later (2) wins though it comes first.
        let (t, r) =
            t.append(&[with_ingest(&[(T0, 1, 2.0, 20), (T0, 1, 1.0, 10)])], no_maintenance.clone()).await.unwrap();
        assert_eq!(r.rows, 1);
        // A later commit carrying an older insertion time loses.
        let (t, _) = t.append(&[with_ingest(&[(T0, 1, 3.0, 15)])], no_maintenance).await.unwrap();
        let sql = "SELECT value FROM iot.r WHERE device = 1";
        assert_eq!(values(&lake, sql).await, [2.0]);
        t.maintain(MaintainOptions::default()).await.unwrap();
        assert_eq!(values(&lake, sql).await, [2.0]);
    }

    #[tokio::test]
    async fn empty_append_commits_nothing() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        let (t2, r) = t.append(&[batch(&[])], AppendOptions::default()).await.unwrap();
        assert_eq!((t2.version(), r.rows, r.files, r.commit.attempts), (t.version(), 0, 0, 0));
    }

    #[tokio::test]
    async fn out_of_range_time_is_an_error() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        assert!(t.append(&[batch(&[(i64::MAX, 1, 1.0)])], AppendOptions::default()).await.is_err());
    }

    /// The append future can be spawned onto a multi-threaded runtime.
    #[tokio::test]
    async fn append_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let (lake, _store) = faulty_lake();
        let table = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
        assert_send(&table.append(&[], AppendOptions::default()));
    }
}
