//! A table version's contract, size and layout: what `timelake info` and Python's `Table.info()` show.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::time::Duration;

use serde::{Serialize, Serializer};
use timelake_format::{FormatVersion, LISTED_SNAPSHOTS};

use crate::error::Result;
use crate::table::Table;
use crate::write::file_partition;
use crate::{Chunk, format_duration, summary};

/// A table version's contract, size and layout, from [`Table::info`].
///
/// Serializes as `timelake info --json` prints it: the chunk and retention by name (`day`, `90d`), snapshot ids
/// as strings.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct TableInfo {
    /// The full name, `namespace.table`.
    pub name: String,
    /// The committed version.
    pub version: u64,
    /// The table's root URL.
    pub location: String,
    /// The Iceberg format version (1, 2 or 3).
    pub format_version: u8,
    /// The Timelake table format (`timelake.format-version`).
    #[serde(serialize_with = "display")]
    pub timelake_format_version: FormatVersion,
    /// The time column; `None` for a plain table.
    pub time_column: Option<String>,
    /// The columns that identify a series.
    pub series_key: Vec<String>,
    /// The time partition size.
    pub chunk: Option<Chunk>,
    /// Whether compaction keeps only the latest row per series and time.
    pub dedup: bool,
    /// The insertion-time column, if the table has one.
    pub ingest_time_column: Option<String>,
    /// Partitions older than this are dropped; `None` keeps them forever.
    #[serde(serialize_with = "retention")]
    pub retention: Option<Duration>,
    /// The current schema's top-level columns.
    pub schema: Vec<ColumnInfo>,
    /// Rows in the live data files.
    pub rows: u64,
    /// Live data files.
    pub files: usize,
    /// Bytes of the live data files.
    pub bytes: u64,
    /// Manifests of the current snapshot.
    pub manifests: usize,
    /// Snapshots in the metadata.
    pub snapshot_count: usize,
    /// When the current snapshot was committed, in milliseconds since the epoch.
    pub last_commit_ms: Option<i64>,
    /// The current snapshot's id.
    #[serde(serialize_with = "optional_display")]
    pub current_snapshot: Option<i64>,
    /// The last-value index of the current snapshot.
    pub last_value_index: Option<String>,
    /// Time partitions, newest first.
    pub partitions: Vec<PartitionInfo>,
    /// The newest [`LISTED_SNAPSHOTS`] snapshots, newest first.
    pub snapshots: Vec<SnapshotInfo>,
}

/// A top-level column.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct ColumnInfo {
    /// The column name.
    pub name: String,
    /// The Arrow type, as Arrow prints it.
    #[serde(rename = "type")]
    pub data_type: String,
    /// Whether the column may hold nulls.
    pub nullable: bool,
}

/// The live files of one time partition.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct PartitionInfo {
    /// The partition as its chunk names it (`2026-01-01`), or `-` for a table without one.
    #[serde(rename = "partition")]
    pub label: String,
    /// Live data files.
    pub files: usize,
    /// Rows in them.
    pub rows: u64,
    /// Bytes of them.
    pub bytes: u64,
}

/// One snapshot, with the counts from its summary (0 when the summary lacks one).
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct SnapshotInfo {
    /// The snapshot id; serialized as a string, since JavaScript numbers can't hold every id.
    #[serde(serialize_with = "display")]
    pub id: i64,
    /// When it was committed, in milliseconds since the epoch.
    pub timestamp_ms: i64,
    /// `append`, `replace`, `overwrite` or `delete`.
    pub operation: String,
    /// Rows it added.
    pub added_records: i64,
    /// Rows it removed.
    pub deleted_records: i64,
    /// Data files it added.
    pub added_files: i64,
    /// Data files it removed.
    pub deleted_files: i64,
    /// Rows in the table after it.
    pub total_records: i64,
}

impl Table {
    /// This version's contract, size and layout. Reads the manifests unless this version already knows its files.
    pub async fn info(&self) -> Result<TableInfo> {
        let (files, manifests) = futures::try_join!(self.files(), self.manifests())?;
        let contract = self.contract();
        let mut partitions: BTreeMap<Option<i32>, PartitionInfo> = BTreeMap::new();
        for f in files.iter() {
            let p = file_partition(&f.data_file);
            let entry = partitions.entry(p).or_insert_with(|| PartitionInfo {
                label: p.zip(contract.chunk).map_or_else(|| "-".to_string(), |(v, chunk)| chunk.label(v)),
                files: 0,
                rows: 0,
                bytes: 0,
            });
            entry.files += 1;
            entry.rows += f.data_file.record_count();
            entry.bytes += f.data_file.file_size_in_bytes();
        }
        let metadata = self.metadata();
        let mut snapshots: Vec<_> = metadata.snapshots().collect();
        snapshots.sort_by_key(|s| Reverse(s.timestamp_ms()));
        Ok(TableInfo {
            name: self.name().to_string(),
            version: self.version(),
            location: self.location().to_string(),
            format_version: metadata.format_version() as u8,
            timelake_format_version: contract.format_version,
            time_column: contract.time_column.clone(),
            series_key: contract.series_key.clone(),
            chunk: contract.chunk,
            dedup: contract.dedup,
            ingest_time_column: contract.ingest_time_column.clone(),
            retention: contract.retention,
            schema: self
                .schema()
                .fields()
                .iter()
                .map(|f| ColumnInfo {
                    name: f.name().clone(),
                    data_type: f.data_type().to_string(),
                    nullable: f.is_nullable(),
                })
                .collect(),
            rows: partitions.values().map(|p| p.rows).sum(),
            files: files.len(),
            bytes: partitions.values().map(|p| p.bytes).sum(),
            manifests: manifests.len(),
            snapshot_count: snapshots.len(),
            last_commit_ms: self.current_snapshot().map(|s| s.timestamp_ms()),
            current_snapshot: metadata.current_snapshot_id(),
            last_value_index: self.last_value_index(),
            partitions: partitions.into_values().rev().collect(),
            snapshots: snapshots.iter().take(LISTED_SNAPSHOTS).map(|s| SnapshotInfo::from(s.as_ref())).collect(),
        })
    }
}

impl From<&iceberg::spec::Snapshot> for SnapshotInfo {
    fn from(s: &iceberg::spec::Snapshot) -> Self {
        let p = &s.summary().additional_properties;
        let count = |k: &str| p.get(k).and_then(|v| v.parse().ok()).unwrap_or(0);
        Self {
            id: s.snapshot_id(),
            timestamp_ms: s.timestamp_ms(),
            operation: s.summary().operation.as_str().to_string(),
            added_records: count(summary::ADDED_RECORDS),
            deleted_records: count(summary::DELETED_RECORDS),
            added_files: count(summary::ADDED_DATA_FILES),
            deleted_files: count(summary::DELETED_DATA_FILES),
            total_records: count(summary::TOTAL_RECORDS),
        }
    }
}

fn display<S: Serializer>(value: &impl Display, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.collect_str(value)
}

#[expect(clippy::ref_option, reason = "serde's serialize_with passes a reference to the field")]
fn optional_display<S: Serializer>(value: &Option<impl Display>, s: S) -> std::result::Result<S::Ok, S::Error> {
    match value {
        Some(value) => s.collect_str(value),
        None => s.serialize_none(),
    }
}

#[expect(clippy::ref_option, reason = "serde's serialize_with passes a reference to the field")]
fn retention<S: Serializer>(retention: &Option<Duration>, s: S) -> std::result::Result<S::Ok, S::Error> {
    retention.map(format_duration).serialize(s)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::property;
    use crate::testing::*;

    const DAY: i64 = 86_400_000_000;

    /// Sizes add up over the partitions, newest first, and the JSON form names the chunk and retention and gives
    /// ids as strings.
    #[tokio::test]
    async fn info_sums_files_by_partition() {
        let (lake, _store) = faulty_lake();
        let mut o = opts();
        o.properties = HashMap::from([(property::RETENTION.into(), "90d".into())]);
        let t = lake.create_table("iot.r", &schema(), o).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0), (T0 + DAY, 1, 2.0)])], no_maintenance()).await.unwrap();
        let (t, _) = t.append(&[batch(&[(T0 + DAY + 1, 2, 3.0)])], no_maintenance()).await.unwrap();

        let info = t.info().await.unwrap();
        assert_eq!((info.rows, info.files, info.snapshot_count), (3, 3, 2));
        assert_eq!(info.bytes, info.partitions.iter().map(|p| p.bytes).sum::<u64>());
        let partitions: Vec<_> = info.partitions.iter().map(|p| (p.label.as_str(), p.files, p.rows)).collect();
        assert_eq!(partitions, [("2026-01-02", 2, 2), ("2026-01-01", 1, 1)]);
        assert_eq!(info.snapshots.first().map(|s| s.total_records), Some(3), "newest first");

        let json = serde_json::to_value(&info).unwrap();
        assert_eq!((&json["chunk"], &json["retention"]), (&"day".into(), &"90d".into()));
        let current = t.metadata().current_snapshot_id().unwrap().to_string();
        assert_eq!(
            (&json["current_snapshot"], &json["snapshots"][0]["id"]),
            (&current.clone().into(), &current.into())
        );
        assert_eq!(json["schema"][0]["name"], "time");
    }
}
