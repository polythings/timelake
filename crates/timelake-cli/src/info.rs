//! `timelake info`: one table's contract, size and layout, as text or JSON.

use std::collections::BTreeMap;
use std::fmt;

use anyhow::Result;
use serde_json::{Value, json};
use timelake::{Chunk, Contract, Table, file_partition, format_duration, summary};

/// Partitions listed in text output; JSON lists all.
const TEXT_PARTITIONS: usize = 10;
/// Snapshots shown in the text output, newest first (the JSON lists more).
const TEXT_SNAPSHOTS: usize = 5;
/// Snapshots listed in JSON output, newest first.
const JSON_SNAPSHOTS: usize = 50;

/// Everything `info` shows about a table version.
pub(crate) struct TableInfo {
    name: String,
    version: u64,
    /// The Iceberg format version (1, 2 or 3).
    format_version: u8,
    location: String,
    contract: Contract,
    /// `(name, type, nullable)` per column.
    columns: Vec<(String, String, bool)>,
    rows: u64,
    files: usize,
    bytes: u64,
    manifests: usize,
    snapshot_count: usize,
    last_commit_ms: Option<i64>,
    last_value_index: Option<String>,
    /// Newest first.
    partitions: Vec<PartitionInfo>,
    /// Newest first.
    snapshots: Vec<SnapshotInfo>,
    current_snapshot: Option<i64>,
}

struct PartitionInfo {
    label: String,
    files: usize,
    rows: u64,
    bytes: u64,
}

struct SnapshotInfo {
    id: i64,
    timestamp_ms: i64,
    operation: String,
    added_records: i64,
    deleted_records: i64,
    added_files: i64,
    deleted_files: i64,
    total_records: i64,
}

impl TableInfo {
    pub(crate) async fn load(t: &Table) -> Result<Self> {
        let (files, manifests) = futures::try_join!(t.files(), t.manifests())?;
        let contract = t.contract().clone();

        let mut by_partition: BTreeMap<Option<i32>, PartitionInfo> = BTreeMap::new();
        for f in &files {
            let p = file_partition(&f.data_file);
            let label = match (p, contract.chunk) {
                (Some(v), Some(chunk)) => chunk.label(v),
                _ => "-".to_string(),
            };
            let entry = by_partition.entry(p).or_insert(PartitionInfo { label, files: 0, rows: 0, bytes: 0 });
            entry.files += 1;
            entry.rows += f.data_file.record_count();
            entry.bytes += f.data_file.file_size_in_bytes();
        }

        let mut snapshots: Vec<_> = t.metadata().snapshots().collect();
        snapshots.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms()));
        let snapshots = snapshots
            .iter()
            .take(JSON_SNAPSHOTS)
            .map(|s| {
                let p = &s.summary().additional_properties;
                let count = |k: &str| p.get(k).and_then(|v| v.parse().ok()).unwrap_or(0);
                SnapshotInfo {
                    id: s.snapshot_id(),
                    timestamp_ms: s.timestamp_ms(),
                    operation: s.summary().operation.as_str().to_string(),
                    added_records: count(summary::ADDED_RECORDS),
                    deleted_records: count(summary::DELETED_RECORDS),
                    added_files: count(summary::ADDED_DATA_FILES),
                    deleted_files: count(summary::DELETED_DATA_FILES),
                    total_records: count(summary::TOTAL_RECORDS),
                }
            })
            .collect();

        Ok(Self {
            name: t.name().to_string(),
            version: t.version(),
            format_version: t.metadata().format_version() as u8,
            location: t.location().to_string(),
            columns: t
                .schema()
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().to_string(), f.is_nullable()))
                .collect(),
            rows: files.iter().map(|f| f.data_file.record_count()).sum(),
            files: files.len(),
            bytes: files.iter().map(|f| f.data_file.file_size_in_bytes()).sum(),
            manifests: manifests.len(),
            snapshot_count: t.metadata().snapshots().len(),
            last_commit_ms: t.current_snapshot().map(|s| s.timestamp_ms()),
            last_value_index: t.last_value_index(),
            partitions: by_partition.into_values().rev().collect(),
            snapshots,
            current_snapshot: t.metadata().current_snapshot_id(),
            contract,
        })
    }

    pub(crate) fn to_json(&self) -> Value {
        let c = &self.contract;
        json!({
            "name": self.name,
            "version": self.version,
            "location": self.location,
            "format_version": self.format_version,
            "time_column": c.time_column,
            "series_key": c.series_key,
            "chunk": c.chunk.map(Chunk::as_str),
            "dedup": c.dedup,
            "ingest_time_column": c.ingest_time_column,
            "retention": c.retention.map(format_duration),
            "schema": self.columns.iter().map(|(name, ty, nullable)| json!({ "name": name, "type": ty, "nullable": nullable })).collect::<Vec<_>>(),
            "rows": self.rows,
            "files": self.files,
            "bytes": self.bytes,
            "manifests": self.manifests,
            "snapshot_count": self.snapshot_count,
            "last_commit_ms": self.last_commit_ms,
            "last_value_index": self.last_value_index,
            "partitions": self.partitions.iter().map(|p| json!({ "partition": p.label, "files": p.files, "rows": p.rows, "bytes": p.bytes })).collect::<Vec<_>>(),
            "snapshots": self.snapshots.iter().map(|s| json!({
                "id": s.id.to_string(),
                "timestamp_ms": s.timestamp_ms,
                "operation": s.operation,
                "added_records": s.added_records,
                "deleted_records": s.deleted_records,
                "added_files": s.added_files,
                "deleted_files": s.deleted_files,
                "total_records": s.total_records,
            })).collect::<Vec<_>>(),
        })
    }
}

/// Bytes in (decimal) megabytes.
pub(crate) fn megabytes(bytes: u64) -> f64 {
    bytes as f64 / 1e6
}

impl fmt::Display for TableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = &self.contract;
        writeln!(f, "table      {} (v{}, Iceberg v{})", self.name, self.version, self.format_version)?;
        writeln!(f, "location   {}", self.location)?;
        if let Some(time_column) = &c.time_column {
            writeln!(f, "time       {time_column} (chunk {})", c.chunk.map_or("-", |c| c.as_str()))?;
            let key = if c.series_key.is_empty() { "-".to_string() } else { c.series_key.join(", ") };
            writeln!(f, "series key {key}")?;
            writeln!(f, "dedup      {}", c.dedup)?;
            writeln!(f, "inserted   {}", c.ingest_time_column.as_deref().unwrap_or("-"))?;
            let retention = c.retention.map_or_else(|| "forever".to_string(), format_duration);
            writeln!(f, "retention  {retention}")?;
        } else {
            writeln!(f, "kind       plain table (no time column)")?;
        }
        let columns: Vec<String> = self.columns.iter().map(|(name, ty, _)| format!("{name} {ty}")).collect();
        writeln!(f, "schema     {}", columns.join(", "))?;
        writeln!(f, "data       {} rows in {} files, {:.1} MB", self.rows, self.files, megabytes(self.bytes))?;
        writeln!(f, "format     {} (timelake.format-version)", c.format_version)?;
        writeln!(f, "snapshots  {} (manifests in current: {})", self.snapshot_count, self.manifests)?;
        for s in self.snapshots.iter().take(TEXT_SNAPSHOTS) {
            let when = chrono::DateTime::from_timestamp_millis(s.timestamp_ms).map(|t| t.format("%Y-%m-%dT%H:%M:%SZ"));
            let current = if Some(s.id) == self.current_snapshot { "  ← current" } else { "" };
            let when = when.map(|w| w.to_string()).unwrap_or_default();
            writeln!(f, "  {:<20} {when} {:<8} {:>12} rows{current}", s.id, s.operation, s.total_records)?;
        }
        writeln!(f, "last-value {}", self.last_value_index.as_deref().unwrap_or("-"))?;
        if c.chunk.is_some() {
            writeln!(f, "partitions {}", self.partitions.len())?;
            for p in self.partitions.iter().take(TEXT_PARTITIONS) {
                writeln!(
                    f,
                    "  {:<14} {:>5} files {:>12} rows {:>10.1} MB",
                    p.label,
                    p.files,
                    p.rows,
                    megabytes(p.bytes)
                )?;
            }
            if self.partitions.len() > TEXT_PARTITIONS {
                writeln!(f, "  … {} older", self.partitions.len() - TEXT_PARTITIONS)?;
            }
        }
        Ok(())
    }
}
