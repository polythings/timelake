//! `timelake info`: one table's contract, size and layout, as text or JSON.

use std::fmt;

use timelake::{TableInfo, format_duration};

/// Partitions listed in text output; JSON lists all.
const TEXT_PARTITIONS: usize = 10;
/// Snapshots shown in the text output, newest first (the JSON lists more).
const TEXT_SNAPSHOTS: usize = 5;

/// Bytes in (decimal) megabytes.
pub(crate) fn megabytes(bytes: u64) -> f64 {
    bytes as f64 / 1e6
}

/// [`TableInfo`] as `timelake info` prints it.
pub(crate) struct Text<'a>(pub(crate) &'a TableInfo);

impl fmt::Display for Text<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let info = self.0;
        writeln!(f, "table      {} (v{}, Iceberg v{})", info.name, info.version, info.format_version)?;
        writeln!(f, "location   {}", info.location)?;
        if let Some(time_column) = &info.time_column {
            writeln!(f, "time       {time_column} (chunk {})", info.chunk.map_or("-", |c| c.as_str()))?;
            let key = if info.series_key.is_empty() { "-".to_string() } else { info.series_key.join(", ") };
            writeln!(f, "series key {key}")?;
            writeln!(f, "dedup      {}", info.dedup)?;
            writeln!(f, "inserted   {}", info.ingest_time_column.as_deref().unwrap_or("-"))?;
            let retention = info.retention.map_or_else(|| "forever".to_string(), format_duration);
            writeln!(f, "retention  {retention}")?;
        } else {
            writeln!(f, "kind       plain table (no time column)")?;
        }
        let columns: Vec<String> = info.schema.iter().map(|c| format!("{} {}", c.name, c.data_type)).collect();
        writeln!(f, "schema     {}", columns.join(", "))?;
        writeln!(f, "data       {} rows in {} files, {:.1} MB", info.rows, info.files, megabytes(info.bytes))?;
        writeln!(f, "format     {} (timelake.format-version)", info.timelake_format_version)?;
        writeln!(f, "snapshots  {} (manifests in current: {})", info.snapshot_count, info.manifests)?;
        for s in info.snapshots.iter().take(TEXT_SNAPSHOTS) {
            let when = chrono::DateTime::from_timestamp_millis(s.timestamp_ms).map(|t| t.format("%Y-%m-%dT%H:%M:%SZ"));
            let current = if Some(s.id) == info.current_snapshot { "  ← current" } else { "" };
            let when = when.map(|w| w.to_string()).unwrap_or_default();
            writeln!(f, "  {:<20} {when} {:<8} {:>12} rows{current}", s.id, s.operation, s.total_records)?;
        }
        writeln!(f, "last-value {}", info.last_value_index.as_deref().unwrap_or("-"))?;
        if info.chunk.is_some() {
            writeln!(f, "partitions {}", info.partitions.len())?;
            for p in info.partitions.iter().take(TEXT_PARTITIONS) {
                writeln!(
                    f,
                    "  {:<14} {:>5} files {:>12} rows {:>10.1} MB",
                    p.label,
                    p.files,
                    p.rows,
                    megabytes(p.bytes)
                )?;
            }
            if info.partitions.len() > TEXT_PARTITIONS {
                writeln!(f, "  … {} older", info.partitions.len() - TEXT_PARTITIONS)?;
            }
        }
        Ok(())
    }
}
