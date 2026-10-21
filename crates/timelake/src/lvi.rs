//! Last-value index: one small Parquet file per snapshot holding the latest row of every series.
//!
//! "Current state of every device" is the most common IoT query: a generic engine scans everything
//! to find each series' newest row (34–317 ms in the first benchmark runs, spec 44, against 2–3 ms
//! in TimescaleDB). The index makes it a single small read. It is maintained incrementally on
//! every append (previous index ⊕ newest rows of the appended data), referenced from the snapshot
//! summary (`timelake.last-value-index`), and plain Parquet, so `DuckDB` can read it too.

use arrow::array::RecordBatch;

use crate::error::Result;
use crate::table::Table;

/// Reads an index (from the lake's cache when possible).
pub(crate) async fn read(table: &Table, uri: &str) -> Result<RecordBatch> {
    let cache = &table.lake().caches().last_value_indexes;
    if let Some(batch) = cache.get(uri) {
        return Ok(batch);
    }
    let bytes = table.lake().storage().read(uri).await?;
    let batch = timelake_format::lvi::decode(bytes, &table.schema())?;
    cache.insert(uri.to_string(), batch.clone());
    Ok(batch)
}

/// A fresh location for the index of a new snapshot.
pub(crate) fn new_uri(table: &Table, snapshot_id: i64) -> String {
    timelake_format::layout::last_value_index_uri(table.location(), snapshot_id, uuid::Uuid::new_v4())
}

/// The index for a new snapshot: the base snapshot's index merged with the newest rows of the
/// appended data. `None` if the table has no time column.
pub(crate) async fn merged(base: &Table, delta: &RecordBatch) -> Result<Option<RecordBatch>> {
    let mut parts = vec![];
    if let Some(uri) = base.last_value_index() {
        parts.push(read(base, &uri).await?);
    }
    parts.push(delta.clone());
    Ok(timelake_format::rows::merge_latest(&base.schema(), base.contract(), &parts)?)
}
