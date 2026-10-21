//! Commits for the shared write path: the engine's commit protocol (`timelake::commit`), run by
//! lakekit's (`lakekit_iceberg::commit`) with Timelake's parts as hooks. Every file of a snapshot
//! (manifests, manifest list, last-value index) is uploaded at once, then `vN+1.metadata.json` is
//! written only if absent. When another writer got there first, the commit reloads the table (from
//! the head when it is current) and tries again on top of it. The version hint and the head are
//! written after the commit; failures there are ignored, as readers don't rely on them.

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;

use arrow_array::{Int64Array, RecordBatch};
use arrow_cast::cast;
use arrow_ord::cmp::gt_eq;
use arrow_schema::DataType;
use arrow_select::filter::filter_record_batch;
use lakekit_iceberg::{CommitHooks, CommitOptions, Extra, Table};

use crate::error::{WriteError, WriteResult};
use crate::iceberg::{DataFile, Entry};
use crate::metadata::TimelakeMetadata;
use crate::rows::merge_latest;
use crate::store::{Env, Store, uuid_v4};
use crate::summary;
use crate::table::{TableState, encode_index};

pub(crate) use lakekit_iceberg::commit::put_metadata;

/// What a commit changes.
#[derive(Clone, Debug)]
pub enum Change {
    /// New data files, and the newest row per series among them (for the last-value index).
    Append {
        /// The files.
        files: Vec<DataFile>,
        /// Newest row per series in them.
        latest: Option<RecordBatch>,
    },
    /// Remove files and add files with the same rows (compaction). Each added file keeps the data
    /// sequence number in `sequence_numbers`.
    Replace {
        /// Files removed.
        removed: Vec<Entry>,
        /// Files added.
        added: Vec<DataFile>,
        /// Data sequence number of each added file.
        sequence_numbers: Vec<i64>,
    },
    /// Remove files (retention). The last-value index drops series older than `lvi_cutoff` (µs).
    Delete {
        /// Files removed.
        removed: Vec<Entry>,
        /// Retention cutoff for the last-value index.
        lvi_cutoff: Option<i64>,
    },
}

impl Change {
    /// The change as lakekit commits it.
    fn iceberg(&self) -> lakekit_iceberg::Change {
        match self {
            Self::Append { files, .. } => lakekit_iceberg::Change::Append { files: files.clone() },
            Self::Replace { removed, added, sequence_numbers } => lakekit_iceberg::Change::Replace {
                removed: removed.clone(),
                added: added.clone(),
                sequence_numbers: sequence_numbers.clone(),
            },
            Self::Delete { removed, .. } => lakekit_iceberg::Change::Delete { removed: removed.clone() },
        }
    }
}

/// How a commit went.
#[derive(Clone, Debug, Default)]
pub struct CommitInfo {
    /// The new snapshot (for a duplicate, the current one).
    pub snapshot_id: i64,
    /// The table version after the commit.
    pub version: u64,
    /// Attempts it took.
    pub attempts: u64,
    /// The batch id was already committed; nothing was written.
    pub duplicate: bool,
}

/// Timelake's part in a commit: the table as Timelake loads it, the last-value index, batch ids,
/// and what a rollup reflects.
struct TimelakeHooks<'a, S> {
    /// The table the current attempt builds on.
    state: TableState,
    change: &'a Change,
    props: &'a HashMap<String, String, S>,
    /// The last-value index the current attempt writes.
    index: Option<RecordBatch>,
}

impl<H: Store + Env, S: BuildHasher> CommitHooks<H> for TimelakeHooks<'_, S> {
    type Error = WriteError;

    async fn reload(&mut self, store: &H, location: &str) -> WriteResult<Table> {
        self.state = TableState::load(store, location).await?;
        Ok(self.state.table())
    }

    fn skip(&mut self, base: &Table) -> bool {
        matches!(self.change, Change::Append { .. })
            && self.props.get(summary::BATCH_ID).is_some_and(|id| base.metadata.history_has_batch(id))
    }

    async fn extra(&mut self, store: &H, _: &Table, snapshot_id: i64) -> WriteResult<Extra> {
        let mut extra = Extra::default();
        // What a rollup reflects stays recorded until a refresh records something newer.
        let base_summary = self.state.metadata.summary();
        for key in [summary::ROLLUP_SOURCE_SNAPSHOT, summary::ROLLUP_SOURCE_SEQUENCE] {
            if let (false, Some(v)) = (self.props.contains_key(key), base_summary.get(key)) {
                extra.summary.insert(key.into(), v.clone());
            }
        }
        self.index = new_index(store, &mut self.state, self.change).await?;
        if let Some(index) = &self.index {
            let uri = format!("{}/metadata/lvi-{snapshot_id}-{}.parquet", self.state.location, uuid_v4(store));
            extra.files.push((uri.clone(), encode_index(index)?));
            extra.summary.insert(summary::LAST_VALUE_INDEX.into(), uri);
        }
        Ok(extra)
    }
}

/// Commits `change` on top of the newest version, retrying on conflicts. `props` go into the
/// snapshot summary.
pub async fn commit<H: Store + Env, S: BuildHasher>(
    store: &H,
    base: TableState,
    change: &Change,
    props: &HashMap<String, String, S>,
) -> WriteResult<(TableState, CommitInfo)> {
    let table = base.table();
    let mut hooks = TimelakeHooks { state: base, change, props, index: None };
    let props: HashMap<String, String> = props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let (table, info) =
        lakekit_iceberg::commit(store, table, &change.iceberg(), &props, &CommitOptions::default(), &mut hooks).await?;
    let info = CommitInfo {
        snapshot_id: info.snapshot_id,
        version: info.version,
        attempts: info.attempts,
        duplicate: info.skipped,
    };
    let base = hooks.state;
    if info.duplicate {
        return Ok((base, info));
    }
    let last_values = match hooks.index {
        Some(batch) => Some(batch),
        None if matches!(change, Change::Replace { .. }) => base.last_values.clone(),
        None => None,
    };
    let mut footers = base.footers;
    let live: HashSet<&str> = table.files.iter().flatten().map(|e| e.file.path.as_str()).collect();
    footers.retain(|path, _| live.contains(path.as_str()));
    let state = TableState {
        contract: table.metadata.contract()?,
        location: table.location,
        version: table.version,
        schema: base.schema,
        metadata: table.metadata,
        files: table.files,
        last_values,
        footers,
    };
    Ok((state, info))
}

/// The last-value index of the new snapshot, if it changes: extended on append, trimmed on delete.
async fn new_index<S: Store>(store: &S, base: &mut TableState, change: &Change) -> WriteResult<Option<RecordBatch>> {
    match change {
        Change::Append { latest: Some(latest), .. } => {
            // Only extend an index that covers all existing rows.
            let rows: u64 =
                base.metadata.summary().get(summary::TOTAL_RECORDS).and_then(|v| v.parse().ok()).unwrap_or(0);
            if base.last_value_index().is_none() && rows > 0 {
                return Ok(None);
            }
            let mut parts: Vec<RecordBatch> = base.last_values(store).await?.into_iter().collect();
            parts.push(latest.clone());
            merge_latest(&base.schema, &base.contract, &parts)
        }
        Change::Append { latest: None, .. } | Change::Replace { .. } => Ok(None),
        Change::Delete { lvi_cutoff, .. } => {
            let (Some(cutoff), Some(time_column)) = (lvi_cutoff, base.contract.time_column.clone()) else {
                return Ok(None);
            };
            let Some(index) = base.last_values(store).await? else { return Ok(None) };
            let times = cast(index.column(index.schema().index_of(&time_column)?), &DataType::Int64)?;
            let keep = gt_eq(&times, &Int64Array::new_scalar(*cutoff))?;
            Ok(Some(filter_record_batch(&index, &keep)?))
        }
    }
}
