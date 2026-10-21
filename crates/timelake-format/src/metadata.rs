//! Iceberg table metadata through lakebed (`lakebed_iceberg::Metadata`: read as JSON, so fields
//! Timelake doesn't know are kept, and updated the way iceberg-rust's metadata builder updates it),
//! and what Timelake reads from it: the contract, batch ids, and last-value indexes.

pub use lakebed_iceberg::arrow::FIELD_ID_KEY;
pub use lakebed_iceberg::metadata::{Metadata, NewSnapshot};

use crate::error::WriteResult;
use crate::{Contract, summary};

/// A snapshot as expiry needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef {
    /// Its id.
    pub id: i64,
    /// When it was made (ms since the epoch).
    pub timestamp_ms: i64,
    /// Its manifest list.
    pub manifest_list: String,
    /// Its last-value index.
    pub last_value_index: Option<String>,
}

/// What Timelake reads from table metadata.
pub trait TimelakeMetadata {
    /// The Timelake contract in the properties.
    fn contract(&self) -> WriteResult<Contract>;
    /// Whether the current snapshot or one of its ancestors committed `batch_id`
    /// (`timelake.batch-id`).
    fn history_has_batch(&self, batch_id: &str) -> bool;
    /// Every snapshot with a manifest list: id, timestamp, manifest list, and last-value index.
    fn snapshot_list(&self) -> Vec<SnapshotRef>;
}

impl TimelakeMetadata for Metadata {
    fn contract(&self) -> WriteResult<Contract> {
        Ok(Contract::from_properties(&self.properties())?)
    }

    fn history_has_batch(&self, batch_id: &str) -> bool {
        self.ancestors().iter().any(|s| s.summary.get(summary::BATCH_ID).map(String::as_str) == Some(batch_id))
    }

    fn snapshot_list(&self) -> Vec<SnapshotRef> {
        self.snapshots()
            .into_iter()
            .filter_map(|s| {
                Some(SnapshotRef {
                    id: s.snapshot_id,
                    timestamp_ms: s.timestamp_ms,
                    last_value_index: s.summary.get(summary::LAST_VALUE_INDEX).cloned(),
                    manifest_list: s.manifest_list?,
                })
            })
            .collect()
    }
}
