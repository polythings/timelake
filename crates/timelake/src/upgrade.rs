//! Bringing a table to the current format (`timelake upgrade`) and undoing it (`rollback`).
//!
//! Every step is an ordinary commit, so nothing is lost: the previous snapshots stay readable,
//! and the upgrade first sets `timelake.keep-snapshots-until` so none of them expires for a while
//! (7 days by default). Within that time, [`Table::rollback`] returns the table to the snapshot it
//! had before the upgrade.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use arrow::compute::concat_batches;
use iceberg::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
use timelake_format::head::NO_HEAD;
use timelake_format::rows::align_batch;
use timelake_format::{FORMAT_VERSION, FormatVersion, duration_millis};

use crate::alter::write_properties;
use crate::commit::{Change, commit, publish_retrying};
use crate::error::{Error, Result};
use crate::maintain::{MaintainOptions, compaction_plan};
use crate::storage::Uploads;
use crate::table::Table;
use crate::{Session, head, property, summary};

/// Options for [`Table::upgrade`].
#[derive(Clone, Debug)]
pub struct UpgradeOptions {
    /// Only work out what would change.
    pub dry_run: bool,
    /// Rewrite files that are unsorted or small, as `maintain` does.
    pub compact: bool,
    /// How long no snapshot may expire, so the upgrade can be rolled back.
    pub keep_snapshots: Duration,
}

impl Default for UpgradeOptions {
    fn default() -> Self {
        Self { dry_run: false, compact: true, keep_snapshots: Duration::from_hours(7 * 24) }
    }
}

/// One change an upgrade makes, in the order it makes them.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UpgradeStep {
    /// Record the table format version, and keep every snapshot until `keep_until_ms`.
    SetFormatVersion {
        /// The format before.
        from: FormatVersion,
        /// The format after: [`FORMAT_VERSION`].
        to: FormatVersion,
    },
    /// Build the last-value index the table lacks (a time-series table written by another engine).
    BuildLastValueIndex,
    /// Rewrite unsorted or small files.
    Compact {
        /// Partitions rewritten.
        partitions: usize,
        /// Data files replaced.
        files: usize,
    },
    /// Write the head object of the final version.
    WriteHead,
}

impl fmt::Display for UpgradeStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SetFormatVersion { from, to } => write!(f, "set table format {from} → {to}"),
            Self::BuildLastValueIndex => f.write_str("build the last-value index"),
            Self::Compact { partitions, files } => {
                write!(f, "rewrite {files} unsorted or small files in {partitions} partitions")
            }
            Self::WriteHead => f.write_str("write the head object"),
        }
    }
}

/// What [`Table::upgrade`] did (or, with `dry_run`, would do).
#[derive(Clone, Debug)]
pub struct UpgradeReport {
    /// The steps, in order; empty if the table was already current.
    pub steps: Vec<UpgradeStep>,
    /// The version before the upgrade.
    pub version_before: u64,
    /// The snapshot before the upgrade: pass it to [`Table::rollback`] to undo it.
    pub snapshot_before: Option<i64>,
    /// No snapshot expires before this time (milliseconds since the epoch), unless the upgrade
    /// had nothing to do.
    pub keep_snapshots_until_ms: Option<i64>,
}

impl Table {
    /// Brings the table to the current format ([`FORMAT_VERSION`]) and layout, without losing
    /// data: every step is a commit, and no snapshot expires for `opts.keep_snapshots`, so
    /// [`Table::rollback`] to `report.snapshot_before` undoes it. Returns the new version.
    pub async fn upgrade(&self, opts: &UpgradeOptions) -> Result<(Table, UpgradeReport)> {
        let table = self.refresh().await?;
        let steps = plan(&table, opts).await?;
        let mut report = UpgradeReport {
            steps,
            version_before: table.version(),
            snapshot_before: table.metadata().current_snapshot_id(),
            keep_snapshots_until_ms: None,
        };
        if opts.dry_run || report.steps.is_empty() {
            return Ok((table, report));
        }
        if report.steps == [UpgradeStep::WriteHead] {
            head::write(&table).await?;
            return Ok((table, report));
        }
        let until = chrono::Utc::now().timestamp_millis().saturating_add(duration_millis(opts.keep_snapshots));
        report.keep_snapshots_until_ms = Some(until);

        // Protect the snapshots before anything else commits, and record the format.
        let mut table = write_properties(
            &table,
            &[
                (property::FORMAT_VERSION.into(), FORMAT_VERSION.to_string()),
                (property::KEEP_SNAPSHOTS_UNTIL.into(), until.to_string()),
            ],
            &[],
        )
        .await?;
        for step in &report.steps {
            table = match step {
                UpgradeStep::BuildLastValueIndex => build_index(&table).await?,
                UpgradeStep::Compact { .. } => {
                    let o = MaintainOptions { retention: false, expire: false, ..Default::default() };
                    table.maintain(o).await?.0
                }
                UpgradeStep::SetFormatVersion { .. } | UpgradeStep::WriteHead => table,
            };
        }
        if report.steps.contains(&UpgradeStep::WriteHead) {
            head::write(&table).await?;
        }
        Ok((table, report))
    }

    /// Makes `snapshot_id`, an earlier snapshot of this table, current again: a new version whose
    /// data is that snapshot's. Later snapshots stay in the history until they expire, so a
    /// rollback can be rolled back too. Returns the new version.
    pub async fn rollback(&self, snapshot_id: i64) -> Result<Table> {
        Ok(publish_retrying(&self.refresh().await?, |base| {
            Box::pin(async move {
                if base.metadata().snapshot_by_id(snapshot_id).is_none() {
                    return Err(Error::NotFound(format!("{}: snapshot {snapshot_id}", base.name())));
                }
                let reference = SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None));
                let metadata = base.next_metadata().set_ref(MAIN_BRANCH, reference)?.build()?.metadata;
                Ok(Some((metadata, None, ())))
            })
        })
        .await?
        .0)
    }
}

/// The steps `table` needs.
async fn plan(table: &Table, opts: &UpgradeOptions) -> Result<Vec<UpgradeStep>> {
    let c = table.contract();
    let mut steps = vec![];
    if c.format_version < FORMAT_VERSION {
        steps.push(UpgradeStep::SetFormatVersion { from: c.format_version, to: FORMAT_VERSION });
    }
    let has_rows = table.summary_value(summary::TOTAL_RECORDS).is_some_and(|n| n != "0");
    if c.is_time_series() && has_rows && table.last_value_index().is_none() {
        steps.push(UpgradeStep::BuildLastValueIndex);
    }
    if opts.compact {
        let o = MaintainOptions { retention: false, expire: false, ..Default::default() };
        let compaction = compaction_plan(table, &o).await?;
        if !compaction.is_empty() {
            let files = compaction.iter().map(|(_, files)| files.len()).sum();
            steps.push(UpgradeStep::Compact { partitions: compaction.len(), files });
        }
    }
    let head_current = head::stored_version(table).await? == Some(table.version());
    if c.head_format != NO_HEAD && (!steps.is_empty() || !head_current) {
        steps.push(UpgradeStep::WriteHead);
    }
    Ok(steps)
}

/// Builds the newest row per series from the data and commits it as the last-value index.
async fn build_index(table: &Table) -> Result<Table> {
    let session = Session::new(table.lake().clone());
    let batches = session.latest(table.name().clone()).await?.collect().await?;
    let schema = table.schema();
    let aligned = batches.iter().map(|b| align_batch(b, &schema)).collect::<Result<Vec<_>, _>>()?;
    let latest = concat_batches(&schema, &aligned)?;
    Ok(commit(table, Change::Index { latest }, HashMap::new(), Uploads::default()).await?.0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::testing::{T0, append_without_index, batch, faulty_lake, opts};
    use crate::{AppendOptions, MaintainOptions};

    fn rows(t: &Table) -> u64 {
        t.summary_value(summary::TOTAL_RECORDS).and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    /// A table as an older Timelake or another engine leaves it: format 0.1.0, no last-value index,
    /// no head, small files.
    async fn old_table() -> (crate::Lake, Table) {
        let (lake, _store) = faulty_lake();
        let mut o = opts();
        o.properties = HashMap::from([(property::HEAD_FORMAT.into(), NO_HEAD.into())]);
        let t = lake.create_table("iot.r", &crate::testing::schema(), o).await.unwrap();
        let unversioned = [(property::FORMAT_VERSION.into(), FormatVersion::UNVERSIONED.to_string())];
        let t = write_properties(&t, &unversioned, &[]).await.unwrap();
        let t = append_without_index(&t, &[batch(&[(T0, 1, 1.0), (T0 + 1, 2, 2.0)])]).await;
        let t = append_without_index(&t, &[batch(&[(T0 + 2, 1, 3.0)])]).await;
        // Writers from now on write heads, as a current Timelake does by default.
        let props = [(property::HEAD_FORMAT.into(), "arrow".to_string())];
        let t = write_properties(&t, &props, &[]).await.unwrap();
        crate::flush().await;
        lake.storage().delete(&timelake_format::layout::head_uri(t.location())).await.unwrap();
        (lake, t)
    }

    #[tokio::test]
    async fn upgrade_brings_a_table_to_the_current_format_and_rolls_back() {
        let (_lake, t) = old_table().await;
        let dry = UpgradeOptions { dry_run: true, ..Default::default() };
        let (same, plan) = t.upgrade(&dry).await.unwrap();
        assert_eq!(
            plan.steps,
            [
                UpgradeStep::SetFormatVersion { from: FormatVersion::UNVERSIONED, to: FORMAT_VERSION },
                UpgradeStep::BuildLastValueIndex,
                UpgradeStep::Compact { partitions: 1, files: 2 },
                UpgradeStep::WriteHead,
            ]
        );
        assert_eq!(same.version(), t.version(), "a dry run commits nothing");

        let (up, report) = t.upgrade(&UpgradeOptions::default()).await.unwrap();
        assert_eq!(report.steps, plan.steps);
        assert_eq!(up.contract().format_version, FORMAT_VERSION);
        assert!(up.last_value_index().is_some());
        assert_eq!((up.files().await.unwrap().len(), rows(&up)), (1, 3));
        assert_eq!(head::stored_version(&up).await.unwrap(), Some(up.version()));
        assert!(up.contract().keep_snapshots_until.is_some());

        // Protected snapshots don't expire, so the rollback finds the old files.
        let (up, m) = up.maintain(MaintainOptions::default()).await.unwrap();
        assert_eq!(m.snapshots_expired, 0);
        let back = up.rollback(report.snapshot_before.unwrap()).await.unwrap();
        assert_eq!((back.files().await.unwrap().len(), rows(&back)), (2, 3));
        assert!(back.last_value_index().is_none());

        // Upgrading again redoes the work; a current table needs nothing.
        let (again, _) = back.upgrade(&UpgradeOptions::default()).await.unwrap();
        let (_, nothing) = again.upgrade(&UpgradeOptions::default()).await.unwrap();
        assert!(nothing.steps.is_empty(), "{:?}", nothing.steps);
    }

    /// After a rollback, a batch from the abandoned snapshots is no longer a duplicate.
    #[tokio::test]
    async fn rolled_back_batches_can_be_appended_again() {
        let (lake, _store) = faulty_lake();
        let t = lake.create_table("iot.r", &crate::testing::schema(), opts()).await.unwrap();
        let o = |id: &str| AppendOptions { batch_id: Some(id.into()), maintain: Some(false), ..Default::default() };
        let (t, _) = t.append(&[batch(&[(T0, 1, 1.0)])], o("a")).await.unwrap();
        let before = t.metadata().current_snapshot_id().unwrap();
        let (t, _) = t.append(&[batch(&[(T0 + 1, 1, 2.0)])], o("b")).await.unwrap();
        let t = t.rollback(before).await.unwrap();
        let (t, r) = t.append(&[batch(&[(T0 + 1, 1, 2.0)])], o("b")).await.unwrap();
        assert_eq!((r.rows, rows(&t)), (1, 2));
        let (_, r) = t.append(&[batch(&[(T0, 1, 1.0)])], o("a")).await.unwrap();
        assert!(r.commit.duplicate);
    }
}
