//! Concurrent commits: writers that lose the race rebase or report a conflict, and leave no
//! files behind.

#![expect(clippy::unwrap_used, reason = "test code")]

pub mod common;

use std::path::Path;

use common::{T0, batch, files_under, no_maintenance, opts, scalar_i64, schema, temp_lake};
use timelake::{AppendOptions, Error, MaintainOptions, Session};

fn compact_only() -> MaintainOptions {
    MaintainOptions { retention: false, expire: false, ..MaintainOptions::default() }
}

/// How many files under `dir` have a name containing `pattern`.
fn count(dir: &Path, pattern: &str) -> usize {
    files_under(dir).iter().filter(|p| p.file_name().unwrap().to_str().unwrap().contains(pattern)).count()
}

/// Compaction planned against an old version still commits after an append: the files it
/// replaces are all still there.
#[tokio::test]
async fn compaction_rebases_over_a_concurrent_append() {
    let (_dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let (t, _) = t.append(&[batch(T0, 1, 1, 2, |_, _| 1.0)], no_maintenance()).await.unwrap();
    let (stale, _) = t.append(&[batch(T0 + 1, 1, 1, 2, |_, _| 2.0)], no_maintenance()).await.unwrap();
    let (latest, _) = stale.append(&[batch(T0 + 2, 1, 1, 2, |_, _| 3.0)], no_maintenance()).await.unwrap();

    // Compact the first two files from the stale version: rebases over the third append.
    let (after, report) = stale.maintain(compact_only()).await.unwrap();
    assert_eq!(report.files_rewritten, 2);
    assert_eq!(after.version(), latest.version() + 1);
    assert_eq!(after.files().await.unwrap().len(), 2);
    assert_eq!(scalar_i64(&Session::new(lake), "SELECT count(*) FROM iot.r").await, 6);
}

/// Two compactions of the same files: the second finds them gone, reports a conflict, and
/// deletes what it wrote.
#[tokio::test]
async fn compactions_of_the_same_files_conflict() {
    let (dir, lake) = temp_lake();
    let t = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let (t, _) = t.append(&[batch(T0, 1, 1, 2, |_, _| 1.0)], no_maintenance()).await.unwrap();
    let (t, _) = t.append(&[batch(T0 + 1, 1, 1, 2, |_, _| 2.0)], no_maintenance()).await.unwrap();

    let (first, _) = t.maintain(compact_only()).await.unwrap();
    let err = t.maintain(compact_only()).await.unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "{err}");

    // The two original files (not expired yet) and the first compaction's output; nothing from
    // the second.
    assert_eq!(count(&dir.path().join("iot/r/data"), ".parquet"), 3);
    assert_eq!(first.files().await.unwrap().len(), 1);
    assert_eq!(scalar_i64(&Session::new(lake), "SELECT count(*) FROM iot.r").await, 4);
}

/// A writer that loses the race to a commit with the same batch id finds it on retry and
/// writes nothing.
#[tokio::test]
async fn batch_id_is_checked_again_after_a_lost_race() {
    let (dir, lake) = temp_lake();
    let base = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let rows = batch(T0, 1, 1, 3, |_, _| 1.0);
    let id = AppendOptions { batch_id: Some("b1".into()), ..no_maintenance() };

    let (winner, _) = base.append(std::slice::from_ref(&rows), id.clone()).await.unwrap();
    let (loser, r) = base.append(&[rows], id).await.unwrap();
    assert!(r.commit.duplicate);
    assert_eq!((r.commit.attempts, r.rows), (2, 0));
    assert_eq!(loser.version(), winner.version());

    assert_eq!(count(&dir.path().join("iot/r/data"), ".parquet"), 1);
    assert_eq!(count(&dir.path().join("iot/r/metadata"), "lvi-"), 1);
    assert_eq!(scalar_i64(&Session::new(lake), "SELECT count(*) FROM iot.r").await, 3);
}

/// The losing attempt's manifests, manifest list and last-value index are deleted before it
/// rebases; only the files of the two committed snapshots remain.
#[tokio::test]
async fn lost_race_leaves_no_files() {
    let (dir, lake) = temp_lake();
    let base = lake.create_table("iot.r", &schema(), opts()).await.unwrap();
    let (_, first) = base.append(&[batch(T0, 1, 1, 3, |_, _| 1.0)], no_maintenance()).await.unwrap();
    let (t, second) = base.append(&[batch(T0 + 1, 1, 1, 3, |_, _| 2.0)], no_maintenance()).await.unwrap();
    assert_eq!((first.commit.attempts, second.commit.attempts), (1, 2));
    assert_eq!(t.metadata().snapshots().len(), 2);

    let metadata = dir.path().join("iot/r/metadata");
    assert_eq!(count(&metadata, "-m.avro"), 2);
    assert_eq!(count(&metadata, "snap-"), 2);
    assert_eq!(count(&metadata, "lvi-"), 2);
    assert_eq!(count(&dir.path().join("iot/r/data"), ".parquet"), 2);
    assert_eq!(scalar_i64(&Session::new(lake), "SELECT count(*) FROM iot.r").await, 6);
}
