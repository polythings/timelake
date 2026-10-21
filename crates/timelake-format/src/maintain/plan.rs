//! What maintenance does, decided without I/O for both writers (spec 20): its options and report,
//! which files compaction merges, which partitions retention drops, which snapshots expire, which
//! metadata files go, and what an expiry must not lose. The engine (`timelake::maintain`) and the
//! shared writer ([`super::maintain`]) read files and commit around these.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::hash::BuildHasher;
use std::time::Duration;

use serde::{Serialize, Serializer};

use crate::layout::metadata_version;
use crate::{Contract, duration_micros};

/// Files smaller than this fraction of the target size count as small (worth compacting).
const SMALL_FILE_NUMERATOR: u64 = 3;
const SMALL_FILE_DENOMINATOR: u64 = 4;
/// Tiered compaction groups small files by size: tier `n` holds files of
/// `TIER_BASE × TIER_GROWTH^n` bytes and up (64 KiB, 256 KiB, 1 MiB, …). The idea is Apache
/// Cassandra's size-tiered compaction strategy.
const TIER_BASE_SHIFT: u32 = 16;
const TIER_GROWTH: u64 = 4;
/// A partition whose files total at most this many target sizes is rewritten whole.
const REWRITE_WHOLE_PARTITION_TARGETS: u64 = 2;
/// The most files one merge reads at once: a merge holds a window of each, so this bounds its
/// memory (a row group per input in the engine, a window of pages in WebAssembly, where 24 inputs
/// kept a Worker within 128 MB). A larger group is merged in parts, which later runs merge again.
pub const MAX_MERGE_INPUTS: usize = 24;
/// Inline maintenance expires snapshots in batches of about this many, not after every commit.
const EXPIRE_BATCH: usize = 20;

/// Which rollups a maintenance run brings up to date (spec 27). Only the engine refreshes rollups:
/// the WebAssembly writer leaves them to it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RollupRefresh {
    /// Every rollup (`timelake maintain`).
    #[default]
    Always,
    /// Those whose last refresh is older than their `timelake.rollup-refresh-interval` (inline
    /// maintenance after an append).
    Due,
    /// None.
    Never,
}

/// Options of a maintenance run. The default does everything, rewriting each partition into
/// sorted files of the target size.
#[derive(Clone, Debug)]
pub struct MaintainOptions {
    /// Merge small or unsorted files.
    pub compact: bool,
    /// Drop partitions older than the table's retention.
    pub retention: bool,
    /// Expire old snapshots and delete the files only they used.
    pub expire: bool,
    /// Compact a partition once it has this many small files (default: 2, i.e. whenever useful).
    pub min_small_files: usize,
    /// Only consider these partitions (`None`: all).
    pub partitions: Option<HashSet<Option<i32>>>,
    /// Size-tiered mode (inline maintenance): merge groups of similarly sized small files instead
    /// of rewriting whole partitions, so write amplification stays logarithmic while micro-batches
    /// stream in.
    pub tiered: bool,
    /// In tiered mode, partitions whose end is this much older than the newest data in the table
    /// are rewritten whole once they have two small files, touched by the append or not: data
    /// stops arriving there, and one sorted, duplicate-free file reads fastest.
    pub cold_after: Option<Duration>,
    /// Stop compacting once the files this run read reach this many bytes (at least one group is
    /// compacted); the next run continues.
    pub max_bytes: Option<u64>,
    /// Which of the table's rollups to bring up to date, after compaction.
    pub rollups: RollupRefresh,
}

impl Default for MaintainOptions {
    fn default() -> Self {
        Self {
            compact: true,
            retention: true,
            expire: true,
            min_small_files: 2,
            partitions: None,
            tiered: false,
            cold_after: None,
            max_bytes: None,
            rollups: RollupRefresh::Always,
        }
    }
}

/// The steps of a maintenance run, each on unless turned off: what `timelake maintain --no-…`,
/// Python's `Table.maintain(…)` and the JavaScript writer's `maintain({…})` choose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintainSteps {
    /// Merge small or unsorted files.
    pub compact: bool,
    /// Drop partitions older than the table's retention.
    pub retention: bool,
    /// Refresh every rollup (the engine only).
    pub rollups: bool,
    /// Expire old snapshots.
    pub expire: bool,
}

impl Default for MaintainSteps {
    fn default() -> Self {
        Self { compact: true, retention: true, rollups: true, expire: true }
    }
}

impl From<MaintainSteps> for MaintainOptions {
    fn from(steps: MaintainSteps) -> Self {
        Self {
            compact: steps.compact,
            retention: steps.retention,
            expire: steps.expire,
            rollups: if steps.rollups { RollupRefresh::Always } else { RollupRefresh::Never },
            ..Self::default()
        }
    }
}

impl MaintainOptions {
    /// What maintenance runs inline after an append that touched `partitions`, on a table whose
    /// snapshots were made at `snapshot_times` (ms since the epoch): size tiers of those partitions
    /// and cold ones, retention, rollups that are due, and expiry in batches.
    pub fn inline(
        contract: &Contract,
        snapshot_times: impl IntoIterator<Item = i64>,
        partitions: HashSet<Option<i32>>,
        now_ms: i64,
    ) -> Self {
        let (n, oldest) = snapshot_times.into_iter().fold((0, i64::MAX), |(n, oldest), t| (n + 1, oldest.min(t)));
        let c = contract;
        Self {
            compact: true,
            retention: c.retention.is_some(),
            expire: n >= c.max_snapshots.saturating_add(EXPIRE_BATCH)
                || (n >= c.min_snapshots.saturating_add(EXPIRE_BATCH) && oldest < c.snapshot_cutoff_ms(now_ms)),
            min_small_files: c.compact_min_files,
            partitions: Some(partitions),
            tiered: true,
            cold_after: c.compact_cold_after,
            max_bytes: None,
            rollups: RollupRefresh::Due,
        }
    }
}

/// What a maintenance run did. Serialized with its `duration` as `seconds`, for Python and
/// JavaScript.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct MaintainReport {
    /// Partitions compacted.
    pub partitions_compacted: usize,
    /// Data files replaced by compaction.
    pub files_rewritten: usize,
    /// Data files compaction wrote.
    pub files_written: usize,
    /// Rows read by compaction.
    pub rows_rewritten: u64,
    /// Bytes of the files compaction read.
    pub bytes_rewritten: u64,
    /// Duplicate `(series, time)` rows compaction removed.
    pub duplicates_removed: u64,
    /// Compaction left files a later run merges: it stopped at `max_bytes`, or merged a group of
    /// more than [`MAX_MERGE_INPUTS`] files in parts.
    pub more_to_compact: bool,
    /// Partitions dropped by retention.
    pub partitions_dropped: usize,
    /// Data files dropped by retention.
    pub files_dropped: usize,
    /// Rollups that took new rows.
    pub rollups_refreshed: usize,
    /// Rows appended to them.
    pub rollup_rows: u64,
    /// Snapshots expired.
    pub snapshots_expired: usize,
    /// Objects deleted after expiry (data files, manifests, metadata files).
    pub objects_deleted: usize,
    /// Time taken.
    #[serde(rename = "seconds", serialize_with = "seconds")]
    pub duration: Duration,
}

fn seconds<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_f64(d.as_secs_f64())
}

impl MaintainReport {
    /// Whether nothing changed.
    pub fn is_noop(&self) -> bool {
        self.files_rewritten == 0
            && self.files_dropped == 0
            && self.snapshots_expired == 0
            && self.objects_deleted == 0
            && self.rollups_refreshed == 0
    }

    /// Counts a merge of `files` files of `bytes` bytes that read `rows_in` rows and wrote
    /// `rows_out`.
    pub fn merged(&mut self, files: usize, bytes: u64, rows_in: u64, rows_out: u64) {
        self.files_rewritten = self.files_rewritten.saturating_add(files);
        self.bytes_rewritten = self.bytes_rewritten.saturating_add(bytes);
        self.rows_rewritten = self.rows_rewritten.saturating_add(rows_in);
        self.duplicates_removed = self.duplicates_removed.saturating_add(rows_in.saturating_sub(rows_out));
    }
}

/// A live data file, as compaction planning sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanFile<'a> {
    /// Its URI.
    pub path: &'a str,
    /// Its time partition.
    pub partition: Option<i32>,
    /// Its size in bytes.
    pub size: u64,
    /// Its data sequence number: the order its rows were committed in.
    pub sequence_number: i64,
    /// The table sort order it follows.
    pub sort_order_id: Option<i32>,
    /// The upper bound of its time column (µs since the epoch), if recorded.
    pub newest_time: Option<i64>,
}

/// Files to merge into new ones: a run of one partition's files next to each other in commit
/// order (a file committed between two merged files may rewrite a key they hold, and must stay
/// newer than the merged file).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeGroup {
    /// The partition.
    pub partition: Option<i32>,
    /// Indexes into the planned files, oldest commit first.
    pub files: Vec<usize>,
}

/// What a compaction run merges.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactionPlan {
    /// The merges, each at most [`MAX_MERGE_INPUTS`] files.
    pub groups: Vec<MergeGroup>,
    /// A later run has more to merge (see [`MaintainReport::more_to_compact`]).
    pub more_to_compact: bool,
}

/// Which of `files` (a table's live files) to merge, in a table with `contract` whose current
/// sort order is `sort_order_id`.
///
/// A partition with at least two small files (`min_small_files` in tiered mode), or any file not
/// in the sort order, is compacted. In tiered mode, runs of small files of one size tier are
/// merged; otherwise a partition holding an unsorted file or at most twice the target size is
/// rewritten whole, and a larger one has its runs of small files merged.
pub fn compaction_plan(
    contract: &Contract,
    sort_order_id: Option<i32>,
    files: &[PlanFile<'_>],
    opts: &MaintainOptions,
) -> CompactionPlan {
    let cold = cold_partitions(contract, files, opts);
    let mut by_partition: BTreeMap<Option<i32>, Vec<usize>> = BTreeMap::new();
    for (i, f) in files.iter().enumerate() {
        if opts.partitions.as_ref().is_none_or(|ps| ps.contains(&f.partition)) || cold.contains(&f.partition) {
            by_partition.entry(f.partition).or_default().push(i);
        }
    }
    let mut groups = vec![];
    for (partition, mut run) in by_partition {
        run.sort_by(|&a, &b| (files[a].sequence_number, files[a].path).cmp(&(files[b].sequence_number, files[b].path)));
        let tiered = opts.tiered && !cold.contains(&partition);
        let min_files = if cold.contains(&partition) { 2 } else { opts.min_small_files.max(2) };
        let picked = if tiered {
            size_tiers(contract, files, &run, min_files)
        } else {
            whole_or_small_runs(contract, sort_order_id, files, run, min_files)
        };
        groups.extend(picked.into_iter().map(|files| MergeGroup { partition, files }));
    }
    limit(files, groups, opts.max_bytes)
}

/// Whether a file is small enough to be worth compacting.
fn is_small(contract: &Contract, f: &PlanFile<'_>) -> bool {
    f.size < contract.target_file_size / SMALL_FILE_DENOMINATOR * SMALL_FILE_NUMERATOR
}

/// Tiered mode: runs (in commit order) of at least `min_files` small files of one size tier.
fn size_tiers(contract: &Contract, files: &[PlanFile<'_>], run: &[usize], min_files: usize) -> Vec<Vec<usize>> {
    let tier = |&i: &usize| {
        let f = &files[i];
        is_small(contract, f).then(|| (f.size >> TIER_BASE_SHIFT).max(1).ilog(TIER_GROWTH))
    };
    run.chunk_by(|a, b| tier(a) == tier(b))
        .filter(|r| tier(&r[0]).is_some() && r.len() >= min_files)
        .map(<[usize]>::to_vec)
        .collect()
}

/// Full mode: the whole partition when it holds an unsorted file or at most
/// [`REWRITE_WHOLE_PARTITION_TARGETS`] target sizes, else its runs of small files.
fn whole_or_small_runs(
    contract: &Contract,
    sort_order_id: Option<i32>,
    files: &[PlanFile<'_>],
    run: Vec<usize>,
    min_files: usize,
) -> Vec<Vec<usize>> {
    let small = |i: &usize| is_small(contract, &files[*i]);
    let n_small = run.iter().filter(|i| small(i)).count();
    let unsorted = !contract.sort_columns().is_empty() && run.iter().any(|&i| files[i].sort_order_id != sort_order_id);
    if n_small < min_files && !(unsorted && min_files <= 2) {
        return vec![];
    }
    let total = run.iter().fold(0u64, |t, &i| t.saturating_add(files[i].size));
    if total <= REWRITE_WHOLE_PARTITION_TARGETS.saturating_mul(contract.target_file_size) || unsorted {
        return vec![run];
    }
    run.chunk_by(|a, b| small(a) == small(b)).filter(|r| small(&r[0]) && r.len() >= 2).map(<[usize]>::to_vec).collect()
}

/// In tiered mode, the partitions whose end is `cold_after` or more before the newest time in the
/// table (the files' upper bounds): data has moved on from them.
fn cold_partitions(contract: &Contract, files: &[PlanFile<'_>], opts: &MaintainOptions) -> HashSet<Option<i32>> {
    let (true, Some(cold_after), Some(chunk)) = (opts.tiered, opts.cold_after, contract.chunk) else {
        return HashSet::new();
    };
    let Some(newest) = files.iter().filter_map(|f| f.newest_time).max() else { return HashSet::new() };
    let cutoff = newest.saturating_sub(duration_micros(cold_after));
    let ended = |p: i32| chunk.end_micros(p).is_ok_and(|end| end <= cutoff);
    files.iter().filter_map(|f| f.partition).filter(|&p| ended(p)).map(Some).collect()
}

/// Splits groups larger than a merge takes into balanced parts, consecutive in commit order, and
/// keeps the groups that fit in `max_bytes` (at least one).
fn limit(files: &[PlanFile<'_>], groups: Vec<MergeGroup>, max_bytes: Option<u64>) -> CompactionPlan {
    let mut split = false;
    let mut out: Vec<MergeGroup> = vec![];
    for g in groups {
        let parts = g.files.len().div_ceil(MAX_MERGE_INPUTS);
        split |= parts > 1;
        let size = g.files.len().div_ceil(parts.max(1)).max(1);
        out.extend(g.files.chunks(size).map(|c| MergeGroup { partition: g.partition, files: c.to_vec() }));
    }
    let mut stopped = false;
    if let Some(max) = max_bytes {
        let mut total = 0u64;
        let keep = out
            .iter()
            .take_while(|g| {
                let first = total == 0;
                total = g.files.iter().fold(total, |t, &i| t.saturating_add(files[i].size));
                first || total <= max
            })
            .count();
        stopped = keep < out.len();
        out.truncate(keep);
    }
    CompactionPlan { groups: out, more_to_compact: split || stopped }
}

/// What retention drops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionPlan {
    /// The retention cutoff (µs since the epoch): the last-value index drops series older than it.
    pub cutoff: i64,
    /// Indexes of the files to drop: those of time partitions that end at or before the cutoff.
    pub files: Vec<usize>,
    /// How many partitions they make.
    pub partitions: usize,
}

/// What retention drops at `now_micros` of files in `partitions` (each file's), or `None` if the
/// table keeps its data (no retention, or no time partitions).
pub fn retention_plan(
    contract: &Contract,
    now_micros: i64,
    partitions: impl IntoIterator<Item = Option<i32>>,
) -> Option<RetentionPlan> {
    let (Some(cutoff), Some(chunk)) = (contract.retention_cutoff(now_micros), contract.chunk) else { return None };
    let expired = |p: i32| chunk.end_micros(p).is_ok_and(|end| end <= cutoff);
    let (mut files, mut dropped) = (vec![], HashSet::new());
    for (i, p) in partitions.into_iter().enumerate() {
        if let Some(p) = p.filter(|&p| expired(p)) {
            files.push(i);
            dropped.insert(p);
        }
    }
    Some(RetentionPlan { cutoff, files, partitions: dropped.len() })
}

/// A snapshot, as expiry sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotTime {
    /// Its id.
    pub id: i64,
    /// When it was made (ms since the epoch).
    pub timestamp_ms: i64,
}

/// The ids of the snapshots to expire, newest first: past the newest `min_snapshots`, and either
/// older than the snapshot retention or beyond `max_snapshots`. The `current` snapshot, and any a
/// branch or tag points at (`referenced`, another engine's among them), always stay.
pub fn expired_snapshots<S: BuildHasher>(
    contract: &Contract,
    snapshots: &[SnapshotTime],
    current: Option<i64>,
    referenced: &HashSet<i64, S>,
    now_ms: i64,
) -> Vec<i64> {
    let c = contract;
    if c.keep_snapshots_until.is_some_and(|until| now_ms < until) {
        return vec![];
    }
    let mut newest_first = snapshots.to_vec();
    newest_first.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms));
    let cutoff = c.snapshot_cutoff_ms(now_ms);
    newest_first
        .iter()
        .enumerate()
        .filter(|(i, s)| {
            Some(s.id) != current
                && !referenced.contains(&s.id)
                && *i >= c.min_snapshots
                && (s.timestamp_ms < cutoff || *i >= c.max_snapshots)
        })
        .map(|(_, s)| s.id)
        .collect()
}

/// What expiry does with a manifest list or manifest that isn't there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Missing {
    /// Already deleted by an earlier, interrupted expiry: nothing more to find (the expired
    /// snapshots).
    Skip,
    /// A snapshot that is kept can't be read: an error, so that nothing is deleted.
    Error,
}

impl Missing {
    /// `Ok` to go on without `uri`, which `snapshot` uses, or the error.
    pub fn check(self, snapshot: i64, uri: &str) -> Result<(), MissingObject> {
        match self {
            Self::Skip => Ok(()),
            Self::Error => Err(MissingObject { snapshot, uri: uri.to_string() }),
        }
    }
}

/// A manifest list or manifest a kept snapshot uses is missing, so expiry deletes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MissingObject {
    /// The snapshot.
    pub snapshot: i64,
    /// The missing object.
    pub uri: String,
}

impl fmt::Display for MissingObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { snapshot, uri } = self;
        write!(
            f,
            "{uri}, used by snapshot {snapshot}, is missing, so expiry deletes nothing; roll back past the snapshot or \
             restore the file"
        )
    }
}

/// The metadata files (of `listed`, the URIs under `layout::metadata_prefix`) that expiry deletes
/// from a table at `version` whose metadata log has `log_len` entries: those older than the log.
pub fn pruned_metadata(listed: impl IntoIterator<Item = String>, version: u64, log_len: usize) -> Vec<String> {
    let keep_from = version.saturating_sub(u64::try_from(log_len).unwrap_or(u64::MAX).saturating_add(1));
    listed.into_iter().filter(|uri| metadata_version(uri).is_some_and(|v| v < keep_from)).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{Chunk, property};

    const MB: u64 = 1 << 20;
    const DAY: i64 = 86_400_000_000;

    fn contract(props: &[(&str, &str)]) -> Contract {
        let mut p: HashMap<String, String> = [
            (property::TIME_COLUMN, "time"),
            (property::SERIES_KEY, "device"),
            (property::CHUNK, "day"),
            (property::TARGET_FILE_SIZE, "100000000"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
        p.extend(props.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())));
        Contract::from_properties(&p).unwrap()
    }

    fn file(path: &'static str, partition: i32, size: u64, sequence_number: i64) -> PlanFile<'static> {
        PlanFile {
            path,
            partition: Some(partition),
            size,
            sequence_number,
            sort_order_id: Some(1),
            newest_time: Chunk::Day.start_micros(partition).ok(),
        }
    }

    fn groups(plan: &CompactionPlan) -> Vec<Vec<usize>> {
        plan.groups.iter().map(|g| g.files.clone()).collect()
    }

    #[test]
    fn full_compaction_rewrites_small_partitions_whole_and_runs_of_small_files_otherwise() {
        let c = contract(&[]);
        let opts = MaintainOptions::default();
        // In commit order: 0, 2, 1. A single small file is left alone.
        let files = [file("a", 1, MB, 1), file("b", 1, MB, 3), file("c", 1, MB, 2), file("d", 2, MB, 1)];
        let plan = compaction_plan(&c, Some(1), &files, &opts);
        assert_eq!(groups(&plan), [vec![0, 2, 1]]);
        assert!(!plan.more_to_compact);

        // Over twice the target: only the runs of small files, never across a large one.
        let big = 90 * MB;
        let files = [
            file("a", 1, MB, 1),
            file("b", 1, MB, 2),
            file("c", 1, big, 3),
            file("d", 1, MB, 4),
            file("e", 1, big, 5),
            file("f", 1, big, 6),
        ];
        assert_eq!(groups(&compaction_plan(&c, Some(1), &files, &opts)), [vec![0, 1]]);
        // An unsorted file rewrites the whole partition.
        let mut unsorted = files;
        unsorted[4].sort_order_id = None;
        assert_eq!(groups(&compaction_plan(&c, Some(1), &unsorted, &opts)), [vec![0, 1, 2, 3, 4, 5]]);
    }

    #[test]
    fn tiered_compaction_merges_runs_of_one_size_tier() {
        let c = contract(&[]);
        let opts = MaintainOptions { tiered: true, min_small_files: 3, ..MaintainOptions::default() };
        let tiny = 1000;
        let files = [
            file("a", 1, tiny, 1),
            file("b", 1, tiny, 2),
            file("c", 1, tiny, 3),
            file("d", 1, 4 * MB, 4),
            file("e", 1, tiny, 5),
            file("f", 1, tiny, 6),
        ];
        assert_eq!(groups(&compaction_plan(&c, Some(1), &files, &opts)), [vec![0, 1, 2]]);
    }

    #[test]
    fn cold_partitions_are_rewritten_whole_once_data_moves_on() {
        let c = contract(&[]);
        let day = |d: i64| i32::try_from(d).unwrap();
        let mut opts = MaintainOptions {
            tiered: true,
            min_small_files: 8,
            cold_after: Some(Duration::from_secs(3600)),
            partitions: Some([Some(day(3))].into()),
            ..MaintainOptions::default()
        };
        let mut files = vec![file("a", day(1), 1000, 1), file("b", day(1), 1000, 2), file("c", day(3), 1000, 3)];
        files[2].newest_time = Some(3 * DAY + 1);
        assert_eq!(groups(&compaction_plan(&c, Some(1), &files, &opts)), [vec![0, 1]]);
        opts.cold_after = None;
        assert_eq!(compaction_plan(&c, Some(1), &files, &opts).groups, []);
    }

    #[test]
    fn large_groups_are_merged_in_parts_and_runs_stop_at_max_bytes() {
        let c = contract(&[]);
        let paths: Vec<String> = (0..50).map(|i| format!("f{i:02}")).collect();
        let files: Vec<PlanFile<'_>> = paths
            .iter()
            .zip(0..)
            .map(|(p, i)| PlanFile {
                path: p,
                partition: Some(1),
                size: MB,
                sequence_number: i,
                sort_order_id: Some(1),
                newest_time: None,
            })
            .collect();
        let plan = compaction_plan(&c, Some(1), &files, &MaintainOptions::default());
        let sizes: Vec<usize> = plan.groups.iter().map(|g| g.files.len()).collect();
        assert_eq!(sizes, [17, 17, 16], "balanced parts of at most {MAX_MERGE_INPUTS}");
        assert_eq!(plan.groups.iter().flat_map(|g| g.files.clone()).collect::<Vec<_>>(), (0..50).collect::<Vec<_>>());
        assert!(plan.more_to_compact);

        let capped = MaintainOptions { max_bytes: Some(20 * MB), ..MaintainOptions::default() };
        let plan = compaction_plan(&c, Some(1), &files[..20], &capped);
        assert_eq!((plan.groups.len(), plan.more_to_compact), (1, false));
        let capped = MaintainOptions { max_bytes: Some(MB), ..MaintainOptions::default() };
        let plan = compaction_plan(&c, Some(1), &files, &capped);
        assert_eq!((plan.groups.len(), plan.more_to_compact), (1, true), "at least one group");
    }

    #[test]
    fn retention_drops_partitions_that_ended_before_the_cutoff() {
        let c = contract(&[(property::RETENTION, "2d")]);
        let now = 10 * DAY;
        let plan = retention_plan(&c, now, [Some(7), Some(8), Some(7), Some(5), None]).unwrap();
        assert_eq!((plan.cutoff, plan.files, plan.partitions), (8 * DAY, vec![0, 2, 3], 2));
        assert_eq!(retention_plan(&contract(&[]), now, [Some(1)]), None);
    }

    #[test]
    fn expiry_keeps_the_newest_the_current_and_referenced_snapshots() {
        let c = contract(&[(property::MIN_SNAPSHOTS, "2"), (property::MAX_SNAPSHOTS, "4")]);
        let now = 10_000_000;
        let snapshots: Vec<SnapshotTime> =
            (1..=6).map(|id| SnapshotTime { id, timestamp_ms: now - 1000 * (7 - id) }).collect();
        // All within the retention (1 hour): only those past max_snapshots go.
        assert_eq!(expired_snapshots(&c, &snapshots, Some(6), &HashSet::<i64>::new(), now), [2, 1]);
        assert_eq!(expired_snapshots(&c, &snapshots, Some(6), &HashSet::from([1]), now), [2]);
        // Older than the retention: all past min_snapshots, but not the current one.
        let later = now + 3_600_000 * 2;
        assert_eq!(expired_snapshots(&c, &snapshots, Some(3), &HashSet::<i64>::new(), later), [4, 2, 1]);
        let kept = contract(&[(property::KEEP_SNAPSHOTS_UNTIL, &(later + 1).to_string())]);
        assert_eq!(expired_snapshots(&kept, &snapshots, Some(6), &HashSet::<i64>::new(), later), Vec::<i64>::new());
    }

    #[test]
    fn old_metadata_files_beyond_the_log_are_pruned() {
        let listed = (1..=6).map(|v| format!("s3://b/t/metadata/v{v}.metadata.json"));
        let pruned = pruned_metadata(listed.chain(["s3://b/t/metadata/version-hint.text".into()]), 6, 2);
        assert_eq!(pruned, ["s3://b/t/metadata/v1.metadata.json", "s3://b/t/metadata/v2.metadata.json"]);
    }

    #[test]
    fn missing_objects_are_skipped_only_for_expired_snapshots() {
        assert_eq!(Missing::Skip.check(1, "m.avro"), Ok(()));
        let err = Missing::Error.check(1, "m.avro").unwrap_err();
        assert!(err.to_string().starts_with("m.avro, used by snapshot 1, is missing"), "{err}");
    }

    #[test]
    fn inline_maintenance_expires_in_batches() {
        let c = contract(&[(property::MAX_SNAPSHOTS, "10"), (property::MIN_SNAPSHOTS, "2")]);
        let now = 1_000_000_000;
        let times = |n: usize| (0..n).map(|_| now);
        assert!(!MaintainOptions::inline(&c, times(29), HashSet::new(), now).expire);
        assert!(MaintainOptions::inline(&c, times(30), HashSet::new(), now).expire);
        let old = MaintainOptions::inline(&c, (0..22).map(|_| 0), HashSet::new(), now);
        assert!(old.expire && old.tiered && old.rollups == RollupRefresh::Due);
    }

    #[test]
    fn steps_turn_off_what_callers_skip() {
        let o = MaintainOptions::from(MaintainSteps { rollups: false, expire: false, ..MaintainSteps::default() });
        assert!(o.compact && o.retention && !o.expire && o.rollups == RollupRefresh::Never);
    }

    #[test]
    fn reports_serialize_their_duration_in_seconds() {
        let r = MaintainReport { duration: Duration::from_millis(1500), ..MaintainReport::default() };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["seconds"], 1.5);
        assert_eq!(json["more_to_compact"], false);
    }
}
