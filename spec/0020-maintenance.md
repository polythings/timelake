---
number: 20
title: Maintenance runs, retention and expiry
area: Maintenance
status: in-progress
updated: 2026-10-20
versions: [0.1.0, 0.3.0, 0.5.0, unreleased]
related: [13, 19, 27]
issues: [13, 18, 50]
---

# 20. Maintenance runs, retention and expiry

**In progress** · updated 2026-10-20 · versions 0.1.0, 0.3.0, 0.5.0, unreleased · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Tuned on real data](milestones/real-data.md), [Technical preview](milestones/technical-preview.md)
· related: [spec 13](0013-wasm-writes.md), [spec 19](0019-compaction.md), [spec 27](0027-rollups.md)

## Summary

The jobs Timelake replaces Glue with (compaction, rollup refresh, retention, snapshot expiry and file cleanup) run
inline after appends when thresholds are met, or as one stateless `timelake maintain` command. Nothing outside
Timelake has to be scheduled. Orphan-file cleanup and sharded runs are open.

## Design

`timelake-maintain` runs as a set of idempotent, independently schedulable tasks:

| Task | What it does | Status |
|------|-------------|--------|
| Compaction | [spec 19](0019-compaction.md) | Built |
| Rollup refresh | Incremental, from the raw table's changed buckets ([spec 27](0027-rollups.md)) | Built (0.5.0) |
| Retention | Per-table TTL (e.g. raw 90 d, 1-min rollup 2 y, 1-h rollup forever). Drops whole partitions (metadata-only when aligned) | Built |
| Snapshot expiry | Expires old snapshots and deletes the files only they reference; `timelake.keep-snapshots-until` keeps them longer (0.3.0, for upgrades). What the kept snapshots use is read in full first: if a manifest list or manifest of one is missing, expiry stops with an error naming it and deletes nothing | Built |
| Orphan cleanup | Deletes files no snapshot references (#50) | Planned |
| Manifest rewrite | Clusters manifests by time so planning stays fast as tables grow | Planned |

- **Inline:** after an append, `auto-maintain` runs the tasks whose thresholds are met (`compact-min-files`,
  `compact-cold-after`, `rollup-refresh-interval`, …), so small deployments need nothing else.
- **`timelake maintain`** (`Table.maintain()`) runs a full pass: every partition rewritten into one file, every rollup
  refreshed (`--no-rollups` to skip), retention and expiry. Stateless: a cron job, a Kubernetes CronJob, a Lambda, or
  triggered by commit events (catalog events, S3 notifications).
- **Concurrency:** safe through Iceberg optimistic commits and conflict-aware retries ([spec 2](0002-commits.md)).
- **Limits per run:** a setting caps the bytes one run rewrites (needed on Workers, where a request has at most 5
  minutes of CPU; useful anywhere inline maintenance shouldn't hold up appends). A run that stops at the cap commits
  what it rewrote; the next run continues. Built in the shared (WASM) maintenance ([spec 13](0013-wasm-writes.md)).

## Open questions and issues

- [#13](https://github.com/polythings/timelake/issues/13) Maintenance: wait a minimum age before deleting files of expired snapshots
- [#18](https://github.com/polythings/timelake/issues/18) Maintenance: --shard i/n so concurrent runs don't repeat work
- [#50](https://github.com/polythings/timelake/issues/50) Maintenance: delete orphan files

## History

- 2026-10-11: maintenance tasks (SPEC.md §12)
- 2026-10-12: inline maintenance, retention and expiry built in v0
- 2026-10-14: `keep-snapshots-until` for upgrades
- 2026-10-17: rollup refresh in maintenance; byte cap per run (WASM)
- 2026-10-19: moved from SPEC.md into spec 15, then into this spec when the specs were regrouped one per feature.
- 2026-10-20: expiry reads what the kept snapshots use in full before it changes anything (#46).
