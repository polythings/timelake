---
number: 8
title: Table format versions and upgrades
area: Tables and storage
status: done
updated: 2026-10-14
versions: [0.3.0, 0.4.0, 0.5.0]
related: [2, 7]
issues: [24, 25]
---

# 8. Table format versions and upgrades

**Done** · updated 2026-10-14 · versions 0.3.0, 0.4.0, 0.5.0 · milestone: [Installable, upgradable, switchable](milestones/upgrades-and-settings.md)
· related: [spec 2](0002-commits.md), [spec 7](0007-table-head.md)

## Summary

Tables record a table format version, a semantic version of its own separate from Timelake's, so a Timelake refuses
tables it can't read or write correctly. `timelake upgrade` brings tables to the current format without data loss, as
ordinary commits that `timelake rollback` can undo.

## Design

- **Table format version** `timelake.format-version`, separate from the software version and a semantic version of
  its own, so compatible changes don't run the number up. A patch bump is compatible both ways. From 1.0, a minor
  bump breaks older writers (they refuse to write) and a major bump older readers (they refuse to read); before 1.0,
  as in Cargo, a minor bump breaks both. Every Timelake reads and writes older formats. The integer `N` written by
  Timelake 0.3.0 reads as `0.N.0`; tables without the property are 0.1.0. Decided 2026-10-14, after 0.3.0 had
  shipped integers. Refusing to read is the simple rule for now; reading a newer format as plain Iceberg (ignoring
  the head and index) is the alternative if refusals get in the way.
- **History:** format 0.2.0 (Timelake 0.3.0/0.4.0: the head), 0.3.0 (Timelake 0.5.0: the insertion-time column).
  The format version is managed by Timelake (create, upgrade) and can't be set by hand.
- **`timelake upgrade`** (`Table.upgrade()` in Python and Rust) records the format version, builds a missing
  last-value index, rewrites unsorted or small files and writes the head. Each step is an ordinary commit, older
  snapshots stay for 7 days (`--keep-snapshots`, table property `timelake.keep-snapshots-until`), and `--dry-run`
  shows the plan. **`timelake rollback <table> <snapshot>`** (`Table.rollback()`) undoes it. Upgrading doesn't add
  columns (`_ingest_ts`): a table's columns are fixed at creation until schema evolution exists.
- **Releases** follow the same semantic-versioning rules; how they are cut and published is in
  [releases](releases.md).

## Open questions and issues

- [#24](https://github.com/polythings/timelake/issues/24) Tests: upgrade fixtures written by each release
- [#25](https://github.com/polythings/timelake/issues/25) CI: declare compatibility on changes to persisted formats

## History

- 2026-10-14: format version and `timelake upgrade` (0.3.0); semantic format versions (0.4.0)
- 2026-10-17: format 0.3.0, the insertion-time column (0.5.0)
- 2026-10-19: moved from SPEC.md into spec 30, then into this spec when the specs were regrouped one per feature.
