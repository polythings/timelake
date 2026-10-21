---
number: 47
title: Table and file formats behind boundaries
area: Tables and storage
status: planned
updated: 2026-10-21
versions: []
related: [1, 2, 7, 9, 12, 13, 21, 24]
issues: [189, 190, 191]
---

# 47. Table and file formats behind boundaries

**Planned** · updated 2026-10-21 · milestone: [Any table format, any file format](milestones/formats.md)
· related: [spec 1](0001-iceberg-tables.md), [spec 2](0002-commits.md), [spec 7](0007-table-head.md), [spec 9](0009-settings.md), [spec 12](0012-late-data-and-dedup.md), [spec 13](0013-wasm-writes.md), [spec 21](0021-reader.md), [spec 24](0024-in-list-pruning.md)

## Summary

The engine, the WASM reader and writer, and maintenance reach tables and data files through two boundaries from
[lakekit](https://github.com/polythings/lakekit): a common table view (Iceberg today) and a data file format (Parquet
today). What makes Timelake a time-series engine stays here: the head, the last-value index, dedup on read,
newest-first scans, rollups, and the SQL functions. Iceberg and Parquet stay the defaults; another format is a choice
a table makes, Vortex first once Iceberg lists it.

## Design

**The two boundaries** are lakekit's:

- **Table formats:** the common table view ([lakekit spec 5](https://github.com/polythings/lakekit/blob/main/spec/0005-table-view.md)).
  - It holds a table's schema, properties and snapshots, and its live files with their statistics, partition values,
    and commit rank.
  - It loads, creates, commits (append, replace, delete) and expires.
- **Data file formats:** `FileFormat` ([lakekit spec 6](https://github.com/polythings/lakekit/blob/main/spec/0006-file-formats.md)).
  - Writers return bytes, statistics and a footer.
  - Readers take a footer and a selection inside the file, ask for byte ranges, and decode them.

**What moves out of Timelake:**

- **The engine's use of iceberg-rust:**
  - the adapter for iceberg-rust's file I/O in `storage.rs`;
  - loading and creating tables;
  - the metadata builder in `commit.rs`;
  - snapshot expiry, branches and tags.

  iceberg-rust stays only as the reference implementation in interop tests ([#189](https://github.com/polythings/timelake/issues/189)).
- **`timelake-format`'s Parquet code:** the file writer (`file.rs`), footers (`footer.rs`), and statistics move to
  `lakekit-parquet` ([#190](https://github.com/polythings/timelake/issues/190)).

**What changes in what stays:**

- **The head** (spec 7) stores each file's footer as opaque bytes with its format's name. This changes the head's
  format, so it needs a head version bump (spec 8).
- **Scans** (spec 21):
  - The provider picks each file's scan by the format its manifest entry records.
  - Parquet's page access plans and the long `IN`-list selections (spec 24) become Parquet's own selection; another
    format brings its own pruning.
  - The scans come from lakekit's DataFusion providers ([lakekit#11](https://github.com/polythings/lakekit/issues/11))
    once they exist, and from Timelake's provider until then.
- **Dedup on read** (spec 12) orders copies of a key by the view's commit rank. In Iceberg that is the data sequence
  number, as today.
- **The block cache** (spec 22) tells which files never change from the table's file list, not from the `.parquet`
  extension.
- **The last-value index** (spec 6) stays a Parquet file whatever the data format, so DuckDB and others can read it.

**Settings** (spec 9):

- `data-format` chooses the format of new files: `parquet` (the default), and later `vortex`.
  - Changing it doesn't rewrite anything at once; compaction rewrites older files as it would anyway.
  - A table may hold files of several formats meanwhile.
- The Parquet settings keep their names and become Parquet's options: `compression`, `row-group-rows`,
  `float-encoding`, and `data-page-rows`.
- A table's table format is fixed when the table is created: Iceberg, the only one for now.

**Other engines.** A table of Iceberg and Parquet files stays readable by DuckDB, Spark, Trino and every other
Iceberg reader.
- **Vortex:** until Iceberg's spec lists it ([apache/iceberg#15915](https://github.com/apache/iceberg/pull/15915),
  [apache/iceberg-rust#2835](https://github.com/apache/iceberg-rust/pull/2835)), only engines that know Vortex can
  read Vortex files. So `vortex` comes off by default, with that said in the docs
  ([#191](https://github.com/polythings/timelake/issues/191)).
- **Table formats other than Iceberg** (Delta Lake, DuckLake) follow lakekit's specs for them; each gets a spec here
  before Timelake writes it.

**Order:**

1. lakekit 0.1 is published and [#186](https://github.com/polythings/timelake/pull/186) is merged.
2. [loko#2](https://github.com/polythings/loko/issues/2) gives `Send` futures, for the engine's multi-threaded executor.
3. Arrow moves to the newest DataFusion's major, as lakekit's D17 has it. DataFusion 56 also brings a native
   `ASOF JOIN`, after which Timelake's own (spec 28) can go.
4. The table view: lakekit#13, here #189.
5. File formats: lakekit#14, here #190.
6. Vortex: lakekit#15, here #191.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D26 | The engine, the WASM reader and writer, and maintenance reach tables and data files only through lakekit's common table view and `FileFormat`; iceberg-rust remains only as the reference in interop tests (supersedes D13 once built) | Accepted |
| D27 | Iceberg and Parquet stay the defaults, and the last-value index stays Parquet whatever the data format, so other engines keep reading every table that doesn't opt in (D1 holds) | Accepted |
| D28 | A table's data format is a setting (`data-format`) that applies to new files; existing files keep theirs until compaction rewrites them, so a table may hold files of several formats | Proposed |

## Open questions and issues

- [#189](https://github.com/polythings/timelake/issues/189) Engine: tables through lakekit's common table view
  instead of iceberg-rust
- [#190](https://github.com/polythings/timelake/issues/190) Data files through a `FileFormat` trait, Parquet first
- [#191](https://github.com/polythings/timelake/issues/191) Vortex data files, once Iceberg lists the format

## History

- 2026-10-21: proposed, so that the engine assumes neither Iceberg nor Parquet, and the format code is shared with
  lakeglass through lakekit.
