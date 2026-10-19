---
number: 15
title: Table maintenance and streaming compaction
area: Writing
status: done
updated: 2026-10-17
related: [12, 16, 21]
issues: [12, 13, 18, 46, 49, 50, 52, 61, 68, 69, 70, 77, 135]
---

# 15. Table maintenance and streaming compaction

**Done** · updated 2026-10-17
· related: [spec 12](0012-late-data-and-dedup.md), [spec 16](0016-wasm-writes.md), [spec 21](0021-rollups.md)

## Summary

The jobs Timelake replaces Glue with: compaction (re-sort, dedup), rollup refresh, retention, snapshot expiry, run inline by the writer or as `timelake maintain`. Compaction is a streaming k-way merge with memory of one batch per input, shared by the engine and the WebAssembly writer.

## Design

`timelake-maintain` runs as a set of idempotent, independently schedulable tasks:

| Task | What it does |
|------|-------------|
| Compaction | Merges small files, re-sorts by `(series_key…, time_column)`, applies dedup, coarsens partitions for old data |
| Rollup refresh | Incremental, snapshot-diff based ([spec 21](0021-rollups.md)) |
| Retention | Per-table TTL (e.g. raw 90 d, 1-min rollup 2 y, 1-h rollup forever). Drops whole partitions (metadata-only when aligned) |
| Snapshot expiry and orphan cleanup | Keeps metadata small and storage costs down |
| Manifest rewrite | Clusters manifests by time so planning stays fast as tables grow |
| Index build | Writes Puffin blobs (series index, last-value index, sketches) |

These tasks can be triggered by a schedule, by commit events (catalog events, S3 notifications), or inline from the writer for small deployments. The core is concurrency-safe via Iceberg optimistic commits and conflict-aware retries.

### Streaming compaction (decided 2026-10-17, built in the engine 2026-10-17)

**Status:** the engine compacts by streaming merge (`timelake_format::merge`, `maintain.rs`). Full compaction of TSBS IoT, 72 hourly files into 3 days: 3.65 → 2.37 s, peak memory 1.95 → 0.53 GB, identical output ([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md)). Per open input about 10 MB (its row group). Writing was 80% of the time on one thread; the shared file writer (`timelake_format::file`) now encodes columns in parallel: 2.37 → 1.17 s, byte-identical files. The shared maintenance (WASM) adds the byte cap per run, page-window reads by the offset index, and at most 24 files per merge ([spec 16](0016-wasm-writes.md)).

**Why.** Compaction read every file of a run into memory, concatenated, sorted, and deduplicated it, so its memory grew with the partition: fine on a server, too much for a 128 MB Cloudflare Worker (F10), and wasteful everywhere.

**Design.** Every data file Timelake writes is sorted by the write order `(series…, time, insertion time)` and free of duplicates on its own ([spec 12](0012-late-data-and-dedup.md)). Compaction becomes a k-way merge of its input files:
- Each input is read as a stream of small batches; the merge keeps the next batch of each input and emits rows in write order. Inputs are taken in commit order, so among rows with the same `(series…, time)` the later commit wins, then the later insertion time, as before.
- Duplicates are dropped as they meet: the last row of each `(series…, time)` is kept when dedup is on.
- Memory is one batch per input, the row group being written, and one upload part, whatever the partition's size. Outputs larger than an upload part go up as multipart uploads.
- An input not sorted in the table's order (written by another engine, or before the table declared one) is sorted on its own first: memory is then bounded by the largest such file, not the partition.
- The merge is a state machine with no I/O (it says which input needs its next batch, and takes batches as they come), so the engine (async, object store) and the WASM writer (JS store) run the same code.

**Optimizations to try, kept if measured faster:**
- Runs, not rows: where the current batch of one input ends before the next key of every other input, the whole run is emitted as a slice, without comparing rows one by one (found by binary search on row-encoded keys). Time-series files mostly cover different series or times, so most of a merge should be runs.
- Copy without decoding: a whole row group, or a whole file, whose key range overlaps no other input is copied to the output as encoded column chunks. This needs each file's (or row group's) first and last composite key; the per-column bounds in Iceberg metadata are too weak for a multi-column series key. If it pays off, the format gains them (below).
- Parallel decoding of inputs on native targets.
- Smaller read batches and row groups where memory is tight (WASM), as settings.

**Format extensions considered** (added to the table format only if a measurement needs them; readers that don't know them ignore them):
- In each data file's Parquet key-value metadata: `timelake.sort` (the write order) and `timelake.unique = true` (no two rows share `(series…, time)`), so compaction and dedup on read can trust a file without the table's history.
- Per data file in the head (and per row group in the footer metadata): the first and last composite key, for copying non-overlapping inputs without decoding.

**Limits per run.** A setting caps the bytes one maintenance run rewrites (needed on Workers, where a request has at most 5 minutes of CPU; useful anywhere inline maintenance shouldn't hold up appends). A run that stops at the cap commits what it rewrote; the next run continues.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D19 | Inline maintenance uses **size-tiered compaction** (merge ≥ N files of similar size within a partition) to keep write amplification logarithmic under micro-batches. Explicit `maintain` rewrites each partition into one sorted, deduplicated file. | Accepted (v0) |

## Open questions and issues

- [#12](https://github.com/polythings/timelake/issues/12) Maintenance: compact partitions whose files overlap, not only small files
- [#13](https://github.com/polythings/timelake/issues/13) Maintenance: wait a minimum age before deleting files of expired snapshots
- [#18](https://github.com/polythings/timelake/issues/18) Maintenance: --shard i/n so concurrent runs don't repeat work
- [#46](https://github.com/polythings/timelake/issues/46) Maintenance: expiry must not delete files when a kept snapshot's manifest list is missing
- [#49](https://github.com/polythings/timelake/issues/49) Maintenance: check a rewrite before committing it
- [#50](https://github.com/polythings/timelake/issues/50) Maintenance: delete orphan files
- [#52](https://github.com/polythings/timelake/issues/52) Maintenance: commit compaction per group, with a byte budget
- [#61](https://github.com/polythings/timelake/issues/61) Tests: property tests for compaction planning, merge and dedup
- [#68](https://github.com/polythings/timelake/issues/68) Maintenance: rewrite each byte at most once in routine compaction
- [#69](https://github.com/polythings/timelake/issues/69) Maintenance: rewrite only the large files a small file overlaps
- [#70](https://github.com/polythings/timelake/issues/70) Maintenance: grow the default target file size with table size
- [#77](https://github.com/polythings/timelake/issues/77) Research: compaction, layout and encodings: tracking issue
- [#135](https://github.com/polythings/timelake/issues/135) Compaction: copy row groups that overlap no other input without decoding

## History

- 2026-10-12: inline size-tiered compaction (D19), retention, expiry built
- 2026-10-17: streaming compaction decided and built
- 2026-10-19: moved from SPEC.md into its own spec.
