---
number: 19
title: Compaction
area: Maintenance
status: done
updated: 2026-10-20
versions: [0.1.0, 0.5.0, 0.6.0, 0.7.0, unreleased]
related: [5, 12, 13, 20]
issues: [12, 49, 52, 61, 66, 68, 69, 70, 77, 135]
---

# 19. Compaction

**Done** · updated 2026-10-20 · versions 0.1.0, 0.5.0, 0.6.0, 0.7.0, unreleased · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Exact answers before compaction](milestones/exact-before-compaction.md), [Writes from JavaScript](milestones/javascript-writes.md)
· related: [spec 5](0005-file-layout.md), [spec 12](0012-late-data-and-dedup.md), [spec 13](0013-wasm-writes.md), [spec 20](0020-maintenance.md)

## Summary

Compaction merges small files into large ones sorted by (series…, time), removing copies of rows (the latest write
wins), so tables stay fast without Glue or Spark jobs. It is a streaming k-way merge with memory of one batch per input,
shared by the engine and the WebAssembly writer. Inline compaction is size-tiered; cold partitions are rewritten into
one file.

## Design

- **What it does:** merges small files, re-sorts by `(series_key…, time_column)`, applies dedup, and (planned)
  coarsens partitions for old data ([spec 5](0005-file-layout.md)).
- **Size-tiered (D19):** inline compaction merges at least `compact-min-files` files of a similar size within a
  partition, keeping write amplification logarithmic under micro-batches. Explicit `timelake maintain` rewrites each
  partition into one sorted, deduplicated file.
- **Commit order (fixed 0.5.0):** compaction merges only runs of files next to each other in commit order, and a merged
  file keeps the data sequence number of the newest file it replaces, so an older write can never win over a newer one
  ([spec 12](0012-late-data-and-dedup.md)).
- **Cold partitions (0.5.0, setting `compact-cold-after`):** once newer data is that far (default 1 hour) past a
  partition's end, inline maintenance rewrites it into one sorted, duplicate-free file when it has more than one file.
  Dedup on read then only touches the current partitions.

### Streaming compaction (decided 2026-10-17, built in the engine 2026-10-17)

**Status:** the engine compacts by streaming merge (`timelake_format::merge`, `maintain.rs`). Full compaction of TSBS
IoT, 72 hourly files into 3 days: 3.65 → 2.37 s, peak memory 1.95 → 0.53 GB, identical output
([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md)). Per open input about 10 MB (its row group).
Writing was 80% of the time on one thread; the shared file writer (`timelake_format::file`) now encodes columns in
parallel: 2.37 → 1.17 s, byte-identical files. The shared maintenance (WASM) adds the byte cap per run and page-window
reads by the offset index ([spec 13](0013-wasm-writes.md)). Both writers merge at most 24 files at once (a larger group
in balanced parts, which the next run merges; unreleased in the engine), planned by the same function
(`timelake_format::maintain::compaction_plan`), so a table compacts to the same files whichever writer runs it. On inputs that alternate
series by series (5-minute batches of every device), gathering rows in one pass and galloping over runs made the merge
up to 5× faster (10 M rows of two inputs 13.9 → 2.6 s, 0.7.0).

**Why.** Compaction read every file of a run into memory, concatenated, sorted, and deduplicated it, so its memory
grew with the partition: fine on a server, too much for a 128 MB Cloudflare Worker, and wasteful everywhere.

**Design.** Every data file Timelake writes is sorted by the write order `(series…, time, insertion time)` and free of
duplicates on its own ([spec 12](0012-late-data-and-dedup.md)). Compaction becomes a k-way merge of its input files:
- Each input is read as a stream of small batches; the merge keeps the next batch of each input and emits rows in
  write order. Inputs are taken in commit order, so among rows with the same `(series…, time)` the later commit wins,
  then the later insertion time, as before.
- Duplicates are dropped as they meet: the last row of each `(series…, time)` is kept when dedup is on.
- Memory is one batch per input, the row group being written, and one upload part, whatever the partition's size.
  Outputs larger than an upload part go up as multipart uploads.
- An input not sorted in the table's order (written by another engine, or before the table declared one) is sorted
  on its own first: memory is then bounded by the largest such file, not the partition.
- The merge is a state machine with no I/O (it says which input needs its next batch, and takes batches as they come),
  so the engine (async, object store) and the WASM writer (JS store) run the same code.

**Optimizations to try, kept if measured faster:**
- Runs, not rows: where the current batch of one input ends before the next key of every other input, the whole run
  is emitted as a slice, without comparing rows one by one (found by binary search on row-encoded keys). Time-series
  files mostly cover different series or times, so most of a merge should be runs.
- Copy without decoding (#135): a whole row group, or a whole file, whose key range overlaps no other input is copied
  to the output as encoded column chunks. This needs each file's (or row group's) first and last composite key; the
  per-column bounds in Iceberg metadata are too weak for a multi-column series key. If it pays off, the format gains
  them (below).
- Parallel decoding of inputs on native targets.
- Smaller read batches and row groups where memory is tight (WASM), as settings.

**Format extensions considered** (added to the table format only if a measurement needs them; readers that don't know
them ignore them):
- In each data file's Parquet key-value metadata: `timelake.sort` (the write order) and `timelake.unique = true` (no
  two rows share `(series…, time)`), so compaction and dedup on read can trust a file without the table's history.
- Per data file in the head (and per row group in the footer metadata): the first and last composite key, for copying
  non-overlapping inputs without decoding.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D19 | Inline maintenance uses **size-tiered compaction** (merge ≥ N files of similar size within a partition) to keep write amplification logarithmic under micro-batches. Explicit `maintain` rewrites each partition into one sorted, deduplicated file. | Accepted |

## Open questions and issues

- [#12](https://github.com/polythings/timelake/issues/12) Maintenance: compact partitions whose files overlap, not only small files
- [#49](https://github.com/polythings/timelake/issues/49) Maintenance: check a rewrite before committing it
- [#52](https://github.com/polythings/timelake/issues/52) Maintenance: commit compaction per group, with a byte budget
- [#61](https://github.com/polythings/timelake/issues/61) Tests: property tests for compaction planning, merge and dedup
- [#66](https://github.com/polythings/timelake/issues/66) Maintenance: keep late data for a closed slot in its own tier
- [#68](https://github.com/polythings/timelake/issues/68) Maintenance: rewrite each byte at most once in routine compaction
- [#69](https://github.com/polythings/timelake/issues/69) Maintenance: rewrite only the large files a small file overlaps
- [#70](https://github.com/polythings/timelake/issues/70) Maintenance: grow the default target file size with table size
- [#77](https://github.com/polythings/timelake/issues/77) Research: compaction, layout and encodings: tracking issue
- [#135](https://github.com/polythings/timelake/issues/135) Compaction: copy row groups that overlap no other input without decoding

## History

- 2026-10-12: size-tiered inline compaction built in v0 (D19)
- 2026-10-16: merges only files next to each other in commit order; cold partitions compact sooner
- 2026-10-17: streaming compaction, columns encoded in parallel
- 2026-10-18: merge up to 5× faster on interleaved inputs
- 2026-10-19: moved from SPEC.md into spec 15, then into this spec when the specs were regrouped one per feature.
- 2026-10-20: one compaction plan for both writers; the engine merges at most 24 files at once too.
