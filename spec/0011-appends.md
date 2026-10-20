---
number: 11
title: Appends
area: Writing
status: done
updated: 2026-10-20
versions: [0.1.0, 0.5.0, 0.6.0, 0.7.0, unreleased]
related: [2, 5, 12, 14, 15, 19]
issues: [54, 67]
---

# 11. Appends

**Done** · updated 2026-10-20 · versions 0.1.0, 0.5.0, 0.6.0, 0.7.0, unreleased · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Writes from JavaScript](milestones/javascript-writes.md), [Tuned on real data](milestones/real-data.md), [Technical preview](milestones/technical-preview.md)
· related: [spec 2](0002-commits.md), [spec 5](0005-file-layout.md), [spec 12](0012-late-data-and-dedup.md), [spec 14](0014-hot-tier.md), [spec 15](0015-backfill.md), [spec 19](0019-compaction.md)

## Summary

An append takes Arrow batches, aligns them to the table's schema, sorts them by (partition, series…, time), keeps the
last of equal keys, writes Parquet in the table's layout and commits it, then runs inline maintenance. Writers batch
rows and commit every few seconds or megabytes; batch IDs make retries safe.

## Design

### Ingestion modes
- **Continuous:** Records stream into `timelake-ingest` ([spec 36](0036-ingest-and-rules.md)) or the Rust/Python
  writer. They are buffered and committed to Iceberg every *T* seconds or *S* MB (defaults: 60 s / 128 MB).
- **Batch / backfill:** Bulk loads of historical data go through the backfill path ([spec 15](0015-backfill.md)).
- **Idempotency:** Each batch carries an ID that is recorded in the snapshot summary, so retried commits are no-ops
  ([spec 2](0002-commits.md)).
- **Seconds-fresh reads** before a commit need the hot tier ([spec 14](0014-hot-tier.md)).

### The write path
1. **Align** the batches to the table's schema; fill `_ingest_ts` with the time the append started unless the input
   brings its own values ([spec 12](0012-late-data-and-dedup.md)). Types are converted only where no value is lost: a
   value that doesn't fit (out of range, unparsable, inexact) is an error naming the column, never a null.
2. **Sort** by `(partition, series…, time, insertion time)` and split by time partition. With dedup on, only the last
   of equal `(series, time)` keys is kept, so every file is free of duplicates on its own (0.5.0). Appends past 1 GiB
   sort groups of up to 1 GiB and merge them, so a string column of more than 2 GiB no longer overflows (0.7.0).
3. **Write** Parquet in the table's layout ([spec 5](0005-file-layout.md)), columns encoded in parallel (0.6.0).
4. **Commit** in two steps ([spec 2](0002-commits.md)), with the extended last-value index
   ([spec 6](0006-last-value-index.md)) and head ([spec 7](0007-table-head.md)).
5. **Maintain** inline when thresholds are met ([spec 20](0020-maintenance.md)).

The same path runs in WebAssembly for JavaScript writers ([spec 13](0013-wasm-writes.md)).

**Recommended:** one writer per table batching rows into appends of a few thousand rows or more. Concurrent writers
are safe (optimistic commits), but each conflict costs a retry, and many tiny appends make many tiny files.

## Open questions and issues

- [#54](https://github.com/polythings/timelake/issues/54) Ingest: backpressure and limits
- [#67](https://github.com/polythings/timelake/issues/67) Ingest: measure arrival delay and derive how long a slot stays open

## History

- 2026-10-11: ingestion modes (SPEC.md §8)
- 2026-10-12: built in v0
- 2026-10-17: dedup within an append; columns encoded in parallel
- 2026-10-18: appends past 1 GiB sorted in groups and merged
- 2026-10-19: moved from SPEC.md into spec 9, then into this spec when the specs were regrouped one per feature.
- 2026-10-20: lossy type conversions on append are errors (#22)
