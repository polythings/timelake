---
number: 6
title: Last-value index
area: Tables and storage
status: in-progress
updated: 2026-10-19
versions: [0.1.0, 0.5.0, unreleased]
related: [7, 12, 21]
issues: [19, 57, 119, 120]
---

# 6. Last-value index

**In progress** · updated 2026-10-19 · versions 0.1.0, 0.5.0, unreleased · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Millions of series](milestones/millions-of-series.md)
· related: [spec 7](0007-table-head.md), [spec 12](0012-late-data-and-dedup.md), [spec 21](0021-reader.md)

## Summary

The latest row of every series, kept in a small Parquet file per snapshot, so "the current state of every device" is
answered in a few requests instead of a scan. Latest-row queries are rewritten onto it automatically. It works at
hundreds of thousands of series; making its cost per commit independent of the series count is open.

## Design

- **Storage:** a Parquet sidecar per snapshot, referenced from the snapshot summary (`timelake.last-value-index`), not
  a Puffin blob: any engine can read it. The head carries it inline while it fits the inline budget
  ([spec 7](0007-table-head.md)).
- **Maintenance:** each append extends the previous index with the newest rows of its batch. Since 2026-10-19 the
  two sorted inputs are merged (k-way, keeping the latest of each series) instead of concatenated and sorted: a
  100,000-row append to a table of 1,000,000 series 264 → 171 ms
  ([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md), P10). Among rows with the same series, the latest
  insertion time wins, then the later commit ([spec 12](0012-late-data-and-dedup.md)).
- **Reads:** the Timelake reader rewrites latest-row queries (`ROW_NUMBER() … = 1`, `ORDER BY time DESC LIMIT 1` per
  series, `timelake latest`) onto the index. v0: 12 requests instead of 1,700–3,600 on S3; with the head, a cold
  `latest` takes one round trip.
- **Scale** (measured 2026-10-18, [the milestone "Millions of series"](milestones/millions-of-series.md)): 1.8 MB and
  0.15 s of a 4.5 s commit at 400k series, 3.4 s at 10M in memory. After the merge, what remains (~80 ms at 1M series)
  is encoding the whole index, about 1.5 s a commit at 10M series. Options: index deltas per commit, merged by readers
  and folded into the base by maintenance (#119); or shards by series hash, which don't help a writer whose batches
  touch every shard.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D18 | The last-value index is a per-snapshot **Parquet sidecar** referenced from the snapshot summary (`timelake.last-value-index`), not a Puffin blob: any engine can read it, and it is rewritten incrementally on each append (previous index ⊕ newest rows of the batch). | Accepted |

## Open questions and issues

- [#19](https://github.com/polythings/timelake/issues/19) Last-value index: drop series past retention so it stays bounded
- [#57](https://github.com/polythings/timelake/issues/57) Query: answer "list the series" from the last-value index
- [#119](https://github.com/polythings/timelake/issues/119) Last-value index: deltas per commit instead of rewriting the whole index
- [#120](https://github.com/polythings/timelake/issues/120) Last-value index for mostly idle series

## History

- 2026-10-12: built in v0 (D18)
- 2026-10-16: insertion time decides the latest row
- 2026-10-19: extended by merging sorted indexes; moved from SPEC.md (spec 6, spec 26) into this spec when the specs
  were regrouped one per feature.
