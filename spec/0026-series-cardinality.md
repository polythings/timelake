---
number: 26
title: Series cardinality
area: Performance and scale
status: in-progress
updated: 2026-10-19
related: [1, 6, 27]
issues: [19, 57, 119, 120, 121, 122]
---

# 26. Series cardinality

**In progress** · updated 2026-10-19
· related: [spec 1](0001-purpose.md), [spec 6](0006-table-contract.md), [spec 27](0027-performance-plan.md)

## Summary

Make everything that grows with the number of series hold up from the typical 100k–400k series to hundreds of millions: the last-value index, join filters, rollups and planning. Measured at 100k, 400k and 1M series.

## Design

Requirement R27 (spec 1): a typical deployment has about 20,000 devices with 5–20 metrics each (100k–400k series);
the top end is millions of devices with hundreds of metrics each (hundreds of millions of series). Everything that
grows with the number of series must hold up at both: the last-value index, rollups, join filters, planning. P10 of
the performance plan (spec 27).

### Measured

- **100k and 400k series** (IIoT `large`, 2026-10-18): ingest at 2.2–2.3 M rows/s, dashboards flat (`latest-area`
  52 → 53 ms); last-value index 1.8 MB, 0.15 s of a 4.5 s commit at 400k (3.4 s at 10M, in memory).
- **k-way merge on interleaved inputs** (2026-10-18): gathering rows in one pass and galloping over runs, up to 5×
  faster when inputs alternate key by key.
- **Extending the last-value index** (2026-10-19): a 100k-row append to a table of 1M series 264 → 171 ms by merging
  the sorted indexes instead of sorting them. What remains (~80 ms) is encoding the whole index, ~1.5 s per commit at
  10M series.

### Known suspects

- The last-value index is one object rewritten on every commit. Options: index deltas per commit, merged by readers
  and folded into the base by maintenance; or shards by series hash, which don't help a writer whose batches touch
  every shard.
- An index for mostly idle series.
- A join's runtime filter lists at most 150 keys (`hash_join_inlist_pushdown_max_distinct_values`), so a dashboard
  over thousands of devices prunes by bounds only.
- `gaps` grows with rows (88 s at 400k series), a rollup case.
- Rollup refresh and planning per series.

## Open questions and issues

- [#19](https://github.com/polythings/timelake/issues/19) Last-value index: drop series past retention so it stays bounded
- [#57](https://github.com/polythings/timelake/issues/57) Query: answer "list the series" from the last-value index
- [#119](https://github.com/polythings/timelake/issues/119) Last-value index: deltas per commit instead of rewriting the whole index
- [#120](https://github.com/polythings/timelake/issues/120) Last-value index for mostly idle series
- [#121](https://github.com/polythings/timelake/issues/121) Pruning for dashboards over thousands of devices: join runtime filters cap at 150 keys
- [#122](https://github.com/polythings/timelake/issues/122) IIoT `gaps` grows with rows (88 s at 400k series)

## History

- 2026-10-18: R27 and P10 added; 100k and 400k measured
- 2026-10-19: last-value index merged instead of sorted
- 2026-10-19: moved from SPEC.md into its own spec.
