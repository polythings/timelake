---
number: 7
title: Millions of series
status: in-progress
updated: 2026-10-20
description: Ingest, latest values and dashboards that stay flat from thousands to millions of series, measured at each size.
specs: [6, 24, 27]
issues: [122]
---

# Millions of series

**In progress** · updated 2026-10-20

## Summary

Requirement R27 ([purpose and requirements](../purpose.md)): a typical deployment has about 20,000 devices with 5–20
metrics each (100k–400k series); the top end is millions of devices with hundreds of metrics each (hundreds of
millions of series). Everything that grows with the number of series must hold up at both: the last-value index,
rollups, join filters, planning. Was P10 of the performance plan.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 6 | [Last-value index](../0006-last-value-index.md) | In progress | 0.1.0, 0.5.0, unreleased |
| 24 | [Pruning with long IN lists](../0024-in-list-pruning.md) | Done | 0.7.0 |
| 27 | [Materialized rollups](../0027-rollups.md) | Done | 0.5.0, 0.7.0, unreleased |

<!-- specs:end -->

## Measured

- **100k and 400k series** (IIoT `large`, 2026-10-18): ingest at 2.2–2.3 M rows/s, dashboards flat (`latest-area`
  52 → 53 ms); last-value index 1.8 MB, 0.15 s of a 4.5 s commit at 400k (3.4 s at 10M, in memory).
- **k-way merge on interleaved inputs** (2026-10-18): gathering rows in one pass and galloping over runs, up to 5×
  faster when inputs alternate key by key ([spec 19](../0019-compaction.md)).
- **Extending the last-value index** (2026-10-19): a 100k-row append to a table of 1M series 264 → 171 ms by merging
  the sorted indexes instead of sorting them. What remains (~80 ms) is encoding the whole index, ~1.5 s per commit at
  10M series ([spec 6](../0006-last-value-index.md)).

- **Join filters listing more keys** (2026-10-20, #121): letting a join's runtime filter list up to 10,000 keys instead
  of 150 made TSBS IoT's fleet joins up to 3.4× slower (avg-load 55 → 189 ms): a fleet's trucks are in every file, so
  the list prunes nothing and costs a lookup per row. The limit stays.

## Open

- The last-value index is one object rewritten on every commit: deltas per commit (#119) or shards
  ([spec 6](../0006-last-value-index.md)); an index for mostly idle series (#120).
- IIoT `gaps` grows with rows (88 s at 400k series), a rollup case (#122). Since 2026-10-19 it is an aggregate
  (`gap_count`, `gap_seconds`: a month 13× faster) that rollups store and answer time ranges from (a month from an
  hourly rollup: 1.34 s → 44 ms on IIoT small); to measure at 400k.
- Rollup refresh and planning per series ([spec 27](../0027-rollups.md)).

## History

- 2026-10-18: planned (P10); 100k and 400k series measured
- 2026-10-19: last-value index merged instead of sorted; was spec 26, a milestone on the roadmap since the specs were
  regrouped one per feature.
- 2026-10-20: a longer join filter list measured and rejected (#121).
