---
number: 23
title: Filter pushdown
area: Reading
status: done
updated: 2026-10-17
versions: [0.7.0]
related: [21, 22]
issues: [75]
---

# 23. Filter pushdown

**Done** · updated 2026-10-17 · versions 0.7.0 · milestone: [Fast first queries on object storage](milestones/fast-first-queries.md)
· related: [spec 21](0021-reader.md), [spec 22](0022-block-cache.md)

## Summary

Late materialization: the reader applies filters while decoding Parquet and decodes the other columns only for rows
that pass. It helps on local disk and costs a round trip per row group on object storage, so the default turns it on
for local files only.

## Design

- **Setting `filter-pushdown`** (`auto`, `true`, `false`; table and session). `auto`, the default, turns it on for local
  files and off on object storage. Timelake's scans don't follow DataFusion's
  `datafusion.execution.parquet.pushdown_filters` (0.7.0, breaking).
- **Measured** (2026-10-17, [`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md), P8): TSBS geomeans 4–5%
  faster on local disk, selective group-bys up to 40%; first queries 12–17% slower at 20 ms per request. The Phase 0
  baseline found the same direction in plain DataFusion: 1.2–1.6× slower on object storage (extra range requests),
  slightly faster locally ([spec 44](0044-timebench.md)).

## Open questions and issues

- [#75](https://github.com/polythings/timelake/issues/75) Query: re-measure Parquet filter pushdown (late materialization)

## History

- 2026-10-17: built and measured (P8)
- 2026-10-19: moved from SPEC.md into spec 20, then into this spec when the specs were regrouped one per feature.
