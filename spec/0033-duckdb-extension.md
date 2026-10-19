---
number: 33
title: DuckDB extension
area: Integrations
status: planned
updated: 2026-10-12
related: [1, 12, 14, 21, 28]
issues: [128]
---

# 33. DuckDB extension

**Planned** · updated 2026-10-12 · milestone: [Any engine can attach](milestones/any-engine.md)
· related: [spec 1](0001-iceberg-tables.md), [spec 12](0012-late-data-and-dedup.md), [spec 14](0014-hot-tier.md), [spec 21](0021-reader.md), [spec 28](0028-function-library.md)

## Summary

A loadable DuckDB extension that makes DuckDB Timelake-aware: dedup on read, index-aware scans, the hot tier, and the
function library, with the same kernels as the Timelake reader. Plain DuckDB already reads every table
([spec 1](0001-iceberg-tables.md)); the extension closes the gap to the Timelake reader for people who keep DuckDB.

## Design

- **What it adds over plain DuckDB:**
  - Dedup on read, so DuckDB doesn't see copies before compaction ([spec 12](0012-late-data-and-dedup.md); Q3, #106).
  - Index-aware scans: the last-value index, the head, and series pruning ([spec 21](0021-reader.md)).
  - Hot-tier merge for seconds-fresh reads ([spec 14](0014-hot-tier.md)).
  - The function library with full parity ([spec 28](0028-function-library.md)), and procedures such as
    `CALL timelake_backfill(…)` ([spec 15](0015-backfill.md)).
- **One function implementation, two engines:** time-series kernels live in an engine-agnostic crate on Arrow arrays.
  Thin adapters expose them as DataFusion UDFs/UDAFs and as DuckDB extension functions (Rust via the DuckDB C extension
  API; *verify aggregate-function coverage*).
- **Until then:** a SQL macro pack for plain DuckDB (#127).

## Open questions and issues

- [#128](https://github.com/polythings/timelake/issues/128) DuckDB extension: dedup on read, index-aware scans and the function library

## History

- 2026-10-11: the DuckDB extension as the user-facing SQL path (SPEC.md §6, D3)
- 2026-10-19: gathered from SPEC.md (specs 2, 5, 17 and 24) into this spec when the specs were regrouped one per
  feature.
