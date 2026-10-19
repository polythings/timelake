---
number: 21
title: Timelake reader
area: Reading
status: in-progress
updated: 2026-10-17
versions: [0.1.0]
related: [1, 6, 12, 22, 27, 33]
issues: [32, 36, 56, 62]
---

# 21. Timelake reader

**In progress** · updated 2026-10-17 · versions 0.1.0 · milestone: [v0: a maintenance-free time-series table on S3](milestones/v0.md)
· related: [spec 1](0001-iceberg-tables.md), [spec 6](0006-last-value-index.md), [spec 12](0012-late-data-and-dedup.md), [spec 22](0022-block-cache.md), [spec 27](0027-rollups.md), [spec 33](0033-duckdb-extension.md)

## Summary

The Timelake reader (CLI, Rust, Python, and a WebAssembly reader for JavaScript) runs SQL on DataFusion over Timelake
tables, and uses what a generic engine doesn't know: dedup on read, the last-value index, rollup routing, series-aware
scans, time-series functions, and I/O tuned for object storage. Plain DuckDB still reads the same tables
([spec 1](0001-iceberg-tables.md)).

## Design

We don't try to beat DuckDB as a general engine. We win by using knowledge a generic engine doesn't have:

| What | Status |
|---|---|
| Pruning by Iceberg column bounds, Parquet row-group and page statistics | Built (v0) |
| **Last-value index:** "current state of every device" without scanning ([spec 6](0006-last-value-index.md)) | Built (v0) |
| **Dedup on read** before compaction ([spec 12](0012-late-data-and-dedup.md)) | Built (0.5.0) |
| **Automatic rollup routing** ([spec 27](0027-rollups.md)) | Built (0.5.0) |
| **I/O tuned for object storage:** one-request table state ([spec 7](0007-table-head.md)), a block cache ([spec 22](0022-block-cache.md)), the scan settings ([spec 23](0023-filter-pushdown.md), [spec 24](0024-in-list-pruning.md), [spec 25](0025-newest-first.md)) | Built (0.3.0–0.7.0) |
| **Sort-order awareness:** no re-sorting for per-series windows and signal aggregates ([spec 26](0026-series-order.md)) | Built (0.7.0); ASOF joins planned |
| **Series-level pruning** from a series index, beyond min/max statistics (#36) | Planned |
| **Hot-tier merge** for instant freshness ([spec 14](0014-hot-tier.md)) | Planned |

**Engine:** a DataFusion `TableProvider` per table, optimizer rules (rollup routing, latest-row rewrites, newest-first,
series order) and the function library ([spec 28](0028-function-library.md)); why DataFusion is in
[embedded engine](engine.md). v0's reader lessons, now defaults: load all of a query's tables concurrently before
planning, read each Parquet footer once, and use more scan partitions on object storage.

**Python:**
```python
import timelake as tl
lake = tl.Lake("s3://bucket/warehouse")
lake.sql("SELECT … time_weighted_mean(…) …")   # full SQL via DataFusion, a pyarrow.Table
lake.latest("iot.sensor")                      # from the last-value index
```
Results are Arrow, so Polars, pandas and DuckDB take them zero-copy.

## Open questions and issues

- [#32](https://github.com/polythings/timelake/issues/32) Query: read string columns as Utf8View
- [#36](https://github.com/polythings/timelake/issues/36) Reader: a series index that covers some files, with statistics pruning for the rest
- [#56](https://github.com/polythings/timelake/issues/56) Query: load each table once when concurrent queries need it
- [#62](https://github.com/polythings/timelake/issues/62) Permissions: re-apply row filters that the scan only uses for pruning

## History

- 2026-10-11: reader design (SPEC.md §9)
- 2026-10-12: built in v0
- 2026-10-19: moved from SPEC.md into spec 17, then into this spec when the specs were regrouped one per feature.
