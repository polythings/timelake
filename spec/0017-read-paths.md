---
number: 17
title: Read paths
area: Reading
status: in-progress
updated: 2026-10-17
related: [18, 19, 20, 21, 24]
issues: [32, 56, 62, 128, 134]
---

# 17. Read paths

**In progress** · updated 2026-10-17
· related: [spec 18](0018-table-head.md), [spec 19](0019-block-cache.md), [spec 20](0020-scan-tuning.md), [spec 21](0021-rollups.md), [spec 24](0024-function-library.md)

## Summary

Two ways to read: plain DuckDB (or any Iceberg engine) on committed data with no Timelake software, and the Timelake reader (Rust, Python, WebAssembly), which adds dedup on read, rollup routing, the last-value index, time-series functions and object-storage tuned I/O.

## Design

### Plain DuckDB (zero Timelake software)
```sql
INSTALL iceberg; LOAD iceberg;
ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'https://catalog…');
SELECT time_bucket(INTERVAL '1 hour', bucket) AS h, avg(avg_value)
FROM lake.iot.sensor_1m WHERE device_id = 'pump-7' AND bucket > now() - INTERVAL 7 DAY
GROUP BY 1 ORDER BY 1;
```
This path sees committed data only, and does not get hot-tier merge, dedup-on-read, or the full function library.

### Timelake reader (Rust + Python): "faster than DuckDB" on time-series shapes
We don't try to beat DuckDB as a general engine. We win by using knowledge a generic engine doesn't have:
1. **Series-level pruning** from Puffin indexes, which goes beyond min/max statistics: open only the files and row groups containing the requested series.
2. **Last-value index:** "Current state of every device" without scanning.
3. **Automatic rollup routing.**
4. **Sort-order awareness:** No re-sorting for per-series windows and ASOF joins.
5. **Hot-tier merge** for instant freshness.
6. **I/O tuned for object storage:** Coalesced range reads, parallel GETs, and a metadata and footer cache (in memory or on local disk).

**Python API sketch:**
```python
import timelake as tl
lake = tl.connect("s3://bucket/warehouse", catalog="rest://…")
tbl = lake.table("iot.sensor")
df = tbl.query(series={"site": "plant-3", "metric": "temp"},
               start="-24h", bucket="5m", agg=["avg", "max"]).to_polars()
lake.sql("SELECT … time_weighted_mean(…) …").to_arrow()   # full SQL via DataFusion
tbl.to_duckdb()                                     # hand off an Arrow stream to DuckDB zero-copy
```

## Open questions and issues

- [#32](https://github.com/polythings/timelake/issues/32) Query: read string columns as Utf8View
- [#56](https://github.com/polythings/timelake/issues/56) Query: load each table once when concurrent queries need it
- [#62](https://github.com/polythings/timelake/issues/62) Permissions: re-apply row filters that the scan only uses for pruning
- [#128](https://github.com/polythings/timelake/issues/128) DuckDB extension: dedup on read, index-aware scans and the function library
- [#134](https://github.com/polythings/timelake/issues/134) Plain DuckDB reads Timelake files on S3 slower than PyArrow-written files (0.52 vs 0.42 s)

## History

- 2026-10-11: specified (SPEC.md §12)
- 2026-10-12: Timelake reader v0 built
- 2026-10-19: moved from SPEC.md into its own spec.
