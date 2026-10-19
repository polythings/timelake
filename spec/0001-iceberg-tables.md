---
number: 1
title: Iceberg tables
area: Tables and storage
status: done
updated: 2026-10-13
versions: [0.1.0]
related: [2, 3, 4, 21]
issues: [104, 134]
---

# 1. Iceberg tables

**Done** · updated 2026-10-13 · versions 0.1.0 · milestone: [v0: a maintenance-free time-series table on S3](milestones/v0.md)
· related: [spec 2](0002-commits.md), [spec 3](0003-object-store-catalog.md), [spec 4](0004-table-contract.md), [spec 21](0021-reader.md)

## Summary

Every Timelake table is a plain Apache Iceberg table of Parquet files, so DuckDB, Spark, Trino, Snowflake and Athena read
it with no Timelake software. Timelake adds its time-series conventions only through Iceberg's extension points (table
properties, snapshot summaries, sidecar files), which other engines ignore.

## Design

**Iceberg is the source of truth. We add "Timelake conventions" on top using only Iceberg's extension points.**

Iceberg already provides the mechanisms we need to add time-series intelligence *without breaking other readers*:

| Extension point | What we store there | Other engines |
|-----------------|--------------------|---------------|
| Table properties (`timelake.*`) | Table kind (raw / rollup / events), time column, series key, dedup policy, retention, rollup definitions | Ignore |
| Snapshot summary properties | Ingest watermarks (WAL offsets), changed time ranges per commit, idempotency/batch IDs | Ignore |
| Puffin statistics files | Series→file index, per-series time ranges, "last value" index, sketches | Ignore unknown blobs (*to verify per engine*) |
| Sort order + partition spec | `(series_key…, time_column)` sort; time + bucket partitioning | Use for pruning |
| Sidecar Iceberg tables | Series/device metadata, rollups, events/alarms | Fully readable |

**Plain DuckDB (zero Timelake software).** Any Iceberg engine reads committed data:
```sql
INSTALL iceberg; LOAD iceberg;
ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'https://catalog…');
SELECT time_bucket(INTERVAL '1 hour', bucket) AS h, avg(avg_value)
FROM lake.iot.sensor_1m WHERE device_id = 'pump-7' AND bucket > now() - INTERVAL 7 DAY
GROUP BY 1 ORDER BY 1;
```
This path sees committed data only, and does not get hot-tier merge, dedup on read or the full function library: those
come with the Timelake reader ([spec 21](0021-reader.md)) or the DuckDB extension ([spec 33](0033-duckdb-extension.md)).
v0 checked that plain DuckDB reads every Timelake table with identical results
([the milestone "v0"](milestones/v0.md)).

### Why not a custom format (yet)
- **Ecosystem:** Iceberg is readable by DuckDB, Spark, Trino, Flink, Snowflake, BigQuery, Athena, and Databricks. A
  custom format would forfeit R1 ([purpose and requirements](purpose.md)).
- The known Iceberg weaknesses for time series are **commit overhead** (each commit writes metadata.json + a manifest
  list + manifests) and **commit throughput** (optimistic concurrency at the catalog). Both are addressable *around*
  Iceberg:
  - Batch commits: a single committer per table, committing every N seconds or N MB.
  - A hot tier for sub-commit freshness ([spec 14](0014-hot-tier.md)).
  - Aggressive manifest compaction and snapshot expiration.
  - Adopting Iceberg v4 improvements (proposed single-file commits / adaptive metadata tree) when they land.
- DuckLake chose a SQL database as its catalog largely to solve small commits and fast metadata. Our hot tier plays
  the same role *optionally*, and keeps the lake itself pure Iceberg.

### When we would revisit
We revisit if benchmarks show that Iceberg metadata planning dominates latency for typical queries even after Puffin
indexes and metadata caching. Even then, the fallback is a **Timelake index layer that accelerates Iceberg**, not a
replacement for it. The table head ([spec 7](0007-table-head.md)) is such a layer.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D1 | Build on Apache Iceberg; no custom table format | Accepted |

## Open questions and issues

- [#104](https://github.com/polythings/timelake/issues/104) Question: Iceberg v2 or v3 as the minimum table version
- [#134](https://github.com/polythings/timelake/issues/134) Plain DuckDB reads Timelake files on S3 slower than PyArrow-written files (0.52 vs 0.42 s)

## History

- 2026-10-11: format choice and extension points (SPEC.md §5)
- 2026-10-19: moved from SPEC.md into spec 3, then into this spec when the specs were regrouped one per feature
  (with the plain DuckDB path from spec 17).
