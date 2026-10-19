---
number: 6
title: Time-series table contract and layout
area: Foundations
status: in-progress
updated: 2026-10-19
related: [7, 12, 15, 26]
issues: [44, 76, 130, 131]
---

# 6. Time-series table contract and layout

**In progress** · updated 2026-10-19
· related: [spec 7](0007-supplemental-timestamps.md), [spec 12](0012-late-data-and-dedup.md), [spec 15](0015-maintenance.md), [spec 26](0026-series-cardinality.md)

## Summary

A table becomes a Timelake table by declaring a time column, optionally a series key, dedup and a chunk interval, as `timelake.*` table properties. The contract drives the layout: time partitions, files sorted by (series…, time), 1M-row row groups, time-series encodings, and a last-value index per snapshot.

## Design

### Timelake tables: a time-series table contract (D5)
Users define their own schema. A table becomes a **Timelake table** when it meets this contract:

| Element | Required? | Meaning |
|---------|-----------|---------|
| `time_column` | **Required**, exactly one | `timestamp` / `timestamptz` (µs; ns on Iceberg v3). Drives partitioning, retention, rollups, late-data detection. |
| `series_key` | Optional (strongly recommended) | One or more columns identifying a series, e.g. `(device_id, metric)`. Drives sort order, dedup, series indexes, bucket partitioning. If absent, the table is treated as a single stream (e.g. an event log). |
| `dedup_key` | Optional | Defaults to `series_key + time_column`. Set to none to disable dedup. |
| `chunk_interval` | Optional | Time partition granularity. Auto-chosen from expected volume if omitted. |
| `_ingest_ts` | Added automatically (opt-out) | Insertion time, set by the writer per append (built 2026-10-16, table format 0.3.0, [spec 12](0012-late-data-and-dedup.md)). Decides which copy of a row is the latest write; powers late-data detection and freshness metrics. |
| Supplemental time columns | Optional | Other times a sample carries (server, gateway, sent, corrected), declared so Timelake understands them ([spec 7](0007-supplemental-timestamps.md), proposed). |
| All other columns | Free-form | Any Iceberg type, including `variant` on v3. Evolves via Iceberg schema evolution (add / rename / widen / drop). |

**Creating a table:**
```sql
CREATE TABLE iot.sensor (
  device_id STRING, metric STRING, ts TIMESTAMPTZ,
  value DOUBLE, quality SMALLINT
) WITH (time_column = 'ts', series_key = ('device_id', 'metric'), chunk_interval = '1 day');
```
```python
lake.create_table("iot.sensor", schema=arrow_schema,
                  time_column="ts", series_key=["device_id", "metric"])
```

**Adopting existing tables:** `timelake adopt <table>` turns an existing Iceberg table (e.g. one in S3 Tables) into a Timelake table. It validates the contract, sets `timelake.*` properties, and lets compaction gradually re-lay out the data. This is the main migration path from "S3 Tables + Glue".

**Internal series ID:** Timelake computes a stable 64-bit hash of `series_key` for indexing and bucketing, kept in indexes only, not as a column (Q4, decided 2026-10-18).

### Documented schema templates (not mandatory)
| Template | Example schema | Best for |
|----------|----------------|----------|
| **Narrow** | `device_id, metric, ts, value, quality` | Heterogeneous devices, thousands of metric types, sparse data |
| **Wide** | `device_id, ts, temp, pressure, rpm, …` | Homogeneous fleets, metrics sampled together |
| **Event** | `device_id, ts, event_type, payload (variant/JSON)` | Alarms, state changes, logs |
| **Series metadata** (dimension) | `device_id, site, asset_type, unit, firmware, …` | Tags kept out of the hot table and joined at query time |

IoT tip: include a `quality` column (OPC-UA-style good/uncertain/bad flag). It is common in industrial data and supported by the function library.

### Physical layout defaults (derived from the contract; all overridable)
- **Partitioning:** `<chunk_interval>(time_column)` plus optional `bucket(N, series_id)` or `identity(tenant)`, chosen automatically from expected volume.
- **Tiered partition granularity:** Recent data is partitioned by `hour(ts)`. After compaction, older data moves to `day(ts)` or `month(ts)`. This uses Iceberg partition evolution plus a rewrite.
- **Sort order:** `(series_key…, time_column)` within each file. This gives excellent compression (delta-encoded timestamps, similar adjacent values) and tight row-group statistics for series pruning.
- **File sizing:** Target 128–512 MB files with row groups sized for selective reads. Tunable per table.
- **Encodings:** Parquet `DELTA_BINARY_PACKED` for the time column, dictionaries for the other columns (floats falling back to `BYTE_STREAM_SPLIT` once the dictionary outgrows its page; setting `float-encoding`), and ZSTD. Measured 2026-10-19 against `BYTE_STREAM_SPLIT` for floats: IIoT readings a third of the size (366 → 119 MB for 5 days) at equal or better speed, TSBS DevOps 38% larger but scans up to twice as fast (`high-cpu-all` 274 → 136 ms), random doubles +5% size and +6% scan time.

### Vortex (Q8, measured 2026-10-11)

Keep a file-format abstraction from day one, for when Iceberg supports pluggable formats. Measured
([`bench/results/VORTEX.md`](../bench/results/VORTEX.md)), via DuckDB and via DataFusion with `vortex-datafusion`:
on local disk Vortex is 20–45% faster, 2.4–4× on single-device queries; on object storage at parity to 1.5× slower with
DataFusion (more requests), 1.3–3× slower with DuckDB (no cache); and its files are larger in our sorted layout. Hence
D16.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D5 | User-defined schemas with a time-series table contract ([spec 6](0006-table-contract.md)) | Accepted |
| D16 | Parquet stays the lake's source of truth (Iceberg-compatible). Vortex is used where Timelake owns the read path and data is local: reader SSD cache, hot tier, edge. Re-evaluate when Iceberg supports Vortex files or Vortex object-store I/O matures (`bench/results/VORTEX.md`). | Proposed |
| D18 | The last-value index is a per-snapshot **Parquet sidecar** referenced from the snapshot summary (`timelake.last-value-index`), not a Puffin blob: any engine can read it, and it is rewritten incrementally on each append (previous index ⊕ newest rows of the batch). | Accepted (v0) |

## Open questions and issues

- [#44](https://github.com/polythings/timelake/issues/44) Write: check whether time-ordered file names concentrate S3 load
- [#76](https://github.com/polythings/timelake/issues/76) Spike: a time-series file format of our own (research)
- [#130](https://github.com/polythings/timelake/issues/130) `timelake adopt`: turn an existing Iceberg table into a Timelake table
- [#131](https://github.com/polythings/timelake/issues/131) Tiered partition granularity: hour for recent data, day or month after compaction

## History

- 2026-10-11: contract and layout (SPEC.md §7.1–7.3)
- 2026-10-16: `_ingest_ts`, table format 0.3.0
- 2026-10-18: Q4 decided, no hidden `_series_id` column
- 2026-10-19: floats written with dictionaries (setting `float-encoding`)
- 2026-10-19: moved from SPEC.md into its own spec.
