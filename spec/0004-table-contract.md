---
number: 4
title: Time-series table contract
area: Tables and storage
status: in-progress
updated: 2026-10-18
versions: [0.1.0, 0.5.0]
related: [5, 9, 10, 12]
issues: [130]
---

# 4. Time-series table contract

**In progress** · updated 2026-10-18 · versions 0.1.0, 0.5.0 · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Data lifecycle](milestones/data-lifecycle.md), [Technical preview](milestones/technical-preview.md)
· related: [spec 5](0005-file-layout.md), [spec 9](0009-settings.md), [spec 10](0010-supplemental-timestamps.md), [spec 12](0012-late-data-and-dedup.md)

## Summary

Users define their own schema; a table becomes a Timelake table by declaring a time column and, optionally, a series
key, dedup and a chunk interval, stored as `timelake.*` table properties. The contract drives the layout, dedup,
retention and rollups. Creating tables with a contract is built; adopting existing Iceberg tables is planned.

## Design

### Timelake tables: a time-series table contract (D5)

| Element | Required? | Meaning |
|---------|-----------|---------|
| `time_column` | **Required**, exactly one | `timestamp` / `timestamptz` (µs; ns on Iceberg v3). Drives partitioning, retention, rollups, late-data detection. |
| `series_key` | Optional (strongly recommended) | One or more columns identifying a series, e.g. `(device_id, metric)`. Drives sort order, dedup, series indexes, bucket partitioning. If absent, the table is treated as a single stream (e.g. an event log). |
| `dedup_key` | Optional | Defaults to `series_key + time_column`. Set to none to disable dedup. |
| `chunk_interval` | Optional | Time partition granularity. Auto-chosen from expected volume if omitted. |
| `_ingest_ts` | Added automatically (opt-out) | Insertion time, set by the writer per append (built 2026-10-16, table format 0.3.0, [spec 12](0012-late-data-and-dedup.md)). Decides which copy of a row is the latest write; powers late-data detection and freshness metrics. |
| Supplemental time columns | Optional | Other times a sample carries (server, gateway, sent, corrected), declared so Timelake understands them ([spec 10](0010-supplemental-timestamps.md), proposed). |
| All other columns | Free-form | Any Iceberg type, including `variant` on v3. Evolves via Iceberg schema evolution (add / rename / widen / drop). |

Tables without a time column (dimensions, e.g. device metadata) are allowed. The contract's elements are settings fixed
at creation ([spec 9](0009-settings.md)); setting them later as properties is an error.

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

**Adopting existing tables (planned, #130):** `timelake adopt <table>` turns an existing Iceberg table (e.g. one in S3
Tables) into a Timelake table. It validates the contract, sets `timelake.*` properties, and lets compaction gradually
re-lay out the data. This is the main migration path from "S3 Tables + Glue".

**Internal series ID:** Timelake computes a stable 64-bit hash of `series_key` for indexing and bucketing, kept in
indexes only, not as a column (Q4, decided 2026-10-18: a hidden `_series_id` column was measured and decided against,
[spec 24](0024-in-list-pruning.md)).

### Documented schema templates (not mandatory)
| Template | Example schema | Best for |
|----------|----------------|----------|
| **Narrow** | `device_id, metric, ts, value, quality` | Heterogeneous devices, thousands of metric types, sparse data |
| **Wide** | `device_id, ts, temp, pressure, rpm, …` | Homogeneous fleets, metrics sampled together |
| **Event** | `device_id, ts, event_type, payload (variant/JSON)` | Alarms, state changes, logs |
| **Series metadata** (dimension) | `device_id, site, asset_type, unit, firmware, …` | Tags kept out of the hot table and joined at query time |

IoT tip: include a `quality` column (OPC-UA-style good/uncertain/bad flag). It is common in industrial data and
supported by the function library.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D5 | User-defined schemas with a time-series table contract | Accepted |

## Open questions and issues

- [#130](https://github.com/polythings/timelake/issues/130) `timelake adopt`: turn an existing Iceberg table into a Timelake table

## History

- 2026-10-11: contract and templates (SPEC.md §7)
- 2026-10-12: built in v0
- 2026-10-16: `_ingest_ts` (table format 0.3.0)
- 2026-10-18: internal series ID decided (Q4)
- 2026-10-19: moved from SPEC.md into spec 6, then into this spec when the specs were regrouped one per feature.
