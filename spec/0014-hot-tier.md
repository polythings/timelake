---
number: 14
title: Hot tier and seconds-fresh reads
area: Writing
status: planned
updated: 2026-10-12
related: [11, 21, 36]
issues: [40, 53, 86, 107]
---

# 14. Hot tier and seconds-fresh reads

**Planned** · updated 2026-10-12 · milestones: [Seconds-fresh data and ingestion](milestones/fresh-data.md), [Technical preview](milestones/technical-preview.md)
· related: [spec 11](0011-appends.md), [spec 21](0021-reader.md), [spec 36](0036-ingest-and-rules.md)

## Summary

Data visible to the Timelake reader within about a second of being written, before it is committed to the lake.
Freshness comes in three modes: lake only (built), an object-store WAL (about 1 s, no extra infrastructure), and a
managed hot store (sub-second); the reader merges the hot tier after each commit's WAL high-water mark.

## Design

### Freshness tiers (users choose; the default will be decided by profiling B vs. C — Q5, #107)

| Mode | Extra infra | Freshness (Timelake reader) | Freshness (plain DuckDB) | Notes |
|------|-------------|-----------------------------|--------------------------|-------|
| **A. Lake-only** (built) | none | commit interval (~1 min) | commit interval | Simplest and cheapest. No moving parts. |
| **B. Object-store WAL** | none (uses the same bucket) | ~1 s | commit interval | Arrow IPC segments appended to object storage, coordinated with conditional writes (S3 `If-None-Match`/`If-Match`; GCS/Azure preconditions). Optionally on S3 Express One Zone for lower latency. Still serverless. |
| **C. Managed hot store** | Valkey/Redis, Postgres, or DuckDB | sub-second | commit interval* | Also serves a "latest value per series" cache and sub-ms point lookups. |

\*Plain DuckDB can see hot data via a generated view that unions the Iceberg scan with the hot store (e.g. DuckDB's
Postgres scanner), or via the DuckDB extension ([spec 33](0033-duckdb-extension.md)).

### Lake/hot-tier consistency
- Each lake commit records the **WAL high-water mark** (per ingest partition) in its snapshot summary.
- A Timelake reader reads a snapshot and then reads from the hot tier only *after* that mark. This avoids gaps and
  double counting, with no coordination service.
- The hot tier is a **cache of uncommitted and recent data**, never the source of truth. It can be wiped and rebuilt
  from the WAL or the lake.
- The hot tier is accessed through a `HotStore` trait so backends are pluggable (R4).

## Open questions and issues

- [#40](https://github.com/polythings/timelake/issues/40) Hot tier: prototype the object-store WAL on Lance's MemWAL design (Q5)
- [#53](https://github.com/polythings/timelake/issues/53) WAL (mode B): design rules
- [#86](https://github.com/polythings/timelake/issues/86) Spike: SlateDB for the object-store WAL (mode B) and a point-lookup index
- [#107](https://github.com/polythings/timelake/issues/107) Question: default hot tier, object-store WAL (mode B) or a managed hot store (mode C)

## History

- 2026-10-11: freshness tiers and consistency (SPEC.md §8)
- 2026-10-19: moved from SPEC.md into spec 9, then into this spec when the specs were regrouped one per feature.
