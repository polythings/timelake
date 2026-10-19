---
number: 9
title: Write path, freshness and the hot tier
area: Writing
status: in-progress
updated: 2026-10-12
related: [10, 12, 16, 35]
issues: [40, 53, 54, 67, 86, 107]
---

# 9. Write path, freshness and the hot tier

**In progress** · updated 2026-10-12
· related: [spec 10](0010-backfill.md), [spec 12](0012-late-data-and-dedup.md), [spec 16](0016-wasm-writes.md), [spec 35](0035-ingest-and-rules.md)

## Summary

Writers buffer and commit every T seconds or S MB, with idempotent batch IDs. Freshness comes in three modes: lake only (built), an object-store WAL (about 1 s), and a managed hot store (sub-second); the reader merges the hot tier after each commit's WAL high-water mark.

## Design

### Ingestion modes
- **Continuous:** Records stream into `timelake-ingest` or the Rust/Python writer. They are buffered and committed to Iceberg every *T* seconds or *S* MB (defaults: 60 s / 128 MB).
- **Batch / backfill:** Bulk loads of historical data go through the backfill path ([spec 10](0010-backfill.md)).
- **Idempotency:** Each batch carries an ID that is recorded in the snapshot summary, so retried commits are no-ops.

### Freshness tiers (users choose; the default will be decided by profiling B vs. C — Q5, #107)

| Mode | Extra infra | Freshness (Timelake reader) | Freshness (plain DuckDB) | Notes |
|------|-------------|-----------------------------|--------------------------|-------|
| **A. Lake-only** | none | commit interval (~1 min) | commit interval | Simplest and cheapest. No moving parts. |
| **B. Object-store WAL** | none (uses the same bucket) | ~1 s | commit interval | Arrow IPC segments appended to object storage, coordinated with conditional writes (S3 `If-None-Match`/`If-Match`; GCS/Azure preconditions). Optionally on S3 Express One Zone for lower latency. Still serverless. |
| **C. Managed hot store** | Valkey/Redis, Postgres, or DuckDB | sub-second | commit interval* | Also serves a "latest value per series" cache and sub-ms point lookups. |

\*Plain DuckDB can see hot data via a generated view that unions the Iceberg scan with the hot store (e.g. DuckDB's Postgres scanner), or via the future Timelake DuckDB extension.

### Lake/hot-tier consistency
- Each lake commit records the **WAL high-water mark** (per ingest partition) in its snapshot summary.
- A Timelake reader reads a snapshot and then reads from the hot tier only *after* that mark. This avoids gaps and double counting, with no coordination service.
- The hot tier is a **cache of uncommitted and recent data**, never the source of truth. It can be wiped and rebuilt from the WAL or the lake.
- The hot tier is accessed through a `HotStore` trait so backends are pluggable (R4).

## Open questions and issues

- [#40](https://github.com/polythings/timelake/issues/40) Hot tier: prototype the object-store WAL on Lance's MemWAL design (Q5)
- [#53](https://github.com/polythings/timelake/issues/53) WAL (mode B): design rules
- [#54](https://github.com/polythings/timelake/issues/54) Ingest: backpressure and limits
- [#67](https://github.com/polythings/timelake/issues/67) Ingest: measure arrival delay and derive how long a slot stays open
- [#86](https://github.com/polythings/timelake/issues/86) Spike: SlateDB for the object-store WAL (mode B) and a point-lookup index
- [#107](https://github.com/polythings/timelake/issues/107) Question: default hot tier, object-store WAL (mode B) or a managed hot store (mode C)

## History

- 2026-10-11: specified (SPEC.md §8.1–8.3)
- 2026-10-12: lake-only mode and idempotent batch IDs built (v0)
- 2026-10-19: moved from SPEC.md into its own spec.
