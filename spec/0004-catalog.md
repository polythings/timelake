---
number: 4
title: Catalog
area: Foundations
status: in-progress
updated: 2026-10-12
related: [3, 18, 40]
issues: [105, 108, 129]
---

# 4. Catalog

**In progress** · updated 2026-10-12
· related: [spec 3](0003-iceberg-and-commits.md), [spec 18](0018-table-head.md), [spec 40](0040-snowflake.md)

## Summary

Tables are committed through the object store alone (`vN.metadata.json` with put-if-absent, Hadoop-catalog compatible), so plain DuckDB and Spark read them without a service. An optional Iceberg REST catalog, `timelake-catalog`, adds commit queueing, metadata caching and scan planning; it is planned, not built.

## Design

A catalog alone doesn't make data faster to scan: file layout, compaction, and indexes do that, and they work with any catalog. What a catalog we control *does* optimize is everything around those:

| Lever | What `timelake-catalog` does | Without it (third-party catalog) |
|-------|------------------------------|----------------------------------|
| **Commit throughput** | Queues commits per table server-side and can coalesce concurrent append-only commits into one snapshot. No retry storms from optimistic concurrency (OCC) conflicts. | Writers retry on conflict; need a single committer per table |
| **Metadata latency** | Caches table metadata, manifests, and Puffin indexes in memory | Every reader re-fetches from object storage |
| **Server-side scan planning** | Implements the Iceberg REST scan-planning endpoints using our series/time indexes, so any engine that delegates planning gets Timelake pruning | Engines only use min/max statistics |
| **Maintenance triggers** | Emits commit events that drive compaction and rollup refresh as work arrives, with no polling or schedules | Cron schedules |
| **Timelake state** | Stores hot-tier watermarks, rollup registry, retention policies, delete audit log, tenant configuration | Stored in table properties / snapshot summaries (works, but slower to query) |
| **Security / multitenancy** | AuthN/Z, scoped credential vending (STS / presigned URLs), per-tenant quotas | Whatever the third-party catalog offers |

**Design constraints (to stay true to R13):**
- **Implements the standard Iceberg REST spec.** DuckDB, Spark, Trino, PyIceberg, and Snowflake attach unchanged.
- **Stateless process:**
  - Default backend is *object storage only*: the current-metadata pointer is swapped with conditional writes, so no database is needed.
  - Postgres is an optional backend for very high commit rates or many tables.
- **Scale-to-zero friendly:** Deployable as a Lambda / Cloud Run function, a container, or **in-process as a library** for single-node and edge setups.
- **Optional:** All Timelake features degrade gracefully on S3 Tables, Glue, Polaris, or Lakekeeper (right-hand column above).

**Build vs. extend:** Lakekeeper is an Apache-2.0 Iceberg REST catalog written in Rust. In Phase 0 we evaluate extending or forking it versus building a focused catalog on `iceberg-rust`. *Open question Q2 ([#105](https://github.com/polythings/timelake/issues/105)).*

## Decisions

| ID | Decision | Status |
|---|---|---|
| D2 | Ship our own Iceberg REST catalog, `timelake-catalog`, that is optional and scales to zero ([spec 4](0004-catalog.md)) | Accepted |
| D17 | v0 catalog layout is Hadoop-catalog compatible: `<table>/metadata/vN.metadata.json` committed with put-if-absent, plus `version-hint.text`. Plain DuckDB `iceberg_scan('<table dir>')` and Spark's HadoopCatalog read tables with no catalog service. A REST endpoint can serve the same layout later ([spec 4](0004-catalog.md)). | Accepted (v0) |

## Open questions and issues

- [#105](https://github.com/polythings/timelake/issues/105) Question: build timelake-catalog on iceberg-rust or extend Lakekeeper
- [#108](https://github.com/polythings/timelake/issues/108) Question: where rollup definitions live, table properties or the catalog
- [#129](https://github.com/polythings/timelake/issues/129) timelake-catalog: an Iceberg REST catalog service

## History

- 2026-10-11: D2 accepted (SPEC.md §5.2)
- 2026-10-12: D17, the object-store catalog of v0, built
- 2026-10-19: moved from SPEC.md into its own spec.
