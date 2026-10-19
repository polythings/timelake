---
number: 32
title: REST catalog
area: Integrations
status: planned
updated: 2026-10-12
related: [3, 34, 35]
issues: [105, 108, 129]
---

# 32. REST catalog

**Planned** · updated 2026-10-12 · milestone: [Any engine can attach](milestones/any-engine.md)
· related: [spec 3](0003-object-store-catalog.md), [spec 34](0034-snowflake.md), [spec 35](0035-multitenancy.md)

## Summary

`timelake-catalog`, an Iceberg REST catalog that is optional and can scale to zero, so Snowflake, Trino, Athena and
Spark attach to Timelake tables in place. Beyond the standard API it queues commits, caches metadata and plans scans
with Timelake's indexes. Planned; the object-store catalog ([spec 3](0003-object-store-catalog.md)) is what is built.

## Design

A catalog alone doesn't make data faster to scan: file layout, compaction, and indexes do that, and they work with any
catalog. What a catalog we control *does* optimize is everything around those:

| Lever | What `timelake-catalog` does | Without it (third-party catalog) |
|-------|------------------------------|----------------------------------|
| **Commit throughput** | Queues commits per table server-side and can coalesce concurrent append-only commits into one snapshot. No retry storms from optimistic concurrency (OCC) conflicts. | Writers retry on conflict; need a single committer per table |
| **Metadata latency** | Caches table metadata, manifests, and Puffin indexes in memory | Every reader re-fetches from object storage |
| **Server-side scan planning** | Implements the Iceberg REST scan-planning endpoints using our series/time indexes, so any engine that delegates planning gets Timelake pruning | Engines only use min/max statistics |
| **Maintenance triggers** | Emits commit events that drive compaction and rollup refresh as work arrives, with no polling or schedules | Cron schedules |
| **Timelake state** | Stores hot-tier watermarks, rollup registry, retention policies, delete audit log, tenant configuration | Stored in table properties / snapshot summaries (works, but slower to query) |
| **Security / multitenancy** | AuthN/Z, scoped credential vending (STS / presigned URLs), per-tenant quotas ([spec 35](0035-multitenancy.md)) | Whatever the third-party catalog offers |

**Design constraints (to stay true to R13):**
- **Implements the standard Iceberg REST spec.** DuckDB, Spark, Trino, PyIceberg, and Snowflake attach unchanged.
- **Stateless process:**
  - Default backend is *object storage only*: the current-metadata pointer is swapped with conditional writes, so no
    database is needed. It serves the object-store catalog's layout, so tables don't change.
  - Postgres is an optional backend for very high commit rates or many tables.
- **Scale-to-zero friendly:** Deployable as a Lambda / Cloud Run function, a container, or **in-process as a library**
  for single-node and edge setups.
- **Optional:** All Timelake features degrade gracefully on S3 Tables, Glue, Polaris, or Lakekeeper (right-hand column
  above).

**Build vs. extend:** Lakekeeper is an Apache-2.0 Iceberg REST catalog written in Rust. We evaluate extending or
forking it versus building a focused catalog on `iceberg-rust` (Q2, #105).

## Decisions

| ID | Decision | Status |
|---|---|---|
| D2 | Ship our own Iceberg REST catalog, `timelake-catalog`, that is optional and scales to zero | Accepted |

## Open questions and issues

- [#105](https://github.com/polythings/timelake/issues/105) Question: build timelake-catalog on iceberg-rust or extend Lakekeeper
- [#108](https://github.com/polythings/timelake/issues/108) Question: where rollup definitions live, table properties or the catalog
- [#129](https://github.com/polythings/timelake/issues/129) timelake-catalog: an Iceberg REST catalog service

## History

- 2026-10-11: catalog design (SPEC.md §5, D2)
- 2026-10-19: moved from SPEC.md into spec 4, then into this spec when the specs were regrouped one per feature.
