---
number: 3
title: Object-store catalog
area: Tables and storage
status: done
updated: 2026-10-12
versions: [0.1.0]
related: [2, 7, 32]
---

# 3. Object-store catalog

**Done** · updated 2026-10-12 · versions 0.1.0 · milestone: [v0: a maintenance-free time-series table on S3](milestones/v0.md)
· related: [spec 2](0002-commits.md), [spec 7](0007-table-head.md), [spec 32](0032-rest-catalog.md)

## Summary

Tables are committed through the object store alone: `vN.metadata.json` written with put-if-absent, plus a version
hint, in the layout of Iceberg's Hadoop catalog. Plain DuckDB and Spark read the tables with no catalog service, and
concurrent writers need nothing but the bucket.

## Design

- **Layout:** `<table>/metadata/vN.metadata.json`, committed with a conditional write that fails if the version
  exists (S3 `If-None-Match`; GCS and Azure preconditions), and `<table>/metadata/version-hint.text`, updated after
  each commit, in the background since the two-step commit ([spec 2](0002-commits.md)).
- **Readers:** plain DuckDB `iceberg_scan('<table dir>')` and Spark's HadoopCatalog read the newest version through
  the hint. The Timelake reader finds it with a listing or the head ([spec 7](0007-table-head.md)).
- **Lake:** a lake is a prefix in a bucket; `timelake-tables.json` lists its tables.
- **Stores:** any store with conditional writes: S3, GCS, Azure, R2, Tigris, MinIO, RustFS, local disk. HTTPS for
  reading only.
- **Later:** the REST catalog ([spec 32](0032-rest-catalog.md)) can serve the same layout, adding commit queueing,
  caching and scan planning; tables don't change.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D17 | v0 catalog layout is Hadoop-catalog compatible: `<table>/metadata/vN.metadata.json` committed with put-if-absent, plus `version-hint.text`. Plain DuckDB `iceberg_scan('<table dir>')` and Spark's HadoopCatalog read tables with no catalog service. A REST endpoint can serve the same layout later. | Accepted |

## Open questions and issues

None open.

## History

- 2026-10-12: built in v0 (D17)
- 2026-10-19: moved from SPEC.md into spec 4, then into this spec when the specs were regrouped one per feature.
