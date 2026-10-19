---
title: Deployment and cost model
updated: 2026-10-11
---

# Deployment and cost model

## Summary

The minimum deployment is a bucket, a catalog and the library in the user's app, plus scheduled maintenance; no always-on Timelake server. Cost comes down through fewer, bigger files, rollups and tiered retention.

## Design

- **Minimum deployment:** One bucket, one Iceberg catalog, and the Timelake library inside the user's app or one ingest container, plus a scheduled `timelake-maintain`. There is no always-on Timelake server.
- **Catalog options:**
  - **`timelake-catalog`** (recommended), in one of three forms:
    - In-process library (single node / edge)
    - Scale-to-zero function (Lambda / Cloud Run)
    - Container
    Object-storage backend by default; Postgres optional.
  - Bring your own: S3 Tables, Glue, Lakekeeper, Polaris, Nessie, Unity, with the reduced capabilities listed in [spec 32](0032-rest-catalog.md).
- **Cost levers:** Fewer, bigger files (lower GET/PUT costs); rollups instead of raw scans; tiered retention; optional cold storage classes for old partitions.

## Open questions and issues

- [#82](https://github.com/polythings/timelake/issues/82) Spike: push new table versions and recent rows to subscribed readers
- [#87](https://github.com/polythings/timelake/issues/87) Spike: serve readers in several regions from one bucket
- [#89](https://github.com/polythings/timelake/issues/89) Research: tiered reads, pushed freshness and many regions: tracking issue
- [#93](https://github.com/polythings/timelake/issues/93) Docs: cost on each cloud, measured where we can

## History

- 2026-10-11: specified (SPEC.md §15)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 28; a background page since the specs were regrouped one per feature.
