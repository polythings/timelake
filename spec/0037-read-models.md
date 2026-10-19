---
number: 37
title: Static read models
area: Services and planned features
status: planned
updated: 2026-10-13
related: [21, 16, 38]
issues: [112]
---

# 37. Static read models

**Planned** · updated 2026-10-13
· related: [spec 21](0021-rollups.md), [spec 16](0016-wasm-writes.md), [spec 38](0038-data-to-events.md)

## Summary

Dashboards' usual requests answered by one GET: read models declared with the frontend's Zod schemas, stored as time tiles in the bucket, and regenerated from each commit's snapshot diff.

## Design

Dashboards almost always ask the same thing: one device or asset, a recent time range, a few fields at a fixed resolution. F2 answers those requests with one object GET from the bucket or a CDN instead of planning a query.

- **Defined from the frontend:** a read model is declared next to the component that renders it, with the Zod schema of the shape it needs (`@timelake/client` already uses Zod) plus its query: source table or rollup, series key (device/asset), fields, aggregates and resolutions. A build step exports the definition (Zod → JSON Schema) to `_timelake/read-models/<name>.json`, so the lake knows what to precompute and the frontend gets typed results.
- **Time tiles:** results are stored like map tiles: per series, per resolution level, per fixed tile (for example 1-minute buckets in day tiles, 1-hour buckets in month tiles) at deterministic paths such as `read-models/<name>/<series>/<resolution>/<tile>.arrow`. Any time range is a few tiles. Closed tiles never change, so they cache forever; only the open (current) tile and tiles hit by late data are rewritten.
- **Invalidation and regeneration on ingest:** each commit knows which series and time partitions it touched (the same snapshot diff that drives incremental rollups). Only the affected tiles are regenerated, by the writer inline or by a commit-triggered job. A small per-model manifest records the snapshot each tile reflects, so readers and caches never mix versions.
- **Cost guardrails:** regeneration is debounced per model (at most once per N seconds) and can be limited to "hot" series, because each refresh is a PUT. Tiles are built from rollups where a matching rollup exists.
- **Browser path:** `timelake-wasm` / `@timelake/client` fetch tiles by path, stitch them, and fall back to a normal query when a model doesn't cover the request.

## Open questions and issues

- [#112](https://github.com/polythings/timelake/issues/112) Question: read model tile format, tile sizes, and how much regeneration a commit may trigger

## History

- 2026-10-13: planned (F2, SPEC.md §21.2)
- 2026-10-19: moved from SPEC.md into its own spec.
