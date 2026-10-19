---
number: 22
title: Block cache
area: Reading
status: done
updated: 2026-10-18
versions: [0.5.0, 0.7.0]
related: [7, 21, 23]
issues: [42, 43, 71, 84, 85, 88]
---

# 22. Block cache

**Done** · updated 2026-10-18 · versions 0.5.0, 0.7.0 · milestone: [Fast first queries on object storage](milestones/fast-first-queries.md)
· related: [spec 7](0007-table-head.md), [spec 21](0021-reader.md), [spec 23](0023-filter-pushdown.md)

## Summary

A caching layer under every read: immutable objects (data files, manifests, index files) in 1 MiB blocks, in memory and optionally on local disk shared by processes, with a read's missing blocks fetched together in GETs of at most 1 MiB. Repeat queries on S3 run at local speed.

## Design

**Status:** built 2026-10-17. Measured on the benchmark instance on S3 (`bench/results/PERFORMANCE.md`, "P5"):
repeat runs of TSBS IoT 54 → 16 ms (geometric mean) with no requests, 1 / 4 / 16 clients 22 / 94 / 288 → 133 / 339 /
398 queries/s; TSBS DevOps repeats 85 → 27 ms. A second process with the disk cache warm: IoT first runs 151 → 71 ms.
256 KiB blocks gained nothing and cost more requests on first runs, so 1 MiB is the default.

**Why.** Within one process, table metadata, manifests and footers are already cached in memory, but every repeat
query on object storage fetches its data again. In the weekly run of 2026-10-16 on S3, repeat runs took 32 ms
(`one-truck-last-hour`, 4 requests) against 3 ms on local disk, and 135 ms (`cpu-max-all-8`, 8 requests) against
14 ms; 16 clients on S3 reached about half the local throughput. One request per query remains either way: the check
for a newer table version, which P6 addresses.

**Design.**
- A caching layer wraps the object store, so every read goes through it: DataFusion's Parquet reads, manifests,
  manifest lists, the last-value index and footers.
- Only immutable objects are cached: `*.parquet` and `*.avro` (data files, manifests, manifest lists, last-value
  index files), whose names carry a UUID and which Timelake never rewrites. The version hint, the head, table
  listings, `vN.metadata.json` and `timelake-tables.json` always go to the store, so freshness is as before.
- Objects are cached in aligned blocks (`timelake_cache_block_bytes`, default 1 MiB). A read takes the blocks it
  covers; missing blocks next to each other are fetched in one ranged GET, and concurrent reads of the same missing
  block share one fetch. The store's answer also gives the object's size, recorded with its blocks; a block whose
  length doesn't fit that size is treated as missing.
- Writes of cacheable objects put their blocks in the cache too, so a process reads what it just wrote without a
  request. Deleting an object drops its blocks.
- Two levels: memory (least recently used blocks evicted past `timelake_cache_memory_bytes`, default 256 MiB), and
  optionally local disk, shared by every process on the machine (`timelake_cache_disk_bytes`, default 4 GiB, in
  `timelake_cache_dir`, default the OS cache directory). Disk blocks are written to a temporary name and renamed into
  place; a process that finds the directory over its cap deletes the least recently used blocks (by modification
  time, refreshed on use at most once a minute). A block deleted while another process reads it is fetched again.
- `timelake_cache = off | memory | disk`: memory by default for object stores and HTTPS, off for local disk.
- Hits, misses and bytes fetched are counted per lake (`Storage::cache_stats`, `Lake.cache_stats()` in Python, and
  per run in the benchmark output).
- Not covered: the WASM reader and the TypeScript client (the browser has its own HTTP cache).

**Measure:** the S3 query-only runs and the 16-client test on the benchmark instance, and a new CLI process running
`one-truck-last-hour` against a warm disk cache, before and after; block sizes of 256 KiB and 1 MiB.

## Open questions and issues

- [#42](https://github.com/polythings/timelake/issues/42) Storage: lower the request rate when S3 throttles
- [#43](https://github.com/polythings/timelake/issues/43) Caches: bound metadata caches by bytes, with LRU eviction
- [#71](https://github.com/polythings/timelake/issues/71) Storage: hedge slow GETs
- [#84](https://github.com/polythings/timelake/issues/84) Cache: on a cold miss, also fetch the series' neighbouring pages (read-ahead)
- [#85](https://github.com/polythings/timelake/issues/85) Spike: keep cached data compressed (Parquet bytes, Arrow or Vortex)
- [#88](https://github.com/polythings/timelake/issues/88) Spike: an optional read-through cache shared by readers in one region

## History

- 2026-10-17: built (P5, SPEC.md §19.3)
- 2026-10-18: concurrent run fetches and 1 MiB requests
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 19; specs regrouped one per feature, with milestones on the roadmap.
