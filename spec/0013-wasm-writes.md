---
number: 13
title: Writes from WebAssembly
area: Writing
status: done
updated: 2026-10-17
versions: [0.6.0]
related: [2, 7, 19]
issues: [37]
---

# 13. Writes from WebAssembly

**Done** · updated 2026-10-17 · versions 0.6.0 · milestone: [Writes from JavaScript](milestones/javascript-writes.md)
· related: [spec 2](0002-commits.md), [spec 7](0007-table-head.md), [spec 19](0019-compaction.md)

## Summary

Append to and maintain tables from Cloudflare Workers and other JavaScript runtimes: the shared write path (rows, file writer, Iceberg manifests, commits, maintenance) in `timelake-format`, driven by `@timelake/client` through a store. Built and measured in workerd.

## Design

**Status:** built and measured in workerd ([`bench/results/WORKERS.md`](../bench/results/WORKERS.md)). `TableWriter` in `@timelake/client` (`/workers` entry with `initWasm` for Workers, which can't compile WebAssembly from bytes) appends and maintains through a `Store` (`r2Store`, `memoryStore`, or the application's). The shared write path lives in `timelake-format` (`write` feature): rows, file writer, Iceberg manifests and metadata, commits, maintenance, over a `Host`. Measured in workerd: appends of 100,000 rows 0.7 s and 37 MiB; compaction of a day of 24 files (1.2M rows) 2.7 s and 95 MiB. Interop tests: the engine and the shared writer append to and maintain one table in turn (`crates/timelake/tests/shared_writer.rs`). Changes from the design below: the read side of compaction reads a window of pages at a time and merges at most 24 files at once; files written from JavaScript have 8,192-row data pages (setting `data-page-rows`); the WASM host caps row groups being written at 16 MiB. Not yet: creating tables from JavaScript, rollup refresh, signing S3 requests.

**Goal.** Append to Timelake tables, and keep them maintained, from JavaScript runtimes where the Rust engine can't run: Cloudflare Workers first, then browsers, Deno, Bun, and Node. Workers run JavaScript and WebAssembly; Python Workers can't load the native Python package.

**Limits it is designed for** (Cloudflare, October 2026): 128 MB of memory per isolate, WebAssembly included; CPU time 10 ms per request on the free plan, 30 s by default and up to 5 minutes on paid plans; 64 MiB per Worker uncompressed. The read bundle is 8.9 MB.

**Design.**
- **Who does what.** The WASM module builds every file and decides every step; it does no I/O. The TypeScript client (`@timelake/client`) sends the requests through a store: an R2 binding, a local directory (Node, tests), memory (tests), or one the application supplies (`get`, `getRange`, `put`, `putIfAbsent`, `delete`, `list`).
- **Append.** The same path as the engine: rows (JavaScript objects, or Arrow IPC) are aligned to the table's schema, given `_ingest_ts`, sorted by `(partition, series…, time)`, deduplicated within the append, and written as Parquet with the engine's settings (ZSTD, page statistics, delta and dictionary encodings, the sort order). Then the manifest, manifest list, updated last-value index and head.
- **Commit.** The two-step commit (P4): upload everything in parallel, then make the one conditional write of `vN+1.metadata.json` (R2 and S3 support create-if-absent). On a conflict, the WASM side rebases onto the new version and retries, reusing the data files. The version hint and head are written after.
- **Iceberg metadata in our own code.** iceberg-rust pulls in tokio, reqwest and ring and doesn't build for `wasm32-unknown-unknown`, so the shared core writes manifests, manifest lists and table metadata itself, as the WASM reader already reads them. Tests keep the two writers equivalent: the engine (iceberg-rust) loads tables the WASM writer wrote and the other way round, plain DuckDB reads them, and the same input gives the same rows.
- **One core for both.** Writing files (sort, split, dedup, Parquet settings, last-value index, head) and the streaming merge ([spec 19](0019-compaction.md)) move to a crate without I/O that the engine and `timelake-wasm` both use, so they can't drift.
- **Inline maintenance.** After an append, the same thresholds as the engine (`auto-maintain`, `compact-min-files`, `compact-cold-after`) run streaming compaction within the per-run byte cap, then retention and snapshot expiry with file cleanup. `timelake maintain` elsewhere is optional, not required.
- **Not in the first version:** rollup refresh (it needs the SQL engine; rollups stay correct, since changed buckets are read from the raw table, and are refreshed by the next engine append or `timelake maintain`), schema changes, and request signing for S3 from the client (applications pass a store that signs).

**Recommended deployment.** One writer per table: a Queues consumer or a Durable Object that batches incoming rows into appends of a few thousand rows or more. Concurrent writers are safe (optimistic commits), but each conflict costs a retry, and many tiny appends make many tiny files.

**Measured before it ships:** peak memory and CPU time per append and per maintenance run in workerd, for appends of 1k, 10k and 100k rows and compaction of 8 small files and of a cold partition, to set the defaults (byte cap, batch and row-group sizes).

## Open questions and issues

- [#37](https://github.com/polythings/timelake/issues/37) WASM reader: plan byte ranges from page indexes, not whole row groups

## History

- 2026-10-17: decided and built (SPEC.md §21.10)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 16; specs regrouped one per feature, with milestones on the roadmap.
