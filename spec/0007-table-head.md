---
number: 7
title: One-request table state and version checks
area: Tables and storage
status: done
updated: 2026-10-21
versions: [0.3.0, 0.6.0, 0.7.0, unreleased]
related: [2, 21, 22]
issues: [133]
---

# 7. One-request table state and version checks

**Done** · updated 2026-10-21 · versions 0.3.0, 0.6.0, 0.7.0, unreleased · milestone: [Fast first queries on object storage](milestones/fast-first-queries.md)
· related: [spec 2](0002-commits.md), [spec 21](0021-reader.md), [spec 22](0022-block-cache.md)

## Summary

Each commit also writes a head object with the table's metadata, live files, the last-value index and Parquet footers, so a cold query plans after one GET instead of six or seven dependent requests. Readers validate it with a listing or a check for the next version, and may reuse a checked version for `version-check-interval`.

## Design

**Status:** P1–P3 built and measured (2026-10-14). From a laptop, cold `one-truck-latest` went 502 → 314 ms on S3 with P1, and 810 → 213 ms on Tigris with P1–P3 ([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md)). The WASM reader and TypeScript client read the head too. Decisions taken on 2026-10-14 are recorded below.

**Goal:** a cold query learns everything it needs to plan in one GET, so a small query takes two dependent round trips (table state, then data or the last-value index) instead of six or seven.

**The head object.** After each commit, the writer also writes `<table>/metadata/timelake-head` with the state of the current version:
- the whole `vN.metadata.json` (schema, partition spec, sort order, properties, snapshots), so writers can commit from a head-loaded table too. With the default limits it is tens of KB and compresses well.
- the live data files: path, partition, row count, size, sequence numbers, and per column the bounds and counts (what the manifests hold).
- the last-value index while it fits the inline budget, otherwise only its path (in the metadata).
- P2: the Parquet footers and page indexes of the live files, in the same budget.

**Decided:**
1. **Validate the head on every load.** Results are always as current as before; trusting the head stays P6's opt-in staleness bound. How (refined while building P2):
   - **Cold load** (the process has no version of the table): on stores that list by prefix (S3, GCS, Azure, local disk), LIST `metadata/v` together with the head GET. The listing names the newest version, so a current head is confirmed in the same round trip and an outdated one is skipped for the metadata chain. Without this, the check for `vN+1` was a second round trip on the critical path, and once footers came from the head (P2) nothing overlapped it. A LIST costs like a PUT on S3 ($0.005 per 1,000), once per table per process.
   - **Warm load** (the process has version N): check whether `vN+1.metadata.json` exists, with no head download. Queries start the check and the scan awaits it; a stale version makes the scan reload the table and plan the newest one. `Lake::table()` and writers wait for it.
   - **Plain HTTPS** (no listing): GET the version hint with the head, then the deferred check.
2. **How to check for `vN+1`, per store:** a HEAD request by default; a LIST of the exact name (one key, `max-keys=1`) on Tigris, where a HEAD of a missing key takes 285 ms and the LIST 40 ms. Storage option `timelake_freshness_check=head|list` overrides it; `list` needs S3, GCS or Azure. The metadata chain uses the same check.
3. **One inline budget** for optional sections, `timelake.head-inline-bytes`, default 1 MiB: the last-value index first, then (P2) footers. The files and metadata are always in the head.
4. **Format: pluggable, Arrow IPC by default.** The head's content is format-neutral: string properties plus named Arrow tables (`metadata`, `files`, `columns`, `last_values`). A `HeadCodec` encodes it, and the head's header (`TLHD`, header version, table version, codec name) names the codec, so readers decode any head whose codec they have. Built in: `arrow` (Arrow IPC streams, zstd) and `json` (Arrow's JSON rows plus serde schemas). Avro, Vortex, BSON or Parquet are one more codec each (all have Arrow bridges), registered with `Lake::register_head_codec`. Tables choose with `timelake.head-format` (`none`: no head; a writer without the chosen codec deletes the head rather than leave a stale one).

**Writing it.** Iceberg metadata is written exactly as before and `vN+1.metadata.json` stays the commit point, so DuckDB, Spark and PyIceberg see no change. The head is a cache of that commit, written in the background like the version hint (P4), so commits don't get slower. The writer reads the first bytes of the stored head (version and ETag) and replaces it only with a conditional write on that ETag, so racing writers never move it back to an older version. Stores without conditional updates (local disk) overwrite. A commit knows its new file list from its base when the base was loaded from a head, so writing the head reads no manifests.

**Reading it.** The head and the listing (or the version hint) are read at the same time. A table without a head (another engine, an older Timelake, a head removed by orphan-file cleanup) loads exactly as before, with no extra round trip; a listing or hint newer than the head skips the head.

**Measured round trips** (unit test `cold_queries_read_the_head_first`): cold `latest` 3 → 1, cold filtered scan 6 → 2.

**Known limit:** on plain HTTPS (no listing), if an older Timelake (which writes no heads) commits more than `write.metadata.previous-versions-max` (20) times after a newer one wrote a head, and expiry deletes `vN+1.metadata.json`, a reader can trust the old head. Fix: delete `metadata/timelake-head` after writing a table with an old version, or upgrade every writer.

**P2, footers.** The head carries the Parquet footers (encoded with `ParquetMetaDataWriter`, page indexes included) of the newest files, newest partition first, in what the last-value index leaves of the budget. The writer reads each new file's footer once in the background (one GET) and keeps footers in memory, so later heads don't read them again. Scans put footers from the head straight into DataFusion's metadata cache.

**P3, warm connections.** While the head GET and the listing are in flight, a cold load sends a few small HEAD requests (on the version hint), so the data reads that follow find open connections. Storage option `timelake_warm_connections`. Measured on S3 and Tigris: at most about 20 ms off cold scans, nothing on point queries, so it is off by default.

**Cost option.** `timelake_version_lookup=hint` makes a cold load read the version hint instead of listing (one more round trip, no LIST). Options are named for the request they change, not for an outcome ("cheap"), so the trade-off is visible; the docs explain each one and why its default was chosen. Requests per operation and their prices are documented in the storage docs ("Requests and cost").

**TypeScript client and WASM reader.** The client GETs the head and the hint together; the WASM reader decodes heads written with the built-in codecs (the codecs and header live in `timelake-format` under its `head` feature, shared with the engine). A HEAD for `vN+1` runs alongside the query's data reads; if a newer version exists, the result is dropped and the query reruns on the metadata chain from `vN+1`. `latest` answers from the inline index without a request, and scans open files from inline footers.

### Staleness bound (P6, built 2026-10-17)

Setting `version-check-interval` (table, session; default 1 s, the planning cache's old lifetime): a reader uses a
version it loaded or checked that long without the per-query check; writers always check, and a process sees its own
commits at once. `0s` checks on every query (one query's lookups share one check). Unit test: a warm `latest` takes 1 → 0
round trips. Not measured on S3 yet. The TypeScript client follows the table's setting too unless it is given
`cacheMs` (2026-10-21; it reused a version for 5 s before). Not built: a head pointer on S3 Express One Zone, for
single-digit-millisecond checks.

## Open questions and issues

- [#133](https://github.com/polythings/timelake/issues/133) Head pointer on S3 Express One Zone for millisecond version checks

## History

- 2026-10-14: P1–P3 built and measured (SPEC.md §19.2)
- 2026-10-17: P6 staleness bound built
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 18; specs regrouped one per feature, with milestones on the roadmap.
- 2026-10-21: the TypeScript client reuses a version for the table's `version-check-interval` unless given `cacheMs`.
