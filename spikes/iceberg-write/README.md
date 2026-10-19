# Spike: iceberg-rust write path (Q7, spec 3)

**Question:** Can `iceberg-rust` 0.10.1 carry Timelake's write path, or do we need to contribute upstream, fork, or build our own layer?

**Code:** removed once the question was answered (D13, spec 3). To run it again, check out the last commit that had it:
```bash
git checkout 2d66939 -- spikes/iceberg-write Cargo.toml Cargo.lock
cargo run --release -p spike-iceberg-write -- v2 100   # or v3; second arg = number of small commits
```

**Environment:** iceberg 0.10.1 (Arrow/Parquet 58), Rust 1.94 (iceberg 0.10.1 requires it; pinned via `rust-toolchain.toml`), DuckDB 1.5.6, local filesystem, memory catalog. Measured 2026-10-11.

## Verified by running

| Capability | v2 | v3 | Notes |
|---|---|---|---|
| Create a table with a `day(ts)` partition spec, sort order `(device_id, metric, ts)`, and `timelake.*` properties | ✅ | ✅ | |
| Partition-split + zstd Parquet write, one `fast_append` commit | ✅ | ✅ | 576k rows → 3 files (one per day), ~250 ms incl. commit |
| Custom snapshot summary properties (watermarks, batch IDs, changed ranges) | ✅ | ✅ | `set_snapshot_properties` |
| Late data into an old partition | ✅ | ✅ | |
| Concurrent appends from a stale base | ✅ | ✅ | 2 sequential + 8 parallel commits: all rebased and retried automatically, **no lost rows** (798,000 expected = 798,000 read) |
| Scan planning prunes by time partition | ✅ | ✅ | last-day filter: 111 of 114 files (exactly the files in overlapping day partitions) |
| Scan planning prunes by series | ➖ | ➖ | No file-level pruning: every live file contains all devices. Expected; this is what bucket partitioning and Puffin series indexes are for ([spec 6](../../spec/0006-table-contract.md), [spec 17](../../spec/0017-read-paths.md)). |
| **Plain DuckDB reads tables written by iceberg-rust** | ✅ | ✅ | `iceberg_scan(metadata.json)`; counts, per-day aggregates, and device + time filters all correct; 112 snapshots visible |

**Commit overhead (local disk, 100 small commits):**
- Commit latency grew from ~12 ms to ~25–29 ms. Each `fast_append` adds one manifest, and the manifest list and metadata.json grow with every snapshot.
- The metadata directory reached 304 files / ~6 MB. That's 3 new files per commit, with no manifest merging.
- On S3, expect roughly 100–300 ms per commit. This is why the spec batches commits and uses a hot tier for freshness.

## Missing in iceberg-rust 0.10.1 (from reading the source)

| Needed for | Missing capability |
|---|---|
| Compaction, dedup, tiered partitioning, GDPR purge (specs 14, 15) | **Rewrite/replace commit** (swap files atomically). Only `fast_append` exists. |
| Logical deletes, `replace` backfill mode (specs 10, 13) | **Row-delta commit**: `fast_append` rejects delete files ("Only data content type is allowed"). **No position-delete or deletion-vector writer** either (only an equality-delete writer). |
| Backfill write-audit-publish (spec 10) | **Commits to a branch**: the snapshot producer always targets `main`. |
| Keeping metadata small under frequent commits | **Manifest merging / rewrite manifests** |
| Storage cleanup, GDPR purge | **File deletion on snapshot expiry, orphan-file removal**: `expire_snapshots` only edits metadata |
| Extending any of the above outside the crate | `TransactionAction`, `SnapshotProducer`, and the `TableCommit` builder are all `pub(crate)` |

## What *is* public, i.e. the escape hatch
Everything needed to produce snapshots ourselves:
- `ManifestWriterBuilder`, `ManifestListWriter::{v2, v3}`
- `TableMetadataBuilder::{add_snapshot, set_branch_snapshot, set_ref, remove_snapshots}`
- `TableUpdate` / `TableRequirement` (serde types, i.e. the Iceberg REST commit payload)
- `DeleteVector`, Puffin reader/writer, all scan/read paths

## Conclusion → recommendation for Q7

`iceberg-rust` is solid for the **append + read** path, and its output is fully DuckDB-compatible on both v2 and v3. Everything *beyond* append is missing. That is most of what makes Timelake valuable: compaction, deletes, branches, cleanup.

Recommendation: **no fork. Build a Timelake commit layer on iceberg-rust's public types, and upstream it over time.**
- `timelake-core::commit` writes manifests and manifest lists itself and produces these snapshot operations: `append`, `replace` (compaction), `row_delta` (deletion vectors), `overwrite`, branch commits, `rewrite_manifests`.
- Commits go out as standard REST `{requirements, updates}` payloads:
  - natively in `timelake-catalog`
  - via a small HTTP client for third-party REST catalogs
- We keep using iceberg-rust for types, reading, scan planning, Puffin, and the Parquet writer.
- We propose our operations upstream as they stabilize. This keeps the delta small and avoids a hard fork.

**Next spikes:**
1. Prove one `replace` commit (compact the 100 live files into 1) built with the public types, and check that DuckDB reads the result correctly.
2. Write a v3 deletion vector + row-delta commit and check DuckDB honors it. This decides Q1 (v2 vs. v3) for deletes.
3. Run against S3/MinIO + a REST catalog to get real commit latencies.

## Reference implementations to learn from (surveyed 2026-10-11)

| Implementation | Rewrite / overwrite | Row delta + DV writer | Branch commits | Expire w/ file cleanup | Manifest merging | Orphan cleanup |
|---|---|---|---|---|---|---|
| Iceberg Java | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ (Spark action) |
| **iceberg-cpp v0.4.0** (2026-09-26) | ✅ `RewriteFiles`, `OverwriteFiles`, `ReplacePartitions` | ✅ `RowDelta`, `DVWriter`, position/equality delete writers | ✅ `ToBranch` | ✅ cleanup levels | ✅ `MergeAppend` | not seen |
| iceberg-go 0.6.0 (2026-06-01) | ✅ `RewriteDataFiles` (bin-pack), overwrite | ✅ `DVWriter`, equality deletes | branch create | ✅ | not listed | ✅ |
| RisingWave fork of iceberg-rust + `iceberg-compaction` | ✅ via DataFusion | applies deletes on rewrite | ✅ `with_to_branch` | ❌ | ? | ❌ |
| Apache iceberg-rust 0.10.1 | ❌ | ❌ | ❌ | metadata only | ❌ | ❌ |

- **iceberg-cpp** is now the most complete non-JVM implementation. Its structure mirrors Java, which makes it the best reference for how each commit operation works.
- We don't link it via FFI: it needs a C++23 toolchain, which is the same cross-compilation concern as embedding DuckDB (D3, spec 5).
