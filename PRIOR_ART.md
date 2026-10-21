# Prior art

Timelake stands on other open-source projects and learns from many more. This file credits the ideas we took and
keeps our notes on what we're studying, so good ideas don't get lost and their authors get the recognition they're
due. We take ideas, never code: every line of Timelake is our own.

Which projects we credit, and how, is in [CONTRIBUTING.md](CONTRIBUTING.md#credit). For how Timelake compares with
other systems as products, see [the landscape](spec/landscape.md).

## What we took

### Foundations

Timelake is built on these. Their licenses ship with our packages through the normal dependency tooling.

| Project | License | What Timelake uses |
|---|---|---|
| [Apache Iceberg](https://iceberg.apache.org) | Apache-2.0 | The table format: metadata, snapshots, manifests, column bounds |
| [iceberg-rust](https://github.com/apache/iceberg-rust) | Apache-2.0 | Iceberg types and metadata. Our commit layer is our own code on its public types (D13) |
| [lakekit](https://github.com/polythings/lakekit) | Apache-2.0 | Iceberg manifests, metadata and commits without I/O, for the WASM reader and writer (D24); a sibling project |
| [Apache Parquet](https://parquet.apache.org), [arrow-rs](https://github.com/apache/arrow-rs) | Apache-2.0 | Data files, the last-value index, footers; Arrow in memory and as the head codec |
| [Apache DataFusion](https://datafusion.apache.org) | Apache-2.0 | The embedded query engine (`time_bucket` is its `date_bin`) |
| [object_store](https://github.com/apache/arrow-rs-object-store) | Apache-2.0 | Every read and write to S3, GCS, Azure and local disk |
| [DuckDB](https://duckdb.org) | MIT | The plain-SQL way to read our tables (`iceberg_scan`); the planned extension |

### Ideas

Methods we adopted or plan to, and implemented ourselves.

| Idea | From | License of the source | Where in Timelake |
|---|---|---|---|
| Size-tiered compaction (D19) | [Apache Cassandra](https://cassandra.apache.org) (SizeTieredCompactionStrategy) | Apache-2.0 | `maintain.rs` |
| Table layout readable with no catalog service (D17) | Iceberg's Hadoop catalog | Apache-2.0 | `lake.rs`; docs Architecture |
| Stage on a branch, check, then publish (backfill, planned) | Iceberg branches (write-audit-publish) | Apache-2.0 | [spec 15](spec/0015-backfill.md) |
| `quality` column (good / uncertain / bad) | OPC UA | Specification |  |
| Tables of aggregates per time bucket, kept as data arrives (rollups) | [Apache Druid](https://druid.apache.org) (rollup) | Apache-2.0 | `rollup.rs`; [spec 27](spec/0027-rollups.md) |
| Answering an aggregate query from a materialized aggregate that matches it | [Apache Calcite](https://calcite.apache.org) (materialized view rewriting) | Apache-2.0 | `query/route.rs`; [spec 27](spec/0027-rollups.md) |
| A time window's first, smallest, largest and last points (`m4`) | [Apache IoTDB](https://iotdb.apache.org) (the time-window `M4` function) | Apache-2.0 | `query/m4.rs`; [spec 31](spec/0031-m4.md) |
| Splitting object-store reads into parallel requests of a few MiB | Durner, Leis, Neumann, "Exploiting Cloud Object Storage for High-Performance Analytics" (VLDB 2023) | Paper | `block_cache.rs` (`timelake_cache_request_bytes`); #65 |
| How commit operations work (rewrite, row delta, branches, expiry) | [Iceberg Java](https://github.com/apache/iceberg), [iceberg-cpp](https://github.com/apache/iceberg-cpp), [iceberg-go](https://github.com/apache/iceberg-go) | Apache-2.0 | References to learn from and test oracles (D13, `spikes/iceberg-write`); our commit layer is our own code |

### Third-party files

Files we use as they are, not code we write. Each keeps its license, listed in [NOTICE](NOTICE).

| Files | From | License |
|---|---|---|
| `dashboard/assets/fonts/` | Geist and Geist Mono, by Vercel and basement.studio | SIL OFL 1.1 |
| `dashboard/components/ui/` | Generated with [shadcn/ui](https://ui.shadcn.com) | MIT |
| `bench/queries/tsbs_iot/` | Ported from [TSBS](https://github.com/timescale/tsbs) | MIT |
| `bench/queries/tpch/` | DuckDB's `tpch` extension (TPC-H queries) | MIT |

## Studies

Notes on projects we've read closely. Checkboxes are ideas we may adopt; tick one when it ships, and credit the
project where it's implemented.

### IceGate

[icegatetech/icegate](https://github.com/icegatetech/icegate) · Apache-2.0 · Rust · studied 2026-10-15 (README and
crate docs, not a full code read)

An observability lake: OpenTelemetry logs, traces and events (metrics planned) in Iceberg + Parquet on object storage,
queried through Loki-, Tempo- and Flight SQL-compatible APIs, with a DataFusion engine. A prototype. It shares
Timelake's stack, not its use case.

**Catalog.** One `root.json` lists every table; a commit replaces it with a compare-and-swap on its ETag (`If-Match`).
That gives atomic multi-table commits, renames and drops, and one read lists every table. The cost: every commit in
the warehouse contends on one object, and other engines find tables only through their REST server. Timelake commits
each table on its own (`vN.metadata.json` put-if-absent, D17), so tables never contend and any engine reads a
table's directory; multi-table atomicity is what the optional REST catalog (D2) would add. Their documented
lost-acknowledgement case (a commit that landed but whose reply was lost gets applied again) is worth remembering for
any commit path without a batch id.

**Fresh data: a Parquet WAL on object storage.** This is [spec 14](spec/0014-hot-tier.md) mode B, working:
1. Ingest buffers for up to 200 ms, writes one Parquet segment per topic with `If-None-Match` under an increasing
   offset, and replies only once the segment is stored.
2. A "shifter" turns segments into one Iceberg file per (tenant, day), commits, and records the committed offset.
3. A query reads the snapshot plus the segments above that offset, in one DataFusion plan.
4. Cleanup deletes segments below `committed − keep_segments_count`.

**Maintenance.** Merges small files and manifests, expires by TTL, removes orphans, and writes bloom filters on ID
columns such as `trace_id`, which aren't in the sort order.

**Ideas to adopt:**
- [ ] WAL rules for mode B ([spec 14](spec/0014-hot-tier.md)): delete segments by offset, never by age; size the kept margin to the longest
  query that may still be reading; a reader that meets a missing segment fails instead of returning fewer rows;
  recover offsets on restart with a search that still works across missing segments.
- [ ] One WAL object per request across all series and tenants, read with fan-out: on S3 a write costs about 10× a
  read.
- [ ] Evaluate Parquet instead of Arrow IPC for WAL segments: statistics let readers skip segments, and plain DuckDB
  can read them. It costs CPU on write.
- [ ] Compaction converges: merge a group only if it lowers the file count. Check that our size tiers
  (`maintain.rs`) guarantee the same, in a test.
- [ ] Opt-in bloom filters for high-cardinality columns outside the sort key, written by compaction only. Our sort
  order already prunes series-key lookups by min/max.
- [ ] Later: Prometheus- or Grafana-compatible read APIs for observability users.

### Earlier studies

- Iceberg write support across Java, C++, Go and Rust: [spikes/iceberg-write](spikes/iceberg-write/README.md).
- Vortex as a file format: [bench/results/VORTEX.md](bench/results/VORTEX.md).

Comparisons with other systems on the same workload are benchmark results, not studies: see `bench/results/`.

### Adding a study

Add a section with the link, license, language and the date you studied it; what it is in two lines; what it does
differently and the trade-off; and a checklist of ideas to adopt. Study the method, not code to copy, and check the
project's license first: open-core, dual-licensed and commercial projects aren't named as a source
([CONTRIBUTING.md](CONTRIBUTING.md#credit)).
