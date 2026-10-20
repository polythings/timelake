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

### Pivot

[pivotlake/pivot](https://github.com/pivotlake/pivot) · MIT OR Apache-2.0 · Rust · studied 2026-10-20 (code read at
`676bb2c`)

An analytics server on Parquet. A fork of DuckDB parses, binds and optimizes SQL over FFI. Pivot's own operators run
the plan on a thread-per-core worker pool with io_uring, direct I/O and a ring of 2 MiB huge pages. Its own tables are
Delta Lake, and it reads Iceberg (read-only, through REST catalogs). It speaks the Postgres wire protocol, is
append-only and has no time-series functions. Timelake takes its engine from DataFusion and puts its work into the
table; Pivot builds its own engine.

**Layout: soft sort and overlap-aware compaction.** A table declares `sort_by`. Compaction splits files at half the
target size:
- Small files merge once they reach the target bytes, or once 100 of them pass a balance test (the total is at least
  5× the largest file, each file counted as at least 5 MB).
- Files at least half full are rewritten for layout. Two files in one partition pair up when more than 20% of each
  file's row groups lie inside the other's sort-key range, measured on the first sort column where they differ. A merge
  takes a full group of six files that all pair with each other and cuts their shared range into narrower files.
  Smaller groups wait for more files.
- A simulation pinned in their tests: 1,000 identical full-range files converge in 472 merges, with 864 files left
  spanning 5–6 keys each.
- Data whose sort key arrives in order, such as time, costs no layout work.

Our planner looks at file size and commit order, not at key overlap (#12, #69).

**Reading.**
- *Metadata.* Every file's footer is read once and kept in memory. A query freezes a snapshot and does no metadata
  I/O. The process sees its own commits at once and polls for other processes' commits every 30 s. The head object
  ([spec 7](spec/0007-table-head.md)) gives us the same in one request, without a long-running process.
- *Dictionary pruning.* For `col = const`, Pivot searches the raw dictionary page before decoding. When the value isn't
  there, the row group's data pages are dropped without being decompressed. This needs `encoding_stats` to prove that
  every page is dictionary-encoded. Pivot's own writer doesn't write them, so the pruning only works on files from other
  writers. On a dictionary-encoded string column, `=` becomes a compare of 16-byte views.
- *Late materialization in two phases.* A narrow scan of the filter and sort columns keeps row-group and row ids. The
  other columns are then read only for the surviving rows, grouped by row group, and pages with no surviving rows are
  not decompressed. Pivot doesn't use the page index, so I/O is still whole column chunks; the saving is CPU.
- *Top-N.* Row groups go out best bound first (smallest minimum when ascending, largest maximum when descending).
  Speculative reads are limited while the bound tightens. The top-N's current k-th value, and a join build's minimum and
  maximum, prune row groups at run time. DataFusion 54 turns on such top-K, join and aggregate dynamic filters by
  default.
- *Iceberg v3 deletion vectors* (their latest commit):
  - Puffin blobs are read through the data cache. The decoder drops deleted rows and refills batches to full size.
  - `COUNT(*)` subtracts the vectors' cardinalities from the manifests.
  - Position and equality delete files are refused, not skipped.

**Caching and I/O.**
- *Two tiers in memory.* One holds compressed bytes: 4 KB-aligned extents packed into 2 MiB slots, filled once however
  many queries ask for them. The other holds decompressed pages with their parsed headers, so a hit skips I/O, parsing
  and decompression.
- *Eviction across both tiers.* One CLOCK covers both, with a target share for compressed bytes. When one tier evicts
  its copy of some bytes, the other tier's copy is protected more, since I/O is needed only once both are gone.
- *Disk tier.* One sparse file per object, with byte X of the object at offset X of the file. After a restart, the
  resident set is rebuilt with `SEEK_DATA`/`SEEK_HOLE`, so the file is its own index. Keys leave out presigned query
  strings.
- *Read-ahead by medium.* Up to 16 row groups or 64 GETs in flight per worker on remote storage, and 1–2 row groups on
  local disk.
- *Counters.* Per-query counts of HTTP requests, bytes and cache hits cost nothing unless a query asks for them.
- *Gaps.* No backoff on 503 or 429, GETs of at most 2 MiB, and no page index.

**Execution.** Mostly beyond what we change in an embedded DataFusion, but worth knowing:
- A worker runs the downstream operator first, while its input is still in cache, and idle workers steal from the
  source end.
- Hash aggregation places slots by the hash's top bits, so each partition is a contiguous range in a table of any size,
  and tables merge without rehashing.
- A filter evaluates conjuncts one at a time. It compacts the batch only when a cost model says later conjuncts will
  save more than the copy costs.
- `LIMIT` cancels the scan below it.
- For `GROUP BY … ORDER BY count DESC LIMIT k`, Pivot skips merge partitions whose summed partial counts can't reach the
  k-th value.

**Practice.**
- A/B runs of two commits on fresh hosts with instance-store disks, failing on a regression of more than 5% in hot
  runs.
- Release binaries built with profile-guided optimization and a raised instruction-set floor (x86-64-v3, Graviton
  features).
- The upstream `parquet` crate serves as the test oracle for their own reader.
- `SET perf=1` profiles one query.
- A `perf.data` viewer that attributes cache and TLB misses (AMD IBS).

**Ideas to adopt:**
- [ ] Overlap-aware compaction for #12 and #69: pair files whose (series, time) ranges share more than a set share of
  their row groups, and merge a full group at once. Prove convergence with a pinned simulation in the property tests
  (#61).
- [ ] Refuse tables with delete files we can't apply. `Table::files` doesn't check a manifest's content, and the WASM
  reader skips delete manifests. So a row-level DELETE by another engine, or an imported table with deletes, returns
  deleted rows. Then read v3 deletion vectors (#41, #104), with `COUNT(*)` from their cardinalities.
- [ ] Late materialization on object storage without the extra round trip (#75): fetch a row group's filter and
  projection column chunks together through the block cache, then filter while decoding, so pushdown saves CPU there
  too.
- [ ] Dictionary pruning for equality on tag columns outside the sort key, as a first step before bloom filters (#33);
  it costs nothing on write if `encoding_stats` are written.
- [ ] Count requests and bytes per query in the engine, not only through s3proxy, for the CI gate (#140).
- [ ] Cache decompressed pages next to compressed bytes, with eviction aware of both copies (#85).
- [ ] A disk tier of one sparse file per object, indexed by its holes, in place of one file per 1 MiB block
  ([spec 22](spec/0022-block-cache.md)): fewer files and nothing to rebuild.
- [ ] Profile-guided optimization for the CLI release binary, kept if measured faster.

### Earlier studies

- Iceberg write support across Java, C++, Go and Rust: [spikes/iceberg-write](spikes/iceberg-write/README.md).
- Vortex as a file format: [bench/results/VORTEX.md](bench/results/VORTEX.md).

Comparisons with other systems on the same workload are benchmark results, not studies: see `bench/results/`.

### Adding a study

Add a section with the link, license, language and the date you studied it; what it is in two lines; what it does
differently and the trade-off; and a checklist of ideas to adopt. Study the method, not code to copy, and check the
project's license first: open-core, dual-licensed and commercial projects aren't named as a source
([CONTRIBUTING.md](CONTRIBUTING.md#credit)).
