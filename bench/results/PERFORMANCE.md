# Performance plan results

Measurements for the steps of the performance plan ([SPEC.md §19.1](../../SPEC.md)), newest first. Each step is
measured from a laptop (M1 Pro) over the internet, against the numbers before it.

## P8: filter pushdown, by store (2026-10-17)

Laptop (M1 Pro), release build, TSBS standard scale, maintained checkpoint, Timelake reader, each query in a fresh
process. Parquet filter pushdown (with filter reordering) decodes a row group's filter columns first and the other
columns only for rows that pass; the setting `filter-pushdown` chooses it.

**Local disk**, one first run and 5 repeats, geomean of per-query medians, each pair run twice:

| Geomean first / best | Off | Pushdown | Pushdown + reorder |
|---|---:|---:|---:|
| IoT (12 queries) | 29.7 / 10.7 ms | 29.7 / 10.3 ms | 29.4 / 10.2 ms |
| DevOps (15 queries), two runs | 30.6 / 16.5, 30.6 / 16.2 ms | | 28.9 / 15.6, 29.2 / 15.5 ms |

- Faster: IoT `long-daily-sessions` −40%, `long-driving-sessions` −30%, `stationary-trucks` −15%; DevOps
  `high-cpu-all` −20%, `double-groupby-1` −15%, `double-groupby-all` −10%: filters that keep few rows.
- Slower: IoT `avg-vs-projected-fuel-consumption` +9–20% and `avg-load` +11%: filters that keep most rows pay for
  evaluating them while decoding. Point queries are unchanged.

**Object storage**, IoT (17 queries) on RustFS behind `tools/s3proxy` with 20 ms per request:

| | Off | Pushdown + reorder |
|---|---:|---:|
| Engine's default storage (block cache in memory), 6–7 runs: median first run, geomean | 169 ms | 181 ms |
| Same, best repeat (served from the cache), geomean | 13.5 ms | 12.6 ms |
| Requests, first run: `avg-load`, `daily-activity` | 17, 14–18 | 29, 29 |
| No block cache (in-process store over the same files, 20 ms per GET), best of 3 cold runs, geomean | 192 ms | 253 ms |

- Pushdown fetches a row group's filter columns, then its other columns: one more round trip per row group, which a
  cold query waits for. The block cache hides part of it.
- The first runs on RustFS are bimodal (the same query takes about 150 or 350 ms in either mode), so the medians
  matter more than single runs.

Decision: `filter-pushdown = auto`, on for local files and off on object storage; `true` for repeat-heavy readers
on object storage. Timelake's scans no longer follow `datafusion.execution.parquet.pushdown_filters`.

Other options measured on the way, not taken:

- **Codec** (IoT, best geomean, data size): ZSTD 3 11.5 ms 200 MB, ZSTD 1 11.1 ms 204 MB, LZ4 12.3 ms 242 MB,
  Snappy 10.8 ms 277 MB. ZSTD 3 stays the default; the codec now follows `write.parquet.compression-codec`.
- **TopK on the newest reading before a time across the fleet** (#72): 75–113 ms; DataFusion's TopK dynamic filter
  prunes 1 of 12 row groups on day files. A scan that reads partitions newest first and stops once the top k are
  settled would be about 3× faster; recorded on #72.
- Profile of `avg-vs-projected-fuel-consumption` (11.7M rows × 3 columns): decoding 248 ms CPU, join 130 ms, partial
  aggregate 171 ms; DataFusion's operators, not Timelake's scan, take most of it.

## Columns encoded in parallel (2026-10-17)

Same machine, data and method as streaming compaction below. The shared file writer (`timelake_format::file`)
encodes each row group's columns on scoped threads; files are byte-identical to iceberg-rust's writer (test
`write::tests::shared_writer_matches_iceberg_rust`).

| | Before (streaming merge) | Parallel columns |
|---|---:|---:|
| `maintain` (72 files → 3), 3 runs | 2.41, 2.37, 2.37 s | 1.21, 1.16, 1.17 s |
| Peak RSS | 529–542 MB | 424–480 MB |
| `import` of `readings.parquet` (11.7M rows, one append, `--no-maintain`), 2 runs | 6.25, 5.82 s (`d703958`) | 4.82, 4.62 s |

User CPU for `maintain` stays about 3.2 s: the same work on more cores.

## Streaming compaction (2026-10-17, SPEC §10.1)

Laptop (M1 Pro), local disk, release build. TSBS IoT readings (11,671,497 rows, 2026-01-01 to 01-03) split into 72
hourly Parquet files and imported with `timelake import --no-maintain` (one file per commit, day partitions), then
`timelake maintain tsbs.readings` on a fresh copy each run; time from the command's report, peak memory from
`/usr/bin/time -l`.

| | Time (3 runs) | Peak RSS |
|---|---:|---:|
| Before (in-memory sort, `d703958`) | 3.73, 3.65, 3.65 s | 1.95, 1.98, 1.95 GB |
| Streaming merge | 2.41, 2.37, 2.37 s | 532, 542, 529 MB |
| Streaming merge, without writing the output | 0.44 s | |
| `maintain` with nothing to do | | 19 MB |

- Output: the three compacted files hold the same rows in the same order as before (compared column by column).
- One day alone (24 files): 362 MB. With `row-group-rows = 131072`: 390 MB for three days instead of 536 MB.
- Read batch rows and read-ahead (1,024–16,384 rows, 1–4 batches): 2.34–2.43 s and 438–571 MB, no trend beyond
  noise; defaults 4,096 rows and 1 batch.
- Data page size (files written with 32 KiB, 128 KiB pages): 347 and 365 MB for one day, against 362 MB with 1 MiB.
- Reading the same 24 files outside the engine (parquet-rs, 8,192-row batches, one batch from each in turn): 67 MB
  synchronously from the files, 134 MB with each file in memory, 155 MB with the async reader. Per open input the
  cost is its row group held while decoding.

## P5: block cache (2026-10-17)

Measured on the benchmark instance (c8gd.2xlarge, us-east-2) on the S3 tables of the weekly run of 2026-10-16, with
`timebench run … --reuse --checkpoints maintained --runs 3 --concurrency 1,4,16` and `TIMEBENCH_TIMELAKE_OPTIONS`
setting the cache; one run per setting, commit `7e38163` (branch `block-cache`). Geometric means over the queries,
requests over all queries:

| TSBS IoT, S3 | Cache off | Memory (default, 1 MiB blocks) | 256 KiB blocks | Disk, second process |
|---|---:|---:|---:|---:|
| First run (new process) | 151 ms, 279 requests | 150 ms, 251 | 150 ms, 279 | 71 ms, 58 |
| Repeat runs | 54 ms, 221 requests | 16 ms, 0 | 16 ms, 0 | 16 ms, 0 |
| Queries/s, 1 / 4 / 16 clients | 22 / 94 / 288 | 133 / 339 / 398 | 133 / 345 / 395 | 136 / 345 / 395 |

| TSBS DevOps, S3 | Cache off | Memory (1 MiB) | 256 KiB blocks |
|---|---:|---:|---:|
| First run | 203 ms, 295 requests | 171 ms, 295 | 262 ms, 430 |
| Repeat runs | 85 ms, 261 requests | 27 ms, 0 | 26 ms, 0 |
| Queries/s, 1 / 4 / 16 clients | 9 / 19 / 21 | 18 / 22 / 21 | 18 / 22 / 21 |

- Repeat runs read nothing from S3: the data comes from memory, and a repeat within a second of the last query reuses
  the table version without the freshness check (as before). Examples: `one-truck-last-hour` 36 → 4 ms,
  `cpu-max-all-8` 133 → 13 ms, `single-groupby-1-8-1` 55 → 8 ms.
- DevOps at 16 clients stays at 21 queries/s: its dashboard queries are limited by computation, not reads.
- 256 KiB blocks: no gain on repeats, and more requests on first runs (DevOps `cpu-max-all-8` 10 → 83 requests,
  1,016 ms), since runs of missing blocks break up more often. 1 MiB stays the default.
- First runs vary: `one-truck-last-hour` took 170 ms with the memory cache against 74 ms without it, in one run each,
  with the same 6 requests; a 1 MiB block reads more bytes than the ranges it covers. Worth more runs.
- A second process with the disk cache warm: first runs 151 → 71 ms, requests 279 → 58.

## P2 + P3: footers in the head, connections opened early (2026-10-14)

- **P2:** the head also carries the Parquet footers (with page indexes) of the newest files, in what the last-value
  index leaves of the 1 MiB budget, so a cold scan doesn't read footers first.
- **Cold load in one round trip:** on stores that list by prefix, a cold load lists `metadata/v` while it reads the
  head. The listing says which version is newest, so a current head needs no second request. A reader that already has
  a version checks only for `vN+1`, without downloading the head again.
- **P3:** a cold load can also send a few small HEAD requests, so the data reads that follow find open connections.
  Measured below, it gained little, so it is off by default (`timelake_warm_connections`).

Unit test (`cold_queries_read_the_head_first`): a cold `latest` waits for 1 round trip (P1: 2, before: 3), a cold
filtered scan for 2 (P1: 3, before: 6).

**Tigris, cold queries only** (`v0/small.py --queries-only --cold-runs 5` on warehouses written by the full runs,
three batches alternating between setups; median of 15 first runs in a fresh `Lake`, ms):

| First query | No head | Head + footers | + 4 warm-up connections |
|---|---:|---:|---:|
| `one-truck-latest` | 810 | 210 | 213 |
| `last-loc` | 762 | 269 | 273 |
| `one-truck-history` | 1,187 | 522 | 497 |
| `fleet-avg-velocity` | 1,094 | 511 | 492 |

**S3 us-east-2, cold queries only** (same method, on a warehouse written by a full run of this code; "no head" is the
P1 run written with `--head-format none`):

| First query | No head | Head + footers | + 4 warm-up connections | `timelake_version_lookup=hint` |
|---|---:|---:|---:|---:|
| `one-truck-latest` | 509 | 240 | 245 | 305 |
| `last-loc` | 601 | 313 | 329 | 366 |
| `one-truck-history` | 1,031 | 560 | 543 | 644 |
| `fleet-avg-velocity` | 991 | 567 | 543 | 621 |

P1 alone measured 318 / 446 / 1,052 / 714 ms on Tigris (one run each, below). Opening connections early saves at most
about 20 ms on the scans and nothing on point queries, within noise on S3; it costs 4 HEAD requests per cold load, so
it is off by default (`timelake_warm_connections=4` turns it on). `hint` costs about one round trip (60 ms on S3) per
cold load and avoids the LIST. Queries-only runs read but don't write, so they are cheap to repeat on a free tier.

## P1: one-request table state (2026-10-14)

After each commit the writer also writes `metadata/timelake-head` in the background: the table metadata, the live files
with their statistics, and the last-value index while it fits in 1 MB. A cold reader loads it instead of the version
hint, metadata, manifest list and manifests, and checks that no newer version exists while the query is planned (HEAD of
`vN+1.metadata.json`; a LIST of that name on Tigris). Unit test (`cold_queries_read_the_head_first` in
`crates/timelake/src/head/mod.rs`): a cold `latest` waits for 2 round trips instead of 3, a filtered scan for 3
instead of 6.

**Small run** (`v0/small.py --windows 50`, with `--head-format none` for "before"; raw lines in
[small.laptop.jsonl](small.laptop.jsonl)). First query in a fresh `Lake`, in ms; S3 is the mean of two runs each, Tigris
one run each:

| First query | S3 before | S3 P1 | Tigris before | Tigris P1 |
|---|---:|---:|---:|---:|
| `one-truck-latest` | 502 | 314 | 868 | 318 |
| `last-loc` | 676 | 421 | 1,097 | 446 |
| `one-truck-history` | 986 | 823 | 1,484 | 1,052 |
| `fleet-avg-velocity` | 1,013 | 780 | 1,143 | 714 |

Warm queries and commits don't change (the head is written after the commit returns). Tigris "before" already uses the
LIST check, which is why it is lower than the 1,005 ms of the P4 run. The rest of a cold query is mostly opening
connections (P3) and reading Parquet footers (P2). The two Tigris runs used about 2,600 writes, 2,000 reads and 70 MB,
inside the free tier.

## P4: two-step commit (2026-10-13)

An append now encodes its data files, manifests, manifest list and last-value index, uploads them all at once, then
makes the one conditional write. The version hint is updated in the background. Unit test: an append waits for 2
round trips instead of 5 (`append_takes_two_round_trips` in `crates/timelake/src/commit.rs`).

**v0 ingest on S3 us-east-2** (1,728 commits, `v0/ingest.py --system timelake --storage s3` through `tools/s3proxy`,
the `aws-s3-p4` record in [v0_ingest.laptop.jsonl](v0_ingest.laptop.jsonl); before: [S3.md](S3.md)):

| | Before | P4 |
|---|---:|---:|
| Commit p50 | 483 ms | 243 ms |
| Commit p95 | 2,007 ms | 1,630 ms |
| Whole ingest | 1,307 s | 795 s |
| Objects / data files written | 1,083 / 286 | 1,083 / 286 |

p95 is mostly the inline compactions (235 of them), which read data back and are bound by bytes, not round trips.

**Tigris vs. S3, small run** (`v0/small.py --windows 50`: 100 commits, then 4 queries, first run in a fresh process
and best of 2 warm runs; Tigris bucket in its global location):

| | S3 us-east-2 | Tigris (global) |
|---|---:|---:|
| Commit p50 / p95 | 198 / 1,086 ms | 252 / 863 ms |
| `one-truck-latest` first / warm | 553 / 1 ms | 1,005 / 1 ms |
| `last-loc` first / warm | 673 / 66 ms | 1,084 / 57 ms |
| `one-truck-history` first / warm | 1,062 / 139 ms | 1,973 / 270 ms |
| `fleet-avg-velocity` first / warm | 1,010 / 160 ms | 1,255 / 170 ms |

Cold queries are slower on Tigris because of one request type, not the store overall: see "Why Tigris was slower
here" below. One small run: indicative only. The run used about 1,000 writes, under 1,000 reads and 33 MB on Tigris, inside its free tier.

### Why Tigris was slower here (2026-10-13)

Per request on a warm connection from the same laptop (`p50 / p90` ms, 10 requests each, boto3):

| Request | S3 us-east-2 | Tigris (global) |
|---|---:|---:|
| GET, 2-byte object | 67 / 72 | 37 / 38 |
| HEAD, existing object | 53 / 61 | 36 / 44 |
| **HEAD, missing object (404)** | **53 / 56** | **283 / 314** |
| GET, missing object (404) | 61 / 70 | 285 / 311 |
| GET, 256 KB range | 70 / 79 | 59 / 67 |
| PUT, 1 KB | 114 / 120 | 79 / 88 |
| PUT, `If-None-Match: *` | 107 / 110 | 82 / 88 |
| LIST, one table's `metadata/` | 82 / 96 | 55 / 58 |
| LIST, keys after `v9` | 51 / 56 | 38 / 42 |

Tigris is faster for every request that finds its object, as it claims. Its own benchmark (1 KB objects, a client in
San Jose next to a Tigris region, YCSB) measures exactly those. A key that doesn't exist costs about 285 ms, likely
because a global bucket has to check beyond the nearest region. Timelake's reader looks up `vN+1.metadata.json`
(normally missing) on every table load, so each cold query on Tigris pays it once or twice. Checking with a LIST of the
`vN+1` prefix instead would take about 40 ms there (SPEC §17.4 and §19.2).
