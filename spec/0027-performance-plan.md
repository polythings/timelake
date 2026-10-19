---
number: 27
title: Performance plan
area: Performance and scale
status: in-progress
updated: 2026-10-19
related: [18, 19, 20, 21, 26, 44, 3]
---

# 27. Performance plan

**In progress** · updated 2026-10-19
· related: [spec 18](0018-table-head.md), [spec 19](0019-block-cache.md), [spec 20](0020-scan-tuning.md), [spec 21](0021-rollups.md), [spec 26](0026-series-cardinality.md), [spec 44](0044-real-data-beta.md), [spec 3](0003-iceberg-and-commits.md)

## Summary

The ordered steps P1–P10 that took first queries on S3 from six or seven dependent round trips to one or two, and what each measured. Each step lands only if it improves first-run latency or requests without slowing other queries, and plain DuckDB still reads every table.

## Design

**Status:** P4 done (2026-10-13); P1–P3 done (2026-10-14, [spec 18](0018-table-head.md)). P3 measured no clear gain and is off by default. P7's first version done (2026-10-16, [spec 21](0021-rollups.md)); P5 done (2026-10-17, [spec 19](0019-block-cache.md)). P6's staleness bound built (2026-10-17, below). Next: P8, the rest of P7, and P6's S3 Express pointer. The history of each step and its effect is on the docs page "Performance history". The codebase and CI/CD cleanup it waited for is done (2026-10-13; the audit and plan are in git history at `2d66939`). Gains below are estimates until measured; measurements are in [`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md).

**Where time goes today** (Timelake reader, laptop → AWS S3 us-east-2, after maintenance, [`bench/results/S3.md`](../bench/results/S3.md)):

| Query | First run | Best run | Requests | MB |
|---|---:|---:|---:|---:|
| `one-truck-latest` | 618 ms | 2 ms | 7 | 0.2 |
| `last-loc` | 737 ms | 68 ms | 14 | 0.3 |
| `long-daily-sessions` | 1.54 s | 0.30 s | 32 | 11 |
| `avg-vs-projected-fuel-consumption` | 2.42 s | 2.39 s | 53 | 37 |

- **Small queries:** first runs are dominated by round trips that each wait for the previous one: version hint → `vN.metadata.json` (and a HEAD on `vN+1`) → manifest list → manifests → last-value index or Parquet footers → data. That is 6–7 dependent steps at about 90 ms each from a laptop.
- **Heavy queries:** bound by bytes read; warm runs are barely faster.
- **Commits:** p50 was 483 ms, from 5 serial stages: data, then manifests and index, then manifest list, then the conditional metadata write, then the hint. P4 made it 2 stages and 243 ms.
- **Warm queries:** every query re-reads the version hint to stay current. That re-read is most of `last-loc`'s 68 ms.

**Steps, in order:**

| # | Change | Expected effect |
|---|---|---|
| P1 | **Done.** **One-request table state.** Each commit also writes a small head object: version, live files with column bounds, and the last-value index (inline when small, else a pointer). Readers fetch it with one GET instead of 4 dependent ones. Iceberg metadata is still written as today for other engines. | Cold `one-truck-latest` 502 → 314 ms on S3, 868 → 318 ms on Tigris, measured from a laptop |
| P2 | **Done.** **Footers in the head object.** Store the Parquet footers and page indexes of live files next to the file list, so scans skip the footer round trip. | Cold scans 2 round trips instead of 3; Tigris `one-truck-history` 1,187 → 522 ms (with P1) |
| P3 | **Done.** **Connection pre-warm.** Open the scan's connections while the head object is in flight. | Measured at most about 20 ms on cold scans, nothing on point queries: off by default |
| P4 | **Done.** **Two-step commit.** Upload data, manifest, index and manifest list in parallel (all are known locally), then make the one conditional write. Update the hint asynchronously. | Commit p50 483 ms → 243 ms measured (whole v0 ingest 1,307 s → 795 s) |
| P5 | **Done ([spec 19](0019-block-cache.md)).** **Block cache**, in memory and optionally on local disk across processes, for data files, manifests and the last-value index ([spec 17](0017-read-paths.md) item 6). | Repeat queries at best-run speed; measured: TSBS IoT repeats on S3 54 → 16 ms, 16 clients 288 → 398 queries/s |
| P6 | **Staleness bound built (2026-10-17):** setting `version-check-interval` (table, session; default 1 s, the planning cache's old lifetime). A reader uses a version it loaded or checked that long without the per-query check; writers always check, and a process sees its own commits at once. `0s` checks on every query (one query's lookups share one check). **Not built:** the head pointer on S3 Express. | One request and round trip less per warm query within the interval (unit test: warm `latest` 1 → 0 round trips); not measured on S3 yet |
| P7 | **First version done ([spec 21](0021-rollups.md)); coarser queries, fewer keys and rollups of rollups done ([spec 21](0021-rollups.md), daily per tag 2.07 → 0.25 s).** **Rollups ([spec 21](0021-rollups.md)).** TSBS daily queries read hourly or daily rollups instead of raw data. | Likely 10–100× on bandwidth-bound dashboard queries; an hourly query over a week of 200 devices went 113 → 21 ms on a laptop (debug build) |
| P8 | **Started (2026-10-17).** **Scan tuning:** late materialization (setting `filter-pushdown`: `auto` is on for local files, off on object storage), request sizing (#65, done for the Rust reader), pruning for `IN` lists of more than 20 values (#103 done, setting `in-list-pruning`: literal lists, and joins' runtime filters on object storage; Q4's hidden `_series_id` decided against), a newest-first scan for `ORDER BY time DESC LIMIT k` (#72, built: setting `newest-first`, 100 newest before a past time on 30 days 298 → 38 ms), and for the k newest buckets of `GROUP BY time_bucket(…) ORDER BY bucket DESC LIMIT k` (built 2026-10-19: TSBS DevOps `groupby-orderby-limit` 254 → 115 ms; the scans' partitions run in parallel under the aggregate, sharing what they've seen), windows over each series in time order read merged in series order (built: setting `window-series-order`, `auto` when the files are in one time partition; IIoT `gaps` 260 → 215 ms, a week of files 2.0 → 3.0 s so `auto` leaves those). | Less data decoded and fetched on selective scans; measured: pushdown cuts TSBS geomeans 4–5% on local disk (selective group-bys up to 40%), costs first queries 12–17% at 20 ms per request; the block cache fetching a read's runs at once cut first queries on S3 23% (162 → 125 ms), and GETs of at most 1 MiB another 9% at 50 MiB/s per request; long `IN` lists 54 → 31 ms (100 tags, IIoT, local) |
| P10 | **Next (2026-10-18).** **Series cardinality (R27):** measure at the typical size (IIoT `large`: 10,000 machines × 10 tags = 100k series, and 400k) and toward the top end (`xlarge`: 1M series, then more), and fix what grows with the number of series. Known suspects: the last-value index is one object rewritten on every commit (at hundreds of millions of series it needs shards by series hash); a join's runtime filter lists at most 150 keys (`hash_join_inlist_pushdown_max_distinct_values`), so a dashboard over thousands of devices prunes by bounds only; rollup refresh and planning per series. | Ingest, latest-value and dashboard queries that stay flat from 1k to 1M series. **Measured (2026-10-18):** 100k and 400k series ingest at 2.2–2.3 M rows/s, dashboards flat (`latest-area` 52 → 53 ms); last-value index 1.8 MB, 0.15 s of a 4.5 s commit at 400k (3.4 s at 10M, in memory). Open: `gaps` grows with rows (88 s at 400k, a rollup case), the index for mostly idle series, and the index's cost per commit, which grows with the series count (2026-10-19, 1M series: a 100k-row append 264 → 171 ms by merging the sorted indexes instead of sorting them; the remaining ~80 ms is encoding the whole index, ~1.5 s a commit at 10M; options: index deltas per commit merged by readers and folded into the base by maintenance, or shards by series hash, which don't help a writer whose batches touch every shard); done: the k-way merge on interleaved inputs (up to 5× faster) |
| P9 | **Planned, after P8 and the rest of P7.** **Real data:** a beta on a company's production-like industrial dataset, with parallel writers fed from a Kinesis stream. Measure ingest throughput, commit contention between writers, query latency and the monthly bill (requests, storage, compute) on real data and access patterns, then tune for what they show. | Settings and defaults chosen from real use instead of synthetic benchmarks only |

**How we measure:** the same v0 suites through `tools/s3proxy` (request counts by kind) on local disk, S3 from a laptop, and S3 in-region. Each step lands only if it improves first-run latency or requests without slowing other queries, and plain DuckDB still reads every table.

## Open questions and issues

None open.

## History

- 2026-10-13: plan written (SPEC.md §19.1)
- 2026-10-13 to 2026-10-19: P1–P8 and P10 built in parts
- 2026-10-19: moved from SPEC.md into its own spec.
