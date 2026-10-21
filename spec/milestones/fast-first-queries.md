---
number: 2
title: Fast first queries on object storage
status: done
versions: 0.2.0–0.7.0
updated: 2026-10-19
description: First queries on S3 in one or two round trips instead of six or seven, repeat queries from a local cache, and scans that read and decode less.
specs: [2, 7, 22, 23, 24, 25, 5]
---

# Fast first queries on object storage

**Done** · versions 0.2.0–0.7.0 · updated 2026-10-19

## Summary

The ordered steps P1–P8 that took first queries on S3 from six or seven dependent round trips to one or two, and
repeat queries to local speed. Each step landed only if it improved first-run latency or requests without slowing
other queries, and plain DuckDB still reads every table. The history of each step and its effect is on the docs page
"Performance history"; measurements are in [`bench/results/PERFORMANCE.md`](../../bench/results/PERFORMANCE.md).

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 2 | [Commits](../0002-commits.md) | Done | 0.1.0, 0.2.0, unreleased |
| 7 | [One-request table state and version checks](../0007-table-head.md) | Done | 0.3.0, 0.6.0, 0.7.0, unreleased |
| 22 | [Block cache](../0022-block-cache.md) | Done | 0.5.0, 0.7.0 |
| 23 | [Filter pushdown](../0023-filter-pushdown.md) | Done | 0.7.0 |
| 24 | [Pruning with long IN lists](../0024-in-list-pruning.md) | Done | 0.7.0 |
| 25 | [Newest-first scans](../0025-newest-first.md) | Done | 0.7.0, unreleased |
| 5 | [File layout and encodings](../0005-file-layout.md) | In progress | 0.1.0, 0.6.0, 0.7.0, unreleased |

<!-- specs:end -->

## Plan and results

**Where time went** (2026-10-13, Timelake reader, laptop → AWS S3 us-east-2, after maintenance, [`bench/results/S3.md`](../../bench/results/S3.md)):

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
| P5 | **Done ([spec 22](../0022-block-cache.md)).** **Block cache**, in memory and optionally on local disk across processes, for data files, manifests and the last-value index. | Repeat queries at best-run speed; measured: TSBS IoT repeats on S3 54 → 16 ms, 16 clients 288 → 398 queries/s |
| P6 | **Staleness bound built (2026-10-17):** setting `version-check-interval` (table, session; default 1 s, the planning cache's old lifetime). A reader uses a version it loaded or checked that long without the per-query check; writers always check, and a process sees its own commits at once. `0s` checks on every query (one query's lookups share one check). **Not built:** the head pointer on S3 Express. | One request and round trip less per warm query within the interval (unit test: warm `latest` 1 → 0 round trips); not measured on S3 yet |
| P7 | Rollups: [moved to the milestone "Time-series SQL"](time-series-sql.md). | |
| P8 | **Done (2026-10-17 to 2026-10-19; specs [23](../0023-filter-pushdown.md), [24](../0024-in-list-pruning.md), [25](../0025-newest-first.md), [26](../0026-series-order.md), [5](../0005-file-layout.md)).** **Scan tuning:** late materialization (setting `filter-pushdown`: `auto` is on for local files, off on object storage), request sizing (#65, done for the Rust reader), pruning for `IN` lists of more than 20 values (#103 done, setting `in-list-pruning`: literal lists, and joins' runtime filters on object storage; Q4's hidden `_series_id` decided against), a newest-first scan for `ORDER BY time DESC LIMIT k` (#72, built: setting `newest-first`, 100 newest before a past time on 30 days 298 → 38 ms), and for the k newest buckets of `GROUP BY time_bucket(…) ORDER BY bucket DESC LIMIT k` (built 2026-10-19: TSBS DevOps `groupby-orderby-limit` 254 → 115 ms; the scans' partitions run in parallel under the aggregate, sharing what they've seen), windows over each series in time order read merged in series order (built: setting `window-series-order`, `auto` when the files are in one time partition; IIoT `gaps` 260 → 215 ms, a week of files 2.0 → 3.0 s so `auto` leaves those). | Less data decoded and fetched on selective scans; measured: pushdown cuts TSBS geomeans 4–5% on local disk (selective group-bys up to 40%), costs first queries 12–17% at 20 ms per request; the block cache fetching a read's runs at once cut first queries on S3 23% (162 → 125 ms), and GETs of at most 1 MiB another 9% at 50 MiB/s per request; long `IN` lists 54 → 31 ms (100 tags, IIoT, local) |
| P9 | Real data: [moved to the milestone "Tuned on real data"](real-data.md). | |
| P10 | Series cardinality: [moved to the milestone "Millions of series"](millions-of-series.md). | |

**How we measure:** the same v0 suites through `tools/s3proxy` (request counts by kind) on local disk, S3 from a laptop, and S3 in-region. Each step lands only if it improves first-run latency or requests without slowing other queries, and plain DuckDB still reads every table.

**Not built:** the head pointer on S3 Express One Zone for millisecond version checks
([spec 7](../0007-table-head.md), #133).

## History

- 2026-10-13: plan written (SPEC.md §19.1); P4 built
- 2026-10-14: P1–P3
- 2026-10-17: P5, P6's staleness bound, P8 started
- 2026-10-18 to 2026-10-19: P8 scan settings
- 2026-10-19: was spec 27; a milestone on the roadmap since the specs were regrouped one per feature, with P7, P9 and
  P10 moved to their own milestones.
