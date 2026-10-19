# Spike: are S3 request and byte counts stable enough for a regression gate?

**Question ([spec 32](../../spec/0032-timebench.md)).** Request and byte counts don't depend on the machine's speed, so CI could catch
regressions on every push: an extra metadata read, a lost pruning step, a commit that writes more objects. That only
works if identical runs give (nearly) identical counts.

**Setup (2026-10-15).** `timebench run tsbs-iot --scale small --target s3 --ttfb-ms 0 --systems
timelake,iceberg,parquet`, three times on a laptop, against a local RustFS behind `tools/s3proxy`. Each run streams
1,728 commits per system, runs maintenance, and runs 17 queries on 5 readers twice (after ingest, after maintenance),
each a first run plus two repeats. The three run files are in [`runs/`](runs/); `compare.py` compares them:

```bash
uv run python spikes/request-counts/compare.py spikes/request-counts/runs/*.jsonl
```

## Result

| System | Phase | Measurements | Identical in every run | Largest spread | Where |
|---|---|--:|--:|--:|---|
| iceberg | ingest, maintain | 2 | 2 | 0.0% | |
| iceberg+duckdb | queries after ingest | 51 | 48 | 1 request | one-truck-latest repeat: 1, 1, 2 |
| iceberg+duckdb | queries after maintenance | 51 | 51 | 0.0% | |
| parquet | ingest, maintain | 2 | 2 | 0.0% | |
| parquet+datafusion | queries after ingest | 51 | 48 | 0.1% | one-truck-latest repeat: 862, 863, 863 |
| parquet+duckdb | queries after ingest | 51 | 44 | 0.4% | one-truck-latest first run: 937, 939, 935 |
| parquet+* | queries after maintenance | 102 | 102 | 0.0% | |
| timelake | ingest | 1 | 0 | 0.1% | 24,580, 24,603, 24,587 requests |
| timelake | maintain | 1 | 0 | 2 requests | 202, 200, 200 |
| timelake+duckdb | queries after ingest | 51 | 49 | 1 request | one-truck-latest first run: 36, 36, 35 |
| timelake+duckdb | queries after maintenance | 51 | 51 | 0.0% | |
| timelake+timelake | queries after ingest | 51 | 34 | 0.3% | low-fuel first-run bytes: 65,226, 65,034, 65,098 |
| timelake+timelake | queries after maintenance | 51 | 39 | 0.2% | low-fuel first-run bytes: 52,594, 52,466, 52,530 |

- **Query request counts are exact or within one or two requests.** After maintenance every reader's counts were
  identical in all three runs. Right after ingest, a few queries differed by one or two requests: which small files
  exist depends on when inline compaction ran relative to the commits.
- **Bytes differ by under 0.5%.** Timelake's own reader varies a little because metadata files hold timestamps and
  ids of different lengths.
- **Ingest varies by 0.1% (about 20 of 24,600 requests) for Timelake**, from the same timing effect; the other
  systems' ingest and maintenance counts were identical.

## Verdict

**A gate would work.** After maintenance, counts were identical in every run, so a gate can require exact request
counts there and catch a single extra metadata read. Right after ingest and for ingest itself, a tolerance of "more
than 5% and more than 3 requests" would have passed all three runs. Timelake alone at `small` scale takes 3–4
minutes through the proxy; Iceberg's 1,728 PyIceberg commits make a full run 15 minutes, so a gate should run
Timelake only and leave the other systems to the weekly run.

What it would not catch: CPU-bound slowdowns (the weekly run's timings do), and anything the small scale doesn't
exercise. Not built: on 2026-10-15 we chose weekly runs and no benchmarks in CI, for now.
