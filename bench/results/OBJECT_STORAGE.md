# Object storage results (RustFS)

> **Run:** 2026-10-11, same machine, data, and engine versions as [BASELINE.md](BASELINE.md).
> **Storage:** RustFS 1.0.1 (Docker, local volume), read through `tools/s3proxy`. The proxy adds **0 ms or 20 ms time-to-first-byte per request** (≈ S3 standard in-region) and counts requests and bytes. Bandwidth is not throttled (real S3 per-connection throughput is lower), so these numbers are *optimistic* for S3.
> **First** = first run in a fresh process (no in-process caches; the realistic case for serverless and Lambda readers). **Best** = best of 3 repeats in the same process.

## Summary (geometric means, seconds)

### tsbs_iot

| engine | local first | local best | 0 ms first | 20 ms first | 20 ms best | 20 ms first ÷ local first | reqs/query (first) | MB/query (first) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| duckdb | 0.120 | 0.068 | 0.251 | 0.328 | 0.137 | 2.7× | 22.7 | 29.1 |
| duckdb-iceberg | 0.104 | 0.056 | 0.284 | 0.413 | 0.122 | 4.0× | 28.5 | 34.7 |
| datafusion | 0.072 | 0.063 | 0.189 | 0.235 | 0.189 | 3.3× | 17.2 | 29.1 |
| polars | 0.100 | 0.047 | 0.286 | 0.336 | 0.126 | 3.4× | 22.9 | 34.0 |

### tpch

| engine | local first | local best | 0 ms first | 20 ms first | 20 ms best | 20 ms first ÷ local first | reqs/query (first) | MB/query (first) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| duckdb | 0.287 | 0.221 | 0.905 | 1.339 | 0.338 | 4.7× | 248.7 | 387.1 |
| datafusion | 0.350 | 0.329 | 1.001 | 1.340 | 1.046 | 3.8× | 192.2 | 470.1 |
| polars | 0.268 | 0.200 | 0.797 | 0.756 | 0.533 | 2.8× | 174.5 | 434.5 |

### clickbench

| engine | local first | local best | 0 ms first | 20 ms first | 20 ms best | 20 ms first ÷ local first | reqs/query (first) | MB/query (first) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| duckdb | 0.093 | 0.039 | 0.276 | 0.398 | 0.086 | 4.3× | 54.7 | 106.5 |
| datafusion | 0.039 | 0.033 | 0.189 | 0.251 | 0.151 | 6.4× | 34.5 | 114.4 |
| polars | 0.095 | 0.033 | 0.256 | 0.264 | 0.138 | 2.8× | 52.9 | 120.5 |

## Key findings

1. **Object storage costs 2.7–6.4× on first runs**, even with a local server: HTTP, S3 signing, and request round trips. Adding 20 ms per request adds another 0.05–0.4 s per query.
   - So for a lake, **request count is the latency budget**:
     - TSBS queries: 17–29 requests on the first run
     - TPC-H: 175–250
     - ClickBench: 35–55
2. **In-process caching decides repeat performance.**
   - DuckDB caches remote file data in memory: its repeat runs make **2 requests** and run close to local-disk speed (TSBS 0.137 s vs 0.068 s local).
   - DataFusion (Python, default config) and Polars re-fetch everything every run: same requests, same latency.
   - → Timelake's reader must ship a **metadata + data cache** (in-memory, plus an optional local-disk tier). The spec already lists a "metadata and footer cache" in §12.2; this makes it a priority, not an optimization.
3. **Iceberg's metadata costs show up on object storage.**

   | Query (DuckDB, 20 ms, first run) | Plain Parquet | Iceberg |
   |---|---|---|
   | `stationary-trucks` | 7 requests, 0.21 s | 18 requests, 0.28 s |
   | `avg-load` | 28 requests, 0.23 s | 32 requests, 0.42 s |

   - The extra requests are metadata.json, manifest list, and manifests per table. Locally (BASELINE) Iceberg was *faster* thanks to sorting; on object storage the metadata round trips eat that gain on the TSBS geomean: 0.413 s vs 0.328 s.
   - → Supports caching metadata in `timelake-catalog` and server-side scan planning ([spec 32](../../spec/0032-rest-catalog.md)): the reader should get its file list in **one** round trip, not 3+ per table.
4. **"Latest value" is the most expensive IoT query on a lake.** `last-loc` downloads **~165 MB** (the full time/lat/long columns) on every engine, 0.4–0.7 s on the first run. TimescaleDB answers it in 2 ms. This is the strongest evidence yet for the **last-value index / hot store** ([spec 14](../../spec/0014-hot-tier.md), [spec 21](../../spec/0021-reader.md)).

## Per-query results
Full per-query tables, including request counts:

```bash
python summarize.py --metric=first tsbs_iot.rustfs-20ms tpch.rustfs-20ms clickbench.rustfs-20ms
```
