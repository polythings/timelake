# TimescaleDB on Tiger Cloud vs. Timelake v0

_2026-10-12. Same TSBS IoT data (23.3M rows) and the same 17 queries as [V0.md](V0.md). Script: [`v0/tsdb_cloud.py`](../v0/tsdb_cloud.py). Raw records: `tsdb_cloud.jsonl`. Every completed query returned the same row count as Timelake._

## Setup
- **Service:** a disposable Tiger Cloud service ("oleks-tiered-storage-test", us-east-1, DEV, **shared CPU and memory**, TimescaleDB 2.30.2, PostgreSQL 18.6). This is the smallest tier, far below a sized production service, so its numbers are a floor for TimescaleDB, not its ceiling.
- **Schema:** TSBS's TimescaleDB layout: hypertables chunked every 12 hours, an index on `(tags_id, time DESC)`, and a normalized `tags` table.
- **States:**
  - `hypertable`: row store plus index, 2.95 GB.
  - `columnstore`: `convert_to_columnstore` on all chunks, segment by `tags_id`, order by `time DESC`; 413 MB.
- **Client:** a laptop. Client times include the network round trip (median `SELECT 1` was 27–35 ms). `server_ms` subtracts that median.
- **Fresh connection per query.** "First run" is not cold on the server: shared buffers stay warm from the load and earlier queries.
- **Not measured: tiered storage.** It must be enabled in the Tiger Console, and on this service the `timescaledb_osm` extension never became active (it can't be installed over SQL: `permission denied for language c`). The service has since been deleted. Measuring tiered storage needs a new service with tiered storage enabled. Then run `load`, `columnstore`, `tier` and `run tiered` (see the script's docstring).

## Results (geometric mean, first run)

**TSBS IoT, 10 queries both TimescaleDB states completed.** On the plain hypertable, `low-fuel` and `high-load` were terminated by the service ("terminating connection due to administrator command"), most likely a resource limit on the shared tier.

| System | Time |
|---|---:|
| TimescaleDB hypertable, server-side | 8.85 s |
| TimescaleDB columnstore, server-side | 2.02 s (all 12 queries: 2.33 s) |
| Timelake reader, RustFS + 20 ms per request (after maintenance) | 0.33 s (all 12: 0.29 s) |
| Timelake reader, local disk (after maintenance) | 0.066 s (all 12: 0.047 s) |

**Single truck (5 queries).**

| System | Time |
|---|---:|
| TimescaleDB hypertable, server-side | 0.072 s |
| TimescaleDB columnstore, server-side | 0.027 s (client 0.068 s) |
| Timelake reader, RustFS + 20 ms per request (after maintenance) | 0.154 s |
| Timelake reader, local disk (after maintenance) | 0.009 s |

## Reading this honestly
- **Analytics (TSBS):** even with the database's data hot in memory and on disk, the columnstore on this small service is about 7× slower than Timelake reading object storage with 20 ms per request. It is about 50× slower than Timelake on local disk. Much of that gap is the shared-CPU tier. A sized service would narrow it, and this run cannot say by how much.
- **Single-series lookups:** TimescaleDB's index and segment-by layout are fast, about 27 ms server-side on the columnstore. Timelake on remote storage (154 ms) pays object-store round trips for metadata and footers. Locally it is faster (9 ms). Lookups like this are the case for Timelake's planned hot tier and reader cache ([spec 17](../../spec/0017-read-paths.md)).
- **Storage:** 413 MB in the TimescaleDB columnstore (database storage) vs 228 MB of Parquet in Timelake (object storage). Tiered storage would move TimescaleDB's chunks to S3 as well, which is the comparison still missing.
- **Different hardware:** a managed cloud database measured over the internet vs a laptop with a latency-injecting proxy. Treat cross-system ratios as indicative.
