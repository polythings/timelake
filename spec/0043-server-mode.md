---
number: 43
title: Server mode
area: Services
status: planned
updated: 2026-10-15
versions: []
related: [14, 21, 42]
issues: [101]
---

# 43. Server mode

**Planned** · updated 2026-10-15 · milestone: [Server mode](milestones/server-mode.md)
· related: [spec 14](0014-hot-tier.md), [spec 21](0021-reader.md), [spec 42](0042-edge-gateway.md)

## Summary

An experiment: the same engine as one long-lived server on local NVMe, with the bucket as the durable copy and Postgres wire and Flight SQL protocols, measured against TimescaleDB before anything is built.

## Design

**The question.** Timelake is already fast on local disk. Could the same engine, run as one long-lived server on an instance with local NVMe, compete with a managed time-series database like TimescaleDB, while the bucket stays the durable copy?

**What we measured so far** (indicative: different hardware, not one benchmark):
- **Latest values:** with a warm process on a laptop's local disk, `last-loc` takes 2.5 ms and one truck's latest row 0.4 ms. In Phase 0, TimescaleDB answered `last-loc` in 2–3 ms and plain lake engines needed 34–317 ms ([spec 44](0044-timebench.md)). The last-value index closed that gap.
- **Analytics:** Timelake's TSBS queries take 22–217 ms on the first run from local disk (12 queries).
  - Tiger Cloud's smallest tier needed 2.0 s (columnstore) and 8.9 s (row store), server time.
  - In Phase 0, lake engines were 4–44× faster than an uncompressed TimescaleDB on the same laptop.
- **What a server would remove:** most of today's cost on S3 is per-process cold start, fetching metadata, footers and the index before the first byte of data. A long-lived process keeps those in memory.

**Design: the same modules, assembled as a server.** Nothing in the core may assume a server; the library, the serverless reader, the edge gateway (F7) and server mode are the same crates put together differently.
- **Storage:** the table format on local NVMe as the primary store. Every commit is also uploaded to the bucket, synchronously (durable on commit) or asynchronously (faster commits, bounded loss window). Old partitions can live only in the bucket and be read through a cache.
- **Memory:** table metadata, Parquet footers, the last-value index and recent data stay in memory, with the hot tier ([spec 14](0014-hot-tier.md)) as the in-process write buffer. That makes data visible in under a second.
- **Protocols (both, decided 2026-10-15):**
  - Postgres wire protocol, so Grafana, psql and BI tools connect unchanged;
  - Arrow Flight SQL, for fast Arrow clients (and JDBC/ADBC drivers);
  - the existing HTTP API and MCP (F1).
- **Operations:** backups and point-in-time restore come from the bucket (snapshots). Failover is a new instance that warms up from the bucket. Readers that don't need the server keep reading the bucket directly.

**The experiment, before building it:**
- **Setup:** one instance with local NVMe (for example c8gd.2xlarge) and the same data (TSBS IoT, then the IIoT benchmark, [spec 45](0045-iiot-benchmark.md)). Compare:
  - TimescaleDB, as a hypertable and as columnstore;
  - Timelake behind a thin server: DataFusion plus Flight SQL or Postgres wire.
  - Then managed Tiger Cloud and InfluxDB 3 at similar $/month.
  - Then TSBS with its own loader and query runner over the Postgres wire protocol, and IoTDB's IoT-benchmark, so results sit next to published ones ([spec 44](0044-timebench.md)).
- **Measure:**
  - p50 and p95 latency, cold and warm;
  - throughput with N concurrent dashboards;
  - sustained ingest and freshness;
  - storage;
  - $ per month;
  - recovery time after losing the instance.
- **Bar to build it:**
  - within about 2× of TimescaleDB on point lookups and concurrency;
  - faster on analytics;
  - cheaper per month;
  - no data loss beyond the configured window when the instance dies.
- **Known gaps to weigh:**
  - one writer per table today, so concurrent writers would go through the server;
  - no multi-statement transactions;
  - Postgres compatibility limited to the SQL that DataFusion supports;
  - one server holds one site's working set.

## Open questions and issues

- [#101](https://github.com/polythings/timelake/issues/101) Research: server mode as stateless cached query nodes over the bucket, a write broker, and a cost model

## History

- 2026-10-13: planned (F8, SPEC.md §21.8)
- 2026-10-15: both protocols decided
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: specs regrouped one per feature, with milestones on the roadmap.
