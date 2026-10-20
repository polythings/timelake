---
number: 45
title: Industrial IoT benchmark
area: Benchmarks
status: in-progress
updated: 2026-10-20
versions: []
related: [44]
---

# 45. Industrial IoT benchmark

**In progress** · updated 2026-10-20 · milestone: [Benchmarks against every alternative](milestones/benchmarks.md)
· related: [spec 44](0044-timebench.md)

## Summary

A plant historian workload: thousands of tags at mixed rates, gateways going offline and resending, duplicates, every opponent on one table, checked against a reference for correct answers, and reported with cost per month and growth.

## Design

TSBS models a truck fleet at ~10 s per device. Industrial plants look different, and that is the market (Polythings' IIoT platform). The IIoT benchmark is a timebench workload (`iiot`) and runs next to TSBS, not instead of it.

**Principles**
- **Every opponent, one table.** Timelake, InfluxDB 3 Core, TimescaleDB, ClickHouse, QuestDB, naive Iceberg (the S3 Tables write pattern) and Lance.
- **Honest, including where we lose.** Timelake is a lakehouse, not a database. A database on local disk answers point lookups in milliseconds, handles many concurrent dashboards and shows data within a second; Timelake doesn't (yet; a cache layer may close part of the gap). We publish those losses next to the wins.
- **Always in context of cost and scale.** Each result sits next to what it costs per month on AWS (compute + storage + requests) and how it changes as data grows. Readers need statements of the form "system X answers in N ms on an instance costing $A/month; Timelake answers in M ms for $B/month of S3", with measured numbers.
- **Reproducible and reviewable.** Generator seeds, configs and versions are committed; each system is tuned per its own docs; vendors are invited to review.

**Data.** A deterministic Rust generator replays plant behavior faster than real time:
| Workload | Shape |
|---|---|
| Process historian | 100k+ tags, narrow rows (tag, time, value, quality), mixed rates: 1 s, 10 s, 60 s and change-of-value |
| Condition monitoring | Vibration bursts at 1–10 kHz from hundreds of machines, uploaded in 1–10 s blocks |
| Store-and-forward | Gateways offline 1–12 h, then backfill out of order while live data continues; 1–2% duplicates (at-least-once delivery) |
| Meters and fleets | Millions of slow series arriving as periodic batch files (cardinality stress) |

**Scale tiers** by raw ingest rate (uncompressed row bytes): **10 MB/h, 100 MB/h, 1 GB/h, 10 GB/h**. Each tier runs long enough to hold 1 day, 7 days and 30 days of data, and queries are measured at each checkpoint, so every chart shows the trend as data grows.

**Queries** an operator, engineer or report actually runs, all while ingest continues: dashboard refresh (latest value of 1,000 tags; last 15 min of 20 tags), 30-day trend downsampled to ~1,000 points, hourly shift report per asset joined with asset metadata, raw window around an alarm (50 tags, ±10 min), time above a limit, time-weighted average, energy from cumulative counters, and a concurrency run (N dashboards refreshing at once).

**Measured for every system and tier**
- Query latency p50/p95, cold and warm, under ingest; concurrent throughput.
- Freshness: arrival → queryable.
- Sustained ingest rate and the smallest setup that keeps up.
- Maintenance work: CPU time, bytes rewritten, extra services or jobs to run.
- Storage: bytes and object count.
- **$ per month on AWS:** instance hours (or Lambda time) for each system's servers or writers, plus S3 storage and requests by type (GET, PUT, LIST, DELETE, counted by `bench/tools/s3proxy`), plus managed fees where they apply (S3 Tables compaction).
- Where each system keeps data: local disk, EBS, object storage, or a mix (ClickHouse on S3 disks, QuestDB and TimescaleDB on local disk, InfluxDB 3 Core on object storage). Results say so next to every number.

**First version (decided and built 2026-10-15)**
- **Scope:** the process historian with store-and-forward. Condition monitoring (vibration blocks) and metering fleets come later.
- **Generator** (`bench/timebench/workloads/plant.py`): Python and NumPy rather than Rust, seeded per machine group and day, so it is fast enough and timebench stays one language; Rust only if generation becomes the slow part. Sites of 10 areas, areas of 10 lines, lines of 10 machines, 10 tags per machine (pressure at 1 s; flow, temperature, current and speed at 10 s; vibration and an energy counter at 60 s; state, alarm and setpoint on change with an hourly heartbeat). Machines run, stop and fault; pressure follows its setpoint and sometimes exceeds the alarm limit; counters reset; sensors drop out and get stuck. Each line's gateway (but every tenth, wired) goes offline for 1–12 h; its backlog arrives afterwards at an hour per 5-minute window, the last 5 minutes before the outage are sent again, and 1.5% of rows arrive twice. Every injected event is written to `events.parquet`, the agent benchmark's ground truth ([spec 46](0046-agent-benchmark.md)).
- **Scales:** `small` 1,000 tags (~10 MB/h raw), `standard` 10,000, `large` 100,000 (30 days each), `xlarge` 1,000,000 (7 days).
- **A run** loads history in bulk (an hour of arrivals per commit) with queries after 1 and 7 days, then the last day live (a 5-minute window per commit, paced, probed), queries at `ingested`, maintenance, queries at `maintained`, then concurrent dashboards. Queries are relative to the data's present (`@AGO(15 minutes)`), so they mean the same at every checkpoint.
- **Correct answers:** a `reference` system keeps every arrival and reads it with duplicates removed; every other system's answer (row count and per-column sums) is checked against it. Systems that keep duplicates or miss late rows show as wrong next to their speed.
- **Systems:** Timelake (dedup on), TimescaleDB (unique index, `ON CONFLICT DO NOTHING`), ClickHouse 26.8 LTS (`ReplacingMergeTree`, read with `FINAL`), QuestDB 10.0 (WAL tables with `DEDUP UPSERT KEYS`, loaded over QWP), InfluxDB 3 Core 3.12 (line protocol; last write wins), naive Iceberg, Lance and plain Parquet. The servers run in Docker at pinned versions (`bench/infra/servers.sh`): first on the weekly instance, one server running at a time (started for its system's turn and stopped after), and part of every weekly run (decided 2026-10-15); published comparison runs move to dedicated instances, one per database plus a client.
- **InfluxDB 3 Core's query file limit:** by default a query reads at most 432 Parquet files, about 3 days of data. It runs twice: with the limit raised as its docs allow (`influxdb3`), and as shipped (`influxdb3-defaults`), so the results show both.
- **Later:** the same workload against the vendors' managed clouds (ClickHouse Cloud, InfluxDB's and QuestDB's managed offerings), measured in us-east-2 at their own prices (decided 2026-10-15).

## Open questions and issues

None open.

## History

- 2026-10-15: first version decided and built (SPEC.md §17.6)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 33; specs regrouped one per feature, with milestones on the roadmap.
- 2026-10-20: the weekly run reads the lake systems' tables on S3 too (until then local disk only), for the cost and latency the comparisons need.
