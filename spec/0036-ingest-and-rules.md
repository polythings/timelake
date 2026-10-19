---
number: 36
title: Ingestion and rules
area: Services
status: planned
updated: 2026-10-11
versions: []
related: [14, 37, 42]
issues: [109, 110]
---

# 36. Ingestion and rules

**Planned** · updated 2026-10-11 · milestones: [Seconds-fresh data and ingestion](milestones/fresh-data.md), [Technical preview](milestones/technical-preview.md)
· related: [spec 14](0014-hot-tier.md), [spec 37](0037-data-to-events.md), [spec 42](0042-edge-gateway.md)

## Summary

A library-first pipeline for ingestion with in-stream rules (thresholds, flatlines, heartbeats, anomalies), deployable embedded, on Lambda, as a container or at the edge, writing alarms to an events table and real-time sinks.

## Design

A pipeline that does **ingestion and in-stream analytics in one place**. It is a **library first**: the same pipeline code runs in every deployment form, and the binary, container, and Lambda handler are thin wrappers around it.

### Deployment forms
| Form | Triggered by | Best for |
|------|--------------|----------|
| **Embedded library** (Rust; Python via bindings) | User code calls `pipeline.push(batch)` | Apps that already receive data |
| **AWS Lambda** (and other FaaS) | Kinesis, SQS, IoT Core rules, API Gateway, S3 object-created events, EventBridge schedule | Bursty or low-volume fleets; scale to zero; no ops |
| **Container / microservice** | Long-lived consumers: MQTT, Kafka/Redpanda, NATS, HTTP, Arrow Flight | Steady high volume; persistent MQTT connections |
| **Edge binary** | Same as container, on a gateway | Factories and sites with unreliable uplinks: buffers locally, then catches up |

All forms share a single config file (`inputs → processors → rules → outputs`), so a pipeline can move from Lambda to a container without changes.

### Pipeline
- **Inputs:** MQTT (IoT first-class), HTTP (JSON/NDJSON), Arrow Flight / Arrow IPC, Kafka/Redpanda, Kinesis, NATS, InfluxDB line protocol, OTLP metrics. OPC-UA and Sparkplug B are candidates.
- **Stages:** decode → schema validation and evolution → enrichment (join with series metadata) → transforms (DataFusion SQL) → **rules** → buffer → WAL / hot tier → lake commit.

### Rules
- **Built-in rules (configurable per series or group):**
  - Threshold and range (with hysteresis)
  - Rate of change / spike
  - Flatline or stuck sensor
  - Missing data / heartbeat timeout
  - Out-of-order or late beyond tolerance
  - Unit and schema anomalies
  - Statistical anomalies (rolling z-score, EWMA)
- **Outputs of rules:** Rows in an **`events` Iceberg table** (queryable history of alarms) plus real-time sinks (webhook, Kafka, SNS/SQS, MQTT topic). Generalized to any event type and any event stream in F3 ([spec 37](0037-data-to-events.md)).

### Rule state without a server (draft — Q17, #110)
| Rule type | State needed | How it works serverless |
|-----------|--------------|-------------------------|
| Stateless (threshold, range, schema) | None | Runs anywhere, any instance |
| Short windows (rate of change, flatline, z-score, hysteresis) | Small per-series state (last values, running stats) | Affinity via the source's partitioning (Kinesis shard / Kafka partition keyed by series), so one consumer sees a series in order. State is checkpointed with the source offset to object storage or the hot store, and restored on cold start. |
| Absence of data (heartbeat, missing data) | "Last seen" per series | Can't be triggered by data that never arrives. A **scheduled sweep** (e.g. EventBridge every minute, or a timer in the container) checks the last-value index / hot store for overdue series. |

### Scaling
- Horizontal.
- Stateless stages scale freely.
- Stateful rules scale with the number of source partitions.
- Containers use consistent hashing on the series key when the source has no partitioning (e.g. MQTT).

## Decisions

| ID | Decision | Status |
|---|---|---|
| D11 | Ingestion is a library-first pipeline (ingest + rules) deployable embedded, as AWS Lambda, as a container, or at the edge ([spec 36](0036-ingest-and-rules.md)) | Accepted |

## Open questions and issues

- [#109](https://github.com/polythings/timelake/issues/109) Question: rule engine scope, a rules DSL or YAML presets with a SQL escape hatch
- [#110](https://github.com/polythings/timelake/issues/110) Question: rule state on Lambda, and the sweep cadence for heartbeat rules

## History

- 2026-10-11: D11 accepted (SPEC.md §13)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 35; specs regrouped one per feature, with milestones on the roadmap.
