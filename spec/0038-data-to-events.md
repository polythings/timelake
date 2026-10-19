---
number: 38
title: Data to events
area: Services and planned features
status: planned
updated: 2026-10-13
related: [35, 39]
issues: [113]
---

# 38. Data to events

**Planned** · updated 2026-10-13
· related: [spec 35](0035-ingest-and-rules.md), [spec 39](0039-computed-metrics.md)

## Summary

Typed events produced from data (rules, SQL definitions, pass-through) and published at least once to any event system, in-stream or triggered by commits, with the events table as the durable record.

## Design

Turn incoming data into events and publish them into whatever event system the user already runs. This generalizes the rule outputs in [spec 35](0035-ingest-and-rules.md).

- **Events are defined, not hard-coded:** an event is a typed record produced from data by a built-in rule (threshold, flatline, heartbeat, …), a SQL definition (state change, "batch arrived", a derived condition), or pass-through of rows (change-data-capture style). Its payload schema is declared (JSON Schema or Zod).
- **Any sink:** pluggable outputs for Kafka/Redpanda, NATS JetStream, MQTT, Kinesis, SQS/SNS, EventBridge, Google Pub/Sub, Azure Event Hubs, Redis Streams, and webhooks. CloudEvents is the default envelope.
- **Two triggers:** in-stream inside `timelake-ingest` for the lowest latency, or commit-driven: a function triggered per commit reads the snapshot diff, so any writer (CLI, Python, Spark) can produce events without running the ingest service.
- **Delivery:** at least once. Every event carries a deterministic ID (table, series, time, rule), so consumers can deduplicate. Events are emitted only after the commit succeeds and are also written to the `events` Iceberg table, which is the durable record and the replay source after a crash (a commit-driven outbox).

## Open questions and issues

- [#113](https://github.com/polythings/timelake/issues/113) Question: commit-driven event trigger, object-store notification, catalog event or poll

## History

- 2026-10-13: planned (F3, SPEC.md §21.3)
- 2026-10-19: moved from SPEC.md into its own spec.
