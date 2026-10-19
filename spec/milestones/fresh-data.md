---
number: 12
title: Seconds-fresh data and ingestion
status: planned
updated: 2026-10-19
description: New rows visible within a second, before they are committed; MQTT, HTTP, and Kafka in, with rules and alerts as data arrives.
specs: [14, 36, 37]
---

# Seconds-fresh data and ingestion

**Planned** · updated 2026-10-19

## Summary

Data visible to readers within a second of being written, through an object-store WAL or a managed hot store, and an
ingestion pipeline that takes MQTT, HTTP and Kafka, evaluates rules as data arrives, and turns data into events on any
stream.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 14 | [Hot tier and seconds-fresh reads](../0014-hot-tier.md) | Planned | – |
| 36 | [Ingestion and rules](../0036-ingest-and-rules.md) | Planned | – |
| 37 | [Data to events](../0037-data-to-events.md) | Planned | – |

<!-- specs:end -->

## History

- 2026-10-19: milestone written when the specs were regrouped one per feature.
