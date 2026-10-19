---
number: 42
title: Edge gateway
area: Services and planned features
status: planned
updated: 2026-10-13
related: [41, 35, 43]
issues: [117]
---

# 42. Edge gateway

**Planned** · updated 2026-10-13
· related: [spec 41](0041-device-sync.md), [spec 35](0035-ingest-and-rules.md), [spec 43](0043-server-mode.md)

## Summary

A single binary on site that devices send data, metrics, logs and blobs to, which buffers and forwards to the lake, runs rules locally, and serves recent data from its cache when the uplink is down.

## Design

A single binary on site (factory, vessel, rig, substation) that devices send data to on the local network. Devices only need to reach the gateway; the gateway knows how to reach the cloud.

- **Local endpoints for every kind of data:**
  - device and IoT data: MQTT (built-in broker or bridge), HTTP JSON, InfluxDB line protocol; Sparkplug B and OPC-UA later;
  - metrics: OTLP and Prometheus remote write;
  - logs: OTLP logs and syslog, stored as time-partitioned log tables (queryable by time, device and level; full-text search stays a non-goal, [spec 1](0001-purpose.md));
  - blobs (images, vibration captures, files): an S3-compatible upload endpoint. The file goes to the bucket as an object, and a row in a blob index table records time, device, key, size and type, so blobs can be found by device and time range like any other series.
- **Discovery:** devices find the gateway by a fixed local name (mDNS), so device config doesn't change between sites.
- **Buffering:** everything is written to the gateway's disk first and forwarded when the uplink allows (the same store-and-forward as F6). It is also the committer for devices that sync through it.
- **Serving from the local cache:** recent data stays on the gateway, and the gateway answers queries locally (the same reader and SQL), so local HMIs, dashboards and machines keep working when the uplink is down. Queries for older data are read through to the lake and cached.
- **Rules at the edge:** the gateway runs the ingest pipeline ([spec 35](0035-ingest-and-rules.md)), so alarms fire locally even when offline, and are also forwarded as events (F3).
- **Security:** devices authenticate to the gateway; only the gateway holds cloud credentials, scoped to its site.
- This turns the "edge binary" deployment form in [spec 35](0035-ingest-and-rules.md) into a product.

## Open questions and issues

- [#117](https://github.com/polythings/timelake/issues/117) Question: device sync and the edge gateway, open source or premium

## History

- 2026-10-13: planned (F7, SPEC.md §21.7)
- 2026-10-19: moved from SPEC.md into its own spec.
