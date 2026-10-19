---
number: 41
title: Device sync
area: Services
status: planned
updated: 2026-10-13
versions: []
related: [14, 42]
issues: [116, 117]
---

# 41. Device sync

**Planned** · updated 2026-10-13 · milestone: [Edge and devices](milestones/edge.md)
· related: [spec 14](0014-hot-tier.md), [spec 42](0042-edge-gateway.md)

## Summary

Devices write to a local table and a sync agent moves committed data to the lake: store and forward, resumable, prioritized, exactly once through idempotent batches, with per-device credentials.

## Design

Devices write to local storage and Timelake moves the data to the lake behind the scenes. The device code never decides how, when or over what the data travels.

- **On the device:** the app writes through the Timelake library to a local table on the device's disk (the same table format; v0 already runs on local disk). Writes are fast and work with no network.
- **Sync agent:** a small background process (or a thread in the library) ships committed local data to the lake and handles everything about the trip:
  - store and forward: offline for hours or weeks, then catch up;
  - resume after a crash or reboot from what the lake has acknowledged;
  - retries with backoff, compression, and a bandwidth cap so sync doesn't starve the device's own traffic;
  - priority: alarms and the latest values first, history after;
  - a local disk quota: data is deleted locally only after the lake acknowledges it, and the oldest acknowledged data goes first when space runs low.
- **Exactly once in the lake:** every uploaded batch carries an idempotent batch ID (already in v0), so resends after a lost acknowledgement never duplicate rows. Device clock skew is recorded and can be corrected on arrival.
- **Many devices, one table:** thousands of devices committing directly to one table would fight over commits. Devices upload files to a staging prefix, and a committer (the edge gateway, F7, or a scheduled job) folds them into the table in batches.
- **Where it syncs to:** an edge gateway (F7) when one is on site, otherwise straight to the object store or an HTTPS endpoint. Devices never hold long-lived cloud keys: each device authenticates with its own identity (a certificate) and receives short-lived credentials scoped to its staging prefix.
- **Targets:** Linux-class devices (gateways, industrial PCs, Raspberry Pi, robots, vehicles). Microcontrollers too small for the library publish over MQTT to a gateway instead.

## Open questions and issues

- [#116](https://github.com/polythings/timelake/issues/116) Question: the device-side store for device sync, a local table or an append log
- [#117](https://github.com/polythings/timelake/issues/117) Question: device sync and the edge gateway, open source or premium

## History

- 2026-10-13: planned (F6, SPEC.md §21.6)
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: specs regrouped one per feature, with milestones on the roadmap.
