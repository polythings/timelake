---
number: 16
title: Edge and devices
status: exploring
updated: 2026-10-19
description: Write on devices and gateways; Timelake moves the data to the lake and serves recent data when the uplink is down.
specs: [41, 42]
---

# Edge and devices

**Exploring** · updated 2026-10-19

## Summary

Devices write to a local table and a sync agent moves committed data to the lake, store and forward; a gateway on site
takes data, metrics, logs and blobs, runs rules locally, and serves recent data from its cache when the uplink is down.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 41 | [Device sync](../0041-device-sync.md) | Planned | – |
| 42 | [Edge gateway](../0042-edge-gateway.md) | Planned | – |

<!-- specs:end -->

## History

- 2026-10-19: milestone written when the specs were regrouped one per feature.
