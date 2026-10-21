---
number: 3
title: Installable, upgradable, switchable
status: done
versions: 0.3.0–0.5.0
updated: 2026-10-17
description: Install from crates.io, PyPI and npm, upgrade tables without data loss, and turn every feature on or off with a setting.
specs: [8, 9]
---

# Installable, upgradable, switchable

**Done** · versions 0.3.0–0.5.0 · updated 2026-10-17

## Summary

Timelake became something to install and keep running: every release on the usual registries, tables that record their
format version and upgrade without data loss, and one registry of settings that turns each feature on or off per
table, session or call. Releases follow [releases](../releases.md).

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 8 | [Table format versions and upgrades](../0008-format-versions.md) | Done | 0.3.0, 0.4.0, 0.5.0 |
| 9 | [Settings](../0009-settings.md) | Done | 0.5.0, unreleased |

<!-- specs:end -->

## History

- 2026-10-14: format versions, `timelake upgrade` and `rollback` (0.3.0); registries and installers (0.4.0)
- 2026-10-17: settings registry (0.5.0)
- 2026-10-19: milestone written when the specs were regrouped one per feature.
