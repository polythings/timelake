---
number: 18
title: Any table format, any file format
status: planned
updated: 2026-10-21
description: The engine reaches tables and data files through lakekit's boundaries, Iceberg and Parquet by default, Vortex once Iceberg lists it.
specs: [47]
---

# Any table format, any file format

**Planned** · updated 2026-10-21

## Summary

The engine stops assuming Iceberg and Parquet: tables go through lakekit's common table view, and data files through
its `FileFormat` trait, shared with lakeglass. Iceberg and Parquet stay the defaults, so every engine that reads
Timelake tables today still does. Vortex data files come once Iceberg's spec lists the format.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 47 | [Table and file formats behind boundaries](../0047-format-boundaries.md) | Planned | – |

<!-- specs:end -->

## History

- 2026-10-21: milestone proposed.
