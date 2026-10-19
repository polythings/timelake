---
number: 5
title: Writes from JavaScript
status: done
versions: 0.6.0
updated: 2026-10-17
description: Append to tables and keep them compacted from Cloudflare Workers and other JavaScript runtimes, in fixed memory.
specs: [13, 19, 11]
---

# Writes from JavaScript

**Done** · versions 0.6.0 · updated 2026-10-17

## Summary

Append to and maintain tables from Cloudflare Workers, Node, Deno, Bun and browsers with the WebAssembly library. The
write path and the streaming compaction it needed (memory of one batch per input, so a 128 MB Worker can compact a day)
are shared with the engine, which got 4× less memory and 3× faster compaction from it.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 13 | [Writes from WebAssembly](../0013-wasm-writes.md) | Done | 0.6.0 |
| 19 | [Compaction](../0019-compaction.md) | Done | 0.1.0, 0.5.0, 0.6.0, 0.7.0 |
| 11 | [Appends](../0011-appends.md) | Done | 0.1.0, 0.5.0, 0.6.0, 0.7.0 |

<!-- specs:end -->

## History

- 2026-10-17: built and measured in workerd (0.6.0)
- 2026-10-19: milestone written when the specs were regrouped one per feature.
