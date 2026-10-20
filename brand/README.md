# Timelake brand

The source of truth for how Timelake looks and talks. The site, README, docs and package descriptions follow it.

## Words

- **Headline:** The open time-series lakehouse
- **Short description** (GitHub, packages, page titles): The open time-series lakehouse. Fast queries on your own object storage, with no database and no maintenance jobs.
- **Three promises, in this order:**
  1. **Fast every day.** Dashboards, trends and latest values read only what they need; latest values come from an index.
  2. **Nothing to maintain.** Compaction, deduplication, retention and snapshot cleanup run inside the writer.
  3. **Pay for storage, not a database.** No instance running all month; the bill is object-storage bytes and requests.
- **Supporting row:** open format · any object store · Apache 2.0.
- **Use:** time-series lakehouse, object store, series, open table format, nothing to run, maintains itself, latest value.
- **Avoid:** serverless; "on Apache Iceberg" in headlines (Iceberg is named under "how it works" and in the docs); time-series store; private preview.
- **Benchmarks are honest and in context** ([D21](../spec/0044-timebench.md)): every claim comes with its cost and scale, and we publish where Timelake is slower.

## Mark

Parallel lines that turn together at sharp, mitered corners, inside a circle. Each line is a series; they change at the same moments, like readings from one machine. Black on light, white on dark; never colored.

**Current: v1** (chosen 2026-10-13 in the editor). Three lines with v0's pulse, about halfway between identical copies and true parallels (`blend: 0.55`), so the gaps stay close to even and the bottom line keeps a clear first rise.

| File | Use |
|---|---|
| `logo/timelake-mark-v1.svg`, `-white.svg` | The mark alone: favicon, avatar, app icon |
| `logo/timelake-lockup-v1.svg`, `-white.svg` | Mark + wordmark, text outlined (no font needed) |

**Wordmark:** `timelake`, lowercase, Geist Medium, letter-spacing −0.045em, text size 70% of the mark's height, gap 24% of it, set to the right, with the mark centered on the lowercase letters (3% of its height below the text box's center) (`WORDMARK` in `marks.mjs`). Geist is under the SIL Open Font License, which allows outlining it in a logo.

**Kept for later:**

| Version | File | Notes |
|---|---|---|
| alt-disc | `logo/timelake-mark-alt-disc.svg` | Solid disc with a low, wide pulse cut out of its lower half |
| alt-ring | `logo/timelake-mark-alt-ring.svg` | Four lines low inside a drawn ring |
| v0, v0.1, v0.2 | `logo/timelake-mark-v0*.svg` | Earlier Pulse versions: five copies, five parallels, three bold parallels |

Everything is generated: edit `marks.mjs` and run `bun brand/marks.mjs` (marks) and `bun brand/lockup.mjs` (lockups; run `bun install` in `brand/` first for Geist and opentype.js). `bun brand/export-web.mjs` copies the current mark into the web app (`dashboard/components/brand/mark-geometry.ts` and the favicon `dashboard/app/icon.svg`).

**Editor:** `bun run brand:editor` opens an interactive page for the mark and wordmark (lines, spacing, boldness, pulse shape, circle style, text) at http://127.0.0.1:4321. It draws with the same geometry code as `marks.mjs`, so what you see is what the generator writes. **Save** writes `designs/<name>.json` (mark spec and wordmark settings) and `designs/<name>.svg`; commit them like any other file. `designs/` holds v1 and both alternates. To promote a design, copy its `spec` into `MARKS` in `marks.mjs`. How the mark was chosen is in `explorations/`.

## Color

Instrument-panel look: near-black panels with hairline borders, mono labels, ASCII illustrations, and **one accent**.
The site follows the system's light or dark setting, and a button in the header switches it; the light theme is the same
panels in paper and ink.

| Token | Dark | Light |
|---|---|---|
| background | `#0B0D0E` | `#F4F5F5` |
| panel | `#101315` | `#FFFFFF` |
| line (hairlines) | `#23292C` | `#D9DEE0` |
| dim (ASCII fields) | `#343C40` | `#C3CACD` |
| dot (unlit squares) | `#23292C` | `#E3E7E9` |
| foreground | `#E6EBED` | `#0D1113` |
| muted | `#8B969B` | `#59656A` |
| **accent: Ice** (Timelake) | `#67D4F0` | `#0B7FA6` |

**Reserved accents** for future Polythings projects or spin-offs (not used by Timelake):

| Accent | Dark | Light |
|---|---|---|
| Phosphor | `#5EE09A` | `#13844F` |
| Amber | `#F0B44C` | `#9A6206` |

## Type

- **Geist** (light and regular) for headlines and text; **Geist Mono** for labels, numbers, code and ASCII illustrations.
- Labels: Geist Mono, uppercase, letter-spacing 0.1em, muted color.
