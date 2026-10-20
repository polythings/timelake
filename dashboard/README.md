# Timelake web

One Next.js app with the Timelake website, docs and benchmark results:

| Route | What |
|---|---|
| `/` | Landing page: what Timelake is, how it works, quick start, benchmark highlights |
| `/docs/**` | Documentation (quick start, how it works, concepts, glossary, CLI, Python, TypeScript, querying, properties, storage, architecture, codebase tour, development, benchmarks) |
| `/spec/<slug>` | The specs and background pages from `spec/` at the repository root, rendered from their Markdown, with every spec in the sidebar. `/spec` opens the first spec |
| `/roadmap`, `/roadmap/<slug>` | The milestones from `spec/milestones/`, each with its specs |
| `/benchmarks` | Benchmark results on AWS S3 (Timelake vs Lance, naive Iceberg and S3 Tables) and local disk; TimescaleDB on Tiger Cloud |

Stack: Next.js 16 (App Router, Cache Components), React 19, bun, shadcn/ui (Base UI), Tailwind v4, Recharts, lucide icons, MDX for the docs.

## Run
```bash
cd dashboard
bun install
bun run dev                               # http://127.0.0.1:3000
```

| Env var | Default | Meaning |
|---|---|---|
| `TIMELAKE_BENCH_RUNS` | `data/runs` | Benchmark runs for `/benchmarks` |

The dev and start scripts bind to 127.0.0.1.

### Benchmark data
`/benchmarks` reads `data/runs/`, a committed copy of the run files in `../bench/results/runs`, so the app builds where only this directory exists. The weekly benchmark workflow refreshes it; after a run of your own:
```bash
bun run sync-results
```

## Layout
| Path | What |
|---|---|
| `app/(site)/` | Landing page, `/docs`, `/benchmarks` (top-nav layout) |
| `app/(site)/docs/**/page.mdx` | Docs pages (see [Docs](#docs)) |
| `lib/benchmarks.ts` | Loads the benchmark runs for `/benchmarks` (geometric means as in `bench/timebench/report.py`) |
| `lib/bench-systems.ts` | System names, colors and the ranking type, shared by the server code and the browser |
| `components/ranking-card.tsx`, `bench-sections.tsx`, `run-info.tsx` | Benchmark pages: rankings as dot bars (wrong answers apart), a section per suite and checkpoint with each query's results and history, and a run's details behind ⓘ |
| `lib/benchmark-catalog.ts` | The benchmarks on `/benchmarks`: status, why, and each methodology page's text ([spec 44](../spec/0044-timebench.md)) |
| `lib/specs.ts` | The specs, milestones and background pages, from `specs.json`, `milestones.json`, `background.json` and `spec-pages.ts`, which `scripts/spec_index.py` (repository root) generates |
| `lib/remark-spec.mjs` | Renders `spec/` Markdown on the site: links between pages stay on the site, the generated headers are left out |
| `scripts/sync-results.ts` | Copies the benchmark runs into `data/runs/` |

## Docs
Each docs page is a Markdown file with JSX allowed (MDX): `app/(site)/docs/<slug>/page.mdx`, served at `/docs/<slug>`.

- **Add a page:** create `app/(site)/docs/<slug>/page.mdx` starting with `export const metadata = { title: "…" }` and a `# Title`, then add it to `lib/docs-nav.ts`.
- **Order:** the sidebar and the previous/next links follow `lib/docs-nav.ts`.
- **Writing:** GitHub-flavored Markdown (tables, autolinks). Fenced code blocks name a language (`bash`, `console`, `python`, `sql`, `ts`, `text`) and are highlighted at build time. Links to `/…` become client-side links. A React component is imported at the top of the page and used as a tag, as `architecture/page.mdx` does with `<ArchitectureDiagram />`.
- **Plumbing:** `next.config.ts` (MDX plugins), `mdx-components.tsx`, and the `.prose-docs` and `--shiki-*` styles in `app/globals.css`. `.mdx` files are not run through oxfmt.

## Deploying to Vercel
- **Project root:** `dashboard/`. **Framework:** Next.js. **Install:** `bun install`, which runs at the repo root because it is a bun workspace with `packages/timelake-client`. **Build:** `bun run build`.
- The site needs no configuration.
