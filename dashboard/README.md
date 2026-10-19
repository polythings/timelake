# Timelake web

One Next.js app with the Timelake website, docs, benchmark results and the Explorer:

| Route | What |
|---|---|
| `/` | Landing page: what Timelake is, how it works, quick start, benchmark highlights |
| `/docs/**` | Documentation (quick start, how it works, concepts, glossary, CLI, Python, TypeScript, querying, properties, storage, architecture, codebase tour, development, benchmarks) |
| `/spec`, `/spec/<slug>` | The specs and background pages from `spec/` at the repository root, rendered from their Markdown |
| `/roadmap`, `/roadmap/<slug>` | The milestones from `spec/milestones/`, each with its specs |
| `/benchmarks` | Benchmark results on AWS S3 (Timelake vs Lance, naive Iceberg and S3 Tables) and local disk; TimescaleDB on Tiger Cloud |
| `/explore` | Explorer: tables, freshness, partitions, snapshots, latest values, a series chart, read-only SQL console |

Stack: Next.js 16 (App Router, Cache Components), React 19, bun, shadcn/ui (Base UI), Tailwind v4, Recharts, lucide icons, MDX for the docs.

## Run
```bash
cargo build --release -p timelake-cli     # from the repo root; the Explorer calls this binary
cd dashboard
bun install
bun run dev                               # http://127.0.0.1:3000
```

The site, docs and benchmarks need nothing else. The Explorer has two backends (`lib/lake/`):

- **WASM** (hosted), used when `TIMELAKE_URL` is set. It reads a warehouse served over HTTPS (Vercel Blob, a public bucket, a CDN) with `@timelake/client` (`packages/timelake-client`), a WebAssembly build of the reader. There's no binary and no engine, and SQL isn't available.
- **CLI** (local), used otherwise. It runs the `timelake` binary (`ls --json`, `info --json`, `latest`, `sql --format json`), so the SQL console works.

When neither is available, the pages show a "not connected" card and the API routes return 503.

| Env var | Default | Meaning |
|---|---|---|
| `TIMELAKE_URL` | (none) | HTTPS warehouse for the WASM backend, e.g. `https://<store>.public.blob.vercel-storage.com/demo` |
| `TIMELAKE_WAREHOUSE` | `../bench/data/lakes/local/tsbs-iot-standard/timelake/lake` (TSBS IoT tables) | Warehouse the Explorer browses: local path or `s3://…` |
| `TIMELAKE_OPTIONS` | (none) | Storage options, comma-separated `k=v` (e.g. `aws_endpoint=http://localhost:9000,aws_allow_http=true`) |
| `TIMELAKE_BIN` | `../target/release/timelake` | CLI binary |
| `TIMELAKE_BENCH_RUNS` | `data/runs` | Benchmark runs for `/benchmarks` |

The SQL console accepts only single read statements (`SELECT`, `WITH`, `EXPLAIN`, `SHOW`, `DESCRIBE`), because DataFusion can write files. The dev and start scripts bind to 127.0.0.1.

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
| `app/(app)/explore/` | Explorer pages (sidebar layout) |
| `app/api/sql`, `app/api/series` | Route handlers used by the SQL console and the series chart |
| `lib/lake/` | Explorer data layer: `types.ts` (the `LakeBackend` interface), `wasm.ts` (hosted, `@timelake/client`), `cli.ts` (local, the CLI), `index.ts` (picks the backend, checks series queries), `error-response.ts` (errors → API responses) |
| `lib/benchmarks.ts` | Loads the benchmark runs for `/benchmarks` (geometric means as in `bench/timebench/report.py`) |
| `lib/benchmark-catalog.ts` | The benchmarks on `/benchmarks`: status, why, and each methodology page's text ([spec 44](../spec/0044-timebench.md)) |
| `lib/specs.ts` | The specs, milestones and background pages, from `specs.json`, `milestones.json`, `background.json` and `spec-pages.ts`, which `scripts/spec_index.py` (repository root) generates |
| `lib/remark-spec.mjs` | Renders `spec/` Markdown on the site: links between pages stay on the site, the generated headers are left out |
| `scripts/sync-results.ts` | Copies the benchmark runs into `data/runs/` |
| `scripts/upload-demo-to-blob.ts` | Uploads the demo warehouse to Vercel Blob (`bun run upload-demo`) |

## Docs
Each docs page is a Markdown file with JSX allowed (MDX): `app/(site)/docs/<slug>/page.mdx`, served at `/docs/<slug>`.

- **Add a page:** create `app/(site)/docs/<slug>/page.mdx` starting with `export const metadata = { title: "…" }` and a `# Title`, then add it to `lib/docs-nav.ts`.
- **Order:** the sidebar and the previous/next links follow `lib/docs-nav.ts`.
- **Writing:** GitHub-flavored Markdown (tables, autolinks). Fenced code blocks name a language (`bash`, `console`, `python`, `sql`, `ts`, `text`) and are highlighted at build time. Links to `/…` become client-side links. A React component is imported at the top of the page and used as a tag, as `architecture/page.mdx` does with `<ArchitectureDiagram />`.
- **Plumbing:** `next.config.ts` (MDX plugins), `mdx-components.tsx`, and the `.prose-docs` and `--shiki-*` styles in `app/globals.css`. `.mdx` files are not run through oxfmt.

## Deploying to Vercel
- **Project root:** `dashboard/`. **Framework:** Next.js. **Install:** `bun install`, which runs at the repo root because it is a bun workspace with `packages/timelake-client`. **Build:** `bun run build`.
- The site, docs and benchmarks need no configuration.
- **Hosted Explorer: the demo warehouse on Vercel Blob.**
  1. Create a **public** Blob store (Vercel → Storage → Blob) and note its base URL, `https://<store-id>.public.blob.vercel-storage.com`.
  2. Build the demo with that URL baked into its metadata (the first 50 TSBS truck ids over 3 days, about 22 MB): `../scripts/build-demo-warehouse.sh https://<store-id>.public.blob.vercel-storage.com/demo`
  3. Upload it, keeping paths: `BLOB_READ_WRITE_TOKEN=… bun run upload-demo https://<store-id>.public.blob.vercel-storage.com/demo`
  4. Check range support. This should print `HTTP/… 206`; without it the client still works but downloads whole files: `curl -sI -H 'Range: bytes=0-99' https://<store-id>.public.blob.vercel-storage.com/demo/tsbs/tags/metadata/version-hint.text`
  5. Set `TIMELAKE_URL=https://<store-id>.public.blob.vercel-storage.com/demo` on the project, for build and runtime alike (the static shell shows it), then deploy.
- **The Explorer is public: no login, read-only.** Each chart reads at most a few MB from Blob. Consider a Vercel Firewall rate limit on `/api/*`.

### Testing the hosted mode locally
```bash
../scripts/build-demo-warehouse.sh                     # builds for http://127.0.0.1:4100/demo
bun ../scripts/serve-range.mjs ../bench/data/demo-site/127.0.0.1_4100_demo 4100 &
TIMELAKE_URL=http://127.0.0.1:4100/demo bun run dev
```
