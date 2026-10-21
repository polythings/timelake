/**
 * Copies the benchmark run files (../bench/results/runs) into data/runs, so the app builds and runs where only this
 * directory exists (e.g. a Vercel deploy rooted here). The weekly benchmark workflow runs it after each run.
 *
 *   bun run sync-results
 */
import { copyFile, mkdir, readdir, rm } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")
const src = path.resolve(root, "../bench/results/runs")
const dst = path.resolve(root, "data/runs")

const files = (await readdir(src)).filter((f) => f.endsWith(".jsonl")).sort()
if (files.length === 0) {
  console.error(`no run files found in ${src}`)
  process.exit(1)
}
await rm(dst, { recursive: true, force: true })
await mkdir(dst, { recursive: true })
for (const f of files) await copyFile(path.join(src, f), path.join(dst, f))
console.log(`copied ${files.length} runs to ${path.relative(root, dst)}`)
