import { createRequire } from "node:module"
import { fileURLToPath } from "node:url"

import createMDX from "@next/mdx"
import type { NextConfig } from "next"
import { createCssVariablesTheme } from "shiki"

import { REPO_URL } from "./lib/site"

const nextConfig: NextConfig = {
  cacheComponents: true,
  // Docs pages are MDX (app/(site)/docs/**/page.mdx).
  pageExtensions: ["ts", "tsx", "mdx"],
  // Files read from disk at runtime: benchmark runs (when /benchmarks revalidates) and the OG image fonts.
  outputFileTracingIncludes: {
    "/benchmarks": ["./data/runs/**"],
    "/benchmarks/[id]": ["./data/runs/**"],
    "/opengraph-image": ["./assets/fonts/**"],
  },
  partialPrefetching: true,
  redirects: async () => [
    { source: "/docs/roadmap", destination: "/roadmap", permanent: true },
    { source: "/docs/table-properties", destination: "/docs/settings", permanent: true },
  ],
  turbopack: {
    rules: {
      "*.css": {
        loaders: ["@tailwindcss/turbopack"],
        as: "*.css",
      },
    },
  },
}

// Turbopack takes plugins by name with JSON options. Code is highlighted at build time; the colors are CSS
// variables (--shiki-*, in globals.css), so one render follows the light and dark themes.
// Plugins are passed as absolute paths: the loader resolves names next to each file, and CHANGELOG.md (rendered at
// /docs/changelog) sits outside dashboard/.
const plugin = createRequire(import.meta.url).resolve
// Spec pages (spec/*.md at /spec): front matter parsed, not shown; links rewritten for the site.
const SPEC_LINKS = new URL("./lib/remark-spec.mjs", import.meta.url)
const withMDX = createMDX({
  extension: /\.mdx?$/,
  options: {
    remarkPlugins: [
      plugin("remark-gfm"),
      [plugin("remark-frontmatter"), ["yaml"]],
      [fileURLToPath(SPEC_LINKS), { repo: REPO_URL }],
    ],
    rehypePlugins: [
      plugin("rehype-slug"),
      [plugin("rehype-autolink-headings"), { behavior: "wrap" }],
      [plugin("rehype-pretty-code"), { theme: createCssVariablesTheme({ name: "site" }), keepBackground: false }],
    ],
  },
})

export default withMDX(nextConfig)
