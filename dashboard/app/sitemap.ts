import type { MetadataRoute } from "next"

import { BENCHMARKS } from "@/lib/benchmark-catalog"
import { WORKLOADS } from "@/lib/benchmarks"
import { DOCS_PAGES } from "@/lib/docs-nav"
import { SITE_URL } from "@/lib/site"
import { BACKGROUND, MILESTONES, SPECS } from "@/lib/specs"

// The site's pages, so search engines index timelake.dev rather than the domains that redirect to it.
// The explorer is left out: it shows the lake configured on the deployment.
export default function sitemap(): MetadataRoute.Sitemap {
  const paths = [
    "/",
    ...DOCS_PAGES.map((p) => p.href),
    "/benchmarks",
    "/benchmarks/timebench",
    ...BENCHMARKS.filter((b) => b.workload && WORKLOADS.some((w) => w.id === b.workload)).map(
      (b) => `/benchmarks/${b.id}`,
    ),
    ...BENCHMARKS.filter((b) => b.methodology).map((b) => `/benchmarks/${b.id}/methodology`),
    "/roadmap",
    ...MILESTONES.map((m) => `/roadmap/${m.slug}`),
    "/spec",
    ...SPECS.map((s) => `/spec/${s.slug}`),
    ...BACKGROUND.map((b) => `/spec/${b.slug}`),
    "/contribute",
  ]
  return paths.map((p) => ({ url: `${SITE_URL}${p}` }))
}
