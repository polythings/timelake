import Link from "next/link"

import { TimelakeLockup } from "@/components/brand/timelake-mark"
import { ScrollGuard } from "@/components/scroll-guard"
import { SiteHeader } from "@/components/site-header"
import { LICENSE_URL, REPO_URL } from "@/lib/site"

export default function SiteLayout({ children }: LayoutProps<"/">) {
  return (
    <div className="flex min-h-svh flex-col">
      <ScrollGuard />
      <SiteHeader />
      <main className="flex-1">{children}</main>
      <footer className="border-t">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-x-8 gap-y-4 px-4 py-8">
          <div className="flex flex-col gap-2">
            <TimelakeLockup size={22} />
            <span className="text-xs text-muted-foreground">The open time-series lakehouse.</span>
          </div>
          <div className="flex flex-col gap-1 text-xs text-muted-foreground sm:items-end">
            <span className="label-mono">
              <Link href="/docs/changelog" className="hover:text-foreground">
                v0
              </Link>{" "}
              ·{" "}
              <a href={LICENSE_URL} className="hover:text-foreground">
                Apache 2.0
              </a>{" "}
              ·{" "}
              <Link href="/contribute" className="hover:text-foreground">
                Contribute
              </Link>{" "}
              ·{" "}
              <a href={REPO_URL} className="hover:text-foreground">
                GitHub
              </a>
            </span>
            <Link href="/docs/credits" className="hover:text-foreground">
              Built on Apache Arrow, Apache DataFusion, Apache Parquet, and Apache Iceberg.
            </Link>
          </div>
        </div>
      </footer>
    </div>
  )
}
