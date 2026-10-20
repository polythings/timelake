"use client"

import { Menu } from "lucide-react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { Suspense } from "react"

import { TimelakeLockup } from "@/components/brand/timelake-mark"
import { DocsNav } from "@/components/docs/docs-sidebar"
import { GithubIcon } from "@/components/github-icon"
import { ThemeToggle } from "@/components/theme-toggle"
import { Button } from "@/components/ui/button"
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { REPO_URL } from "@/lib/site"
import { cn } from "@/lib/utils"

const LINKS = [
  { href: "/docs", label: "Docs" },
  { href: "/benchmarks", label: "Benchmarks" },
  { href: "/explore", label: "Explorer" },
  { href: "/roadmap", label: "Roadmap" },
  { href: "/spec", label: "Specs" },
  { href: "/contribute", label: "Contribute" },
]

function NavLinks({ pathname }: { pathname: string | null }) {
  return (
    <nav className="hidden items-center gap-1 md:flex">
      {LINKS.map((l) => (
        <Link
          key={l.href}
          href={l.href}
          className={cn(
            "rounded-md px-3 py-1.5 font-mono text-[13px] text-muted-foreground transition-colors hover:text-foreground",
            pathname?.startsWith(l.href) && "text-foreground",
          )}
        >
          {l.label}
        </Link>
      ))}
    </nav>
  )
}

function ActiveNavLinks() {
  return <NavLinks pathname={usePathname()} />
}

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-40 border-b bg-background/80 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-6xl items-center gap-4 px-4">
        <Link href="/" aria-label="Timelake home" className="flex items-center">
          <TimelakeLockup size={26} />
        </Link>
        <Suspense fallback={<NavLinks pathname={null} />}>
          <ActiveNavLinks />
        </Suspense>
        <div className="flex-1" />
        <span className="hidden font-mono text-[13px] text-muted-foreground sm:inline">
          Apache 2.0 ·{" "}
          <Tooltip>
            <TooltipTrigger className="cursor-help underline decoration-dotted underline-offset-4 hover:text-foreground">
              v0
            </TooltipTrigger>
            <TooltipContent side="bottom">
              Timelake is at v0 and not ready for production yet. Formats and APIs may change between releases.
            </TooltipContent>
          </Tooltip>
        </span>
        <Button
          variant="ghost"
          size="icon"
          aria-label="Timelake on GitHub"
          nativeButton={false}
          render={<a href={REPO_URL} target="_blank" rel="noreferrer" />}
        >
          <GithubIcon className="size-4" />
        </Button>
        <ThemeToggle />
        <Sheet>
          <SheetTrigger render={<Button variant="ghost" size="icon" className="md:hidden" aria-label="Open menu" />}>
            <Menu className="size-4" />
          </SheetTrigger>
          <SheetContent side="left" className="w-72 overflow-y-auto">
            <SheetHeader>
              <SheetTitle>
                <TimelakeLockup size={22} />
              </SheetTitle>
            </SheetHeader>
            <div className="space-y-6 px-4 pb-6">
              <div className="space-y-1">
                {[{ href: "/", label: "Home" }, ...LINKS].map((l) => (
                  <Link key={l.href} href={l.href} className="block py-1 text-sm">
                    {l.label}
                  </Link>
                ))}
                <a href={REPO_URL} className="block py-1 text-sm">
                  GitHub
                </a>
              </div>
              <DocsNav />
            </div>
          </SheetContent>
        </Sheet>
      </div>
    </header>
  )
}
