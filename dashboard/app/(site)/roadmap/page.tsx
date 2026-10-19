import { Check } from "lucide-react"
import type { Metadata } from "next"
import Link from "next/link"

import { GithubIcon } from "@/components/github-icon"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { COMING_UP, EXPLORING, NOW, ROADMAP_UPDATED, type RoadmapItem } from "@/lib/roadmap"
import { REPO_URL } from "@/lib/site"
import { cn } from "@/lib/utils"

export const metadata: Metadata = { title: "Roadmap" }

function StatusBadge({ status }: { status: NonNullable<RoadmapItem["status"]> }) {
  return status === "done" ? (
    <Badge variant="secondary">
      <Check data-icon="inline-start" />
      Done
    </Badge>
  ) : (
    <Badge variant="outline">Next</Badge>
  )
}

function ItemCard({ item, exploring = false }: { item: RoadmapItem; exploring?: boolean }) {
  return (
    <Card
      className={cn(exploring && "bg-transparent ring-0 outline-1 -outline-offset-1 outline-border outline-dashed")}
    >
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <CardTitle>{item.title}</CardTitle>
          {item.status && <StatusBadge status={item.status} />}
        </div>
        <CardDescription>{item.description}</CardDescription>
      </CardHeader>
    </Card>
  )
}

function Section({ title, items, exploring }: { title: string; items: RoadmapItem[]; exploring?: boolean }) {
  return (
    <section className="space-y-4">
      <h2 className="text-xl font-medium tracking-tight">{title}</h2>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {items.map((item) => (
          <ItemCard key={item.title} item={item} exploring={exploring} />
        ))}
      </div>
    </section>
  )
}

export default function RoadmapPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-14 px-4 py-12">
      <header className="max-w-3xl space-y-3">
        <h1 className="text-3xl font-semibold tracking-tight">Roadmap</h1>
        <p className="text-muted-foreground">What we&apos;re building next. Updated {ROADMAP_UPDATED}.</p>
      </header>

      <Section title="Now" items={NOW} />
      <Section title="Coming up" items={COMING_UP} />
      <Section title="Exploring" items={EXPLORING} exploring />

      <section className="max-w-3xl space-y-4 border-t pt-10">
        <h2 className="text-xl font-medium tracking-tight">Build it with us</h2>
        <p className="text-muted-foreground">
          Missing something you need, or want to help? Open an issue, read the design behind each item, or see how to
          contribute.
        </p>
        <div className="flex flex-wrap gap-3">
          <Button nativeButton={false} render={<a href={`${REPO_URL}/issues/new`} target="_blank" rel="noreferrer" />}>
            <GithubIcon />
            Open an issue
          </Button>
          <Button variant="outline" nativeButton={false} render={<Link href="/spec" />}>
            Read the design
          </Button>
          <Button variant="outline" nativeButton={false} render={<Link href="/contribute" />}>
            How to contribute
          </Button>
          <Button variant="outline" nativeButton={false} render={<Link href="/docs/changelog" />}>
            Changelog
          </Button>
        </div>
      </section>
    </div>
  )
}
