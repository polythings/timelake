import { RunDate, RunTabs, type RunView } from "@/components/bench-run"
import { SectionHead } from "@/components/section-head"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TUNING_LABELS, TUNING_NOTES, type Run, runLabel, tuningOf } from "@/lib/benchmarks"
import { slugify } from "@/lib/utils"

/**
 * A section of results with its own choice of tuning (where both have run) and storage (where more than one has): a
 * tab per kind of storage that has the section's data (AWS S3 first), each showing that run's results, its date, and
 * its details behind ⓘ.
 */
export function ResultSection({ title, note, views }: { title: string; note?: string; views: RunView[] }) {
  if (views.length === 0) return null
  const tunings = [...new Set(views.map((v) => tuningOf(v.run)))]
  return (
    <section id={slugify(title)} className="flex scroll-mt-20 flex-col gap-4">
      <SectionHead label={title}>{note}</SectionHead>
      {tunings.length > 1 ? (
        <Tabs defaultValue={tunings[0]} className="gap-4">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
            <TabsList aria-label="Tuning">
              {tunings.map((t) => (
                <TabsTrigger key={t} value={t} className="min-h-8 px-2.5 sm:min-h-7">
                  {TUNING_LABELS[t] ?? t}
                </TabsTrigger>
              ))}
            </TabsList>
            <p className="text-xs text-muted-foreground">
              {tunings.map((t) => `${TUNING_LABELS[t] ?? t}: ${TUNING_NOTES[t] ?? t}.`).join(" ")}
            </p>
          </div>
          {tunings.map((t) => (
            <TabsContent key={t} value={t}>
              <StorageTabs views={views.filter((v) => tuningOf(v.run) === t)} />
            </TabsContent>
          ))}
        </Tabs>
      ) : (
        <StorageTabs views={views} tuning={tunings[0]} />
      )}
    </section>
  )
}

/** A tab per kind of storage, each with its run's line and results; one storage gets its line and no tabs. */
function StorageTabs({ views, tuning }: { views: RunView[]; tuning?: string }) {
  const [first] = views
  if (!first) return null
  if (views.length === 1)
    return (
      <div className="flex flex-col gap-4">
        <RunLine run={first.run} storage tuning={tuning} />
        {first.node}
      </div>
    )
  return (
    <RunTabs
      views={views.map((v) => ({
        run: v.run,
        node: (
          <>
            <RunLine run={v.run} tuning={tuning} />
            {v.node}
          </>
        ),
      }))}
    />
  )
}

/**
 * A run's line above its results: its storage where there's no tab to say it, its tuning where there's no switch to
 * say it, its date, and its details behind ⓘ.
 */
function RunLine({ run, storage, tuning }: { run: Run; storage?: boolean; tuning?: string }) {
  return (
    <p className="flex flex-wrap items-center gap-x-1.5 gap-y-1 text-xs text-muted-foreground">
      {storage && <span className="font-mono tracking-[0.06em] text-foreground uppercase">{runLabel(run)}</span>}
      {storage && <span aria-hidden>·</span>}
      {tuning && (
        <>
          <a href="#tunings" className="text-foreground underline decoration-dotted underline-offset-4">
            {TUNING_LABELS[tuning] ?? tuning}
          </a>
          <span aria-hidden>·</span>
        </>
      )}
      <RunDate run={run} />
    </p>
  )
}
