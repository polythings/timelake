import { Panel } from "@/components/ranking-card"
import { SectionHead } from "@/components/section-head"
import { READERS, SYSTEM_INFO, readerDescription, systemLabel, systemOrder } from "@/lib/bench-systems"
import { TUNING_NOTES, type Run } from "@/lib/benchmarks"
import { slugify } from "@/lib/utils"

export const ENTRIES_TITLE = "What each entry means"

/**
 * What every entry in the results is: each system, how it's written and maintained, and what reads it. A ranking's
 * entry links here (`#entry-<system>`).
 */
export function EntriesSection({ latest }: { latest: Run[] }) {
  const systems = new Map<string, Set<string>>()
  for (const r of latest) {
    for (const [system, readers] of Object.entries(r.header.readers)) {
      const known = systems.get(system) ?? new Set<string>()
      for (const reader of readers) known.add(reader)
      systems.set(system, known)
    }
  }
  const listed = [...systems].sort(([a], [b]) => systemOrder(a) - systemOrder(b))
  return (
    <section id={slugify(ENTRIES_TITLE)} className="flex scroll-mt-20 flex-col gap-4">
      <SectionHead label={ENTRIES_TITLE}>
        Each system is written, maintained, and read the way its own docs suggest, on the same data. An entry in a
        ranking is a system, or a system and the reader that queried it, such as Lance + DuckDB.
      </SectionHead>
      <p id="tunings" className="scroll-mt-20 text-sm text-muted-foreground">
        Every system runs in two tunings. <span className="text-foreground">Tuned</span>: {TUNING_NOTES.tuned}.{" "}
        <span className="text-foreground">Default</span>: {TUNING_NOTES.default}. Results are tuned unless a
        section&apos;s switch says otherwise.
      </p>
      <div className="grid gap-4 lg:grid-cols-2">
        {listed.map(([system, readers]) => (
          <Entry key={system} system={system} readers={[...readers]} />
        ))}
      </div>
    </section>
  )
}

/** One system's entry: what it is, how it's written and maintained, and each reader that queried it. */
function Entry({ system, readers }: { system: string; readers: string[] }) {
  const info = SYSTEM_INFO[system]
  return (
    <div id={`entry-${system}`} className="scroll-mt-20 rounded-lg target:ring-2 target:ring-ring">
      <Panel title={systemLabel(system)} note={info?.summary} className="h-full">
        <dl className="grid gap-x-4 gap-y-2 text-sm sm:grid-cols-[6.5rem_minmax(0,1fr)]">
          {info && (
            <>
              <dt className="text-muted-foreground">Written</dt>
              <dd>{info.writes}</dd>
              <dt className="text-muted-foreground">Maintenance</dt>
              <dd>{info.maintenance}</dd>
              <dt className="text-muted-foreground">When</dt>
              <dd>{info.when}</dd>
              {info.tuned && (
                <>
                  <dt className="text-muted-foreground">Tuned</dt>
                  <dd>{info.tuned}</dd>
                </>
              )}
            </>
          )}
          <dt className="text-muted-foreground">Read by</dt>
          <dd className="flex flex-col gap-1">
            {readers.map((reader) => (
              <span key={reader}>
                <span className="font-medium">{READERS[reader] ?? reader}</span>: {readerDescription(system, reader)}
              </span>
            ))}
          </dd>
        </dl>
      </Panel>
    </div>
  )
}
