/** The specs (spec/*.md at the repository root), as scripts/spec_index.py lists them in specs.json. */
import SPEC_LIST from "./specs.json"

export type SpecStatus = "done" | "in-progress" | "planned" | "dropped"

export type Spec = {
  number: number
  /** The file name without `.md`, which is also the page's path: /spec/<slug>. */
  slug: string
  title: string
  area: string
  status: SpecStatus
  updated: string
  summary: string
  related: number[]
  issues: number[]
}

export const SPECS = SPEC_LIST as Spec[]

export const AREAS = [...new Set(SPECS.map((s) => s.area))]

export const STATUS_LABEL: Record<SpecStatus, string> = {
  done: "Done",
  "in-progress": "In progress",
  planned: "Planned",
  dropped: "Dropped",
}

export const STATUS_MEANING: Record<SpecStatus, string> = {
  done: "Built and in effect.",
  "in-progress": "Partly built; the open parts have issues.",
  planned: "Agreed or proposed, not built yet.",
  dropped: "Measured or discussed and not done.",
}

export function specByNumber(n: number): Spec | undefined {
  return SPECS.find((s) => s.number === n)
}
