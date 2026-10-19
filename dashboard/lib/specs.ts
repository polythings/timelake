/**
 * The specs, milestones and background pages (spec/ at the repository root), as scripts/spec_index.py lists them in
 * specs.json, milestones.json and background.json. The roadmap is the milestones.
 */
import BACKGROUND_LIST from "./background.json"
import MILESTONE_LIST from "./milestones.json"
import SPEC_LIST from "./specs.json"

export type SpecStatus = "done" | "in-progress" | "planned" | "dropped"
export type MilestoneStatus = "done" | "in-progress" | "planned" | "exploring"

export type Spec = {
  number: number
  /** The file name without `.md`, which is also the page's path: /spec/<slug>. */
  slug: string
  title: string
  area: string
  status: SpecStatus
  updated: string
  /** The releases that changed it, oldest first; `unreleased` for changes not in a release yet. */
  versions: string[]
  summary: string
  related: number[]
  issues: number[]
  /** The slugs of the milestones it is part of. */
  milestones: string[]
}

export type Milestone = {
  number: number
  /** The file name without `.md` in spec/milestones/, and the page's path: /roadmap/<slug>. */
  slug: string
  title: string
  status: MilestoneStatus
  /** The releases it shipped in, such as `0.1.0`, `0.2.0–0.7.0`, or `0.5.0–` while in progress. */
  versions: string
  updated: string
  /** One line in plain words, for the roadmap. */
  description: string
  summary: string
  specs: number[]
  issues: number[]
}

/** A page in spec/ that isn't a spec: purpose, architecture, landscape, … */
export type BackgroundPage = { slug: string; title: string; updated: string; summary: string }

export const SPECS = SPEC_LIST as Spec[]
export const MILESTONES = MILESTONE_LIST as Milestone[]
export const BACKGROUND = BACKGROUND_LIST as BackgroundPage[]

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

/** Milestone statuses in the order the roadmap shows them. */
export const MILESTONE_STATUS_LABEL: Record<MilestoneStatus, string> = {
  "in-progress": "In progress",
  planned: "Planned",
  exploring: "Exploring",
  done: "Done",
}

export const MILESTONE_STATUS_MEANING: Record<MilestoneStatus, string> = {
  "in-progress": "Being built now.",
  planned: "Next, in order of priority.",
  exploring: "Ideas being looked into, which may not happen.",
  done: "Built and released.",
}

export function specByNumber(n: number): Spec | undefined {
  return SPECS.find((s) => s.number === n)
}

export function milestoneBySlug(slug: string): Milestone | undefined {
  return MILESTONES.find((m) => m.slug === slug)
}

/** A list of releases in words: `0.5.0, 0.7.0, unreleased`; `since 0.5.0` for an open range. */
export function versionsText(versions: string[] | string): string {
  if (typeof versions === "string") return versions.endsWith("–") ? `since ${versions.slice(0, -1)}` : versions
  return versions.join(", ")
}
