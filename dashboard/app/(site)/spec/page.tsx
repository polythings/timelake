import { notFound, redirect } from "next/navigation"

import { SPECS } from "@/lib/specs"

/** /spec opens the first spec; the sidebar lists the rest. */
export default function SpecIndex() {
  const first = SPECS[0]
  if (!first) notFound()
  redirect(`/spec/${first.slug}`)
}
