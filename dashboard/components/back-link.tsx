import { ChevronLeft } from "lucide-react"
import Link from "next/link"

/** A link back up to the page this one sits under, above its title. */
export function BackLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <Link href={href} className="flex w-fit items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
      <ChevronLeft className="size-4" />
      {children}
    </Link>
  )
}
