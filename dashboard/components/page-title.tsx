import { cn } from "@/lib/utils"

/** A page's large light heading. */
export function PageTitle({ children, className }: { children: React.ReactNode; className?: string }) {
  return <h1 className={cn("text-4xl leading-[1.05] font-light tracking-tight sm:text-5xl", className)}>{children}</h1>
}
