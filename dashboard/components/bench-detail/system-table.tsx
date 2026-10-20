import { Panel } from "@/components/ranking-card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { systemLabel } from "@/lib/bench-systems"
import { cn } from "@/lib/utils"

/** A table's first column stays in place while the rest scrolls sideways on narrow screens. */
const FIRST = "sticky left-0 z-10 max-w-32 min-w-24 bg-card whitespace-normal sm:max-w-none"

/**
 * A card with a row per system: the system's name in a first column that stays put while the others scroll sideways
 * on a phone, and a column per entry of `columns`, numbers right-aligned.
 */
export function SystemTable({
  title,
  note,
  columns,
  children,
}: {
  title: React.ReactNode
  note: React.ReactNode
  columns: string[]
  children: React.ReactNode
}) {
  return (
    <Panel title={title} note={note} className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className={FIRST}>System</TableHead>
            {columns.map((c) => (
              <TableHead key={c} className="text-right">
                {c}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>{children}</TableBody>
      </Table>
      <p className="text-[11px] text-muted-foreground sm:hidden">Scroll the table sideways for every column.</p>
    </Panel>
  )
}

/** A system's row: its name, Timelake's in bold unless `plain` (a row that says what went wrong), then its cells. */
export function SystemRow({
  system,
  plain = false,
  children,
}: {
  system: string
  plain?: boolean
  children: React.ReactNode
}) {
  return (
    <TableRow>
      <TableCell className={cn(FIRST, !plain && system === "timelake" && "font-medium")}>
        {systemLabel(system)}
      </TableCell>
      {children}
    </TableRow>
  )
}

/** A right-aligned cell of figures. */
export function NumberCell({ children, className }: { children: React.ReactNode; className?: string }) {
  return <TableCell className={cn("text-right font-mono tabular-nums", className)}>{children}</TableCell>
}
