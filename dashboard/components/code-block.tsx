import { CopyButton } from "@/components/copy-button"

export function CodeBlock({ title, children }: { title?: string; children: string }) {
  return (
    <div className="overflow-hidden rounded-lg border bg-muted/40">
      {title && <div className="border-b px-4 py-2 font-mono text-xs text-muted-foreground">{title}</div>}
      <div className="group/code relative">
        <pre className="overflow-x-auto p-4 pr-12 font-mono text-[13px] leading-6">
          <code>{children}</code>
        </pre>
        <CopyButton />
      </div>
    </div>
  )
}
