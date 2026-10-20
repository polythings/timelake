"use client"

import { Check, Copy } from "lucide-react"
import { useState } from "react"

import { cn } from "@/lib/utils"

/**
 * Copies the text of the code block it sits in (the nearest `pre` in its parent, a `group/code`) to the clipboard. With a
 * mouse it shows while the block is hovered; on touch screens always.
 */
export function CopyButton({ className }: { className?: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      type="button"
      aria-label={copied ? "Copied" : "Copy code"}
      onClick={async (e) => {
        const text = e.currentTarget.parentElement?.querySelector("pre")?.innerText ?? ""
        try {
          await navigator.clipboard.writeText(text.replace(/\n$/, ""))
          setCopied(true)
          setTimeout(() => setCopied(false), 1500)
        } catch {
          setCopied(false)
        }
      }}
      className={cn(
        "absolute top-1.5 right-1.5 inline-flex size-8 items-center justify-center rounded-md border bg-background/80 text-muted-foreground backdrop-blur transition-colors transition-opacity hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none pointer-fine:opacity-0 pointer-fine:group-hover/code:opacity-100 pointer-fine:focus-visible:opacity-100",
        className,
      )}
    >
      {copied ? <Check className="size-3.5" aria-hidden /> : <Copy className="size-3.5" aria-hidden />}
    </button>
  )
}
