"use client"

import { useSyncExternalStore } from "react"

const HEADINGS = "article.prose-docs h2[id]"

/** Re-reads the headings whenever the article changes, as on moving to another page. */
function subscribe(onChange: () => void) {
  const article = document.querySelector("article.prose-docs")
  if (!article) return () => {}
  const observer = new MutationObserver(onChange)
  observer.observe(article, { childList: true, subtree: true })
  return () => observer.disconnect()
}

/** The headings as one string, "id\ttext" per line, so an unchanged page reads as unchanged. */
const snapshot = () =>
  [...document.querySelectorAll<HTMLHeadingElement>(HEADINGS)].map((h) => `${h.id}\t${h.textContent}`).join("\n")

/** The page's sections (its h2s with ids), as links beside a long docs page. */
export function OnThisPage() {
  const headings = useSyncExternalStore(subscribe, snapshot, () => "")
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      const [id = "", text = ""] = line.split("\t")
      return { id, text }
    })
  if (headings.length < 3) return null
  return (
    <nav aria-label="On this page" className="flex flex-col gap-2 text-xs">
      <p className="font-mono tracking-[0.06em] text-muted-foreground uppercase">On this page</p>
      <ul className="flex flex-col">
        {headings.map((h) => (
          <li key={h.id}>
            <a href={`#${h.id}`} className="block py-1 text-muted-foreground hover:text-foreground">
              {h.text}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  )
}
