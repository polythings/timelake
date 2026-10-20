"use client"

import { useEffect } from "react"

/** How long after the last scroll event the page takes the pointer again. */
const SETTLE_MS = 150

/**
 * Marks <html> with `data-scrolling` while the page scrolls, which turns off pointer events for a mouse or trackpad
 * (globals.css). Content sliding under a still cursor would otherwise set off hover effects, chart tooltips, and the
 * hero on the way past.
 */
export function ScrollGuard() {
  useEffect(() => {
    const root = document.documentElement
    let timer: ReturnType<typeof setTimeout> | undefined
    const settle = () => {
      delete root.dataset.scrolling
      timer = undefined
    }
    const scroll = () => {
      if (timer === undefined) root.dataset.scrolling = ""
      else clearTimeout(timer)
      timer = setTimeout(settle, SETTLE_MS)
    }
    window.addEventListener("scroll", scroll, { passive: true })
    return () => {
      window.removeEventListener("scroll", scroll)
      clearTimeout(timer)
      settle()
    }
  }, [])
  return null
}
