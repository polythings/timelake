import type { CSSProperties } from "react"

/**
 * A static texture of dim squares (3px with 1px gaps, like the dot charts), for backgrounds and dividers. `ground` is
 * the color between the squares: the surface it sits on.
 */
export function dotPaper(ground = "var(--card)"): CSSProperties {
  return {
    backgroundColor: "var(--dot)",
    backgroundImage: `linear-gradient(to right, ${ground} 1px, transparent 1px), linear-gradient(to bottom, ${ground} 1px, transparent 1px)`,
    backgroundSize: "4px 4px",
  }
}
