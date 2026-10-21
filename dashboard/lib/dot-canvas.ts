/** What the site's dot pictures share: the grid of squares, colors from the theme, sizing a canvas, the dot font. */

/** A square, the gap after it, and the two together: the grid of the races, the hero and the cards' pictures. */
export const DOT = 3
export const GAP = 1
export const CELL = DOT + GAP

/** A CSS custom property's value on `el`, e.g. cssVar(c, "--dot"). */
export function cssVar(el: Element, name: string): string {
  return getComputedStyle(el).getPropertyValue(name).trim()
}

/** "var(--chart-3)" → the color it stands for on `el`; other colors pass through. */
export function resolveColor(el: Element, color: string): string {
  const m = /^var\((--[\w-]+)\)$/.exec(color)
  return m?.[1] ? cssVar(el, m[1]) : color
}

/**
 * Gives a canvas a bitmap of `width`×`height` CSS pixels at the screen's pixel ratio (only where that changed, since
 * resizing a bitmap clears it), sets the transform so drawing is in CSS pixels, and clears it.
 */
export function fitCanvas(c: HTMLCanvasElement, ctx: CanvasRenderingContext2D, width: number, height: number) {
  const dpr = window.devicePixelRatio || 1
  const [w, h] = [Math.round(width * dpr), Math.round(height * dpr)]
  if (c.width !== w) c.width = w
  if (c.height !== h) c.height = h
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
  ctx.clearRect(0, 0, width, height)
}

/** 5×7 dot-matrix glyphs, top row first: digits, a few signs, and the letters units need. */
export const DOT_FONT: Record<string, string[]> = {
  "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
  "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
  "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
  "3": ["11110", "00001", "00001", "01110", "00001", "00001", "11110"],
  "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
  "5": ["11111", "10000", "11110", "00001", "00001", "10001", "01110"],
  "6": ["00110", "01000", "10000", "11110", "10001", "10001", "01110"],
  "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
  "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
  "9": ["01110", "10001", "10001", "01111", "00001", "00010", "01100"],
  ".": ["00000", "00000", "00000", "00000", "00000", "01100", "01100"],
  ",": ["00000", "00000", "00000", "00000", "01100", "00100", "01000"],
  " ": ["000", "000", "000", "000", "000", "000", "000"],
  "–": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
  "×": ["00000", "00000", "10001", "01010", "00100", "01010", "10001"],
  "%": ["11001", "11010", "00010", "00100", "01000", "01011", "10011"],
  m: ["00000", "00000", "11010", "10101", "10101", "10101", "10101"],
  s: ["00000", "00000", "01111", "10000", "01110", "00001", "11110"],
  r: ["00000", "00000", "10110", "11001", "10000", "10000", "10000"],
  e: ["00000", "00000", "01110", "10001", "11111", "10000", "01110"],
  q: ["00000", "00000", "01101", "10011", "01111", "00001", "00001"],
  k: ["10000", "10000", "10010", "10100", "11000", "10100", "10010"],
  B: ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
  M: ["10001", "11011", "10101", "10101", "10001", "10001", "10001"],
}
