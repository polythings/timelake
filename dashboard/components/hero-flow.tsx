"use client"

import { useEffect, useRef } from "react"

import { HeroSim, type HeroTheme } from "@/components/hero-sim"
import { CELL, cssVar, fitCanvas } from "@/lib/dot-canvas"
import { watchTheme } from "@/lib/theme"

/** Milliseconds per tick while hovered: one new point, or one step of a reader's chart. */
const TICK_MS = 55

/**
 * The hero as a pipeline, left to right: you publish points with the cursor; they are written to the object store
 * (batched in memory, committed as files in day partitions, compacted); and readers query the table: the files a query
 * needs light up in the reading color, and the reader draws the series downsampled its own way. Readers see the table
 * as of the last commit, so their chart lags the live one and moves a commit at a time.
 *
 * The readers are tabs on top: Rotate, the default, takes them in turn, and hovering a reader's tab keeps that one on
 * screen. Hovering the chart publishes and the reader reloads as data commits; hovering the rest publishes nothing.
 * The picture and its state are components/hero-sim.ts; this wires it to the canvas, a timer and the pointer.
 */
export function HeroFlow() {
  const canvas = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const c = canvas.current
    const ctx = c?.getContext("2d")
    const area = c?.parentElement
    if (!c || !ctx || !area) return
    const sim = new HeroSim()
    const font = getComputedStyle(document.body).getPropertyValue("--font-mono") || "monospace"

    // The theme's colors, read again on each draw so a theme change shows.
    const draw = () => {
      const theme: HeroTheme = {
        paper: cssVar(c, "--dot"),
        pen: cssVar(c, "--primary"),
        muted: cssVar(c, "--muted-foreground"),
        reads: cssVar(c, "--chart-4"),
        tone: cssVar(c, "--reader-tone"),
        font,
      }
      const [w, h] = [c.clientWidth, c.clientHeight]
      fitCanvas(c, ctx, w, h)
      sim.draw(ctx, w, h, theme)
    }

    let timer: ReturnType<typeof setInterval> | null = null
    const stop = () => {
      if (timer) clearInterval(timer)
      timer = null
    }
    // When not publishing or rotating (or no longer hovered), nothing needs ticks once the chart is loaded and drawn,
    // until the cursor does something.
    const tick = () => {
      const state = sim.tick()
      draw()
      if (state === "idle") stop()
    }
    const run = () => {
      if (!timer) timer = setInterval(tick, TICK_MS)
    }

    draw()
    // With reduced motion the hero stays a still picture: nothing follows the pointer.
    const still = window.matchMedia("(prefers-reduced-motion: reduce)").matches
    const move = (e: PointerEvent) => {
      const r = c.getBoundingClientRect()
      sim.move((e.clientX - r.left) / CELL, (e.clientY - r.top) / CELL, Math.floor(r.height / CELL))
      run()
    }
    // The reader reloads as soon as the cursor comes in, so the readers visibly respond to hovering.
    const enter = () => {
      if (sim.enter()) run()
    }
    // Leaving stops publishing and rotating; the next tick stops the hero once the reader has finished loading and
    // drawing in.
    const leave = () => sim.leave()
    if (!still) {
      area.addEventListener("pointerenter", enter)
      area.addEventListener("pointermove", move)
      area.addEventListener("pointerleave", leave)
    }
    const ro = new ResizeObserver(draw)
    ro.observe(c)
    const unwatch = watchTheme(draw)
    return () => {
      stop()
      unwatch()
      area.removeEventListener("pointerenter", enter)
      area.removeEventListener("pointermove", move)
      area.removeEventListener("pointerleave", leave)
      ro.disconnect()
    }
  }, [])

  return <canvas ref={canvas} aria-hidden className="absolute inset-0 h-full w-full" />
}
