// Names, times and durations parse as the engine parses them.
import { afterEach, expect, test } from "bun:test"

import "../src/index" // registers the embedded WebAssembly module
import { ensureWasm } from "../src/init"
import { durationMicros, parseTableName, tablesIndex, timeMicros } from "../src/util"
import { parseTableName as wasmParseTableName, tablesIndexUri } from "../src/wasm/timelake_wasm.js"

const zone = process.env.TZ
afterEach(() => {
  if (zone === undefined) delete process.env.TZ
  else process.env.TZ = zone
})

const BAD_NAMES = ["", ".", "iot.", ".readings", "a.b.c", "iot/readings", "../x", "a b", "naïve", 'iot.read"ings']

test("table names follow the engine's TableName", () => {
  expect(parseTableName("iot.readings")).toEqual({ namespace: "iot", name: "readings" })
  expect(parseTableName("my-table_2")).toEqual({ namespace: "default", name: "my-table_2" })
  for (const bad of BAD_NAMES) {
    expect(() => parseTableName(bad)).toThrow("must be [namespace.]table")
  }
})

// A table handle parses its name before the WebAssembly module is loaded, so the client keeps a copy of the rules.
test("the client's table names and index are timelake-format's", () => {
  ensureWasm()
  for (const name of ["iot.readings", "my-table_2", "A-Z_0.9", ...BAD_NAMES]) {
    let wasm: unknown
    try {
      wasm = wasmParseTableName(name)
    } catch {
      expect(() => parseTableName(name)).toThrow("must be [namespace.]table")
      continue
    }
    expect(parseTableName(name)).toEqual(wasm as { namespace: string; name: string })
  }
  expect(tablesIndex("s3://b/lake")).toBe(tablesIndexUri("s3://b/lake"))
})

test("times without an offset are UTC in any time zone", () => {
  process.env.TZ = "America/New_York"
  expect(new Date(2026, 0, 1).getTimezoneOffset()).toBe(300)
  const noon = Date.UTC(2026, 0, 1, 12) * 1000
  expect(timeMicros("2026-01-01T12:00:00")).toBe(noon)
  expect(timeMicros("2026-01-01 12:00")).toBe(noon)
  expect(timeMicros("2026-01-01T12:00:00Z")).toBe(noon)
  expect(timeMicros("2026-01-01T13:30:00+01:30")).toBe(noon)
  expect(timeMicros("2026-01-01T07:00:00-0500")).toBe(noon)
  expect(timeMicros("2026-01-01")).toBe(Date.UTC(2026, 0, 1) * 1000)
  // A string keeps microseconds; numbers are milliseconds.
  expect(timeMicros("2026-01-01T12:00:00.123456Z")).toBe(noon + 123_456)
  expect(timeMicros(Date.UTC(2026, 0, 1, 12))).toBe(noon)
  expect(timeMicros(new Date(Date.UTC(2026, 0, 1, 12)))).toBe(noon)
  for (const bad of ["Jan 1 2026", "2026-01-01T12", "2026-13-01", "noon", ""]) {
    expect(() => timeMicros(bad)).toThrow("invalid time")
  }
})

test("durations follow the engine's parse_duration", () => {
  const s = 1_000_000
  expect(durationMicros("45s")).toBe(45 * s)
  expect(durationMicros("10m")).toBe(600 * s)
  expect(durationMicros("1h")).toBe(3600 * s)
  expect(durationMicros("3 hours")).toBe(3 * 3600 * s)
  expect(durationMicros(" 2W " as "2w")).toBe(14 * 86_400 * s)
  expect(durationMicros("1 day")).toBe(86_400 * s)
  expect(durationMicros("1y")).toBe(365 * 86_400 * s)
  expect(durationMicros(1500)).toBe(1.5 * s) // milliseconds
  for (const bad of ["1.5h", "500ms", "h", "-1h", "1x", "", "1 fortnight"]) {
    expect(() => durationMicros(bad as "1h")).toThrow("invalid duration")
  }
  expect(() => durationMicros("999999999999y")).toThrow("too long")
  // Past 2^53 microseconds, which a JavaScript number can't hold exactly.
  expect(() => durationMicros("300y")).toThrow("too long")
})
