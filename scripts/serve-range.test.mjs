// bun test scripts
import { afterAll, beforeAll, expect, test } from "bun:test"
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"

import { createRangeServer, parseRange } from "./serve-range.mjs"

const dir = mkdtempSync(path.join(tmpdir(), "serve-range-"))
const root = path.join(dir, "site")
let server
let base

beforeAll(async () => {
  mkdirSync(root)
  writeFileSync(path.join(root, "data.txt"), "0123456789")
  // A sibling folder whose name starts with the root's name.
  mkdirSync(`${root}-secret`)
  writeFileSync(path.join(`${root}-secret`, "key.txt"), "secret")
  server = createRangeServer(root)
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))
  base = `http://127.0.0.1:${server.address().port}`
})

afterAll(() => {
  server.close()
  rmSync(dir, { recursive: true, force: true })
})

test("serves byte ranges", async () => {
  const r = await fetch(`${base}/data.txt`, { headers: { Range: "bytes=2-4" } })
  expect(r.status).toBe(206)
  expect(r.headers.get("content-range")).toBe("bytes 2-4/10")
  expect(await r.text()).toBe("234")
  const suffix = await fetch(`${base}/data.txt`, { headers: { Range: "bytes=-3" } })
  expect(await suffix.text()).toBe("789")
  expect((await fetch(`${base}/data.txt`, { headers: { Range: "bytes=20-" } })).status).toBe(416)
})

test("rejects paths outside the root, including same-prefix siblings", async () => {
  const name = path.basename(root)
  expect((await fetch(`${base}/..%2F${name}-secret%2Fkey.txt`)).status).toBe(403)
  expect((await fetch(`${base}/..%2F..%2Fetc%2Fpasswd`)).status).toBe(403)
})

test("reads ranges as RFC 9110 does", () => {
  expect(parseRange("bytes=2-4", 10)).toEqual({ start: 2, end: 4 })
  expect(parseRange("bytes=2-", 10)).toEqual({ start: 2, end: 9 })
  expect(parseRange("bytes=8-20", 10)).toEqual({ start: 8, end: 9 })
  expect(parseRange("bytes=-3", 10)).toEqual({ start: 7, end: 9 })
  // A suffix longer than the file is the whole file.
  expect(parseRange("bytes=-500", 10)).toEqual({ start: 0, end: 9 })
  expect(parseRange("bytes=10-", 10)).toBe("unsatisfiable")
  expect(parseRange("bytes=-0", 10)).toBe("unsatisfiable")
  expect(parseRange("bytes=-5", 0)).toBe("unsatisfiable")
  // Invalid or multiple ranges are ignored: the whole file.
  for (const header of [undefined, "", "bytes=-", "bytes=5-2", "bytes=0-1,4-5", "items=0-1"]) {
    expect(parseRange(header, 10)).toBeUndefined()
  }
})

test("answers a suffix longer than the file with the whole file", async () => {
  const r = await fetch(`${base}/data.txt`, { headers: { Range: "bytes=-500" } })
  expect(r.status).toBe(206)
  expect(r.headers.get("content-range")).toBe("bytes 0-9/10")
  expect(await r.text()).toBe("0123456789")
  const empty = await fetch(`${base}/data.txt`, { headers: { Range: "bytes=-" } })
  expect(empty.status).toBe(200)
  expect(await empty.text()).toBe("0123456789")
})

test("answers a malformed path with 400 and keeps serving", async () => {
  expect((await fetch(`${base}/%E0%A4%A`)).status).toBe(400)
  expect((await fetch(`${base}/data.txt`)).status).toBe(200)
})
