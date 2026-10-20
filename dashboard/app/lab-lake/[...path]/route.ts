import { readFile } from "node:fs/promises"
import path from "node:path"

/** The lab's demo table (dashboard/lab-lake), written with the CLI. */
const ROOT = path.resolve(/*turbopackIgnore: true*/ process.cwd(), "lab-lake")

/**
 * Serves the demo table to the lab page's browser reader, with the byte ranges it asks for. Local development only:
 * the lab and its data aren't part of the public site.
 */
export async function GET(request: Request, { params }: RouteContext<"/lab-lake/[...path]">) {
  if (process.env.NODE_ENV === "production") return new Response(null, { status: 404 })
  const file = path.resolve(ROOT, ...(await params).path)
  if (!file.startsWith(ROOT + path.sep)) return new Response(null, { status: 404 })
  let bytes: Buffer
  try {
    bytes = await readFile(file)
  } catch {
    return new Response(null, { status: 404 })
  }
  const range = parseRange(request.headers.get("range"), bytes.length)
  if (range === "unsatisfiable") {
    return new Response(null, { status: 416, headers: { "content-range": `bytes */${bytes.length}` } })
  }
  if (!range) return new Response(new Uint8Array(bytes), { headers: { "accept-ranges": "bytes" } })
  const { start, end } = range
  return new Response(new Uint8Array(bytes.subarray(start, end + 1)), {
    status: 206,
    headers: { "accept-ranges": "bytes", "content-range": `bytes ${start}-${end}/${bytes.length}` },
  })
}

/**
 * The byte range (`end` inclusive) a `Range` header asks of `size` bytes, as RFC 9110 reads a single range (the same
 * rules as `scripts/serve-range.mjs`): `"unsatisfiable"` when it starts past the end, `undefined` when the header is
 * to be ignored (none, several ranges, or an invalid one). A suffix longer than the object is the whole object.
 */
function parseRange(header: string | null, size: number): { start: number; end: number } | "unsatisfiable" | undefined {
  const m = /^bytes=(\d*)-(\d*)$/.exec(header ?? "")
  const [first = "", last = ""] = m ? [m[1], m[2]] : []
  if (!m || (first === "" && last === "")) return undefined
  if (first === "") {
    const suffix = Number(last)
    if (suffix === 0 || size === 0) return "unsatisfiable"
    return { start: Math.max(0, size - suffix), end: size - 1 }
  }
  const start = Number(first)
  if (last !== "" && Number(last) < start) return undefined
  if (start >= size) return "unsatisfiable"
  return { start, end: last === "" ? size - 1 : Math.min(Number(last), size - 1) }
}
