// Static file server with HTTP Range, HEAD and CORS, for testing Timelake's HTTPS read path
// locally (the way Vercel Blob or a CDN serves a warehouse).
//   bun scripts/serve-range.mjs <dir> [port]       (port 0 picks a free one)
import { createReadStream, statSync } from "node:fs"
import { createServer } from "node:http"
import path from "node:path"
import { fileURLToPath } from "node:url"

/**
 * The byte range `{ start, end }` (inclusive) a `Range` header asks of an object of `size` bytes, as RFC 9110 reads a
 * single range: `"unsatisfiable"` for a range that starts past the end (answer 416), and `undefined` for a header to
 * ignore (answer 200 with the whole object): none, several ranges, or one that isn't valid. A suffix longer than the
 * object (`bytes=-500` of 100 bytes) is the whole object.
 */
export function parseRange(header, size) {
  const m = /^bytes=(\d*)-(\d*)$/.exec(header ?? "")
  if (!m || (m[1] === "" && m[2] === "")) return undefined
  if (m[1] === "") {
    const suffix = Number(m[2])
    if (suffix === 0 || size === 0) return "unsatisfiable"
    return { start: Math.max(0, size - suffix), end: size - 1 }
  }
  const start = Number(m[1])
  if (m[2] !== "" && Number(m[2]) < start) return undefined
  if (start >= size) return "unsatisfiable"
  return { start, end: m[2] === "" ? size - 1 : Math.min(Number(m[2]), size - 1) }
}

/** Sends `file` (bytes `start` to `end`) to `res`, ending the response if the file can't be read. */
function send(res, file, options) {
  createReadStream(file, options)
    .on("error", () => res.destroy())
    .pipe(res)
}

/** An HTTP server for the files under `dir`; call `.listen(port, host)` on it. */
export function createRangeServer(dir) {
  const root = path.resolve(dir)
  return createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*")
    res.setHeader("Access-Control-Allow-Headers", "Range")
    res.setHeader("Access-Control-Expose-Headers", "Content-Range, Content-Length, ETag")
    if (req.method === "OPTIONS") return res.writeHead(204).end()
    let rel
    try {
      rel = decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname)
    } catch {
      return res.writeHead(400).end() // a malformed escape such as "%E0%A4%A"
    }
    const file = path.join(root, rel)
    if (!file.startsWith(root + path.sep)) return res.writeHead(403).end()
    let st
    try {
      st = statSync(file)
      if (!st.isFile()) throw new Error("not a file")
    } catch {
      return res.writeHead(404).end()
    }
    const headers = {
      "Accept-Ranges": "bytes",
      "Last-Modified": st.mtime.toUTCString(),
      ETag: `"${st.size}-${st.mtimeMs}"`,
    }
    const range = parseRange(req.headers.range, st.size)
    if (range === "unsatisfiable") return res.writeHead(416, { "Content-Range": `bytes */${st.size}` }).end()
    if (range) {
      const { start, end } = range
      res.writeHead(206, {
        ...headers,
        "Content-Range": `bytes ${start}-${end}/${st.size}`,
        "Content-Length": end - start + 1,
      })
      return req.method === "HEAD" ? res.end() : send(res, file, { start, end })
    }
    res.writeHead(200, { ...headers, "Content-Length": st.size })
    return req.method === "HEAD" ? res.end() : send(res, file)
  })
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const root = path.resolve(process.argv[2] ?? ".")
  const server = createRangeServer(root)
  server.listen(Number(process.argv[3] ?? 4100), "127.0.0.1", () => {
    console.log(`serving ${root} on http://127.0.0.1:${server.address().port}`)
  })
}
