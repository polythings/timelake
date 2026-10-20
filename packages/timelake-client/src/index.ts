/**
 * @timelake/client: read Timelake tables over HTTPS, and append to them, from the browser, Node,
 * Bun, or serverless functions. This entry embeds the WebAssembly module (as base64, compiled on
 * first use). In Cloudflare Workers, which can't compile WebAssembly at runtime, use
 * `@timelake/client/workers` with the `.wasm` file instead.
 */

import { setEmbedded } from "./init.ts"
import { wasmBase64 } from "./wasm/inline.js"

setEmbedded(() => {
  const bin = atob(wasmBase64)
  const bytes = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i)
  return bytes
})

export * from "./workers.ts"
