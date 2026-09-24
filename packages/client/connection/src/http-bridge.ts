/**
 * node:http ↔ WHATWG fetch bridge for the /api transport (host side of the
 * web carrier; the fetch-shaped handler itself is transport-agnostic).
 */

import type { IncomingMessage } from 'node:http'
import { Readable } from 'node:stream'
import type { Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { constants as zlibConstants, createBrotliCompress, createGzip } from 'node:zlib'
import type { ConnectionFetchHandler } from './rpc.ts'

/** Default carrier cap for all HTTP RPC bodies: sized for the default
 * aggregate image limit (200 MiB) after base64 expansion plus envelope
 * headroom (~267.7 MiB required), rounded up for slack. The bridge buffers
 * each body in memory, so this cap is also the per-request resident bound. */
export const DEFAULT_MAX_REQUEST_BODY_BYTES = 300 * 1024 * 1024

interface BridgeServerResponse {
  readonly destroyed: boolean
  readonly writableEnded: boolean
  on(event: 'close', listener: () => void): this
  off(event: 'close' | 'drain', listener: () => void): this
  once(event: 'close' | 'drain', listener: () => void): this
  writeHead(statusCode: number, headers?: Record<string, string>): unknown
  write(chunk: Uint8Array): boolean
  end(): unknown
}
/** Content types the API bridge compresses; streaming event channels stay raw. */
const COMPRESSIBLE_CONTENT_TYPES = new Set([
  'application/json',
  'application/javascript',
  'application/xml',
  'image/svg+xml',
  'text/css',
  'text/html',
  'text/javascript',
  'text/plain',
  'text/xml',
])

/** Small responses are cheaper to send raw than to gzip and announce. */
const MIN_COMPRESS_BYTES = 1024

/**
 * Choose the response content coding the bridge may apply. Gzip wins ties:
 * it is universal, fast on the host, and within a few percent of Brotli for
 * the JSON transcripts this carrier mostly ships.
 * @param acceptEncoding - request `accept-encoding` value (or values).
 * @param contentType - response `content-type` value.
 * @param contentLength - response `content-length` value, when declared.
 * @returns the negotiated content coding, or undefined when none applies.
 */
export function selectContentEncoding(
  acceptEncoding: string | string[] | undefined,
  contentType: string | null | undefined,
  contentLength: string | null | undefined,
): 'gzip' | 'br' | undefined {
  if (acceptEncoding === undefined) return undefined
  const type = (contentType ?? '').split(';', 1)[0]?.trim().toLowerCase() ?? ''
  if (!COMPRESSIBLE_CONTENT_TYPES.has(type)) return undefined
  if (contentLength !== undefined && contentLength !== null) {
    const length = Number(contentLength)
    if (Number.isFinite(length) && length < MIN_COMPRESS_BYTES) return undefined
  }
  let gzip = 0
  let br = 0
  let any = 0
  for (const raw of [acceptEncoding].flat()) {
    for (const entry of raw.split(',')) {
      const [rawName = '', ...params] = entry.trim().split(';').map(part => part.trim())
      const name = rawName.toLowerCase()
      let q = 1
      for (const param of params) {
        const [rawKey = '', rawValue = ''] = param.split('=')
        if (rawKey.trim().toLowerCase() === 'q') {
          const parsed = Number(rawValue.trim())
          if (Number.isFinite(parsed)) q = parsed
        }
      }
      if (q <= 0) continue
      if (name === 'gzip') gzip = Math.max(gzip, q)
      else if (name === 'br') br = Math.max(br, q)
      else if (name === '*') any = Math.max(any, q)
    }
  }
  gzip = Math.max(gzip, any)
  br = Math.max(br, any)
  if (gzip <= 0 && br <= 0) return undefined
  return gzip >= br ? 'gzip' : 'br'
}

/**
 * Bridge one node:http request to the fetch-shaped handler (client close
 * aborts; response writes respect backpressure and stop on disconnect).
 * @param req - incoming node:http request.
 * @param res - node:http response the bridge writes and owns to completion.
 * @param apiHandler - fetch-shaped API carrier the request is dispatched to.
 * @param maxRequestBodyBytes - maximum bytes buffered for a buffered route.
 */
export async function bridge(
  req: IncomingMessage,
  res: BridgeServerResponse,
  apiHandler: ConnectionFetchHandler,
  maxRequestBodyBytes = DEFAULT_MAX_REQUEST_BODY_BYTES,
): Promise<void> {
  const abort = new AbortController()
  // Client-disconnect detection MUST hang off the response, not the request:
  // since Node 16, IncomingMessage 'close' fires as soon as the request body is
  // fully consumed (immediately for a bodyless GET), which would abort a
  // streaming response right after open. ServerResponse 'close' fires on connection teardown;
  // writableEnded distinguishes a normal end() from the client going away.
  res.on('close', () => {
    if (!res.writableEnded) abort.abort()
  })
  /* v8 ignore next 2 -- node:http always sets url/method on server requests. */
  const url = new URL(req.url ?? '/', 'http://dsh.internal')
  const method = req.method ?? 'GET'
  const headers = Object.fromEntries(
    Object.entries(req.headers).filter(([, value]) => typeof value === 'string') as [string, string][],
  )
  const bodyMode = apiHandler.requestBodyMode({ method, url })
  let request: Request
  if (bodyMode === 'buffered') {
    const declaredLength = req.headers['content-length']
    if (declaredLength !== undefined && Number(declaredLength) > maxRequestBodyBytes) {
      res.writeHead(413, { connection: 'close' })
      res.end()
      req.destroy()
      return
    }
    const chunks: Buffer[] = []
    let received = 0
    for await (const chunk of req) {
      const buffer = chunk as Buffer
      received += buffer.byteLength
      if (received > maxRequestBodyBytes) {
        res.writeHead(413, { connection: 'close' })
        res.end()
        req.destroy()
        return
      }
      chunks.push(buffer)
    }
    request = new Request(url, {
      method,
      headers,
      ...chunks.length > 0 ? { body: Buffer.concat(chunks) } : {},
      signal: abort.signal,
    })
  } else {
    request = new Request(url, {
      method,
      headers,
      body: Readable.toWeb(req) as ReadableStream<Uint8Array>,
      signal: abort.signal,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' })
  }
  const response = await apiHandler.fetch(request)
  const requestUnread = bodyMode === 'streaming' && !req.readableEnded
  const responseHeaders = Object.fromEntries(response.headers.entries())
  if (requestUnread) responseHeaders.connection = 'close'
  const contentEncoding = responseHeaders['content-encoding'] === undefined
    ? selectContentEncoding(req.headers['accept-encoding'], responseHeaders['content-type'], responseHeaders['content-length'])
    : undefined
  if (response.body !== null && contentEncoding !== undefined) {
    delete responseHeaders['content-length']
    responseHeaders['content-encoding'] = contentEncoding
    responseHeaders.vary = responseHeaders.vary === undefined || responseHeaders.vary.trim() === ''
      ? 'Accept-Encoding'
      : `${responseHeaders.vary}, Accept-Encoding`
    res.writeHead(response.status, responseHeaders)
    // Undici's `ReadableStream<Uint8Array>` and Node's stream/web generic
    // disagree on the ArrayBufferLike variance; the body is the same stream.
    const source = Readable.fromWeb(response.body as unknown as Parameters<typeof Readable.fromWeb>[0])
    // Brotli's default quality (11) costs seconds on megabyte payloads; q4
    // still beats gzip on size while keeping host CPU comparable to it.
    const compressor = contentEncoding === 'gzip'
      ? createGzip()
      : createBrotliCompress({ params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 4 } })
    // `res` is a real ServerResponse, but this module only names the members it
    // uses; the node:stream/promises overloads want the full Writable surface.
    await pipeline(source, compressor, res as unknown as Writable)
    if (requestUnread) req.destroy()
    return
  }
  res.writeHead(response.status, responseHeaders)
  if (response.body === null) {
    res.end()
    if (requestUnread) req.destroy()
    return
  }
  for await (const chunk of response.body) {
    // Drain without writing after disconnect: cancelling Node multipart bodies
    // can race their producer and reject with ERR_INVALID_STATE.
    if (abort.signal.aborted) continue
    // Backpressure: a false return means the socket buffer is full — wait for drain
    // instead of buffering unboundedly (slow or suspended consumers). 'close' also
    // resolves so a mid-wait disconnect cannot park this loop forever.
    if (!res.write(chunk) && !res.destroyed) {
      await new Promise<void>((resolve) => {
        const done = (): void => {
          res.off('drain', done)
          res.off('close', done)
          resolve()
        }
        res.once('drain', done)
        res.once('close', done)
      })
    }
  }
  res.end()
  if (requestUnread) req.destroy()
}
