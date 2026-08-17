import { EventEmitter } from 'node:events'
import { createServer, request as httpRequest } from 'node:http'
import type { AddressInfo } from 'node:net'
import { Readable } from 'node:stream'
import { brotliDecompressSync, gunzipSync } from 'node:zlib'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { afterEach, describe, expect, it } from 'vitest'
import { bridge, selectContentEncoding } from '../src/http-bridge.ts'

describe('HTTP bridge abort', () => {
  it('destroys a declared-oversize request instead of draining it', async () => {
    const destroyed: true[] = []
    const request = Readable.from([]) as unknown as IncomingMessage
    Object.assign(request, {
      url: '/api/session.prompt',
      method: 'POST',
      headers: { 'content-type': 'application/json', 'content-length': '999999' },
      destroy: () => { destroyed.push(true) },
    })
    let status: number | undefined
    let headers: unknown
    const response = Object.assign(new EventEmitter(), {
      writableEnded: false,
      writeHead(code: number, values?: unknown) { status = code; headers = values; return this },
      write() { return true },
      end(this: { writableEnded: boolean }) { this.writableEnded = true; return this },
    }) as unknown as ServerResponse

    await bridge(request, response, {
      requestBodyMode: () => 'buffered',
      fetch: () => { throw new Error('a rejected request must never reach the handler') },
    }, 1000)
    // The socket must not stay parked draining a body the client can trickle
    // at will after the rejection — same discipline as the chunked overrun.
    expect(status).toBe(413)
    expect(headers).toMatchObject({ connection: 'close' })
    expect(destroyed).toHaveLength(1)
  })

  it('aborts a pending native picker request when the browser disconnects', async () => {
    const body = JSON.stringify({
      type: 'client-request', rpcId: 'picker-1', method: 'directoryPicker/pick', payload: { args: {} },
    })
    const request = Readable.from([Buffer.from(body)]) as unknown as IncomingMessage
    Object.assign(request, {
      url: '/api/directoryPicker/pick',
      method: 'POST',
      headers: { 'content-type': 'application/json' },
    })

    const response = Object.assign(new EventEmitter(), {
      writableEnded: false,
      writeHead() { return this },
      write() { return true },
      end() { this.writableEnded = true; return this },
    }) as unknown as ServerResponse

    let resolveStarted!: () => void
    const started = new Promise<void>((resolve) => { resolveStarted = resolve })
    let carrierSignal: AbortSignal | undefined
    const pending = bridge(request, response, {
      requestBodyMode: () => 'buffered',
      fetch: async (input) => {
        const fetchRequest = input
        carrierSignal = fetchRequest.signal
        resolveStarted()
        if (!fetchRequest.signal.aborted) {
          await new Promise<void>((resolve) => {
            fetchRequest.signal.addEventListener('abort', () => { resolve() }, { once: true })
          })
        }
        return Response.json({ aborted: fetchRequest.signal.aborted })
      },
    }, Number.MAX_SAFE_INTEGER)
    await started
    response.emit('close')
    await pending
    expect(carrierSignal?.aborted).toBe(true)
  })

  it('streams a declared 2.19 GiB request before the body ends and bypasses the JSON buffer cap', async () => {
    const request = new Readable({ read() {} }) as unknown as IncomingMessage
    Object.assign(request, {
      url: '/api/session/uploadFileBinary?sessionId=s1',
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
        'content-length': String(Math.ceil(2.19 * 1024 ** 3)),
      },
    })
    let status: number | undefined
    const responseBytes: Uint8Array[] = []
    const response = Object.assign(new EventEmitter(), {
      writableEnded: false,
      writeHead(code: number) { status = code; return this },
      write(chunk: Uint8Array) { responseBytes.push(chunk); return true },
      end(this: { writableEnded: boolean }) { this.writableEnded = true; return this },
    }) as unknown as ServerResponse

    let resolveStarted!: () => void
    const started = new Promise<void>((resolve) => { resolveStarted = resolve })
    const received: Uint8Array[] = []
    const pending = bridge(request, response, {
      requestBodyMode: () => 'streaming',
      fetch: async (input) => {
        resolveStarted()
        if (input.body === null) throw new Error('streaming request lost its body')
        for await (const chunk of input.body) received.push(chunk)
        return new Response('stored')
      },
    }, 1)

    await started
    expect(received).toEqual([])
    request.push(Buffer.from([1, 2]))
    request.push(Buffer.from([3, 4]))
    request.push(null)
    await pending
    expect(status).toBe(200)
    expect(received).toEqual([Uint8Array.of(1, 2), Uint8Array.of(3, 4)])
    expect(Buffer.concat(responseBytes).toString()).toBe('stored')
  })

  it('closes an unread streaming request after returning an early validation response', async () => {
    const destroyed: true[] = []
    const request = new Readable({ read() {} }) as unknown as IncomingMessage
    Object.assign(request, {
      url: '/api/session/uploadFileBinary',
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      destroy: () => { destroyed.push(true) },
    })
    let status: number | undefined
    let headers: unknown
    const response = Object.assign(new EventEmitter(), {
      writableEnded: false,
      writeHead(code: number, values?: unknown) { status = code; headers = values; return this },
      write() { return true },
      end(this: { writableEnded: boolean }) { this.writableEnded = true; return this },
    }) as unknown as ServerResponse

    await bridge(request, response, {
      requestBodyMode: () => 'streaming',
      fetch: () => Promise.resolve(new Response(null, { status: 415 })),
    }, 1)
    expect(status).toBe(415)
    expect(headers).toMatchObject({ connection: 'close' })
    expect(destroyed).toEqual([true])
  })
})

describe('HTTP bridge content encoding', () => {
  const servers: ReturnType<typeof createServer>[] = []
  afterEach(async () => {
    await Promise.all(servers.splice(0).map(server => new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error === undefined) resolve()
        else reject(error)
      })
    })))
  })

  async function start(handler: (req: IncomingMessage, res: ServerResponse) => Promise<void>): Promise<number> {
    const server = createServer((req, res) => {
      handler(req, res).catch((error: unknown) => {
        res.destroy(error instanceof Error ? error : new Error(String(error)))
      })
    })
    servers.push(server)
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(0, '127.0.0.1', () => {
        server.off('error', reject)
        resolve()
      })
    })
    return (server.address() as AddressInfo).port
  }

  async function post(port: number, acceptEncoding: string): Promise<{ headers: IncomingMessage['headers']; body: Buffer }> {
    return new Promise((resolve, reject) => {
      const req = httpRequest({
        host: '127.0.0.1',
        port,
        path: '/api/session.list',
        method: 'POST',
        headers: { 'accept-encoding': acceptEncoding, 'content-type': 'application/json' },
      }, (res) => {
        const chunks: Buffer[] = []
        res.on('data', (chunk: Buffer) => { chunks.push(chunk) })
        res.on('end', () => { resolve({ headers: res.headers, body: Buffer.concat(chunks) }) })
      })
      req.on('error', reject)
      req.end('{}')
    })
  }

  it('gzip-compresses a JSON API response and announces the coding', async () => {
    const payload = 'x'.repeat(4096)
    const port = await start(async (req, res) => {
      await bridge(req, res, { requestBodyMode: () => 'buffered', fetch: async () => Response.json({ payload }) })
    })
    const { headers, body } = await post(port, 'gzip')
    expect(headers['content-encoding']).toBe('gzip')
    expect(headers.vary).toContain('Accept-Encoding')
    expect(JSON.parse(gunzipSync(body).toString())).toEqual({ payload })
  })

  it('brotli-compresses when brotli is the only accepted coding', async () => {
    const payload = 'y'.repeat(4096)
    const port = await start(async (req, res) => {
      await bridge(req, res, { requestBodyMode: () => 'buffered', fetch: async () => Response.json({ payload }) })
    })
    const { headers, body } = await post(port, 'br')
    expect(headers['content-encoding']).toBe('br')
    expect(JSON.parse(brotliDecompressSync(body).toString())).toEqual({ payload })
  })

  it('keeps streaming event channels uncompressed', async () => {
    const frame = 'data: {"type":"stream/error"}\n\n'
    const port = await start(async (req, res) => {
      await bridge(req, res, {
        requestBodyMode: () => 'buffered',
        fetch: async () => new Response(frame, { headers: { 'content-type': 'text/event-stream' } }),
      })
    })
    const { headers, body } = await post(port, 'gzip')
    expect(headers['content-encoding']).toBeUndefined()
    expect(body.toString()).toBe(frame)
  })
})

describe('selectContentEncoding', () => {
  it('negotiates q-values and prefers gzip on ties', () => {
    expect(selectContentEncoding('gzip, br', 'application/json', undefined)).toBe('gzip')
    expect(selectContentEncoding('gzip;q=0, br;q=0.5', 'application/json', undefined)).toBe('br')
    expect(selectContentEncoding('*', 'application/json; charset=utf-8', undefined)).toBe('gzip')
    expect(selectContentEncoding('br', 'application/json', undefined)).toBe('br')
  })

  it('skips unsupported, disabled, streaming, and small responses', () => {
    expect(selectContentEncoding(undefined, 'application/json', undefined)).toBeUndefined()
    expect(selectContentEncoding('gzip;q=0', 'application/json', undefined)).toBeUndefined()
    expect(selectContentEncoding('gzip', 'text/event-stream', undefined)).toBeUndefined()
    expect(selectContentEncoding('gzip', 'application/json', '512')).toBeUndefined()
    expect(selectContentEncoding('gzip', 'application/octet-stream', undefined)).toBeUndefined()
  })
})
