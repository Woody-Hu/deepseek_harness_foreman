/**
 * Mock cloud control plane (for tests): object storage + system message bus,
 * simulating the cloud-side dependencies.
 *
 * Endpoints:
 *   PUT    /storage/<bucket>/<key>   upload an artifact (raw bytes; bucket = agentId)
 *   GET    /storage/<bucket>/<key>   download an artifact ("restore from remote object storage" before a run)
 *   DELETE /storage/<bucket>/<key>   delete an artifact (checkpoint retention drops expired packs)
 *   POST   /bus/events               message bus event (e.g. sandbox.reclaim-requested)
 *
 * Events and object contents are kept in memory or on disk for assertions.
 */
import { createServer } from 'node:http'
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

/**
 * Start the mock control-plane server.
 * @param {{dir?: string}} [options] persist objects to this directory instead of memory
 * @returns {Promise<{server: import('node:http').Server, baseUrl: string, port: number, events: object[], storageDir: string|undefined, close(): Promise<void>}>}
 */
export function startMockControlPlane(options = {}) {
  const events = []
  const storageDir = options.dir
  const buckets = new Map() // bucket -> Map(key -> Buffer); in-memory when no dir is given
  const server = createServer((request, response) => {
    const url = new URL(request.url, 'http://localhost')
    const parts = url.pathname.split('/').filter(Boolean)
    const send = (code, body, type = 'application/json') => {
      response.writeHead(code, { 'content-type': type })
      response.end(body)
    }

    if (request.method === 'POST' && url.pathname === '/bus/events') {
      const chunks = []
      request.on('data', (chunk) => { chunks.push(chunk) })
      request.on('end', () => {
        const event = JSON.parse(Buffer.concat(chunks).toString('utf8'))
        event.receivedAt = Date.now()
        events.push(event)
        send(200, JSON.stringify({ accepted: true }))
      })
      return
    }

    if (parts[0] === 'storage' && parts.length >= 3) {
      const bucket = parts[1]
      const key = parts.slice(2).join('/')
      if (request.method === 'PUT') {
        const chunks = []
        request.on('data', (chunk) => { chunks.push(chunk) })
        request.on('end', async () => {
          const buffer = Buffer.concat(chunks)
          if (storageDir !== undefined) {
            const file = join(storageDir, bucket, key)
            await mkdir(dirname(file), { recursive: true })
            await writeFile(file, buffer)
          } else {
            if (!buckets.has(bucket)) buckets.set(bucket, new Map())
            buckets.get(bucket).set(key, buffer)
          }
          send(200, JSON.stringify({ stored: key, size: buffer.length }))
        })
        return
      }
      if (request.method === 'GET') {
        (async () => {
          const buffer = storageDir !== undefined
            ? await readFile(join(storageDir, bucket, key))
            : buckets.get(bucket)?.get(key)
          if (buffer === undefined) { send(404, JSON.stringify({ error: 'not found' })); return }
          send(200, buffer, 'application/octet-stream')
        })().catch(() => send(404, JSON.stringify({ error: 'not found' })))
        return
      }
      if (request.method === 'DELETE') {
        (async () => {
          if (storageDir !== undefined) await rm(join(storageDir, bucket, key), { force: true })
          else buckets.get(bucket)?.delete(key)
          send(200, JSON.stringify({ deleted: key }))
        })().catch(() => send(404, JSON.stringify({ error: 'not found' })))
        return
      }
    }
    send(404, JSON.stringify({ error: 'unknown route' }))
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      resolve({
        server,
        baseUrl: `http://127.0.0.1:${port}`,
        port,
        events,
        storageDir,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}
