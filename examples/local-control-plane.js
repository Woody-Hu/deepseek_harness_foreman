/**
 * Local control plane for the examples: a real, minimal implementation of
 * the cloud-side surface foreman talks to (src/control-plane.js) — an
 * artifact store (bucketed object storage) plus a system message bus —
 * running on 127.0.0.1 and backed by a directory on disk.
 *
 * Endpoints (same wire contract as the cloud control plane):
 *   PUT    /storage/<bucket>/<key>   upload an artifact (raw bytes; bucket = agentId)
 *   GET    /storage/<bucket>/<key>   download an artifact
 *   DELETE /storage/<bucket>/<key>   delete an artifact (checkpoint retention)
 *   POST   /bus/events               publish a bus event (e.g. sandbox.reclaim-requested)
 *
 * This is not a mock: objects are written to `<dir>/storage/<bucket>/<key>`
 * and bus events are appended to `<dir>/bus-events.jsonl`, so a `--keep` run
 * leaves a fully inspectable control-plane state on disk. The only difference
 * from the cloud deployment is the address (127.0.0.1) and the scale.
 */
import { createServer } from 'node:http'
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

/**
 * Start the local control-plane server.
 * @param {{dir: string}} options root directory for storage and bus events
 * @returns {Promise<{server: import('node:http').Server, baseUrl: string, port: number, dir: string, close(): Promise<void>}>}
 */
export function startLocalControlPlane({ dir }) {
  const storageRoot = join(dir, 'storage')
  const busLog = join(dir, 'bus-events.jsonl')
  const server = createServer((request, response) => {
    const url = new URL(request.url, 'http://localhost')
    const parts = url.pathname.split('/').filter(Boolean)
    const send = (code, body, type = 'application/json') => {
      response.writeHead(code, { 'content-type': type })
      response.end(body)
    }
    const readBody = () => new Promise((resolve) => {
      const chunks = []
      request.on('data', (chunk) => { chunks.push(chunk) })
      request.on('end', () => { resolve(Buffer.concat(chunks)) })
    })

    if (request.method === 'POST' && url.pathname === '/bus/events') {
      readBody().then(async (buffer) => {
        const event = JSON.parse(buffer.toString('utf8'))
        await mkdir(dir, { recursive: true })
        await writeFile(busLog, `${JSON.stringify({ ...event, receivedAt: new Date().toISOString() })}\n`, { flag: 'a' })
        send(200, JSON.stringify({ accepted: true }))
      })
      return
    }

    if (parts[0] === 'storage' && parts.length >= 3) {
      const bucket = parts[1]
      const key = parts.slice(2).join('/')
      const file = join(storageRoot, bucket, key)
      if (request.method === 'PUT') {
        readBody().then(async (buffer) => {
          await mkdir(dirname(file), { recursive: true })
          await writeFile(file, buffer)
          send(200, JSON.stringify({ stored: key, size: buffer.length }))
        })
        return
      }
      if (request.method === 'GET') {
        readFile(file)
          .then((buffer) => send(200, buffer, 'application/octet-stream'))
          .catch(() => send(404, JSON.stringify({ error: 'not found' })))
        return
      }
      if (request.method === 'DELETE') {
        rm(file, { force: true })
          .then(() => send(200, JSON.stringify({ deleted: key })))
          .catch(() => send(404, JSON.stringify({ error: 'not found' })))
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
        dir,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}
