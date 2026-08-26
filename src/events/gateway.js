/**
 * Foreman outbound gateway.
 *
 * Data plane: internal frames are adapted by the formatter into an outbound
 * EventOut stream (native / openai-chat ...) and delivered per `delivery` —
 * 'sse' writes to subscribers, 'bus' publishes onto the message bus, 'both'
 * does both. The replay buffer stores rendered wire lines (Last-Event-ID
 * resumption replays consistently regardless of format). The management
 * plane (/status, POST /hitl) is always available regardless of delivery mode.
 */
import { createServer } from 'node:http'
import { createEventFormatter, renderSseLine } from './formats.js'

export class SseGateway {
  /**
   * @param {object} [options]
   * @param {object} [options.formatter] a createEventFormatter() instance (default: native passthrough)
   * @param {object} [options.bus] a createEventBus() instance (required when delivery includes 'bus')
   * @param {'sse'|'bus'|'both'} [options.delivery] delivery target (default 'sse')
   */
  constructor({ formatter, bus, delivery = 'sse' } = {}) {
    this.formatter = formatter ?? createEventFormatter('native')
    this.bus = bus
    this.delivery = delivery
    if (delivery !== 'sse' && bus === undefined) {
      throw new Error(`sse-gateway: delivery ${delivery} requires a bus`)
    }
    this.emitted = [] // {id, line} — the rendered outbound stream (replay source)
    this.subscribers = new Set()
    this.hitlHandler = undefined
    this.server = createServer((request, response) => {
      if (request.url === '/events') {
        response.writeHead(200, {
          'content-type': 'text/event-stream',
          'cache-control': 'no-cache',
          connection: 'keep-alive',
        })
        // SSE comment line: flush the response headers immediately. Otherwise,
        // when a formatter (e.g. openai-chat) produces zero output before the
        // first session.event, nothing is written after writeHead and the
        // subscriber's fetch hangs in the headers phase until it times out.
        response.write(': connected\n\n')
        const lastId = Number.parseInt(request.headers['last-event-id'] ?? '-1', 10)
        for (const entry of this.emitted) {
          if (entry.id > lastId) response.write(entry.line)
        }
        this.subscribers.add(response)
        request.on('close', () => { this.subscribers.delete(response) })
        return
      }
      if (request.url === '/status') {
        response.writeHead(200, { 'content-type': 'application/json' })
        response.end(JSON.stringify(this.status ?? {}))
        return
      }
      // HITL answer entry point (called back after external-system approval):
      // body {sessionId, approvalId, outcome}
      if (request.url === '/hitl' && request.method === 'POST') {
        const chunks = []
        request.on('data', (chunk) => { chunks.push(chunk) })
        request.on('end', async () => {
          if (this.hitlHandler === undefined) {
            response.writeHead(501, { 'content-type': 'application/json' })
            response.end(JSON.stringify({ error: 'hitl not available on this channel' }))
            return
          }
          try {
            const result = await this.hitlHandler(JSON.parse(Buffer.concat(chunks).toString('utf8')))
            response.writeHead(200, { 'content-type': 'application/json' })
            response.end(JSON.stringify(result))
          } catch (error) {
            response.writeHead(400, { 'content-type': 'application/json' })
            response.end(JSON.stringify({ error: String(error) }))
          }
        })
        return
      }
      response.writeHead(404).end()
    })
  }

  /** Register the HITL answer handler (channels with the hitl capability route decisions through it). */
  setHitlHandler(handler) { this.hitlHandler = handler }

  listen() {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        resolve(this.server.address().port)
      })
    })
  }

  /** Publish an internal frame: adapt -> render -> deliver per configuration (SSE subscribers / bus). */
  publish(payload, status) {
    for (const entry of this.formatter.push(payload)) {
      const id = this.emitted.length
      const line = renderSseLine(entry, id)
      this.emitted.push({ id, line })
      if (this.delivery !== 'bus') {
        for (const subscriber of this.subscribers) subscriber.write(line)
      }
      if (this.bus !== undefined && this.delivery !== 'sse') {
        // The bus receives the same stream data as SSE; publishing never
        // throws or blocks (failure isolation lives inside the bus)
        if (entry.type === 'done') void this.bus.publish({ kind: 'stream.done', sessionId: payload?.sessionId })
        else void this.bus.publish(entry.payload)
      }
    }
    if (status !== undefined) this.status = status
  }

  close() {
    // End all SSE subscriber connections first, and WAIT for each response
    // to flush before server.close(): destroying a socket with an unflushed
    // write buffer resets the connection and the subscriber loses the tail
    // of the stream (observed with slow consumers on large streams).
    const flushes = [...this.subscribers].map((subscriber) => new Promise((resolve) => {
      subscriber.end(() => { resolve() })
    }))
    this.subscribers.clear()
    return (async () => {
      await Promise.all(flushes)
      return await new Promise((resolve) => { this.server.close(() => { resolve() }) })
    })()
  }
}
