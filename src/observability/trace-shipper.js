/**
 * TraceShipper — asynchronous, non-blocking trace forwarding.
 *
 * Purpose: stream traces to the cloud monitoring system gradually, fully
 * async, never impacting the main flow. Structure:
 *
 *   dsh(OTLP exporter) --POST--> local receiver(127.0.0.1:0) --enqueue(fast, immediate 200)--> memory queue
 *                                ▲ dsh always gets an instant 200,        │
 *                                │ upstream failures/slowness fully isolated
 *                                ▼ background drain loop
 *                               cloud monitoring (OTLP endpoint) <--retry+backoff--┘
 *
 * Failure semantics (three layers, none affect the main flow):
 *   - The receiver never rejects: enqueue answers 200 immediately; a full
 *     queue drops the oldest entry and counts it (keeps the newest telemetry);
 *   - Upstream failures retry: exponential backoff x retries, then the item is
 *     dropped and counted (traces are a best-effort signal);
 *   - Shutdown flush: stop({flushMs}) drains the queue before the sandbox is
 *     reclaimed (the graceful path guarantees delivery).
 *
 * Dynamic credentials: headersEnv resolves from env on every drain (injected
 * or rotated per task by the cloud, never written to disk). headers are static
 * configuration headers (complementary to credentials, e.g. fixed routing).
 */
import { createServer } from 'node:http'

export class TraceShipper {
  /**
   * @param {object} options
   * @param {string} options.upstreamUrl cloud monitoring OTLP endpoint (e.g. https://…/v1/logs)
   * @param {object} [options.headers] static request headers (sent with every forward)
   * @param {object} [options.headersEnv] dynamic credential headers: {'Authorization': 'FOREMAN_TRACE_TOKEN'} (resolved per drain)
   * @param {number} [options.maxQueue] queue cap (drops the oldest beyond it)
   * @param {number} [options.retries] per-item forward retry count
   * @param {number} [options.retryBaseMs] backoff base (exponential)
   */
  constructor(options) {
    this.options = { maxQueue: 2048, retries: 4, retryBaseMs: 100, ...options }
    if (typeof options.upstreamUrl !== 'string' || options.upstreamUrl.length === 0) {
      throw new Error('trace-shipper: upstreamUrl is required')
    }
    this.queue = []
    this.draining = false
    this.closed = false
    this.stats = {
      received: 0, forwarded: 0, droppedOverflow: 0, droppedRetries: 0, retries: 0, inflight: 0,
    }
  }

  /** Start the local receiver + background drain loop; returns itself. */
  async start() {
    this.server = createServer((request, response) => {
      const chunks = []
      request.on('data', (chunk) => { chunks.push(chunk) })
      request.on('end', () => {
        const body = Buffer.concat(chunks)
        this.enqueue(body)
        response.writeHead(200).end() // instant 200: the dsh exporter never waits for the cloud upstream
      })
    })
    await new Promise((resolve) => { this.server.listen(0, '127.0.0.1', resolve) })
    this.port = this.server.address().port
    this.drainLoop().catch(() => { /* drain-loop failures stay contained; counted as droppedRetries */ this.stats.droppedRetries += this.queue.length; this.queue = [] })
    return this
  }

  /** The endpoint dsh telemetry is wired to (DSH_TELEMETRY_OTLP_URL points here). */
  get endpoint() { return `http://127.0.0.1:${this.port}/v1/logs` }

  enqueue(body) {
    if (this.closed) { this.stats.droppedOverflow += 1; return }
    if (this.queue.length >= this.options.maxQueue) {
      this.queue.shift() // full: drop the oldest, keep the newest telemetry
      this.stats.droppedOverflow += 1
    }
    this.queue.push(body)
    this.stats.received += 1
  }

  async drainLoop() {
    for (;;) {
      if (this.queue.length === 0) {
        if (this.closed) return
        await new Promise((resolve) => { setTimeout(resolve, 20) })
        continue
      }
      const body = this.queue.shift()
      this.stats.inflight += 1
      try {
        await this.forwardWithRetry(body)
        this.stats.forwarded += 1
      } catch {
        this.stats.droppedRetries += 1
      } finally {
        this.stats.inflight -= 1
      }
    }
  }

  async forwardWithRetry(body) {
    for (let attempt = 0; ; attempt += 1) {
      let permanent = false // 4xx-class statuses are not retryable: give up immediately
      try {
        const response = await fetch(this.options.upstreamUrl, {
          method: 'POST',
          headers: { 'content-type': 'application/json', ...this.options.headers, ...this.resolveDynamicHeaders() },
          body,
        })
        if (response.ok) return
        if (response.status < 500) permanent = true
        throw new Error(`upstream ${response.status}`)
      } catch (error) {
        if (permanent || attempt >= this.options.retries) throw error
        this.stats.retries += 1
        await new Promise((resolve) => { setTimeout(resolve, this.options.retryBaseMs * 2 ** attempt) })
      }
    }
  }

  /** Resolve dynamic credential headers from env at drain time (injected/rotated per task). */
  resolveDynamicHeaders() {
    const headers = {}
    for (const [header, envName] of Object.entries(this.options.headersEnv ?? {})) {
      const value = process.env[envName]
      if (value !== undefined) headers[header] = value
    }
    return headers
  }

  /**
   * Shut down: flush first (drain the queue within the deadline), then close
   * the receiver. Called before the cloud reclaims the sandbox so the graceful
   * path delivers every produced trace (best effort: leftovers past the
   * deadline are dropped and counted).
   */
  async stop({ flushMs = 5000 } = {}) {
    this.closed = true
    const deadline = Date.now() + flushMs
    while ((this.queue.length > 0 || this.stats.inflight > 0) && Date.now() < deadline) {
      await new Promise((resolve) => { setTimeout(resolve, 25) })
    }
    await new Promise((resolve) => { this.server.close(() => { resolve() }) })
    return { ...this.stats, pending: this.queue.length }
  }
}
