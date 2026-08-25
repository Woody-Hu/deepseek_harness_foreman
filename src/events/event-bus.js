/**
 * EventBus — streaming publisher for the outbound event bus.
 *
 * Purpose (optional delivery mode): instead of returning SSE directly from the
 * foreman layer, stream data message-by-message onto a message bus. The bus
 * consumes the same EventOut stream as the SSE carrier (see formats.js) — the
 * delivery configuration decides the target: 'sse' | 'bus' | 'both'.
 *
 * Implementations (kind):
 *   memory  in-process recording (for test assertions: messages are ordered
 *           and retrievable)
 *   http    POST each message to a relay/agent-gateway (internal queue + retry
 *           backoff; publishing never throws and never blocks the caller —
 *           the same failure isolation semantics as TraceShipper)
 *
 * Dynamic credentials: headersEnv resolves from env on every delivery (the
 * same injection channel as snapshot/trace credentials).
 */

/**
 * @param {object} config
 * @param {'memory'|'http'} config.kind
 * @param {string} [config.url]         http: relay endpoint (one POST per message)
 * @param {object} [config.headers]     http: static headers
 * @param {object} [config.headersEnv]  http: dynamic credential headers {'Authorization': 'FOREMAN_BUS_TOKEN'}
 * @param {number} [config.maxQueue]    http: queue cap (drop oldest beyond it and count)
 * @param {number} [config.retries]     http: per-message retry count
 * @param {number} [config.retryBaseMs] http: backoff base
 * @returns {{kind, messages?: object[], publish(message: object): Promise<void>, start?(): Promise<object>, stop(opts?): Promise<object>, stats?: object}}
 */
export function createEventBus(config) {
  if (config.kind === 'memory') {
    const messages = []
    return {
      kind: 'memory',
      messages,
      async publish(message) { messages.push(message) },
      async stop() { return { published: messages.length } },
    }
  }
  if (config.kind === 'http') {
    const options = { maxQueue: 4096, retries: 3, retryBaseMs: 100, ...config }
    if (typeof options.url !== 'string' || options.url.length === 0) {
      throw new Error('event-bus(http): url is required')
    }
    return new HttpEventBus(options)
  }
  throw new Error(`event-bus: unknown kind ${String(config.kind)} (memory | http)`)
}

class HttpEventBus {
  constructor(options) {
    this.options = options
    this.queue = []
    this.closed = false
    this.stats = { published: 0, droppedOverflow: 0, droppedRetries: 0, retries: 0, inflight: 0 }
  }

  /** Publishing never throws and never blocks: enqueue and return, delivery happens in a background loop. */
  async publish(message) {
    if (this.closed) { this.stats.droppedOverflow += 1; return }
    if (this.queue.length >= this.options.maxQueue) {
      this.queue.shift()
      this.stats.droppedOverflow += 1
    }
    this.queue.push(message)
  }

  /** Background delivery loop (started by foreman before the first publish). */
  async start() {
    this.drainLoop().catch(() => { this.stats.droppedRetries += this.queue.length; this.queue = [] })
    return this
  }

  async drainLoop() {
    for (;;) {
      if (this.queue.length === 0) {
        if (this.closed) return
        await new Promise((resolve) => { setTimeout(resolve, 15) })
        continue
      }
      const message = this.queue.shift()
      this.stats.inflight += 1
      try {
        await this.postWithRetry(message)
        this.stats.published += 1
      } catch {
        this.stats.droppedRetries += 1
      } finally {
        this.stats.inflight -= 1
      }
    }
  }

  async postWithRetry(message) {
    for (let attempt = 0; ; attempt += 1) {
      let permanent = false
      try {
        const headers = { 'content-type': 'application/json', ...this.options.headers }
        for (const [header, envName] of Object.entries(this.options.headersEnv ?? {})) {
          const value = process.env[envName] // resolve per delivery: dynamic credentials
          if (value !== undefined) headers[header] = value
        }
        const response = await fetch(this.options.url, { method: 'POST', headers, body: JSON.stringify(message) })
        if (response.ok) return
        if (response.status < 500) permanent = true
        throw new Error(`relay ${response.status}`)
      } catch (error) {
        if (permanent || attempt >= this.options.retries) throw error
        this.stats.retries += 1
        await new Promise((resolve) => { setTimeout(resolve, this.options.retryBaseMs * 2 ** attempt) })
      }
    }
  }

  async stop({ flushMs = 5000 } = {}) {
    this.closed = true
    const deadline = Date.now() + flushMs
    while ((this.queue.length > 0 || this.stats.inflight > 0) && Date.now() < deadline) {
      await new Promise((resolve) => { setTimeout(resolve, 20) })
    }
    return { ...this.stats, pending: this.queue.length }
  }
}
