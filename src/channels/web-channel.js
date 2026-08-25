/**
 * Web channel driver: launches `dsh web` (dsh-base + dsh-web-app bundle with a
 * cloud-delivered patch overlay).
 *
 * Exposes the same channel interface as the SDK channel (sdk-channel.js) and is
 * selected by foreman.js based on configuration. Differences and advantages:
 *   - The transport is HTTP (unary POST /api/<method> + POST /api/respond) plus
 *     a WebSocket downlink (/api/events.mux) — stdio is no longer occupied.
 *     A plain GET against the mux endpoint gets a 426; the SSE framing only
 *     exists as an internal carrier of the apiproxy, so the Web composition
 *     effectively exposes a WS downlink.
 *   - Session resume is native: a "cold" persisted session is prompted
 *     directly and the api-remotes agent resolver resumes it automatically
 *     (no adapter plugin needed).
 *   - HITL is available: the mux stream pushes approval/requested frames with
 *     a stable rpcId, and responses go through POST /api/respond
 *     (ClientResponse envelope, answered with an RpcReceipt).
 *
 * Key wiring:
 *   DSH_HOME=<workdir>/dsh-home     session log root = dshHomePath('sessions')
 *                                   (isolated from the profile)
 *   session.create({cwd})           the session workspace = session.header.cwd,
 *                                   which drives the sandbox root and the base
 *                                   directory for relative paths (the process
 *                                   cwd is only a fallback)
 *   DEEPSEEK_BASE_URL               points at the model endpoint (env-injected
 *                                   only, never written to disk)
 *   webserver {host:127.0.0.1, port:0} (from the overlay) plus the stdout
 *                                   readiness line "dsh web: http://127.0.0.1:<port>"
 */
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { cp, mkdir } from 'node:fs/promises'
import { join } from 'node:path'

/** Readiness timeout (tsx cold start + dsh web composition loading is slow). */
const BOOT_TIMEOUT_MS = 90_000

/** HTTP client for POST /api/<method> (ClientRequest envelope) and POST /api/respond. */
class WebApiClient {
  constructor(baseUrl) {
    this.baseUrl = baseUrl
  }

  /**
   * Invoke a unary RPC method.
   * @returns {Promise<unknown>} result.value (the business success value)
   * @throws {Error} result.error (business error, message includes the code) or a non-200 carrier
   */
  async request(method, payload) {
    const response = await fetch(`${this.baseUrl}/api/${method}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'client-request', rpcId: randomUUID(), method, payload }),
    })
    if (!response.ok) throw new Error(`/api/${method} carrier error ${response.status}: ${await response.text()}`)
    const body = await response.json()
    if (body.result?.ok === true) return body.result.value
    const error = body.result?.error
    throw new Error(`/api/${method} ${error?.code ?? 'unknown'}: ${error?.message ?? JSON.stringify(body)}`)
  }

  /**
   * Answer a server-request (HITL approval) via POST /api/respond with a
   * ClientResponse envelope.
   * @returns {Promise<{accepted: boolean, reason?: string}>} RpcReceipt
   */
  async respond(rpcId, value) {
    const response = await fetch(`${this.baseUrl}/api/respond`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'client-response', rpcId, result: { ok: true, value } }),
    })
    if (!response.ok) throw new Error(`/api/respond carrier error ${response.status}`)
    return await response.json()
  }
}

/**
 * @param {object} options
 * @param {string} options.repoRoot dsh repository root
 * @param {string} options.patchPath cloud-delivered web profile patch overlay (absolute path)
 * @param {string} options.workspaceDir session workspace (absolute path; must stay identical
 *   across turns — resume locates the session by cwd)
 * @param {string} options.dshHome DSH_HOME (session log root = <dshHome>/sessions)
 * @param {object} options.modelEnv { DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL }
 * @param {object} options.telemetry { mode, otlpUrl }
 * @param {object} [options.envExtra] extra env entries injected into the child process
 *   (tenant/trace ids etc., never written to disk)
 * @param {string} [options.pluginsDir] runner-bundled local plugin directory (materialized to
 *   <dshHome>/profiles/web/plugins — the loader root of the profile launch face is anchored
 *   at the profile directory)
 */
export class WebChannel {
  constructor(options) {
    this.options = options
    this.events = []
    this.turnWaiters = []
    this.timings = {}
  }

  get sessionRoot() { return join(this.options.dshHome, 'sessions') }

  /**
   * Launch `dsh web` and open the mux WebSocket downlink.
   * handlers: { onEvent(sessionId, event), onApproval(frame), onApprovalResolved(frame) }
   * @returns {Promise<{url: string, port: number}>}
   */
  async start(handlers) {
    const t0 = Date.now()
    const { repoRoot, patchPath, workspaceDir, dshHome, modelEnv, telemetry } = this.options
    this.handlers = handlers
    // Materialize local plugins into the profile directory: the loader root of the
    // profile launch face (dsh web = profile 'web') is anchored at
    // $DSH_HOME/profiles/web/ — ./plugins/* relative names in the patch overlay
    // resolve against the profile directory (the --patch file itself is read
    // in place; relative names do not resolve against its location).
    if (this.options.pluginsDir !== undefined) {
      const profilePlugins = join(dshHome, 'profiles', 'web', 'plugins')
      await mkdir(profilePlugins, { recursive: true })
      await cp(this.options.pluginsDir, profilePlugins, { recursive: true })
    }
    this.child = spawn(process.execPath, [
      '--import', 'tsx',
      join(repoRoot, 'apps/cli/src/bin.ts'),
      'web',
      '--patch', patchPath,
      '--no-open',
    ], {
      // cwd=repoRoot (tsx resolves from the repository node_modules). The
      // workspace boundary is not decided by the process cwd: sandbox-policy
      // and fs tools resolve relative paths against session.header.cwd — set
      // below by ensureSession's session.create({cwd: workspaceDir}) and kept
      // identical across turns.
      cwd: repoRoot,
      env: {
        ...process.env,
        ...(this.options.envExtra ?? {}), // extra injection (tenant/trace ids etc. — same in-memory channel as secrets)
        DSH_HOME: dshHome,
        DEEPSEEK_API_KEY: modelEnv.DEEPSEEK_API_KEY,
        DEEPSEEK_BASE_URL: modelEnv.DEEPSEEK_BASE_URL,
        DSH_TELEMETRY_MODE: telemetry.mode,
        DSH_TELEMETRY_OTLP_URL: telemetry.otlpUrl,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    this.child.stderr.on('data', (chunk) => {
      this.stderrTail = `${(this.stderrTail ?? '') + chunk.toString('utf8')}`.slice(-4000)
    })

    // Readiness signal: web-app prints "dsh web: http://127.0.0.1:<port>" once
    // the Loader tree has settled.
    const { url, port } = await new Promise((resolve, reject) => {
      let stdout = ''
      const timer = setTimeout(() => {
        reject(new Error(`dsh web readiness timeout; stdout=${stdout.slice(-2000)} stderr=${this.stderrTail ?? ''}`))
      }, BOOT_TIMEOUT_MS)
      const onLine = (line) => {
        const match = /^dsh web: (http:\/\/127\.0\.0\.1:(\d+))/.exec(line)
        if (match === null) return
        clearTimeout(timer)
        resolve({ url: match[1], port: Number(match[2]) })
      }
      let buffer = ''
      this.child.stdout.on('data', (chunk) => {
        buffer += chunk.toString('utf8')
        const parts = buffer.split('\n')
        buffer = parts.pop() ?? ''
        for (const part of parts) onLine(part.trim())
      })
      this.child.once('exit', (code) => {
        clearTimeout(timer)
        reject(new Error(`dsh web exited before readiness (code=${code}); stderr=${this.stderrTail ?? ''}`))
      })
    })
    this.url = url
    this.port = port
    this.api = new WebApiClient(url)
    await this.openMux()
    this.timings.bootMs = Date.now() - t0
    return { url, port }
  }

  /**
   * Open the mux downlink: GET /api/events.mux is a WebSocket downlink (a plain
   * GET gets 426 Upgrade Required; SSE framing only exists as an internal
   * carrier of toFetchHandler). The protocol is downlink-only: the server
   * pushes ServerRequest JSON frames while all client uplink goes over HTTP
   * POST (unary via /api/<method>, approval answers via /api/respond) —
   * sending messages into this socket gets it closed with 1008. Uses the
   * built-in Node >=22 WebSocket client (no dependency needed).
   */
  async openMux() {
    const socket = new WebSocket(`ws://127.0.0.1:${this.port}/api/events.mux`)
    socket.addEventListener('message', (event) => {
      let envelope
      try { envelope = JSON.parse(String(event.data)) } catch { return }
      this.onMuxFrame(envelope)
    })
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`mux websocket open timeout; stderr=${this.stderrTail ?? ''}`))
      }, 15_000)
      socket.addEventListener('open', () => { clearTimeout(timer); resolve() })
      socket.addEventListener('error', () => {
        clearTimeout(timer)
        reject(new Error(`mux websocket error; stderr=${this.stderrTail ?? ''}`))
      })
    })
    this.muxSocket = socket
  }

  onMuxFrame(envelope) {
    const payload = envelope.payload ?? {}
    switch (payload.type) {
      case 'session/event':
        this.events.push(payload.event)
        for (const waiter of [...this.turnWaiters]) waiter(payload.event)
        this.handlers.onEvent(payload.sessionId, payload.event)
        return
      case 'approval/requested':
        this.handlers.onApproval({ rpcId: envelope.rpcId, ...payload })
        return
      case 'approval/resolved':
        this.handlers.onApprovalResolved(payload)
        return
      case 'stream/error':
        this.muxError = payload.error
        return
      default:
        return // session/subscribed, session/queue, projection etc. are pure push frames this driver does not consume
    }
  }

  /**
   * Idempotently ensure a session exists: if session.list contains the id
   * (including cold persisted sessions) reuse it, otherwise session.create
   * (the external session id doubles as the dsh session id — no mapping table).
   */
  async ensureSession(sessionId) {
    const { items } = await this.api.request('session.list', {})
    if (items.some((item) => String(item.sessionId) === sessionId)) return { created: false }
    await this.api.request('session.create', { sessionId, cwd: this.options.workspaceDir })
    return { created: true }
  }

  /**
   * Send a task and wait for turn/end.
   * Cold sessions need no explicit resume: prompting triggers the api-remotes
   * agent resolver to resume automatically.
   */
  async prompt(sessionId, text, { timeoutMs = 120_000 } = {}) {
    const t0 = Date.now()
    const ensure = await this.ensureSession(sessionId)
    const baselineSeq = Math.max(-1, ...this.events.map((event) => event.seq ?? -1))
    const turnEnd = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.turnReject = undefined
        reject(new Error(`turn/end timeout; muxError=${JSON.stringify(this.muxError ?? null)} stderr=${this.stderrTail ?? ''}`))
      }, timeoutMs)
      this.turnReject = (reason) => {
        clearTimeout(timer)
        this.turnReject = undefined
        this.turnWaiters.length = 0
        reject(reason)
      }
      const waiter = (event) => {
        if (event.type !== 'turn/end' || (event.seq ?? -1) <= baselineSeq) return
        clearTimeout(timer)
        this.turnReject = undefined
        this.turnWaiters.splice(this.turnWaiters.indexOf(waiter), 1)
        resolve(event)
      }
      this.turnWaiters.push(waiter)
    })
    try {
      await this.api.request('session.prompt', {
        sessionId,
        mode: 'queue',
        content: [{ type: 'text', text }],
      })
    } catch (error) {
      // First-turn race fallback: prompt reports session-not-found before the
      // create has landed — run ensure + prompt once more.
      if (String(error).includes('session-not-found')) {
        await this.api.request('session.create', { sessionId, cwd: this.options.workspaceDir })
        await this.api.request('session.prompt', { sessionId, mode: 'queue', content: [{ type: 'text', text }] })
      } else {
        throw error
      }
    }
    const end = await turnEnd
    this.timings.turnMs = Date.now() - t0
    return { created: ensure.created, reason: end.data.reason }
  }

  /**
   * Abandon waiting for the current turn (crash experiments only): clears the
   * timeout timer and rejects the pending prompt() so process exit is not held
   * up by a 120s timer. Only call this right before kill().
   */
  abandonPendingTurn() {
    this.turnReject?.(new Error('turn abandoned: dsh process is being killed'))
  }

  /** Answer a pending approval (POST /api/respond; the authoritative outcome is the mux approval/resolved frame). */
  async respondApproval(rpcId, { sessionId, approvalId, outcome }) {
    return await this.api.respond(rpcId, { sessionId, approvalId, outcome })
  }

  /** Graceful shutdown: SIGTERM (dsh settles pending approvals as cancelled) -> SIGKILL on timeout. */
  async shutdown() {
    this.muxSocket?.close()
    const exitCodePromise = new Promise((resolve) => { this.child.once('exit', (code) => resolve(code)) })
    this.child.kill('SIGTERM')
    const timeout = (ms) => new Promise((resolve) => { setTimeout(() => { resolve('timeout') }, ms) })
    let code = await Promise.race([exitCodePromise, timeout(20_000)])
    if (code === 'timeout') {
      this.child.kill('SIGKILL')
      code = await exitCodePromise
    }
    return code
  }

  /** Hard kill (simulating a sandbox crash/reclaim): no cleanup, pending approvals stay dangling in the log. */
  kill() {
    this.muxSocket?.close()
    this.child.kill('SIGKILL')
    return new Promise((resolve) => { this.child.once('exit', (code) => resolve(code)) })
  }
}
