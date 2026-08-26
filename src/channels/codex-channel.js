/**
 * Codex Harness app-server channel driver: launches `codex app-server --stdio`
 * and speaks the v2 app-server protocol over JSONL (ADR-0005,
 * docs/design/codex-channel.md — all wire shapes live-verified against
 * codex-cli 0.149.1).
 *
 * Channel interface (same contract as SdkChannel / WebChannel):
 *   start({ onEvent, onStatus }) -> { threadId }
 *   prompt(sessionId, text, opts) -> { reason }
 *   shutdown() -> exitCode    kill() -> exitCode
 *   sessionRoot -> CODEX_HOME
 *
 * Model wiring: codex resolves the model from CODEX_HOME/config.toml, so the
 * channel (re)writes it at start — a `model_providers.foreman` entry with
 * wire_api = "responses" (0.149.1 rejects "chat") and env_key pointing at an
 * env var injected into the child only (secrets never on disk).
 *
 * Session identity: codex generates its own thread ids, so the external
 * sessionId -> threadId mapping is persisted at CODEX_HOME/threads-index.json
 * (archived/restored with the session logs by foreman); a restored mapping
 * routes start() through thread/resume.
 */
import { spawn } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

/** Env var name carrying the API key into the codex child process. */
export const CODEX_API_KEY_ENV = 'FOREMAN_CODEX_API_KEY'

/** JSONL JSON-RPC-style client for the codex app-server protocol. */
class CodexRpcClient {
  constructor(stdout) {
    this.nextId = 1
    this.pending = new Map() // id -> { resolve, reject }
    this.notificationHandlers = []
    this.requestHandlers = []
    let buffer = ''
    stdout.setEncoding('utf8')
    stdout.on('data', (chunk) => {
      buffer += chunk
      let index
      while ((index = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, index)
        buffer = buffer.slice(index + 1)
        if (line.trim() === '') continue
        let message
        try { message = JSON.parse(line) } catch { continue } // non-protocol stderr noise on stdout is skipped
        this.dispatch(message)
      }
    })
  }

  dispatch(message) {
    if (message.id !== undefined && message.method === undefined) {
      const waiter = this.pending.get(message.id)
      if (waiter === undefined) return
      this.pending.delete(message.id)
      if (message.error !== undefined) waiter.reject(new Error(`codex ${message.error.code}: ${message.error.message}`))
      else waiter.resolve(message.result)
      return
    }
    if (message.method === undefined) return
    if (message.id !== undefined) {
      // Server-initiated request (approval requests): handlers answer or a
      // default decision is sent back.
      for (const handler of this.requestHandlers) handler(message, this)
      return
    }
    for (const handler of this.notificationHandlers) handler(message.method, message.params ?? {})
  }

  request(method, params) {
    const id = this.nextId++
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      this.send({ id, method, params })
    })
  }

  notify(method, params) { this.send({ method, params }) }

  /** Answer a server-initiated request. */
  respond(id, result) { this.send({ id, result }) }

  send(message) {
    if (this.stdin === undefined || this.stdin.destroyed) return
    this.stdin.write(`${JSON.stringify(message)}\n`)
  }

  onNotification(handler) { this.notificationHandlers.push(handler) }
  onRequest(handler) { this.requestHandlers.push(handler) }
}

/** Map codex turn.status to the internal turn/end reason kind. */
function turnEndReason(status) {
  if (status === 'completed') return 'completed'
  if (status === 'cancelled' || status === 'interrupted') return 'cancelled'
  return 'failed'
}

/**
 * @param {object} options
 * @param {string} [options.binary] codex binary (default 'codex')
 * @param {string[]} [options.args] app-server args (default ['app-server','--stdio'])
 * @param {string} options.workspaceDir session workspace (absolute; must be identical across runs)
 * @param {string} [options.codexHome] CODEX_HOME (thread store + config.toml; default <workspaceDir>/.codex)
 * @param {string} options.sessionId external session id (thread-id mapping key)
 * @param {string} [options.model] model name written to config.toml (default 'gpt-5.1-codex')
 * @param {object} [options.provider] custom model provider { name, baseUrl, envKey }
 * @param {string} [options.apiKey] API key injected via env (CODEX_API_KEY_ENV) — never written to disk
 * @param {string} [options.baseUrl] model endpoint base URL (overrides provider.baseUrl); requires Responses-API wire
 * @param {string} [options.approvalPolicy] default 'never'
 * @param {string} [options.sandbox] 'read-only' | 'workspace-write' | 'danger-full-access'
 * @param {number} [options.timeoutMs] turn timeout (default 300000)
 * @param {object} [options.envExtra] extra env injected into the child process
 */
export class CodexChannel {
  constructor(options = {}) {
    // undefined values must not override the defaults (callers pass sparse option objects)
    const provided = Object.fromEntries(Object.entries(options).filter(([, value]) => value !== undefined))
    this.options = {
      binary: 'codex',
      args: ['app-server', '--stdio'],
      model: 'gpt-5.1-codex',
      approvalPolicy: 'never',
      sandbox: 'workspace-write',
      timeoutMs: 300_000,
      ...provided,
    }
    this.events = []
    this.timings = {}
    this.phase = 'constructed'
    this.sessionId = options.sessionId ?? 'unknown'
    this.turnNumber = 0
    this.stepNumber = 0
    this.seq = 0
    this.threadId = undefined
    this.stderrTail = ''
  }

  get sessionRoot() { return this.options.codexHome ?? join(this.options.workspaceDir, '.codex') }

  /**
   * Write CODEX_HOME/config.toml (model + provider; the API key stays in env).
   *
   * Codex appends `[projects."<path>"] trust_level = "trusted"` entries to this
   * file at runtime (observed against 0.149.1). Rewriting the file wholesale
   * would drop them, downgrade the workspace to untrusted on the next run and
   * change command-execution semantics (approval-gated turns whose item
   * notifications are suppressed) — so codex-managed sections are preserved.
   */
  async writeModelConfig() {
    const configPath = join(this.sessionRoot, 'config.toml')
    const baseUrl = this.options.baseUrl ?? this.options.provider?.baseUrl
    const lines = [`model = "${this.options.model}"`]
    if (baseUrl !== undefined) {
      lines.push('model_provider = "foreman"', '', '[model_providers.foreman]', 'name = "Foreman"',
        `base_url = "${baseUrl}"`, 'wire_api = "responses"')
      // env_key only when a key is actually injected — a dangling env_key makes
      // codex fail every model request with a missing-variable error
      if (this.options.apiKey !== undefined) lines.push(`env_key = "${CODEX_API_KEY_ENV}"`)
    }
    let content = `${lines.join('\n')}\n`
    try {
      const existing = await readFile(configPath, 'utf8')
      const match = existing.match(/(?<=\n)\[projects[^\]]*\][\s\S]*$/) // codex-written trust entries
      if (match !== null) content += `\n${match[0].trim()}\n`
    } catch { /* no config yet */ }
    await writeFile(configPath, content)
  }

  /** Load the persisted sessionId -> threadId mapping (restored with the session archive). */
  async loadThreadIndex() {
    try {
      return JSON.parse(await readFile(join(this.sessionRoot, 'threads-index.json'), 'utf8'))
    } catch { return {} }
  }

  async saveThreadIndex() {
    const index = await this.loadThreadIndex()
    index[this.sessionId] = this.threadId
    await writeFile(join(this.sessionRoot, 'threads-index.json'), JSON.stringify(index, null, 2))
  }

  /**
   * Launch the app-server subprocess and complete the handshake + thread setup.
   * handlers: { onEvent(sessionId, event), onStatus({ sessionId, status }) }
   */
  async start(handlers) {
    const t0 = Date.now()
    this.handlers = handlers
    await mkdir(this.sessionRoot, { recursive: true })
    await this.writeModelConfig()

    this.child = spawn(this.options.binary, this.options.args, {
      cwd: this.options.workspaceDir,
      env: {
        ...process.env,
        ...(this.options.envExtra ?? {}),
        CODEX_HOME: this.sessionRoot,
        ...(this.options.apiKey !== undefined ? { [CODEX_API_KEY_ENV]: this.options.apiKey } : {}),
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    this.child.stderr.setEncoding('utf8')
    this.child.stderr.on('data', (chunk) => { this.stderrTail = `${this.stderrTail}${chunk}`.slice(-4000) })
    this.child.once('exit', (code) => {
      this.exitCode = code
      this.phase = 'exited'
      this.turnReject?.(new Error(`codex app-server exited (code=${String(code)}); stderr=${this.stderrTail.slice(-800)}`))
    })

    this.rpc = new CodexRpcClient(this.child.stdout)
    this.rpc.stdin = this.child.stdin
    this.rpc.onNotification((method, params) => this.onNotification(method, params))
    // Server-initiated requests: approval requests are auto-accepted for now
    // (full HITL forwarding is a roadmap item); unknown requests are declined.
    this.rpc.onRequest((message, client) => {
      if (message.method === 'approval/request') client.respond(message.id, { decision: 'accept' })
      else client.respond(message.id, { error: { code: -32601, message: `foreman: unhandled server request ${message.method}` } })
    })

    await this.rpc.request('initialize', {
      clientInfo: { name: 'foreman', version: '0.1.0' },
      capabilities: {},
    })
    this.rpc.notify('initialized', {})

    // Thread resume (persisted mapping present) or fresh start. sandbox and
    // approvalPolicy must be re-sent on resume: 0.149.1 otherwise defaults the
    // resumed thread to a read-only sandbox (verified — thread/resume accepts
    // both fields per the generated JSON schema).
    const index = await this.loadThreadIndex()
    const knownThreadId = index[this.sessionId]
    if (knownThreadId !== undefined) {
      try {
        const result = await this.rpc.request('thread/resume', {
          threadId: knownThreadId,
          cwd: this.options.workspaceDir,
          sandbox: this.options.sandbox,
          approvalPolicy: this.options.approvalPolicy,
        })
        this.threadId = result?.thread?.id ?? knownThreadId
        this.resumed = true
      } catch {
        this.threadId = undefined // stale mapping (thread store not restored): fall through to a fresh thread
      }
    }
    if (this.threadId === undefined) {
      const result = await this.rpc.request('thread/start', {
        cwd: this.options.workspaceDir,
        approvalPolicy: this.options.approvalPolicy,
        sandbox: this.options.sandbox,
      })
      this.threadId = result?.thread?.id
      if (this.threadId === undefined) throw new Error('codex: thread/start returned no thread id')
      await this.saveThreadIndex()
    }

    this.phase = 'running'
    this.timings.bootMs = Date.now() - t0
    return { threadId: this.threadId, resumed: this.resumed === true }
  }

  /** Emit an internal frame through the channel's onEvent handler. */
  emit(type, data) {
    const event = {
      kind: 'session.event',
      sessionId: this.sessionId,
      seq: ++this.seq,
      type,
      time: Date.now(),
      data,
    }
    this.events.push(event)
    this.handlers.onEvent(this.sessionId, event)
    return event
  }

  /** Map verified codex notifications onto internal frames (design doc §4). */
  onNotification(method, params) {
    const item = params.item
    switch (method) {
      case 'item/agentMessage/delta': {
        if (typeof params.delta === 'string' && params.delta.length > 0) {
          this.emit('assistant/chunk', {
            turn: this.turnNumber,
            step: this.stepNumber,
            chunk: { type: 'text-delta', index: 0, text: params.delta },
          })
        }
        return
      }
      case 'item/completed': {
        if (item?.type === 'agentMessage' && typeof item.text === 'string' && item.text.length > 0) {
          this.stepNumber += 1
          this.emit('assistant/message', {
            turn: this.turnNumber,
            step: this.stepNumber,
            message: { content: [{ type: 'text', text: item.text }] },
          })
        } else if (item?.type === 'commandExecution') {
          this.emit('tool/result', { callId: item.id, meta: { diffs: [] } })
        }
        return
      }
      case 'item/started': {
        if (item?.type === 'commandExecution') {
          this.emit('tool/call', {
            name: 'exec_command',
            arguments: { command: item.command },
            callId: item.id,
          })
        }
        return
      }
      case 'turn/completed': {
        const event = this.emit('turn/end', { reason: { kind: turnEndReason(params.turn?.status) } })
        this.turnEndResolve?.(event)
        return
      }
      case 'thread/status/changed': {
        const status = params.status?.type
        if (status !== undefined) this.handlers.onStatus?.({ sessionId: this.sessionId, status })
        return
      }
      default:
        return // configWarning / warning / account/* / remoteControl/* / …: skipped, forward-compatible
    }
  }

  /**
   * Send a task and wait for turn/completed.
   * @returns {Promise<{ reason: { kind: string } }>}
   */
  async prompt(sessionId, text, { timeoutMs = this.options.timeoutMs } = {}) {
    const t0 = Date.now()
    this.turnNumber += 1
    this.stepNumber = 0

    const turnEnd = new Promise((resolve, reject) => {
      this.turnReject = reject
      const timer = setTimeout(() => {
        this.turnEndResolve = undefined
        this.turnReject = undefined
        // Best-effort interrupt, then surface the timeout.
        this.rpc.request('turn/interrupt', {}).catch(() => {})
        reject(new Error(`codex turn timeout after ${String(timeoutMs)}ms; stderr=${this.stderrTail.slice(-800)}`))
      }, timeoutMs)
      this.turnEndResolve = (event) => {
        clearTimeout(timer)
        this.turnEndResolve = undefined
        this.turnReject = undefined
        resolve(event)
      }
    })

    await this.rpc.request('turn/start', {
      threadId: this.threadId,
      input: [{ type: 'text', text }],
    })

    const end = await turnEnd
    this.timings.turnMs = Date.now() - t0
    return { reason: end.data.reason }
  }

  /** Graceful shutdown: close stdin, escalate to SIGTERM/SIGKILL on timeout. */
  async shutdown() {
    if (this.child === undefined) return 0
    const exit = new Promise((resolve) => { this.child.once('exit', (code) => resolve(code ?? 0)) })
    this.child.stdin.end()
    const wait = (ms) => new Promise((resolve) => { setTimeout(() => resolve('timeout'), ms) })
    let code = await Promise.race([exit, wait(15_000)])
    if (code === 'timeout') {
      this.child.kill('SIGTERM')
      code = await Promise.race([exit, wait(10_000)])
    }
    if (code === 'timeout') {
      this.child.kill('SIGKILL')
      code = await exit
    }
    this.phase = 'stopped'
    return code
  }

  /** Hard kill (crash/reclaim simulation): immediate SIGKILL, no cleanup. */
  kill() {
    if (this.child === undefined) return Promise.resolve(0)
    this.phase = 'killed'
    this.child.kill('SIGKILL')
    return new Promise((resolve) => { this.child.once('exit', (code) => resolve(code ?? 0)) })
  }
}
