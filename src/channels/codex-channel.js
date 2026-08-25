/**
 * Codex Harness app-server channel driver: launches `codex app-server --stdio`
 * and communicates over JSON-RPC 2.0-lite over JSONL (stdin/stdout).
 *
 * Implements the same channel interface as SdkChannel and WebChannel, consumed
 * by foreman.js:
 *   start(handlers) -> handshake result
 *   prompt(sessionId, text, opts) -> { reason }
 *   shutdown() -> exitCode
 *   kill() -> exitCode
 *
 * Protocol lifecycle (ADR-0005, docs/design/codex-channel.md):
 *   initialize → initialized → thread/start → turn/start
 *   → item/* notifications → turn/completed → (next turn or close)
 *
 * Session identity: thread_id = external sessionId (no mapping table).
 */
import { spawn } from 'node:child_process'
import { join } from 'node:path'
import { createInterface } from 'node:readline'

/** Codex app-server boot timeout (cold start with tsx/compiled binary). */
const BOOT_TIMEOUT_MS = 60_000

/** JSON-RPC 2.0-lite client for the Codex app-server protocol. */
class CodexJsonRpcClient {
  constructor(stdout) {
    this.nextId = 1
    this.pending = new Map() // id -> { resolve, reject }
    this.notificationHandlers = []
    this.lineBuffer = ''
    this.reader = createInterface({ input: stdout, crlfDelay: Infinity })
    this.reader.on('line', (line) => {
      if (!line.trim()) return
      let message
      try { message = JSON.parse(line) } catch { return }
      this.dispatch(message)
    })
  }

  dispatch(message) {
    // Response (has id, no method)
    if (message.id !== undefined && message.method === undefined) {
      const waiter = this.pending.get(message.id)
      if (waiter === undefined) return
      this.pending.delete(message.id)
      if (message.error !== undefined) {
        waiter.reject(new Error(`codex JSON-RPC ${message.error.code}: ${message.error.message}`))
      } else {
        waiter.resolve(message.result)
      }
      return
    }
    // Notification (has method, no id) — includes all item/* and turn/* events
    if (message.method !== undefined) {
      for (const handler of this.notificationHandlers) {
        handler(message.method, message.params ?? {})
      }
    }
  }

  request(method, params) {
    const id = this.nextId++
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      this.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`)
    })
  }

  /** Send a notification (no response expected). */
  notify(method, params) {
    this.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method, params })}\n`)
  }

  onNotification(handler) { this.notificationHandlers.push(handler) }

  /** Bind stdin after the subprocess is spawned. */
  bindStdin(stdin) { this.stdin = stdin }
}

/**
 * @param {object} options
 * @param {string} [options.binary] codex binary path (default: 'codex')
 * @param {string[]} [options.args] extra args to the codex app-server (default: ['app-server', '--stdio'])
 * @param {string} options.workspaceDir session workspace (absolute path)
 * @param {string} [options.codexHome] CODEX_HOME directory (persisted thread store; default: join(workspaceDir, '.codex'))
 * @param {string} [options.model] model to use (default: 'gpt-5.1-codex')
 * @param {string} [options.approvalPolicy] approval policy (default: 'never')
 * @param {object} [options.modelEnv] { API_KEY, BASE_URL } — env-injected, never written to disk
 * @param {object} [options.envExtra] extra env entries injected into the child process
 * @param {number} [options.timeoutMs] turn timeout (default: 300000)
 */
export class CodexChannel {
  constructor(options) {
    this.options = {
      binary: 'codex',
      args: ['app-server', '--stdio'],
      model: 'gpt-5.1-codex',
      approvalPolicy: 'never',
      timeoutMs: 300_000,
      ...options,
    }
    this.events = []
    this.timings = {}
    this.threadId = undefined
    this.phase = 'constructed'
  }

  get sessionRoot() {
    // Codex stores thread data in CODEX_HOME/threads/
    return this.options.codexHome ?? join(this.options.workspaceDir, '.codex')
  }

  /**
   * Launch the codex app-server subprocess and complete the handshake.
   * handlers: { onEvent(sessionId, event), onStatus(status) }
   * @returns {Promise<{ threadId: string, ... }>}
   */
  async start(handlers) {
    const t0 = Date.now()
    const { binary, args, workspaceDir, modelEnv, envExtra, codexHome } = this.options
    this.handlers = handlers

    this.child = spawn(binary, args, {
      cwd: workspaceDir,
      env: {
        ...process.env,
        ...(envExtra ?? {}),
        CODEX_HOME: codexHome ?? join(workspaceDir, '.codex'),
        // API credentials injected via env (never written to disk)
        ...(modelEnv?.API_KEY !== undefined ? { CODEX_API_KEY: modelEnv.API_KEY } : {}),
        ...(modelEnv?.BASE_URL !== undefined ? { CODEX_BASE_URL: modelEnv.BASE_URL } : {}),
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    this.child.stderr.on('data', (chunk) => {
      this.stderrTail = `${(this.stderrTail ?? '') + chunk.toString('utf8')}`.slice(-4000)
    })
    this.child.once('exit', (code) => {
      this.exitCode = code
      this.phase = 'exited'
    })

    this.rpc = new CodexJsonRpcClient(this.child.stdout)
    this.rpc.bindStdin(this.child.stdin)

    // Map Codex notifications to internal frames
    this.rpc.onNotification((method, params) => {
      this.onCodexNotification(method, params)
    })

    // Initialize handshake: initialize request → initialized notification
    await this.rpc.request('initialize', {
      clientInfo: { name: 'foreman', version: '0.1.0' },
      capabilities: {},
    })
    this.rpc.notify('initialized', {})

    // Create a thread (or resume an existing one)
    const threadResult = await this.rpc.request('thread/start', {
      model: this.options.model,
      cwd: workspaceDir,
      approvalPolicy: this.options.approvalPolicy,
    })
    this.threadId = threadResult.thread?.id ?? threadResult.id
    this.phase = 'running'
    this.timings.bootMs = Date.now() - t0
    return { threadId: this.threadId }
  }

  /**
   * Map Codex app-server notifications to internal frames.
   * Codex notification methods and their internal frame equivalents:
   */
  onCodexNotification(method, params) {
    const sessionId = this.threadId ?? 'unknown'
    const item = params.item ?? params

    switch (method) {
      // Text delta from an agent message
      case 'item/agentMessage/delta': {
        if (item.delta?.type === 'text' && typeof item.delta.text === 'string') {
          const frame = {
            kind: 'session.event',
            sessionId,
            seq: this.events.length + 1,
            type: 'assistant/chunk',
            time: Date.now(),
            data: {
              turn: this.turnNumber ?? 0,
              step: this.stepNumber ?? 0,
              chunk: { type: 'text-delta', index: 0, text: item.delta.text },
            },
          }
          this.events.push(frame)
          this.handlers.onEvent(sessionId, frame)
        }
        return
      }

      // Agent message completed (no more deltas)
      case 'item/agentMessage/complete':
      case 'item/completed': {
        if (item.type === 'agentMessage') {
          const texts = []
          if (item.content && Array.isArray(item.content)) {
            for (const block of item.content) {
              if (block.type === 'text') texts.push(block.text)
            }
          }
          if (texts.length > 0) {
            const frame = {
              kind: 'session.event',
              sessionId,
              seq: this.events.length + 1,
              type: 'assistant/message',
              time: Date.now(),
              data: {
                turn: this.turnNumber ?? 0,
                step: this.stepNumber ?? 0,
                message: { content: item.content ?? [{ type: 'text', text: texts.join('') }] },
              },
            }
            this.events.push(frame)
            this.handlers.onEvent(sessionId, frame)
          }
        }
        return
      }

      // Tool use started
      case 'item/toolUse/started': {
        const frame = {
          kind: 'session.event',
          sessionId,
          seq: this.events.length + 1,
          type: 'tool/call',
          time: Date.now(),
          data: {
            name: item.name ?? 'unknown',
            arguments: item.input ?? item.arguments ?? {},
            callId: item.id ?? `call_${this.events.length}`,
          },
        }
        this.events.push(frame)
        this.handlers.onEvent(sessionId, frame)
        return
      }

      // Tool result started
      case 'item/toolResult/started': {
        const frame = {
          kind: 'session.event',
          sessionId,
          seq: this.events.length + 1,
          type: 'tool/result',
          time: Date.now(),
          data: {
            callId: item.id ?? `call_${this.events.length}`,
            meta: { diffs: [] },
          },
        }
        this.events.push(frame)
        this.handlers.onEvent(sessionId, frame)
        return
      }

      // Approval requested
      case 'item/approval/requested': {
        const frame = {
          kind: 'approval/requested',
          sessionId,
          approvalId: item.id ?? `appr_${this.events.length}`,
          toolName: item.toolName ?? 'unknown',
          reason: item.reason ?? {},
        }
        this.handlers.onEvent(sessionId, frame)
        return
      }

      // Approval resolved
      case 'item/approval/resolved': {
        const frame = {
          kind: 'approval/resolved',
          sessionId,
          approvalId: item.id,
          outcome: item.outcome ?? 'approved',
        }
        this.handlers.onEvent(sessionId, frame)
        return
      }

      // Turn completed
      case 'turn/completed': {
        const status = params.turn?.status ?? item.status ?? 'completed'
        const frame = {
          kind: 'session.event',
          sessionId,
          seq: this.events.length + 1,
          type: 'turn/end',
          time: Date.now(),
          data: {
            reason: {
              kind: status === 'completed' ? 'completed' : (status === 'cancelled' ? 'cancelled' : 'error'),
            },
          },
        }
        this.events.push(frame)
        this.handlers.onEvent(sessionId, frame)
        this.turnEndResolve?.(frame)
        this.turnNumber = (this.turnNumber ?? 0) + 1
        this.stepNumber = 0
        return
      }

      // Turn interrupted (timeout or cancellation)
      case 'turn/interrupted': {
        const frame = {
          kind: 'session.event',
          sessionId,
          seq: this.events.length + 1,
          type: 'turn/end',
          time: Date.now(),
          data: { reason: { kind: 'cancelled' } },
        }
        this.events.push(frame)
        this.handlers.onEvent(sessionId, frame)
        this.turnEndResolve?.(frame)
        return
      }

      // Initialized notification (handshake completion)
      case 'initialized':
        // no-op: handshake is complete
        return

      default:
        // Unknown notification types are silently skipped (resilience to
        // future Codex app-server protocol additions)
        return
    }
  }

  /**
   * Send a task and wait for turn/completed.
   */
  async prompt(sessionId, text, { timeoutMs = this.options.timeoutMs } = {}) {
    const t0 = Date.now()
    this.stepNumber = (this.stepNumber ?? 0) + 1

    // Wait for turn/completed notification
    const turnEnd = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`codex turn/end timeout; stderr=${this.stderrTail ?? ''}`))
      }, timeoutMs)
      this.turnEndResolve = (frame) => {
        clearTimeout(timer)
        this.turnEndResolve = undefined
        resolve(frame)
      }
    })

    try {
      await this.rpc.request('turn/start', {
        threadId: this.threadId,
        input: [{ type: 'text', text }],
        cwd: this.options.workspaceDir,
      })
    } catch (error) {
      // First-turn race fallback: retry if thread is not ready
      if (String(error).includes('thread-not-found') || String(error).includes('not found')) {
        // Re-create the thread and retry
        const threadResult = await this.rpc.request('thread/start', {
          model: this.options.model,
          cwd: this.options.workspaceDir,
          approvalPolicy: this.options.approvalPolicy,
        })
        this.threadId = threadResult.thread?.id ?? threadResult.id
        // Re-create the prompt
        await this.rpc.request('turn/start', {
          threadId: this.threadId,
          input: [{ type: 'text', text }],
          cwd: this.options.workspaceDir,
        })
      } else {
        throw error
      }
    }

    const end = await turnEnd
    this.timings.turnMs = Date.now() - t0
    return { reason: end.data.reason }
  }

  /**
   * Graceful shutdown: close stdin (triggers app-server exit), wait for exit.
   */
  async shutdown() {
    if (this.child === undefined) return 0
    // Close stdin to signal graceful shutdown to the app-server
    this.child.stdin.end()
    const exitCodePromise = new Promise((resolve) => { this.child.once('exit', (code) => resolve(code ?? 0)) })
    const timeout = (ms) => new Promise((resolve) => { setTimeout(() => { resolve('timeout') }, ms) })
    let code = await Promise.race([exitCodePromise, timeout(15_000)])
    if (code === 'timeout') {
      this.child.kill('SIGTERM')
      code = await Promise.race([exitCodePromise, timeout(10_000)])
    }
    if (code === 'timeout') {
      this.child.kill('SIGKILL')
      code = await exitCodePromise
    }
    this.phase = 'stopped'
    return code
  }

  /**
   * Hard kill (simulating a sandbox crash/reclaim): no cleanup.
   */
  kill() {
    if (this.child === undefined) return Promise.resolve(0)
    this.child.kill('SIGKILL')
    this.phase = 'killed'
    return new Promise((resolve) => { this.child.once('exit', (code) => resolve(code ?? 0)) })
  }
}