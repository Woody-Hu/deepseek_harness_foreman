/**
 * SDK channel driver: launches the dsh JSON-RPC runtime over stdio NDJSON
 * (`dsh-jsonrpc-agent` bin of the @deepseek-ai/dsh-sdk-jsonrpc-demo npm
 * distribution — ADR-0012; no source checkout involved).
 *
 * Implements the rpcId request/response pairing of the SDK protocol. Channel
 * interface (mirrored by web-channel.js, consumed by foreman.js):
 *   start(handlers) -> handshake result; prompt(sessionId, text, opts) -> { reason }
 *   shutdown() -> exitCode; sessionRoot -> session log root directory
 */
import { spawn } from 'node:child_process'
import { dirname } from 'node:path'

/** stdio NDJSON JSON-RPC 2.0 client. */
class JsonRpcClient {
  constructor(stdin, stdout) {
    this.stdin = stdin
    this.nextId = 1
    this.pending = new Map()
    this.notificationHandlers = []
    this.buffer = ''
    stdout.on('data', (chunk) => {
      this.buffer += chunk.toString('utf8')
      const parts = this.buffer.split('\n')
      this.buffer = parts.pop() ?? ''
      for (const line of parts) {
        if (!line.trim()) continue
        let message
        try { message = JSON.parse(line) } catch { continue }
        this.dispatch(message)
      }
    })
  }

  dispatch(message) {
    if (message.id !== undefined && message.method === undefined) {
      const waiter = this.pending.get(message.id)
      if (waiter === undefined) return
      this.pending.delete(message.id)
      if (message.error !== undefined) waiter.reject(new Error(`JSON-RPC ${message.error.code}: ${message.error.message}`))
      else waiter.resolve(message.result)
      return
    }
    if (message.method !== undefined) {
      for (const handler of this.notificationHandlers) handler(message.method, message.params ?? {})
    }
  }

  request(method, params) {
    const id = this.nextId++
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      this.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`)
    })
  }

  onNotification(handler) { this.notificationHandlers.push(handler) }
}

/**
 * @param {object} options
 * @param {string} options.configPath absolute path of cordis.yml (the child's cwd is its directory)
 * @param {string} options.workspaceDir session workspace (absolute; must be identical across runs)
 * @param {string} options.sessionRoot session log root (DSH_SESSION_ROOT)
 * @param {object} options.modelEnv { DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL }
 * @param {object} options.telemetry { mode, otlpUrl }
 * @param {object} [options.envExtra] extra env injected into the dsh child process (tenant/trace ids — same non-persisted channel as secrets)
 * @param {string} [options.provider] initialize parameter (default deepseek-official)
 * @param {string} [options.model] initialize parameter (default deepseek-v4-pro)
 * @param {string} [options.command] harness binary override (default 'dsh-jsonrpc-agent' from PATH — ADR-0012)
 */
export class SdkChannel {
  constructor(options) {
    this.options = options
    this.events = []
    this.timings = {}
  }

  get sessionRoot() { return this.options.sessionRoot }

  /** Launch the dsh child process and complete the initialize handshake. handlers: { onEvent, onStatus } */
  async start(handlers) {
    const t0 = Date.now()
    const { configPath, workspaceDir, sessionRoot, modelEnv, telemetry } = this.options
    this.child = spawn(this.options.command ?? 'dsh-jsonrpc-agent', [configPath], {
      // The child's cwd is the config directory: `./plugins/*.mjs` relative
      // plugin references in cordis.yml resolve against the config location
      // (the runner materializes them there).
      cwd: dirname(configPath),
      // Secrets exist only in the child process environment (memory), never in files
      env: {
        ...process.env,
        ...(this.options.envExtra ?? {}), // additional injection (tenant/trace ids etc., same non-persisted channel)
        DEEPSEEK_API_KEY: modelEnv.DEEPSEEK_API_KEY,
        DEEPSEEK_BASE_URL: modelEnv.DEEPSEEK_BASE_URL,
        DSH_CWD: workspaceDir,
        DSH_SESSION_ROOT: sessionRoot,
        DSH_TELEMETRY_MODE: telemetry.mode,
        DSH_TELEMETRY_OTLP_URL: telemetry.otlpUrl,
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    this.child.stderr.on('data', (chunk) => {
      this.stderrTail = `${(this.stderrTail ?? '') + chunk.toString('utf8')}`.slice(-4000)
    })

    this.rpc = new JsonRpcClient(this.child.stdin, this.child.stdout)
    this.rpc.onNotification((method, params) => {
      if (method === 'session.event') {
        this.events.push(params.event)
        handlers.onEvent(params.sessionId, params.event)
        return
      }
      if (method === 'session.status') handlers.onStatus(params)
    })

    const init = await this.rpc.request('initialize', {
      cwd: workspaceDir,
      provider: this.options.provider ?? 'deepseek-official',
      model: this.options.model ?? 'deepseek-v4-pro',
    })
    this.timings.bootMs = Date.now() - t0
    return init
  }

  /** Send a task and wait for completion (turn/end). */
  async prompt(sessionId, text, { timeoutMs = 120_000 } = {}) {
    const t0 = Date.now()
    const turnEnd = new Promise((resolve, reject) => {
      const timer = setTimeout(() => { reject(new Error(`turn/end timeout; stderr=${this.stderrTail ?? ''}`)) }, timeoutMs)
      const check = () => {
        const end = [...this.events].reverse().find((event) => event.type === 'turn/end')
        if (end !== undefined) { clearTimeout(timer); resolve(end) } else setTimeout(check, 25)
      }
      check()
    })
    const receipt = await this.rpc.request('session/prompt', {
      sessionId,
      contentBlocks: [{ type: 'text', text }],
    })
    const end = await turnEnd
    this.timings.turnMs = Date.now() - t0
    return { receipt, reason: end.data.reason }
  }

  /** Graceful shutdown: shutdown -> wait for exit; SIGTERM after timeout; SIGKILL as the final fallback. */
  async shutdown() {
    // Attach the exit listener before sending shutdown: there is a race between
    // the response arriving and the process exiting — attaching afterwards
    // would miss the exit event forever (once the process is dead, kill is useless too).
    const exitCodePromise = new Promise((resolve) => { this.child.once('exit', (code) => resolve(code)) })
    const timeout = (ms) => new Promise((resolve) => { setTimeout(() => { resolve('timeout') }, ms) })
    try {
      await Promise.race([this.rpc.request('shutdown'), exitCodePromise, timeout(15_000)])
    } catch { /* the request failing means the child already died: continue waiting for exit */ }
    let code = await Promise.race([exitCodePromise, timeout(15_000)])
    if (code === 'timeout') {
      this.child.kill('SIGTERM')
      code = await Promise.race([exitCodePromise, timeout(10_000)])
    }
    if (code === 'timeout') {
      this.child.kill('SIGKILL')
      code = await exitCodePromise
    }
    return code
  }

  /** Hard kill (simulating a sandbox crash/reclaim): no cleanup at all. */
  kill() {
    this.child.kill('SIGKILL')
    return new Promise((resolve) => { this.child.once('exit', (code) => resolve(code)) })
  }
}
