/**
 * Foreman — the sandbox runner orchestrator.
 *
 * Responsibilities:
 *   prepare()  restore composition config and the workspace from "object
 *              storage" (the control plane) by agentId+session; establish the
 *              workspace manifest baseline (the "before" state of the
 *              authoritative change set); restore session logs
 *   start()    launch the harness over the configured channel; secrets are
 *              injected via env only (never written to disk); simultaneously
 *              open the SSE gateway (GET /events) forwarding streaming events
 *              to the cloud (redacted before forwarding); HITL approval
 *              requests of hitl-capable channels are also forwarded through
 *              this gateway (answered via POST /hitl)
 *   prompt()   send a task and wait for turn/end (the completion signal)
 *   shutdown() graceful stop / kill() hard kill (simulates a crash, for
 *              dangling-approval experiments)
 *   collect()  gather this run's products: the final answer, session logs,
 *              the manifest change set, fs tool diffs
 *   publish()  redact + package the workspace and upload it to object storage
 *              + upload session logs/traces + emit message bus events to
 *              reclaim the sandbox
 *
 * Channels are selected via the channel registry (ADR-0009/ADR-0012,
 * channels/registry.js): canonical ids 'dsh-sdk' / 'dsh-web' / 'codex'
 * (legacy aliases 'stdio'/'web' accepted) map to factories with declared
 * capabilities (hitl, composition config, session-log root) — the
 * orchestrator does not branch on channel identity.
 *
 * Session identity: the external session id doubles as the harness session id
 * where the harness accepts caller-provided ids; the codex channel maps
 * sessionId -> codex threadId through a persisted index (codex generates its
 * own thread ids).
 *
 * Trace dual path:
 *   A. the harness's native session-telemetry-otel -> OTLP collector (wired
 *      through DSH_TELEMETRY_* environment variables)
 *   B. foreman forwarding from the event stream (SSE gateway + trace buffer,
 *      redactJson applied before forwarding)
 *
 * Workspace incremental sync (checkpoints configuration): restore/upload uses
 * a "full first pack + incremental pack chain"; skip-list style tiered
 * retention balances pack count against pack size (level = v2(turn), each
 * level keeps only the most recent few). Independent transfers overlap at
 * session boundaries only (ADR-0011): prepare downloads and publish uploads
 * run concurrently; checkpoint packs build/upload inside publish()'s single
 * authoritative retention pass.
 */
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { redactJson } from './core/redact.js'
import {
  archiveDirectory, changesFromSessionEvents, diffManifests, extractArchive, fileManifest, packageWorkspace,
} from './core/workspace.js'
import { deleteArtifact, downloadArtifact, publishBusEvent, uploadArtifact } from './control-plane.js'
import { channelEntry, createChannel } from './channels/registry.js'
import { createEventFormatter } from './events/formats.js'
import { SseGateway } from './events/gateway.js'
import { loadForemanConfig, resolveConfigPath } from './config.js'
import { TraceShipper } from './observability/trace-shipper.js'
import { createSnapshotSink } from './storage/snapshot-sink.js'
import { createEventBus } from './events/event-bus.js'
import { GitWorkspace } from './core/git-workspace.js'
import { CheckpointKeeper, INDEX_KEY, applyChangePack, buildChangePack, extractChangePack, packKey } from './core/checkpoint.js'

export { SseGateway }

/**
 * @param {object} options
 * @param {string} options.repoRoot dsh repository root (cwd of the tsx source launch)
 * @param {string} options.workdir isolated directory for this run (workspace/sessions/artifacts live under it)
 * @param {'dsh-sdk'|'dsh-web'|'codex'} [options.channel] harness channel, canonical ids per
 *   ADR-0009 (legacy aliases 'stdio'/'web' accepted); default 'dsh-sdk'; also
 *   settable via the config file's harness.channel (constructor wins)
 * @param {string} options.agentId object storage bucket (agent dimension)
 * @param {string} options.sessionId external session id (caller-defined; reuse across runs = session resume)
 * @param {object} options.modelEnv { DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL } — env-injected only, never written to disk
 * @param {object} options.controlPlane { baseUrl } control-plane handle
 * @param {object} options.telemetry { mode, otlpUrl } trace path A wiring
 * @param {string} [options.pluginsDir] runner-bundled adapter plugin directory (materialized next to the config; dsh-sdk channel)
 * @param {string[]} [options.secretValues] known secret values (for masking)
 * @param {object} [options.envExtra] extra env entries injected into the harness child process
 *   (e.g. tenant/trace ids — injected through the same non-persistent channel
 *   as secrets; expanded into the spawn env by the driver layer)
 * @param {object} [options.codex] codex channel options (constructor level; override the config
 *   file's harness.codex): { binary?, args?, model?, baseUrl?, apiKey?, approvalPolicy?,
 *   sandbox?, timeoutMs? } — baseUrl points the harness at a Responses-API endpoint;
 *   apiKey is injected via env only, never written to disk
 * @param {object} [options.traceShipper] async trace shipping:
 *   { upstreamUrl, headers?, headersEnv?, maxQueue?, retries?, retryBaseMs? } —
 *   when enabled, dsh's OTLP points at the shipper's local receiver and a
 *   background queue forwards to cloud monitoring (isolating the main flow)
 * @param {object} [options.snapshot] workspace snapshot storage:
 *   createSnapshotSink configuration { kind:'local'|'object-store', ... } —
 *   credentials resolved dynamically from env (per call)
 * @param {object} [options.events] outbound event stream: { protocol?, delivery:'sse'|'bus'|'both',
 *   model?, bus? = createEventBus configuration }. `protocol` selects the outbound SSE
 *   protocol by registry id/alias (native | openai-chat | openai-responses/codex, ADR-0001);
 *   the legacy `format` key is honored as an alias with lower precedence.
 *   Per-key precedence (ADR-0002): constructor option > config file > default.
 * @param {string} [options.configPath] runner configuration file (foreman.config.json) —
 *   also settable via the FOREMAN_CONFIG env var; protocol/delivery/model/bus
 *   defaults come from it when the constructor does not override them
 * @param {object} [options.git] local workspace git: { enabled, secretPatterns?, branch?,
 *   identity? = { name, email } } — secret values default to secretValues;
 *   baseline/turn commits + secret interception; branch/identity default to
 *   'main' / 'foreman <foreman@localhost>'
 * @param {object} [options.checkpoints] workspace incremental sync chain (requires git.enabled):
 *   { recentKeep?, perLevel?, rebaseAfter? } — restore/upload uses "full
 *   first pack + incremental packs"; skip-list style tiered retention
 *   balances pack count against size; rebaseAfter bounds the restore chain
 *   length (0 = never rebase); packs build/upload inside publish()'s
 *   retention pass (ADR-0011)
 */
export class Foreman {
  #entry = undefined // resolved channel registry entry (set by #resolveChannel)

  constructor(options) {
    this.options = options // channel stays raw: canonical resolution needs the (async) config load
    if (this.options.checkpoints !== undefined && this.options.git?.enabled !== true) {
      throw new Error('foreman: checkpoints require git.enabled')
    }
    this.events = []
    this.traceFrames = []
    this.phase = 'constructed'
    this.timings = {}
    this.pendingApprovals = new Map() // approvalId -> { rpcId, frame } (hitl-capable channels)
    this.external = {} // results/stats of external wiring like shipper/bus/git/checkpoints (merged into publish result)
    this.promptTurns = 0 // turns completed (and committed) this run
    this.lastTurnCommit = undefined // the last per-turn commit result (reported at collect)
    this.ckptSyncRecords = [] // per-pack publish sync timings { turn, from, ms, phase: 'publish' } (observability)
  }

  get workspaceDir() { return join(this.options.workdir, 'workspace') }
  /**
   * Session log root (per-channel, from the channel registry): dsh-sdk =
   * workdir/.sessions; dsh-web = DSH_HOME/sessions; codex = workdir/.codex
   * (CODEX_HOME — thread store + sessionId->threadId index).
   */
  get sessionRoot() {
    return this.channelEntry().sessionRoot(this.options.workdir)
  }
  get artifactsDir() { return join(this.options.workdir, 'artifacts') }

  /** Load + cache the runner configuration file (no-op when absent). */
  async #ensureConfig() {
    if (this.config === undefined) this.config = await loadForemanConfig(resolveConfigPath(this.options))
    return this.config
  }

  /**
   * The registry entry for the selected channel (ADR-0012). Requires
   * #resolveChannel() to have run (prepare/start call it first).
   */
  channelEntry() {
    if (this.#entry === undefined) throw new Error('foreman: channel not resolved yet (call prepare()/start() first)')
    return this.#entry
  }

  /**
   * Resolve the harness channel (ADR-0009/ADR-0012): constructor option >
   * config file (harness.channel) > 'dsh-sdk'; legacy aliases map to canonical
   * ids and unknown ids fail loud. Cached (idempotent).
   */
  async #resolveChannel() {
    const config = await this.#ensureConfig()
    if (this.#entry === undefined) {
      this.#entry = channelEntry(this.options.channel ?? config.harness?.channel ?? 'dsh-sdk')
      this.channelId = this.#entry.id
    }
    return this.#entry
  }

  /** Restore composition config and the workspace from object storage; establish the manifest baseline. */
  async prepare() {
    const t0 = Date.now()
    await mkdir(this.options.workdir, { recursive: true })
    const entry = await this.#resolveChannel()
    const { agentId, sessionId, controlPlane } = this.options

    // Composition config (channels that declare one — the codex channel needs
    // none; its model wiring lives in CODEX_HOME/config.toml).
    if (entry.compositionFile !== undefined) {
      const configFile = entry.compositionFile
      const configBuffer = await downloadArtifact(controlPlane, agentId, `${sessionId}/${configFile}`)
      this.configPath = join(this.options.workdir, configFile)
      await writeFile(this.configPath, configBuffer)

      // Runner-bundled adapter plugins (the cloud owns the composition config,
      // the runner binary owns its own adapter layer):
      //   dsh-sdk channel: materialized next to the config (the demo bin's
      //                    loader root = the config file's directory; cordis.yml
      //                    references ./plugins/* relative paths)
      //   dsh-web channel: materialized by the web driver to
      //                    $DSH_HOME/profiles/web/ (the loader root of the
      //                    profile launch face is anchored at the profile
      //                    directory, see web-channel.start)
      if (this.channelId === 'dsh-sdk' && this.options.pluginsDir !== undefined) {
        await cp(this.options.pluginsDir, join(this.options.workdir, 'plugins'), { recursive: true })
      }
    }

    // Session log download starts immediately and overlaps the workspace
    // restore (independent objects — ADR-0011). Restore: extract into
    // sessionRoot; the harness (dsh: sessionId+cwd; codex: the threads-index
    // mapping restored with it) continues the history — cross-sandbox session
    // resume. Note: resume requires the workspace's absolute path to match
    // the previous round (cloud sandboxes use fixed mount points).
    const sessionsDownload = downloadArtifact(controlPlane, agentId, `${sessionId}/sessions.tar.gz`)
      .then((buffer) => ({ buffer }))
      .catch(() => ({ buffer: null })) // first round: no session logs in object storage yet

    // Workspace "before" state (checkpoint mode prefers the incremental pack
    // chain; the seed only provides the first-round cold-start state):
    // a checkpoints.json index present -> the workspace is rebuilt from the
    // "full first pack + incremental packs" (the seed is already folded into
    // the first pack); no index -> first round, extract the seed (same as the
    // plain full-package path)
    let ckptIndex = null
    if (this.options.checkpoints !== undefined) {
      try {
        ckptIndex = JSON.parse(
          (await downloadArtifact(controlPlane, agentId, `${sessionId}/${INDEX_KEY}`)).toString('utf8'),
        )
      } catch { /* no index = first round; fall back to the seed state */ }
    }
    if (ckptIndex !== null) {
      await mkdir(this.workspaceDir, { recursive: true })
    } else {
      const workspaceBundle = join(this.options.workdir, 'seed-workspace.tar.gz')
      const seedBuffer = await downloadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`)
      await writeFile(workspaceBundle, seedBuffer)
      await extractArchive(workspaceBundle, this.workspaceDir)
    }

    const { buffer: sessionsBuffer } = await sessionsDownload
    if (sessionsBuffer !== null) {
      const sessionsBundle = join(this.options.workdir, 'seed-sessions.tar.gz')
      await writeFile(sessionsBundle, sessionsBuffer)
      await extractArchive(sessionsBundle, this.sessionRoot)
    }

    this.baselineManifest = await fileManifest(this.workspaceDir)

    // Local workspace git: the restored workspace gets a baseline commit (the
    // "before" state); turn commits (per prompt when checkpoints are enabled,
    // otherwise at collect()) produce the authoritative change set (git diff);
    // secret files are intercepted before committing (kept out of history and
    // out of the .git upload). Checkpoint mode: restore = replay the
    // incremental pack chain (one commit per pack, replaying git history), and
    // this round's change baseline = the restore end state (the last commit on
    // the chain)
    if (this.options.git?.enabled === true) {
      this.git = new GitWorkspace({
        cwd: this.workspaceDir,
        secretValues: this.options.secretValues ?? [],
        secretPatterns: this.options.git.secretPatterns,
        branch: this.options.git.branch,
        identity: this.options.git.identity,
      })
      await this.git.ensureRepo()
      if (ckptIndex !== null) {
        this.ckpt = {
          keeper: new CheckpointKeeper(this.options.checkpoints),
          oids: new Map(),
          turn: 0,
          index: ckptIndex,
        }
        const restored = []
        // Packs download concurrently (independent objects — ADR-0011) and
        // apply sequentially (the git replay chain is ordered)
        const packBuffers = await Promise.all(
          ckptIndex.packs.map((pack) => downloadArtifact(controlPlane, agentId, `${sessionId}/${pack.object}`)),
        )
        for (const [position, pack] of ckptIndex.packs.entries()) {
          const archiveFile = join(this.options.workdir, `restore-${pack.turn}.tar.gz`)
          await writeFile(archiveFile, packBuffers[position])
          const packDir = join(this.options.workdir, `restore-${pack.turn}`)
          await extractChangePack(archiveFile, packDir)
          const applied = await applyChangePack(this.git, packDir, `foreman: checkpoint ${pack.turn} (replayed)`)
          const oid = applied.oid ?? await this.git.headOid() // an empty change pack lands on the current HEAD
          this.ckpt.oids.set(pack.turn, oid === '' ? undefined : oid)
          this.ckpt.turn = pack.turn
          restored.push(pack.turn)
        }
        this.git.baselineOid = this.ckpt.oids.get(this.ckpt.turn)
        this.external.gitBaseline = {
          restoredCheckpoints: restored,
          restoredToTurn: this.ckpt.turn,
        }
      } else {
        const baseline = await this.git.commitBaseline()
        this.external.gitBaseline = { oid: baseline.oid, files: baseline.files.length, violations: baseline.violations }
        if (this.options.checkpoints !== undefined) {
          this.ckpt = {
            keeper: new CheckpointKeeper(this.options.checkpoints),
            oids: new Map([[0, baseline.oid]]),
            turn: 0,
            index: { format: 1, rebasedAt: 0, packs: [] },
          }
        }
      }
    }
    this.phase = 'prepared'
    this.timings.prepareMs = Date.now() - t0
  }

  /** Launch the harness over the channel + gateway (format/delivery) + trace shipper + handshake/readiness. */
  async start() {
    const t0 = Date.now()
    const { telemetry } = this.options

    // Trace shipper: dsh's OTLP is repointed at the local receiver; cloud
    // forwarding is isolated asynchronously.
    if (this.options.traceShipper !== undefined) {
      this.shipper = await new TraceShipper(this.options.traceShipper).start()
      this.telemetryForChannel = { ...telemetry, otlpUrl: this.shipper.endpoint }
    } else {
      this.telemetryForChannel = telemetry
    }

    // Outbound event stream: formatter + bus + delivery mode. Protocol
    // selection precedence (ADR-0002): constructor option > runner config
    // file (configPath / FOREMAN_CONFIG) > built-in default 'native'.
    await this.#resolveChannel()
    const events = { ...this.config.events, ...this.options.events }
    if (events.bus !== undefined) {
      this.bus = createEventBus(events.bus)
      await this.bus.start?.() // the http bus has a background delivery loop; the memory bus has no start
    }
    this.gateway = new SseGateway({
      formatter: createEventFormatter(events.protocol ?? events.format ?? 'native', { model: events.model }),
      bus: this.bus,
      delivery: events.delivery ?? 'sse',
    })
    this.ssePort = await this.gateway.listen()

    const onEvent = (sessionId, event) => {
      this.events.push(event)
      const frame = {
        kind: 'session.event',
        sessionId,
        seq: event.seq,
        type: event.type,
        time: event.time,
        data: redactJson(event.data, this.options.secretValues ?? []), // path B: redact before forwarding
      }
      this.traceFrames.push(frame)
      this.gateway.publish(frame, { phase: this.phase, lastSeq: event.seq })
    }
    const onStatus = (status) => {
      const frame = { kind: 'session.status', sessionId: status.sessionId, status: status.status }
      this.traceFrames.push(frame)
      this.gateway.publish(frame)
    }

    // Channel wiring through the registry (ADR-0012): the entry declares its
    // capabilities; the orchestrator wires approval handling generically for
    // hitl-capable channels instead of branching on channel identity.
    const { hitl } = this.channelEntry()
    const handlers = { onEvent, onStatus }
    if (hitl === true) {
      handlers.onApproval = (requested) => {
        this.pendingApprovals.set(requested.approvalId, requested)
        this.gateway.publish({
          kind: 'approval/requested',
          sessionId: requested.sessionId,
          approvalId: requested.approvalId,
          toolName: requested.toolName,
          reason: redactJson(requested.reason, this.options.secretValues ?? []),
        })
      }
      handlers.onApprovalResolved = (resolved) => {
        this.pendingApprovals.delete(resolved.approvalId)
        this.gateway.publish({
          kind: 'approval/resolved',
          sessionId: resolved.sessionId,
          approvalId: resolved.approvalId,
          outcome: resolved.outcome,
        })
      }
    }
    const { channel } = createChannel(this.channelId, {
      options: this.options,
      config: this.config,
      handlers,
      workspaceDir: this.workspaceDir,
      sessionRoot: this.sessionRoot,
      configPath: this.configPath,
      telemetry: this.telemetryForChannel,
    })
    this.channel = channel
    const init = await this.channel.start(handlers)
    if (hitl === true) {
      // External systems answer through the gateway's POST /hitl:
      // {sessionId, approvalId, outcome} -> the channel's respondApproval
      this.gateway.setHitlHandler(async ({ approvalId, outcome }) => {
        const pending = this.pendingApprovals.get(approvalId)
        if (pending === undefined) return { accepted: false, reason: 'not-pending' }
        return await this.channel.respondApproval(pending.rpcId, {
          sessionId: pending.sessionId,
          approvalId,
          outcome,
        })
      })
      this.webInfo = init
    }
    this.phase = 'running'
    this.timings.bootMs = this.channel.timings.bootMs
    this.gateway.publish({
      kind: 'foreman.phase', phase: 'running', sessionId: this.options.sessionId,
      channel: this.channelId,
      ...(init?.url !== undefined ? { dshUrl: init.url } : {}),
      ...(init?.threadId !== undefined ? { threadId: init.threadId, resumed: init.resumed === true } : {}),
    })
    return init
  }

  /**
   * Send a task and wait for completion (turn/end). With checkpoints enabled
   * the turn is committed immediately after completion (per-turn pack
   * granularity); pack build/upload happens in publish()'s retention pass
   * (ADR-0011).
   */
  async prompt(text, { timeoutMs = 120_000 } = {}) {
    const t0 = Date.now()
    const result = await this.channel.prompt(this.options.sessionId, text, { timeoutMs })
    this.lastTurnEndReason = result.reason
    this.timings.turnMs = Date.now() - t0
    this.promptTurns += 1
    if (this.ckpt !== undefined) {
      const t1 = Date.now()
      this.ckpt.turn += 1
      const turn = await this.git.commitTurn(`#${this.ckpt.turn}`)
      const oid = turn.oid ?? await this.git.headOid()
      this.ckpt.oids.set(this.ckpt.turn, oid === '' ? undefined : oid)
      this.lastTurnCommit = turn
      this.timings.turnCommitMs = Date.now() - t1
    }
    return result
  }

  /** Graceful shutdown (each channel implements its own: quit request/signal -> timeout fallback; flush external wiring when publish did not run). */
  async shutdown() {
    this.exitCode = await this.channel.shutdown()
    this.phase = 'stopped'
    if (this.shipper !== undefined && this.shipperStats === undefined) {
      this.shipperStats = await this.shipper.stop({ flushMs: 5000 })
    }
    if (this.bus !== undefined && this.busStats === undefined) {
      this.busStats = await this.bus.stop({ flushMs: 5000 })
    }
    await this.gateway.close()
    return this.exitCode
  }

  /** Hard kill (simulating a sandbox crash/reclaim): no cleanup; pending approvals stay dangling in the log. */
  async kill() {
    this.channel.abandonPendingTurn?.() // clear the pending turn's timeout timer (crash experiment)
    this.exitCode = await this.channel.kill()
    this.phase = 'killed'
    await this.gateway.close()
    return this.exitCode
  }

  /** Collect products: the final answer / session logs / manifest change set / fs diffs. */
  async collect() {
    const assistants = this.events.filter((event) => event.type === 'assistant/message')
    const final = assistants[assistants.length - 1]
    const textBlocks = (final?.data.message.content ?? []).filter((block) => block.type === 'text')
    this.finalAnswer = textBlocks.map((block) => block.text).join('\n')

    const after = await fileManifest(this.workspaceDir)
    this.manifestDiff = diffManifests(this.baselineManifest, after)
    this.fsChanges = changesFromSessionEvents(this.events)
    this.sessionLogFiles = []
    try {
      const walk = async (dir, prefix) => {
        for (const entry of await readdir(dir, { withFileTypes: true })) {
          const rel = prefix === '' ? entry.name : `${prefix}/${entry.name}`
          if (entry.isDirectory()) await walk(join(dir, entry.name), rel)
          else if (entry.name.endsWith('.jsonl') || entry.name.endsWith('.jsonl.zstd')) this.sessionLogFiles.push(rel)
        }
      }
      await walk(this.sessionRoot, '')
    } catch { /* a missing session root counts as no logs */ }

    // Git closure: this round's commit (after secret interception) + the
    // authoritative change set since the baseline + uncommitted residue.
    // Checkpoint mode: turns were already committed per prompt; the
    // collect-time commit remains only for runs without a completed turn
    // (zero-prompt runs and non-checkpoint flows).
    if (this.git !== undefined) {
      let turn = this.lastTurnCommit
      if (turn === undefined) {
        if (this.ckpt !== undefined) this.ckpt.turn += 1
        turn = await this.git.commitTurn(this.ckpt !== undefined ? `#${this.ckpt.turn}` : this.options.sessionId)
        if (this.ckpt !== undefined) {
          const oid = turn.oid ?? await this.git.headOid()
          this.ckpt.oids.set(this.ckpt.turn, oid === '' ? undefined : oid)
        }
      }
      this.gitInfo = {
        baselineOid: this.git.baselineOid,
        turnCommit: turn.committed ? { oid: turn.oid, files: turn.files } : undefined,
        changedSinceBaseline: await this.git.changedSinceBaseline(),
        uncommitted: await this.git.uncommitted(),
        violations: this.git.violations,
      }
    }
    this.phase = 'collected'
    return {
      finalAnswer: this.finalAnswer,
      manifestDiff: this.manifestDiff,
      fsChanges: this.fsChanges,
      sessionLogFiles: this.sessionLogFiles,
      git: this.gitInfo,
    }
  }

  /**
   * Redact + package + upload to storage (control plane + optional snapshot
   * sink) + message bus events (including sandbox reclaim). Before the
   * reclaim event is emitted, asynchronous external wiring (trace shipper /
   * event bus) is flushed — the graceful path guarantees every produced trace
   * and stream payload has been delivered before the system reclaims the
   * sandbox.
   */
  async publish() {
    const t0 = Date.now()
    const { agentId, sessionId, controlPlane, secretValues = [] } = this.options
    await rm(this.artifactsDir, { recursive: true, force: true })
    await mkdir(this.artifactsDir, { recursive: true })
    const workspaceArchive = join(this.artifactsDir, 'workspace.tar.gz')
    const sessionsArchive = join(this.artifactsDir, 'sessions.tar.gz')
    const tracePath = join(this.artifactsDir, 'trace.jsonl')
    // Packaging runs concurrently (independent sources — ADR-0011). The
    // session archive excludes harness-owned transient scratch: the harness
    // process is still alive at publish time and codex keeps mutating
    // CODEX_HOME/.tmp (in-flight plugin clones), which would fail the archive
    // with "file changed as we read it" — .tmp is regenerated on demand and
    // never carries durable session state (ADR-0005).
    const [packaged] = await Promise.all([
      packageWorkspace(this.workspaceDir, workspaceArchive, { secretValues }),
      archiveDirectory(this.sessionRoot, sessionsArchive, { exclude: ['./.tmp'] }),
      writeFile(tracePath, this.traceFrames.map((frame) => JSON.stringify(frame)).join('\n') + '\n'),
    ])

    const result = {
      agentId,
      sessionId,
      status: this.lastTurnEndReason?.kind === 'completed' ? 'ok' : 'error',
      turnEndReason: this.lastTurnEndReason,
      finalAnswer: this.finalAnswer,
      changedFiles: this.manifestDiff,
      fsDiffs: this.fsChanges.diffs,
      toolCalls: this.fsChanges.toolCalls.map((call) => call.name),
      eventCount: this.events.length,
      timings: this.timings,
      exitCode: this.exitCode,
      git: this.gitInfo, // baseline/turn commit oids, authoritative change set, secret interception violations
    }
    await writeFile(join(this.artifactsDir, 'result.json'), JSON.stringify(result, null, 2))

    // Checkpoint chain: retention policy -> incremental pack upload/rebuild ->
    // expired pack deletion -> index update (the single authoritative pass,
    // ADR-0011)
    if (this.ckpt !== undefined) {
      this.external.checkpoints = await this.syncCheckpoints()
      result.checkpoints = this.external.checkpoints
    }

    // Flush asynchronous external wiring (delivery guarantee before reclaim);
    // stats merge into result for cloud-side observability
    if (this.shipper !== undefined && this.shipperStats === undefined) {
      this.shipperStats = await this.shipper.stop({ flushMs: 5000 })
    }
    if (this.bus !== undefined && this.busStats === undefined) {
      this.busStats = await this.bus.stop({ flushMs: 5000 })
    }
    result.traceShipper = this.shipperStats
    result.eventBus = this.busStats
    await writeFile(join(this.artifactsDir, 'result.json'), JSON.stringify(result, null, 2))

    // The artifact batch uploads concurrently (independent objects —
    // ADR-0011)
    const uploads = [
      ['result.json', await readFile(join(this.artifactsDir, 'result.json'))],
      ['workspace.tar.gz', await readFile(workspaceArchive)],
      ['sessions.tar.gz', await readFile(sessionsArchive)],
      ['trace.jsonl', await readFile(tracePath)],
    ]
    await Promise.all(uploads.map(async ([name, buffer]) => {
      await uploadArtifact(controlPlane, agentId, `${sessionId}/${name}`, buffer)
    }))
    // Snapshot sink: when configured, the same batch of artifacts is sent
    // through the storage abstraction (credentials resolved dynamically from
    // env on each call)
    if (this.options.snapshot !== undefined) {
      const sink = createSnapshotSink(this.options.snapshot)
      this.external.snapshotUploads = await Promise.all(uploads.map(async ([name, buffer]) => {
        const { url } = await sink.put(`${sessionId}/${name}`, buffer)
        return { key: `${sessionId}/${name}`, url }
      }))
    }

    await publishBusEvent(controlPlane, {
      type: 'run.completed',
      agentId,
      sessionId,
      status: result.status,
      changedFileCount: result.changedFiles.added.length + result.changedFiles.modified.length + result.changedFiles.removed.length,
    })
    // Let the system reclaim the sandbox: workspace and session are archived
    // and asynchronous wiring is flushed — no unsaved state remains inside
    await publishBusEvent(controlPlane, {
      type: 'sandbox.reclaim-requested',
      agentId,
      sessionId,
      artifacts: uploads.map(([name]) => `${sessionId}/${name}`),
    })
    this.phase = 'published'
    this.timings.publishMs = Date.now() - t0
    return { ...packaged, result, external: this.external }
  }

  /**
   * Checkpoint chain sync (called inside publish):
   *   1. the skip-list retention policy computes the desired pack list (this
   *      round's new packs + merged packs for drifted anchors)
   *   2. diff against the current index: new turns are uploaded, packs whose
   *      anchor drifted are rebuilt from local git history under the same
   *      name, dropped turns have their objects deleted
   *   3. rebaseAfter triggers a rebaseline: the current tree is packed as a
   *      from=null full pack resetting the chain head (bounding the restore chain)
   *   4. the checkpoints.json index is written back to object storage
   *      (restore replays packs in order)
   * @returns {Promise<{turn: number, rebasedAt: number, kept: number, uploaded: number, rebuilt: number, deleted: number, packs: Array<{turn: number, from: number|null, object: string}>}>}
   */
  async syncCheckpoints() {
    const { agentId, sessionId, controlPlane } = this.options
    const ckpt = this.ckpt
    const stats = { kept: 0, uploaded: 0, rebuilt: 0, deleted: 0 }
    const currentTurn = ckpt.turn
    const index = ckpt.index
    const rebasedAtBefore = index.rebasedAt ?? 0
    const rebaseAfter = this.options.checkpoints?.rebaseAfter ?? 0
    let rebasedAt = rebasedAtBefore
    let desired
    if (currentTurn === 0) {
      // no completed turn this run: the chain state is exactly what was
      // restored — nothing to plan, nothing to upload, nothing to drop
      return { turn: 0, rebasedAt: rebasedAtBefore, ...stats, packs: index.packs }
    }
    if (rebaseAfter > 0 && currentTurn - rebasedAtBefore >= rebaseAfter) {
      rebasedAt = currentTurn
      desired = [{ turn: currentTurn, from: null }]
    } else {
      desired = ckpt.keeper.planPacks(currentTurn - rebasedAtBefore)
        .map(({ turn, from }) => ({
          turn: rebasedAtBefore + turn,
          from: from === null
            ? (rebasedAtBefore > 0 ? rebasedAtBefore : null) // relative 0 = chain head (the rebase pack, or the empty tree)
            : rebasedAtBefore + from,
        }))
      // The chain-head pack (the full pack produced by a rebase) must be kept
      // with the chain — it is the starting point of a restore
      if (rebasedAtBefore > 0 && !desired.some((entry) => entry.turn === rebasedAtBefore)) {
        desired = [{ turn: rebasedAtBefore, from: null }, ...desired]
      }
    }
    const desiredTurns = new Set(desired.map((entry) => entry.turn))
    for (const entry of desired) {
      const existing = index.packs.find((pack) => pack.turn === entry.turn)
      if (existing !== undefined && existing.from === entry.from) { stats.kept += 1; continue }
      const toRef = ckpt.oids.get(entry.turn)
      const fromRef = entry.from === null ? null : ckpt.oids.get(entry.from)
      if (toRef === undefined) throw new Error(`foreman: checkpoint ${String(entry.turn)} commit missing`)
      if (entry.from !== null && fromRef === undefined) {
        throw new Error(`foreman: checkpoint ${String(entry.from)} commit missing`)
      }
      const packStartedAt = Date.now()
      const packPath = join(this.artifactsDir, packKey(entry.turn))
      await buildChangePack(this.git, {
        fromRef, toRef, fromTurn: entry.from, toTurn: entry.turn, outPath: packPath,
      })
      await uploadArtifact(controlPlane, agentId, `${sessionId}/${packKey(entry.turn)}`, await readFile(packPath))
      this.ckptSyncRecords.push({ turn: entry.turn, from: entry.from, ms: Date.now() - packStartedAt, phase: 'publish' })
      stats[existing === undefined ? 'uploaded' : 'rebuilt'] += 1
    }
    for (const pack of index.packs) {
      if (!desiredTurns.has(pack.turn)) {
        await deleteArtifact(controlPlane, agentId, `${sessionId}/${pack.object}`)
        stats.deleted += 1
      }
    }
    ckpt.index = {
      format: 1,
      rebasedAt,
      packs: desired.map((entry) => ({ turn: entry.turn, from: entry.from, object: packKey(entry.turn) })),
    }
    await uploadArtifact(
      controlPlane, agentId, `${sessionId}/${INDEX_KEY}`,
      Buffer.from(JSON.stringify(ckpt.index, null, 2)),
    )
    return { turn: currentTurn, rebasedAt, ...stats, packs: ckpt.index.packs }
  }
}

/** Convenience: create an isolated working directory for one run. */
export async function makeWorkdir(label) {
  return await mkdtemp(join(tmpdir(), `foreman-${label}-`))
}
