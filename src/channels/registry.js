/**
 * Channel registry (ADR-0012): the single source of truth for harness channel
 * ids and their instantiation. `config.js` validates `harness.channel` against
 * the id list here; `Foreman` resolves the factory and calls it with a uniform
 * context — the orchestrator never branches on channel identity.
 *
 * Adding a channel = one registry entry (factory + capabilities) + tests; see
 * ROADMAP "How to extend".
 */
import { join } from 'node:path'
import { SdkChannel } from './sdk-channel.js'
import { WebChannel } from './web-channel.js'
import { CodexChannel } from './codex-channel.js'

/**
 * Build the SdkChannel/WebChannel/CodexChannel constructor options from the
 * uniform channel context.
 *
 * @param {object} ctx orchestrator-owned inputs
 * @param {object} ctx.options Foreman constructor options (repoRoot, modelEnv, envExtra, ...)
 * @param {object} ctx.config loaded runner config file (harness.* sections)
 * @param {object} ctx.handlers { onEvent, onStatus, onApproval, onApprovalResolved }
 * @param {string} ctx.workspaceDir workspace directory
 * @param {string} ctx.sessionRoot session log root for the selected channel
 * @param {string} [ctx.configPath] composition config path (dsh channels)
 * @param {object} ctx.telemetry telemetry wiring for the harness child (trace shipper applied)
 */
const factories = {
  'dsh-sdk': {
    title: 'DeepSeek Harness SDK (JSON-RPC over NDJSON stdio)',
    compositionFile: 'cordis.yml',
    sessionRoot: (workdir) => join(workdir, '.sessions'),
    create: ({ options, configPath, workspaceDir, sessionRoot, telemetry }) =>
      new SdkChannel({
        repoRoot: options.repoRoot,
        configPath,
        workspaceDir,
        sessionRoot,
        modelEnv: options.modelEnv,
        telemetry,
        envExtra: options.envExtra,
        provider: options.provider,
        model: options.model,
      }),
  },
  'dsh-web': {
    title: 'DeepSeek Harness web apiproxy (HTTP + WebSocket)',
    hitl: true,
    compositionFile: 'web-patch.yml',
    sessionRoot: (workdir) => join(workdir, 'dsh-home', 'sessions'),
    create: ({ options, configPath, workspaceDir, telemetry }) =>
      new WebChannel({
        repoRoot: options.repoRoot,
        patchPath: configPath,
        workspaceDir,
        dshHome: join(options.workdir, 'dsh-home'),
        modelEnv: options.modelEnv,
        telemetry,
        envExtra: options.envExtra,
        pluginsDir: options.pluginsDir,
      }),
  },
  'codex': {
    title: 'Codex Harness app-server (JSON-RPC over stdio JSONL)',
    sessionRoot: (workdir) => join(workdir, '.codex'),
    create: ({ options, config, workspaceDir, sessionRoot }) => {
      const harness = config?.harness ?? {}
      const codex = harness.codex ?? {}
      return new CodexChannel({
        workspaceDir,
        codexHome: sessionRoot,
        sessionId: options.sessionId,
        binary: codex.binary,
        args: codex.args,
        model: codex.model,
        approvalPolicy: codex.approvalPolicy,
        sandbox: codex.sandbox,
        timeoutMs: codex.timeoutMs,
        baseUrl: codex.provider?.baseUrl,
        envExtra: options.envExtra,
        ...(options.codex ?? {}), // constructor-level overrides (incl. apiKey — env-injected only)
      })
    },
  },
}

/** Canonical channel ids (ADR-0009). */
export const CHANNELS = Object.keys(factories)

/** Legacy channel ids accepted as aliases (deprecated). */
const CHANNEL_ALIASES = { stdio: 'dsh-sdk', web: 'dsh-web' }

/**
 * Resolve a channel selection to its canonical id (ADR-0009).
 * @throws {Error} unknown channel id (with the accepted values listed)
 */
export function resolveChannelId(id) {
  const canonical = CHANNEL_ALIASES[id] ?? id
  if (factories[canonical] === undefined) {
    throw new Error(`foreman: unknown channel '${String(id)}' (accepted: ${CHANNELS.join(', ')}; legacy aliases: ${Object.keys(CHANNEL_ALIASES).join(', ')})`)
  }
  return canonical
}

/**
 * The registry entry for a canonical id (capabilities + factory). Unknown ids fail loud.
 * Entry fields: { title, hitl?, compositionFile?, sessionRoot(workdir), create(ctx) }.
 */
export function channelEntry(id) {
  const canonical = resolveChannelId(id)
  return { id: canonical, ...factories[canonical] }
}

/**
 * Instantiate the channel for a canonical id (ADR-0012). Unknown ids fail loud.
 * @returns {{ channel: object, hitl: boolean, title: string }}
 */
export function createChannel(id, ctx) {
  const entry = channelEntry(id)
  return { channel: entry.create(ctx), hitl: entry.hitl === true, title: entry.title }
}
