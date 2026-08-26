/**
 * Runner configuration file (ADR-0002): `foreman.config.json`.
 *
 * The file is opt-in (an explicit `configPath` option or the FOREMAN_CONFIG
 * environment variable). Per-key precedence: constructor option > config file
 * > built-in default. Shape is validated and failures are loud — a typo'd
 * protocol or channel selection must never degrade silently.
 *
 * Schema (unknown keys fail loud):
 *   {
 *     "events": {
 *       "protocol": <protocol registry id or alias>,   // ADR-0001
 *       "delivery": "sse" | "bus" | "both",
 *       "model":    <string>,                          // chunk/response model field
 *       "bus":      <createEventBus configuration>
 *     },
 *     "harness": {                                      // ADR-0005 / ADR-0009
 *       "channel": "dsh-sdk" | "dsh-web" | "codex" (+ legacy aliases),
 *       "codex": { binary, args, model, provider, approvalPolicy, sandbox, timeoutMs }
 *     }
 *   }
 */
import { readFile } from 'node:fs/promises'

const KNOWN_TOP_KEYS = new Set(['events', 'harness'])
const KNOWN_EVENTS_KEYS = new Set(['protocol', 'delivery', 'model', 'bus'])
const KNOWN_HARNESS_KEYS = new Set(['channel', 'codex'])
const KNOWN_CODEX_KEYS = new Set(['binary', 'args', 'model', 'provider', 'approvalPolicy', 'sandbox', 'timeoutMs'])
const KNOWN_PROVIDER_KEYS = new Set(['name', 'baseUrl', 'envKey'])
const CODEX_SANDBOX_MODES = new Set(['read-only', 'workspace-write', 'danger-full-access'])

/** Canonical channel ids (ADR-0009). */
export const CHANNELS = ['dsh-sdk', 'dsh-web', 'codex']

/** Legacy channel ids accepted as aliases (deprecated). */
const CHANNEL_ALIASES = { stdio: 'dsh-sdk', web: 'dsh-web' }

/**
 * Resolve a channel selection to its canonical id (ADR-0009).
 * @throws {Error} unknown channel id (with the accepted values listed)
 */
export function resolveChannelId(id) {
  const canonical = CHANNEL_ALIASES[id] ?? id
  if (!CHANNELS.includes(canonical)) {
    throw new Error(`foreman: unknown channel '${String(id)}' (accepted: ${CHANNELS.join(', ')}; legacy aliases: ${Object.keys(CHANNEL_ALIASES).join(', ')})`)
  }
  return canonical
}

/** Resolve the config file path: explicit option > FOREMAN_CONFIG env > none. */
export function resolveConfigPath(options = {}) {
  return options.configPath ?? process.env.FOREMAN_CONFIG ?? undefined
}

/**
 * Load and validate a runner configuration file.
 * @param {string|undefined} path absolute or relative path; undefined = no config
 * @returns {Promise<object>} validated config ({} when no path is given)
 * @throws {Error} unreadable file, invalid JSON, or unknown keys
 */
export async function loadForemanConfig(path) {
  if (path === undefined) return {}
  let text
  try {
    text = await readFile(path, 'utf8')
  } catch (error) {
    throw new Error(`foreman-config: cannot read config file '${path}': ${error.message}`)
  }
  let parsed
  try {
    parsed = JSON.parse(text)
  } catch (error) {
    throw new Error(`foreman-config: invalid JSON in '${path}': ${error.message}`)
  }
  validateConfig(parsed, path)
  return parsed
}

function validateConfig(config, path) {
  if (config === null || typeof config !== 'object' || Array.isArray(config)) {
    throw new Error(`foreman-config: top level of '${path}' must be an object`)
  }
  const unknownTop = Object.keys(config).filter((key) => !KNOWN_TOP_KEYS.has(key))
  if (unknownTop.length > 0) {
    throw new Error(`foreman-config: unknown top-level key(s) in '${path}': ${unknownTop.join(', ')} (known: ${[...KNOWN_TOP_KEYS].join(', ')})`)
  }
  validateEvents(config.events, path)
  validateHarness(config.harness, path)
}

function validateEvents(events, path) {
  if (events === undefined) return
  if (events === null || typeof events !== 'object' || Array.isArray(events)) {
    throw new Error(`foreman-config: 'events' in '${path}' must be an object`)
  }
  const unknownEvents = Object.keys(events).filter((key) => !KNOWN_EVENTS_KEYS.has(key))
  if (unknownEvents.length > 0) {
    throw new Error(`foreman-config: unknown key(s) under 'events' in '${path}': ${unknownEvents.join(', ')} (known: ${[...KNOWN_EVENTS_KEYS].join(', ')})`)
  }
  if (events.protocol !== undefined && typeof events.protocol !== 'string') {
    throw new Error(`foreman-config: events.protocol in '${path}' must be a string`)
  }
  if (events.delivery !== undefined && !['sse', 'bus', 'both'].includes(events.delivery)) {
    throw new Error(`foreman-config: events.delivery in '${path}' must be one of sse | bus | both`)
  }
  if (events.model !== undefined && typeof events.model !== 'string') {
    throw new Error(`foreman-config: events.model in '${path}' must be a string`)
  }
  if (events.bus !== undefined && (events.bus === null || typeof events.bus !== 'object')) {
    throw new Error(`foreman-config: events.bus in '${path}' must be an object`)
  }
}

function validateHarness(harness, path) {
  if (harness === undefined) return
  if (harness === null || typeof harness !== 'object' || Array.isArray(harness)) {
    throw new Error(`foreman-config: 'harness' in '${path}' must be an object`)
  }
  const unknown = Object.keys(harness).filter((key) => !KNOWN_HARNESS_KEYS.has(key))
  if (unknown.length > 0) {
    throw new Error(`foreman-config: unknown key(s) under 'harness' in '${path}': ${unknown.join(', ')} (known: ${[...KNOWN_HARNESS_KEYS].join(', ')})`)
  }
  if (harness.channel !== undefined) {
    if (typeof harness.channel !== 'string') {
      throw new Error(`foreman-config: harness.channel in '${path}' must be a string`)
    }
    resolveChannelId(harness.channel) // throws with the accepted values on typos
  }
  if (harness.codex !== undefined) validateCodex(harness.codex, path)
}

function validateCodex(codex, path) {
  if (codex === null || typeof codex !== 'object' || Array.isArray(codex)) {
    throw new Error(`foreman-config: harness.codex in '${path}' must be an object`)
  }
  const unknown = Object.keys(codex).filter((key) => !KNOWN_CODEX_KEYS.has(key))
  if (unknown.length > 0) {
    throw new Error(`foreman-config: unknown key(s) under 'harness.codex' in '${path}': ${unknown.join(', ')} (known: ${[...KNOWN_CODEX_KEYS].join(', ')})`)
  }
  const stringKeys = ['binary', 'model', 'approvalPolicy']
  for (const key of stringKeys) {
    if (codex[key] !== undefined && typeof codex[key] !== 'string') {
      throw new Error(`foreman-config: harness.codex.${key} in '${path}' must be a string`)
    }
  }
  if (codex.args !== undefined && (!Array.isArray(codex.args) || codex.args.some((arg) => typeof arg !== 'string'))) {
    throw new Error(`foreman-config: harness.codex.args in '${path}' must be an array of strings`)
  }
  if (codex.sandbox !== undefined && !CODEX_SANDBOX_MODES.has(codex.sandbox)) {
    throw new Error(`foreman-config: harness.codex.sandbox in '${path}' must be one of ${[...CODEX_SANDBOX_MODES].join(' | ')}`)
  }
  if (codex.timeoutMs !== undefined && (typeof codex.timeoutMs !== 'number' || codex.timeoutMs <= 0)) {
    throw new Error(`foreman-config: harness.codex.timeoutMs in '${path}' must be a positive number`)
  }
  if (codex.provider !== undefined) {
    if (codex.provider === null || typeof codex.provider !== 'object' || Array.isArray(codex.provider)) {
      throw new Error(`foreman-config: harness.codex.provider in '${path}' must be an object`)
    }
    const unknownProvider = Object.keys(codex.provider).filter((key) => !KNOWN_PROVIDER_KEYS.has(key))
    if (unknownProvider.length > 0) {
      throw new Error(`foreman-config: unknown key(s) under 'harness.codex.provider' in '${path}': ${unknownProvider.join(', ')} (known: ${[...KNOWN_PROVIDER_KEYS].join(', ')})`)
    }
    for (const key of KNOWN_PROVIDER_KEYS) {
      if (codex.provider[key] !== undefined && typeof codex.provider[key] !== 'string') {
        throw new Error(`foreman-config: harness.codex.provider.${key} in '${path}' must be a string`)
      }
    }
  }
}
