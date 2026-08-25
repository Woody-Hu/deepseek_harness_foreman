/**
 * Runner configuration file (ADR-0002): `foreman.config.json`.
 *
 * The file is opt-in (an explicit `configPath` option or the FOREMAN_CONFIG
 * environment variable). Per-key precedence: constructor option > config file
 * > built-in default. Shape is validated and failures are loud — a typo'd
 * protocol selection must never degrade silently to the native stream.
 *
 * Schema (unknown keys fail loud; the surface starts with `events` and grows
 * along the same rules):
 *   {
 *     "events": {
 *       "protocol": <protocol registry id or alias>,   // ADR-0001
 *       "delivery": "sse" | "bus" | "both",
 *       "model":    <string>,                          // chunk/response model field
 *       "bus":      <createEventBus configuration>
 *     }
 *   }
 */
import { readFile } from 'node:fs/promises'

const KNOWN_TOP_KEYS = new Set(['events'])
const KNOWN_EVENTS_KEYS = new Set(['protocol', 'delivery', 'model', 'bus'])

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
  const events = config.events
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
