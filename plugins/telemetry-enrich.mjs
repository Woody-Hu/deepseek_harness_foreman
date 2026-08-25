/**
 * foreman telemetry-enrich — deployment-side telemetry attribute pipeline
 * (a generic, spec-driven engine).
 *
 * Three-party ownership of the generalized mechanism:
 *   - spec (what to record): the config.attributes rule table of this
 *     plugin's entry in the cloud-owned cordis.yml/patch — adding a
 *     monitoring dimension = adding one rule, zero code;
 *   - values (where they come from): foreman injects them via env (the same
 *     non-persistent channel as secrets) — flat variables (from.env) plus a
 *     single JSON run-context object (from.context, fields by dot path);
 *   - engine (how to record): this plugin, written once, independent of
 *     monitoring dimensions.
 *
 * Evaluation semantics:
 *   - Load-time parsing (fail loud, following dsh conventions): env/literal/
 *     context JSON are all read once during apply (env is process-stable);
 *     configuration errors (unknown source, duplicate key, bad when/transform,
 *     invalid context JSON) throw and fail the plugin load.
 *   - Export-time never throws: an exception thrown inside a waterfall would
 *     detain the whole record (the coordinator is fail-closed), so at runtime
 *     everything is a skip — skip when `when` does not match, skip when the
 *     key already exists on the record (injection is additive; dsh's own
 *     session./event. prefixed keys win), skip when the source value is
 *     absent (absent data != configuration error).
 *
 * Mounted on the `session-telemetry/record` waterfall (a documented
 * record-transform extension point whose official use is redaction rules);
 * listeners must delegate to next() and transform its return value, never
 * mutate the input in place. The mechanism is channel-agnostic: the web and
 * stdio compositions mount the same plugin with the same spec.
 */
import { createHash } from 'node:crypto'

export const name = 'foreman-telemetry-enrich'

/** Default env name of the JSON run context (injected via foreman envExtra). */
const CONTEXT_ENV_DEFAULT = 'FOREMAN_RUN_CONTEXT'

/**
 * @param {object} [config] the plugin entry config
 * @param {string} [config.contextEnv] env name of the context JSON (default FOREMAN_RUN_CONTEXT)
 * @param {Array} [config.attributes] rule table: { key, from: {env}|{context}, value,
 *   when: {eventTypes}, transform: {hash} } — exactly one source
 */
export function apply(ctx, config = {}) {
  const rules = parseRules(config)
  const context = readContext(config.contextEnv ?? CONTEXT_ENV_DEFAULT)

  ctx.on('session-telemetry/record', (_record, next) => {
    const out = next()
    const extra = {}
    for (const rule of rules) {
      if (rule.matches !== undefined && !rule.matches(String(out.attributes['event.type'] ?? ''))) continue
      if (out.attributes[rule.key] !== undefined) continue
      const value = rule.get(context)
      if (value === undefined) continue
      extra[rule.key] = rule.hash ? hashOf(value) : value
    }
    if (Object.keys(extra).length === 0) return out
    return { ...out, attributes: { ...out.attributes, ...extra } }
  })
}

/** Parse and validate the rule table at load time; configuration errors throw here (fail loud). */
function parseRules(config) {
  if (config.attributes === undefined) return []
  if (!Array.isArray(config.attributes)) {
    throw new Error('telemetry-enrich: config.attributes must be a list of rules')
  }
  const rules = []
  const seen = new Set()
  for (const raw of config.attributes) {
    const rule = parseRule(raw)
    if (seen.has(rule.key)) throw new Error(`telemetry-enrich: duplicate attribute key ${rule.key}`)
    seen.add(rule.key)
    rules.push(rule)
  }
  return rules
}

/** One rule -> { key, get(context), matches?(eventType), hash }. */
function parseRule(raw) {
  if (typeof raw !== 'object' || raw === null) throw new Error('telemetry-enrich: rule must be a mapping')
  const { key, from, value, when, transform } = raw
  if (typeof key !== 'string' || key.length === 0) throw new Error('telemetry-enrich: rule needs a non-empty string key')
  if (from !== undefined && value !== undefined) {
    throw new Error(`telemetry-enrich: rule ${key} has both from and value — pick one source`)
  }
  let get
  if (from !== undefined) {
    if (typeof from !== 'object' || from === null) throw new Error(`telemetry-enrich: rule ${key} from must be a mapping`)
    const keys = Object.keys(from)
    if (keys.length !== 1) throw new Error(`telemetry-enrich: rule ${key} from must have exactly one of env/context`)
    if (keys[0] === 'env') {
      if (typeof from.env !== 'string' || from.env.length === 0) {
        throw new Error(`telemetry-enrich: rule ${key} from.env must be a non-empty string`)
      }
      const resolved = process.env[from.env] // env is process-stable; read once at load time
      get = () => resolved
    } else if (keys[0] === 'context') {
      if (typeof from.context !== 'string' || from.context.length === 0) {
        throw new Error(`telemetry-enrich: rule ${key} from.context must be a dot path`)
      }
      const path = from.context.split('.')
      get = (context) => pick(context, path)
    } else {
      throw new Error(`telemetry-enrich: rule ${key} unknown source ${keys[0]} (env | context)`)
    }
  } else if (value !== undefined) {
    const literal = value
    get = () => literal
  } else {
    throw new Error(`telemetry-enrich: rule ${key} needs a source: from: {env|context} or value`)
  }
  let matches
  if (when !== undefined) {
    if (typeof when !== 'object' || when === null || !Array.isArray(when.eventTypes)
      || when.eventTypes.length === 0 || !when.eventTypes.every((t) => typeof t === 'string' && t.length > 0)) {
      throw new Error(`telemetry-enrich: rule ${key} when must be { eventTypes: [string, ...] }`)
    }
    const patterns = when.eventTypes // trailing '*' = prefix match ('tool/*'), otherwise exact match
    matches = (type) => patterns.some((p) => p.endsWith('/*') ? type.startsWith(p.slice(0, -1)) : type === p)
  }
  let hash = false
  if (transform !== undefined) {
    const transformKeys = Object.keys(transform)
    if (transformKeys.length !== 1 || transformKeys[0] !== 'hash' || transform.hash !== true) {
      throw new Error(`telemetry-enrich: rule ${key} transform only supports { hash: true }`)
    }
    hash = true
  }
  return { key, get, matches, hash }
}

/** Parse the JSON run context: not injected -> {}; injected but invalid -> throw (self-checkable at load time, fail loud). */
function readContext(envName) {
  const raw = process.env[envName]
  if (raw === undefined) return {}
  try {
    const parsed = JSON.parse(raw)
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error('not an object')
    }
    return parsed
  } catch (error) {
    throw new Error(`telemetry-enrich: env ${envName} is not a JSON object: ${String(error)}`)
  }
}

/** Pick a field by dot path; any missing segment -> undefined (the attribute is skipped at export time). */
function pick(source, path) {
  let current = source
  for (const segment of path) {
    if (typeof current !== 'object' || current === null) return undefined
    current = current[segment]
  }
  return current
}

/** First 16 hex chars of sha256 (for pseudonymization, e.g. user.pseudonym). */
function hashOf(value) {
  return createHash('sha256').update(String(value)).digest('hex').slice(0, 16)
}
