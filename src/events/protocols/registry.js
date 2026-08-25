/**
 * Protocol registry — the generalization point of the outbound SSE layer
 * (ADR-0001). Every outbound protocol is a self-contained adapter module
 * ({ id, aliases?, title, description, create(options) }); this registry maps
 * ids/aliases to adapter definitions. Built-ins register at load; external
 * code may register more before wiring a gateway.
 */
import native from './native.js'
import openaiChat from './openai-chat.js'
import openaiResponses from './openai-responses.js'
import anthropicMessages from './anthropic-messages.js'

const byKey = new Map() // id | alias -> definition
const ordered = [] // definitions in registration order

/**
 * Register a protocol adapter definition.
 * @param {object} definition { id, aliases?, title, description, create(options) }
 */
export function registerProtocol(definition) {
  if (definition === null || typeof definition !== 'object') {
    throw new Error('protocols: definition must be an object')
  }
  const { id, aliases = [], title, description, create } = definition
  if (typeof id !== 'string' || id.length === 0) throw new Error('protocols: definition.id must be a non-empty string')
  if (typeof title !== 'string') throw new Error(`protocols: '${id}' definition.title must be a string`)
  if (typeof description !== 'string') throw new Error(`protocols: '${id}' definition.description must be a string`)
  if (typeof create !== 'function') throw new Error(`protocols: '${id}' definition.create must be a function`)
  const keys = [id, ...aliases]
  for (const key of keys) {
    if (typeof key !== 'string' || key.length === 0) throw new Error(`protocols: '${id}' has a non-string alias`)
    if (byKey.has(key)) throw new Error(`protocols: protocol key '${key}' is already registered`)
  }
  for (const key of keys) byKey.set(key, definition)
  ordered.push(definition)
}

/**
 * Resolve a protocol id or alias to its definition.
 * @param {string} id
 * @throws {Error} unknown id (message lists every available protocol)
 */
export function resolveProtocol(id) {
  const definition = byKey.get(id)
  if (definition !== undefined) return definition
  const available = listProtocols()
    .map((protocol) => protocol.aliases.length > 0 ? `${protocol.id} (${protocol.aliases.join(', ')})` : protocol.id)
    .join(' | ')
  throw new Error(`protocols: unknown protocol '${String(id)}' (available: ${available})`)
}

/** List registered protocols (stable registration order). */
export function listProtocols() {
  return ordered.map(({ id, aliases = [], title, description }) => ({ id, aliases: [...aliases], title, description }))
}

registerProtocol(native)
registerProtocol(openaiChat)
registerProtocol(openaiResponses)
registerProtocol(anthropicMessages)
