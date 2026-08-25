/**
 * Outbound event stream format adapters — façade over the protocol registry
 * (ADR-0001). The per-protocol implementations live in ./protocols/; this
 * module keeps the historical entry points (`createEventFormatter`,
 * `renderSseLine`) stable for existing callers (foreman.js, tests).
 *
 * Internally the foreman gateway uses its own frames (native envelope:
 * {kind, sessionId, ...}); externally different SSE protocols are adapted per
 * configuration. Adapters are pure transforms: gateway frames in, an EventOut
 * stream out ({type:'data', payload} or {type:'done'}), rendered by the carrier:
 *   - SSE carrier:   data -> `id: N\ndata: <json>\n\n`; done -> `data: [DONE]\n\n`
 *   - message bus carrier: data -> one publish per payload; done -> a
 *     stream.done marker message
 * The same adapted output serves both SSE and the bus ("streaming SSE data
 * into the bus" = the same EventOut stream).
 *
 * Built-in protocols (registry ids; `format` accepts ids and aliases):
 *   - native            foreman frames passed through unchanged (default; the
 *                       only lossless format — carries every event)
 *   - openai-chat       OpenAI Chat Completions streaming chunk protocol
 *   - openai-responses  OpenAI Responses API streaming events (alias: codex)
 */
import { resolveProtocol } from './protocols/registry.js'

/**
 * Create a stateful formatter instance (per-turn memory).
 * @param {string} format protocol id or alias (see ./protocols/registry.js)
 * @param {object} [options] protocol-specific options (e.g. { model })
 * @returns {{push(frame: object): Array<{type:'data', payload: object}|{type:'done'}>}}
 */
export function createEventFormatter(format, options = {}) {
  try {
    return resolveProtocol(format).create(options)
  } catch (error) {
    // Preserve the historical error wording for unknown formats (fail loud).
    if (String(error.message).includes('unknown protocol')) {
      const available = /\(available: (.*)\)/.exec(String(error.message))?.[1] ?? ''
      throw new Error(`event-formats: unknown format ${String(format)} (available: ${available})`)
    }
    throw error
  }
}

/** Render an EventOut entry as an SSE wire line (including the trailing blank line). */
export function renderSseLine(entry, id) {
  if (entry.type === 'done') return 'data: [DONE]\n\n'
  if (entry.event !== undefined) {
    return `event: ${entry.event}\nid: ${id}\ndata: ${JSON.stringify(entry.payload)}\n\n`
  }
  return `id: ${id}\ndata: ${JSON.stringify(entry.payload)}\n\n`
}
