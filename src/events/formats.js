/**
 * Outbound event stream format adapters.
 *
 * Internally the foreman gateway uses its own frames (native envelope:
 * {kind, sessionId, ...}); externally different SSE formats are adapted per
 * configuration. Adapters are pure transforms: gateway frames in, an EventOut
 * stream out ({type:'data', payload} or {type:'done'}), rendered by the carrier:
 *   - SSE carrier:   data -> `id: N\ndata: <json>\n\n`; done -> `data: [DONE]\n\n`
 *   - message bus carrier: data -> one publish per payload; done -> a
 *     stream.done marker message
 * The same adapted output serves both SSE and the bus ("streaming SSE data
 * into the bus" = the same EventOut stream).
 *
 * Built-in formats:
 *   - native      foreman frames passed through unchanged (default; carries
 *                 every event: tools/approvals/status/...)
 *   - openai-chat OpenAI Chat Completions streaming chunk protocol:
 *       first chunk  choices[0].delta = { role:'assistant', content:'' }
 *       text chunks  choices[0].delta = { content } (from assistant/chunk text-deltas)
 *       final chunk  choices[0].delta = {}, finish_reason='stop'
 *       termination  data: [DONE]
 *     Mapping boundary (intentional): tool calls, approvals and status events
 *     have no slot in the OpenAI text chunk protocol and are not emitted
 *     (such consumers only care about text deltas); use the native format for
 *     the full event stream. assistant/message events without a delta source
 *     (e.g. replay-derived) are emitted as a fallback so no text is lost.
 */
import { randomUUID } from 'node:crypto'

/**
 * Create a stateful formatter instance (per-turn role-chunk/streamed-step memory).
 * @param {'native'|'openai-chat'} format
 * @param {object} [options]
 * @param {string} [options.model] model field of openai-chat chunks (default 'foreman-agent')
 * @returns {{push(frame: object): Array<{type:'data', payload: object}|{type:'done'}>}}
 */
export function createEventFormatter(format, options = {}) {
  if (format === 'native') return { push: (frame) => [{ type: 'data', payload: frame }] }
  if (format === 'openai-chat') return new OpenAiChatFormatter(options)
  throw new Error(`event-formats: unknown format ${String(format)} (native | openai-chat)`)
}

class OpenAiChatFormatter {
  constructor({ model = 'foreman-agent' } = {}) {
    this.model = model
    this.turn = undefined // { id, created, roleSent, streamedSteps: Set<string> }
  }

  push(frame) {
    if (frame?.kind !== 'session.event') return []
    const { type, data } = frame
    if (type === 'assistant/chunk') return this.onChunk(data)
    if (type === 'assistant/message') return this.onMessage(data)
    if (type === 'turn/end') return this.onTurnEnd(frame)
    return []
  }

  onChunk(data) {
    if (data?.chunk?.type !== 'text-delta') return []
    const turn = this.beginTurn(data.turn)
    turn.streamedSteps.add(`${data.turn}:${data.step}`)
    const out = []
    if (!turn.roleSent) {
      turn.roleSent = true
      out.push(this.chunk({ role: 'assistant', content: '' }))
    }
    out.push(this.chunk({ content: data.chunk.text }))
    return out
  }

  onMessage(data) {
    // Skip steps already emitted via deltas; emit whole-block as a fallback
    // when there was no delta source (replay-derived messages).
    const turn = this.beginTurn(data?.turn)
    if (turn.streamedSteps.has(`${data?.turn}:${data?.step}`)) return []
    const texts = (data?.message?.content ?? [])
      .filter((block) => block.type === 'text')
      .map((block) => block.text)
      .join('')
    if (texts.length === 0) return []
    const out = []
    if (!turn.roleSent) {
      turn.roleSent = true
      out.push(this.chunk({ role: 'assistant', content: '' }))
    }
    out.push(this.chunk({ content: texts }))
    return out
  }

  onTurnEnd() {
    const turn = this.turn
    this.turn = undefined
    // Emit finish + DONE even for an empty stream (consumers get a validly
    // terminated empty completion stream).
    const anchor = turn ?? {
      id: `chatcmpl-${randomUUID().replace(/-/g, '').slice(0, 24)}`,
      created: Math.floor(Date.now() / 1000),
    }
    return [this.chunk({}, 'stop', anchor), { type: 'done' }]
  }

  /** Lazily open a turn when the first text arrives: assign a stable chunk id + created. */
  beginTurn(turnNumber) {
    if (this.turn === undefined) {
      this.turn = {
        id: `chatcmpl-${randomUUID().replace(/-/g, '').slice(0, 24)}`,
        created: Math.floor(Date.now() / 1000),
        roleSent: false,
        streamedSteps: new Set(),
        turnNumber,
      }
    }
    return this.turn
  }

  chunk(delta, finishReason = null, turn = this.turn) {
    return {
      type: 'data',
      payload: {
        id: turn?.id ?? `chatcmpl-${randomUUID().replace(/-/g, '').slice(0, 24)}`,
        object: 'chat.completion.chunk',
        created: turn?.created ?? Math.floor(Date.now() / 1000),
        model: this.model,
        choices: [{ index: 0, delta, finish_reason: finishReason }],
      },
    }
  }
}

/** Render an EventOut entry as an SSE wire line (including the trailing blank line). */
export function renderSseLine(entry, id) {
  if (entry.type === 'done') return 'data: [DONE]\n\n'
  return `id: ${id}\ndata: ${JSON.stringify(entry.payload)}\n\n`
}
