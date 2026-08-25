/**
 * Protocol: `openai-chat` — OpenAI Chat Completions streaming chunk protocol.
 *
 * Mapping (one dsh turn = one completion):
 *   first chunk  choices[0].delta = { role:'assistant', content:'' }
 *   text chunks  choices[0].delta = { content } (from assistant/chunk text-deltas)
 *   final chunk  choices[0].delta = {}, finish_reason='stop'
 *   termination  carrier done -> data: [DONE]
 *
 * Mapping boundary (intentional): tool calls, approvals and status events
 * have no slot in the OpenAI text chunk protocol and are not emitted (such
 * consumers only care about text deltas); use the native format for the full
 * event stream. assistant/message events without a delta source (e.g.
 * replay-derived) are emitted as a fallback so no text is lost.
 */
import { randomUUID } from 'node:crypto'

export default {
  id: 'openai-chat',
  title: 'OpenAI Chat Completions',
  description: 'chat.completion.chunk streaming protocol (role chunk / content deltas / finish / [DONE]).',
  create(options = {}) {
    return new OpenAiChatFormatter(options)
  },
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
