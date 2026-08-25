/**
 * Protocol: `openai-responses` (alias `codex`) — OpenAI Responses API
 * streaming events, the event family consumed by the Codex product line.
 *
 * Mapping (one dsh turn = one response; see ADR-0003 for the normative table):
 *   first mapped item of a turn  response.created
 *   assistant/chunk text-delta   response.output_item.added (message) +
 *                                response.content_part.added (first delta of a step) /
 *                                response.output_text.delta (every delta)
 *   assistant/message (no delta  item/part added (if needed) + one
 *   source, replay-derived)      output_text.delta carrying the whole text
 *   tool/call                    output_item.added (function_call) +
 *                                function_call_arguments.delta/.done +
 *                                output_item.done (arguments are complete in
 *                                dsh; there is no streaming phase to replay)
 *   turn/end (completed)         output_text.done + content_part.done +
 *                                output_item.done for open message items,
 *                                then response.completed
 *   turn/end (other reasons)     same closing sequence, then response.failed
 *
 * Mapping boundary (intentional): tool/result payloads, approvals, status and
 * phase frames have no representation as a response output item and are not
 * emitted; use the native format for the full event stream. Non-text content
 * blocks (e.g. thinking) are not mapped. dsh emits no token usage on turn/end,
 * so response.completed carries usage: null (no fabricated numbers).
 *
 * The carrier-level done marker ([DONE] on SSE, stream.done on the bus) closes
 * the turn uniformly across protocols; response.completed/response.failed is
 * the protocol-level completion.
 */
import { randomUUID } from 'node:crypto'

const shortId = (prefix) => `${prefix}_${randomUUID().replace(/-/g, '').slice(0, 24)}`

export default {
  id: 'openai-responses',
  aliases: ['codex'],
  title: 'OpenAI Responses (Codex)',
  description: 'Responses API streaming events: response.created / output_item.added / output_text.delta / response.completed.',
  create(options = {}) {
    return new OpenAiResponsesFormatter(options)
  },
}

class OpenAiResponsesFormatter {
  constructor({ model = 'foreman-agent' } = {}) {
    this.model = model
    this.turn = undefined // see beginTurn()
  }

  push(frame) {
    if (frame?.kind !== 'session.event') return []
    const { type, data } = frame
    if (type === 'assistant/chunk') return this.onChunk(data)
    if (type === 'assistant/message') return this.onMessage(data)
    if (type === 'tool/call') return this.onToolCall(data)
    if (type === 'turn/end') return this.onTurnEnd(data)
    return []
  }

  onChunk(data) {
    if (data?.chunk?.type !== 'text-delta') return []
    const turn = this.beginTurn(data.turn)
    const out = []
    const item = this.messageItem(turn, data.turn, data.step, out)
    out.push(...this.textDelta(item, data.chunk.text))
    return out
  }

  onMessage(data) {
    const turn = this.beginTurn(data?.turn)
    const key = `${data?.turn}:${data?.step}`
    // Steps already streamed via deltas are not repeated
    if (turn.items.get(key)?.streamed === true) return []
    const texts = (data?.message?.content ?? [])
      .filter((block) => block.type === 'text')
      .map((block) => block.text)
      .join('')
    if (texts.length === 0) return []
    const out = []
    const item = this.messageItem(turn, data?.turn, data?.step, out)
    out.push(...this.textDelta(item, texts))
    return out
  }

  onToolCall(data) {
    const turn = this.turn ?? this.beginTurn(undefined)
    const out = [...this.createdEvents(turn)]
    const outputIndex = turn.outputIndex++
    const itemId = shortId('fc')
    const callId = typeof data?.callId === 'string' ? data.callId : shortId('call')
    const name = data?.name ?? 'unknown'
    const args = JSON.stringify(data?.arguments ?? {}) ?? '{}'
    const item = {
      type: 'function_call',
      id: itemId,
      call_id: callId,
      name,
      arguments: args,
      status: 'completed',
    }
    turn.output[outputIndex] = item
    out.push({ type: 'data', payload: { type: 'response.output_item.added', output_index: outputIndex, item: { ...item, arguments: '', status: 'in_progress' } } })
    out.push({ type: 'data', payload: { type: 'response.function_call_arguments.delta', item_id: itemId, output_index: outputIndex, delta: args } })
    out.push({ type: 'data', payload: { type: 'response.function_call_arguments.done', item_id: itemId, output_index: outputIndex, arguments: args } })
    out.push({ type: 'data', payload: { type: 'response.output_item.done', output_index: outputIndex, item } })
    return out
  }

  onTurnEnd(data) {
    const turn = this.turn
    this.turn = undefined
    // An empty turn still terminates properly: a fresh response with an empty
    // output (consumers always observe a validly terminated stream).
    const active = turn ?? this.beginTurn(undefined)
    const out = turn === undefined ? this.createdEvents(active) : []
    for (const item of active.items.values()) {
      if (item.type !== 'message' || item.closed) continue
      item.closed = true
      const messageItem = {
        type: 'message',
        id: item.itemId,
        role: 'assistant',
        status: 'completed',
        content: [{ type: 'output_text', text: item.text, annotations: [] }],
      }
      out.push({ type: 'data', payload: { type: 'response.output_text.done', item_id: item.itemId, output_index: item.outputIndex, content_index: 0, text: item.text } })
      out.push({ type: 'data', payload: { type: 'response.content_part.done', item_id: item.itemId, output_index: item.outputIndex, content_index: 0, part: { type: 'output_text', text: item.text, annotations: [] } } })
      out.push({ type: 'data', payload: { type: 'response.output_item.done', output_index: item.outputIndex, item: messageItem } })
      active.output[item.outputIndex] = messageItem
    }
    const completed = data?.reason?.kind === 'completed'
    out.push({
      type: 'data',
      payload: {
        type: completed ? 'response.completed' : 'response.failed',
        response: {
          id: active.responseId,
          object: 'response',
          created_at: active.createdAt,
          status: completed ? 'completed' : 'failed',
          model: this.model,
          output: active.output,
          error: completed ? null : { code: 'turn_end', message: data?.reason?.kind ?? 'unknown' },
          usage: null,
        },
      },
    })
    out.push({ type: 'done' })
    return out
  }

  /** Allocate turn state on the first mapped frame (lazily; a turn id is stable until turn/end). */
  beginTurn(turnNumber) {
    if (this.turn === undefined) {
      this.turn = {
        number: turnNumber,
        responseId: shortId('resp'),
        createdAt: Math.floor(Date.now() / 1000),
        createdSent: false,
        outputIndex: 0,
        output: [],   // completed output items (function_call items + closed message items), by output_index
        items: new Map(), // `${turn}:${step}` -> message item record
      }
    }
    return this.turn
  }

  /** response.created, exactly once per turn. */
  createdEvents(turn) {
    if (turn.createdSent) return []
    turn.createdSent = true
    return [{
      type: 'data',
      payload: {
        type: 'response.created',
        response: {
          id: turn.responseId,
          object: 'response',
          created_at: turn.createdAt,
          status: 'in_progress',
          model: this.model,
          output: [],
          error: null,
          usage: null,
        },
      },
    }]
  }

  /** Get or open the message item for a (turn, step); emits created/added events into `out`. */
  messageItem(turn, turnNumber, step, out) {
    const key = `${turnNumber}:${step}`
    let item = turn.items.get(key)
    if (item === undefined) {
      out.push(...this.createdEvents(turn))
      const outputIndex = turn.outputIndex++
      item = { type: 'message', itemId: shortId('msg'), outputIndex, text: '', streamed: true, closed: false }
      turn.items.set(key, item)
      turn.output[outputIndex] = null // placeholder; filled when the item closes
      out.push({ type: 'data', payload: { type: 'response.output_item.added', output_index: outputIndex, item: { type: 'message', id: item.itemId, role: 'assistant', status: 'in_progress', content: [] } } })
      out.push({ type: 'data', payload: { type: 'response.content_part.added', item_id: item.itemId, output_index: outputIndex, content_index: 0, part: { type: 'output_text', text: '', annotations: [] } } })
    }
    return item
  }

  textDelta(item, text) {
    item.text += text
    return [{ type: 'data', payload: { type: 'response.output_text.delta', item_id: item.itemId, output_index: item.outputIndex, content_index: 0, delta: text } }]
  }
}
