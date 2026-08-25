/**
 * Protocol: `anthropic-messages` (alias `claude`) — Anthropic Messages API
 * streaming events, the event family consumed by the Claude Code product line.
 *
 * Mapping (one dsh turn = one Message; see ADR-0006 for the normative table):
 *   first mapped frame of a turn  message_start (Message with empty content,
 *                                 stop_reason: null, usage: { input_tokens: 0, output_tokens: 0 })
 *   assistant/chunk text-delta    content_block_start (text block, index=0) on first
 *                                 delta of the turn, then content_block_delta (text_delta)
 *                                 per delta
 *   assistant/message no delta    content_block_start (text) + one content_block_delta
 *   source (replay-derived)       carrying the whole text
 *   tool/call                     content_block_start (tool_use) + content_block_delta
 *                                 (input_json_delta) + content_block_stop
 *   turn/end (completed)          content_block_stop for every open text block, then
 *                                 message_delta (stop_reason: end_turn) + message_stop
 *   turn/end (other reasons)      content_block_stop for every open text block, then
 *                                 message_delta (stop_reason: stop_sequence) + message_stop
 *
 * Mapping boundary (intentional): tool/result payloads, approvals, status and
 * phase frames have no representation as a Message content block and are not
 * emitted; use the native format for the full event stream. Non-text content
 * blocks (e.g. thinking, signature) are not mapped in the initial implementation.
 * Token usage is not fabricated — message_delta carries usage: null.
 *
 * The carrier-level done marker ([DONE] on SSE) closes the turn uniformly
 * across protocols; message_stop is the protocol-level stream close.
 */
import { randomUUID } from 'node:crypto'

const shortId = (prefix) => `${prefix}_${randomUUID().replace(/-/g, '').slice(0, 24)}`

export default {
  id: 'anthropic-messages',
  aliases: ['claude'],
  title: 'Anthropic Messages (Claude)',
  description: 'Anthropic Messages API streaming events: message_start / content_block_start / content_block_delta / message_stop.',
  create(options = {}) {
    return new AnthropicMessagesFormatter(options)
  },
}

class AnthropicMessagesFormatter {
  constructor({ model = 'claude-opus-4-6' } = {}) {
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
    // message_start on first mapped frame of the turn
    if (!turn.messageSent) {
      out.push(...this.messageStart(turn))
    }
    // Manage text block: close any open tool_use block, start text block if needed
    this.ensureTextBlock(turn, out)
    const block = turn.openBlocks.at(-1)
    block.text += data.chunk.text
    turn.streamedSteps.add(`${data.turn}:${data.step}`)
    out.push(this.event('content_block_delta', {
      type: 'content_block_delta',
      index: block.index,
      delta: { type: 'text_delta', text: data.chunk.text },
    }))
    return out
  }

  onMessage(data) {
    const turn = this.beginTurn(data?.turn)
    const key = `${data?.turn}:${data?.step}`
    // Steps already streamed via deltas are not repeated
    if (turn.streamedSteps.has(key)) return []
    const texts = (data?.message?.content ?? [])
      .filter((block) => block.type === 'text')
      .map((block) => block.text)
      .join('')
    if (texts.length === 0) return []
    const out = []
    if (!turn.messageSent) {
      out.push(...this.messageStart(turn))
    }
    this.ensureTextBlock(turn, out)
    const block = turn.openBlocks.at(-1)
    block.text += texts
    out.push(this.event('content_block_delta', {
      type: 'content_block_delta',
      index: block.index,
      delta: { type: 'text_delta', text: texts },
    }))
    turn.streamedSteps.add(key)
    return out
  }

  onToolCall(data) {
    const turn = this.turn ?? this.beginTurn(undefined)
    const out = []
    if (!turn.messageSent) {
      out.push(...this.messageStart(turn))
    }
    // Close any open text block before starting a tool_use block
    this.closeTextBlock(turn, out)
    const blockIndex = turn.contentIndex++
    const toolUseId = typeof data?.callId === 'string' ? data.callId : shortId('toolu')
    const name = data?.name ?? 'unknown'
    const args = JSON.stringify(data?.arguments ?? {}) ?? '{}'
    const block = { index: blockIndex, type: 'tool_use', id: toolUseId, name, text: '', closed: false }
    turn.openBlocks.push(block)
    // tool_use block is completed immediately (dsh has complete arguments)
    out.push(this.event('content_block_start', {
      type: 'content_block_start',
      index: blockIndex,
      content_block: {
        type: 'tool_use',
        id: toolUseId,
        name,
        input: {},
      },
    }))
    out.push(this.event('content_block_delta', {
      type: 'content_block_delta',
      index: blockIndex,
      delta: { type: 'input_json_delta', partial_json: args },
    }))
    out.push(this.event('content_block_stop', {
      type: 'content_block_stop',
      index: blockIndex,
    }))
    block.closed = true
    return out
  }

  onTurnEnd(data) {
    const turn = this.turn
    this.turn = undefined
    // An empty turn still produces a valid message_start → message_delta → message_stop
    const active = turn ?? this.beginTurn(undefined)
    const out = turn === undefined ? this.messageStart(active) : []
    // Close every open block
    for (const block of active.openBlocks) {
      if (block.closed) continue
      block.closed = true
      if (block.type === 'text') {
        out.push(this.event('content_block_stop', {
          type: 'content_block_stop',
          index: block.index,
        }))
      }
    }
    const completed = data?.reason?.kind === 'completed'
    out.push(this.event('message_delta', {
      type: 'message_delta',
      delta: { stop_reason: completed ? 'end_turn' : data?.reason?.kind ?? 'stop_sequence' },
      usage: null,
    }))
    out.push(this.event('message_stop', { type: 'message_stop' }))
    out.push({ type: 'done' })
    return out
  }

  /** Allocate turn state on the first mapped frame (lazily; a message id is stable until turn/end). */
  beginTurn(turnNumber) {
    if (this.turn === undefined) {
      this.turn = {
        number: turnNumber,
        messageId: shortId('msg'),
        createdAt: Math.floor(Date.now() / 1000),
        messageSent: false,
        contentIndex: 0,
        openBlocks: [], // [{ index, type, text, closed }]
        streamedSteps: new Set(),
      }
    }
    return this.turn
  }

  /** message_start, exactly once per turn. */
  messageStart(turn) {
    if (turn.messageSent) return []
    turn.messageSent = true
    return [this.event('message_start', {
      type: 'message_start',
      message: {
        id: turn.messageId,
        type: 'message',
        role: 'assistant',
        content: [],
        model: this.model,
        stop_reason: null,
        usage: { input_tokens: 0, output_tokens: 0 },
      },
    })]
  }

  /** Ensure an open text block exists (close tool_use blocks first). */
  ensureTextBlock(turn, out) {
    // Close any open tool_use block
    if (turn.openBlocks.length > 0 && turn.openBlocks.at(-1).type !== 'text') {
      const block = turn.openBlocks.pop()
      block.closed = true
      out.push(this.event('content_block_stop', { type: 'content_block_stop', index: block.index }))
    }
    // If no open text block, start one
    if (turn.openBlocks.length === 0 || turn.openBlocks.at(-1).type !== 'text') {
      const block = { index: turn.contentIndex++, type: 'text', text: '', closed: false }
      turn.openBlocks.push(block)
      out.push(this.event('content_block_start', {
        type: 'content_block_start',
        index: block.index,
        content_block: { type: 'text', text: '' },
      }))
    }
  }

  /** Close the current text block (used before tool_use blocks). */
  closeTextBlock(turn, out) {
    if (turn.openBlocks.length === 0) return
    const block = turn.openBlocks.at(-1)
    if (block.type !== 'text' || block.closed) return
    block.closed = true
    turn.openBlocks.pop()
    out.push(this.event('content_block_stop', { type: 'content_block_stop', index: block.index }))
  }

  /** Helper: create an EventOut data entry with an SSE event name. */
  event(name, payload) {
    return { type: 'data', event: name, payload }
  }
}