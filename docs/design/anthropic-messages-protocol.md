# Anthropic Messages Protocol Adapter — Design

This document is the normative design for the anthropic-messages outbound protocol
adapter: interfaces, data models, and mechanisms. Decisions and rationale live in
[ADR-0006](../adr/0006-anthropic-messages-protocol.md); this document specifies
*how* it is realized.

## 1. Overview

The `anthropic-messages` adapter translates internal foreman frames into the
Anthropic Messages API streaming event sequence. It is a pure outbound transform
that follows the same adapter contract as the existing adapters (ADR-0001 §2.1).

```
                 ┌──────────────────────────────────────────────┐
                 │ AnthropicMessagesFormatter                    │
                 │                                              │
  push(frame) ──▶│  message_start (first mapped frame of turn)  │──▶ EventOut[]
                 │  content_block_start (text/tool_use blocks)  │
                 │  content_block_delta (text_delta/)           │
                 │  content_block_stop                          │
                 │  message_delta                               │
                 │  message_stop                                │
                 │  [DONE] (carrier-level)                      │
                 └──────────────────────────────────────────────┘
```

## 2. Data model

### 2.1 Anthropic event types

| Event | Carrier event name | Payload | When |
|---|---|---|---|
| `message_start` | `message_start` | `{ type, message: { id, type, role, content: [], model, stop_reason, usage } }` | First mapped frame of a turn |
| `content_block_start` | `content_block_start` | `{ type, index, content_block: { type, text } }` | First delta of a content block |
| `content_block_delta` | `content_block_delta` | `{ type, index, delta: { type, text } }` | Every text delta |
| `content_block_stop` | `content_block_stop` | `{ type, index }` | End of a content block |
| `message_delta` | `message_delta` | `{ type, delta: { stop_reason }, usage }` | Turn completion |
| `message_stop` | `message_stop` | `{ type }` | Stream close |

### 2.2 Internal frame mapping

| Internal frame | Adapter output |
|---|---|
| `assistant/chunk` (text-delta, first of turn) | `message_start` + `content_block_start` (text) + `content_block_delta` (text_delta) |
| `assistant/chunk` (text-delta, subsequent) | `content_block_delta` (text_delta) |
| `assistant/message` (no delta source) | `content_block_start` (text) + `content_block_delta` (whole text) |
| `tool/call` | `content_block_start` (tool_use) + `content_block_delta` (input_json_delta) + `content_block_stop` |
| `turn/end` (completed) | `content_block_stop` (open blocks) + `message_delta` (end_turn) + `message_stop` |
| `turn/end` (other) | `content_block_stop` (open blocks) + `message_delta` (stop_reason) + `message_stop` |

## 3. Interface

### 3.1 Adapter definition

```js
// src/events/protocols/anthropic-messages.js
export default {
  id: 'anthropic-messages',
  aliases: ['claude'],
  title: 'Anthropic Messages (Claude)',
  description: 'Anthropic Messages API streaming events: message_start / content_block_start / content_block_delta / message_stop.',
  create(options = {}) {
    return new AnthropicMessagesFormatter(options)
  },
}
```

### 3.2 Formatter state

```js
class AnthropicMessagesFormatter {
  constructor({ model = 'claude-opus-4-6' } = {}) {
    this.model = model
    this.turn = undefined // see beginTurn()
  }
}
```

Turn state:

```js
this.turn = {
  number,              // dsh turn number
  messageId,           // msg_* id (stable per turn)
  createdAt,           // unix timestamp
  messageSent,         // message_start emitted?
  contentIndex,        // next content block index
  openBlocks: [],      // [{ index, type, text, closed }]
}
```

### 3.3 `push(frame)` → EventOut[]

```js
push(frame) {
  if (frame?.kind !== 'session.event') return []
  const { type, data } = frame
  if (type === 'assistant/chunk') return this.onChunk(data)
  if (type === 'assistant/message') return this.onMessage(data)
  if (type === 'tool/call') return this.onToolCall(data)
  if (type === 'turn/end') return this.onTurnEnd(data)
  return []
}
```

## 4. Mechanisms

### 4.1 Turn lifecycle

The turn is lazily allocated on the first mapped frame:

```
beginTurn(turnNumber):
  if turn is undefined:
    turn = { number, messageId: 'msg_...', createdAt: now,
             messageSent: false, contentIndex: 0, openBlocks: [] }
```

An empty turn (no mapped frames before `turn/end`) still produces a valid
`message_start` → `message_delta` (end_turn) → `message_stop` sequence.

### 4.2 Text block accumulation

Text deltas are accumulated in the open text block. On `turn/end`, the
`content_block_stop` carries the accumulated text in the `content_block` payload:

```
onTurnEnd(data):
  for each open block:
    close block: emit content_block_stop(block)
  emit message_delta(stop_reason, usage: null)
  emit message_stop()
  emit { type: 'done' }
```

### 4.3 Tool call blocks

`tool/call` frames create a `tool_use` content block:

```
onToolCall(data):
  out = []
  out.push(message_start if not yet sent)
  blockIndex = turn.contentIndex++
  out.push(content_block_start(blockIndex, tool_use { name, id, input: {} }))
  out.push(content_block_delta(blockIndex, input_json_delta(partial_json)))
  out.push(content_block_stop(blockIndex))
  return out
```

The `tool_use` block is completed immediately (dsh tool calls have complete
arguments; there is no streaming phase to replay).

### 4.4 Error handling

- Unknown frame types: silently skipped (returns `[]`).
- Non-text content blocks (thinking, signature): not mapped in the initial
  implementation — silently dropped.
- `message_delta` carries `usage: null` (dsh does not emit token usage on
  `turn/end`; no fabricated numbers).

## 5. Carrier integration

The adapter produces the same `EventOut[]` stream as the existing adapters. The
SSE carrier renders it as:

```
event: message_start
id: 0
data: {"type":"message_start","message":{...}}

event: content_block_delta
id: 1
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: message_stop
id: 5
data: {"type":"message_stop"}

data: [DONE]
```

The bus carrier receives the same payloads. The `[DONE]` marker is the uniform
carrier-level turn separator (ADR-0001 §1.2).

## 6. Golden transcript examples

### 6.1 Text-only turn

Input frames:
```
chunk(1, 0, "The workspace is ")
chunk(1, 0, "restored and ")
chunk(1, 0, "ready.")
message(1, 0, "The workspace is restored and ready.")
turnEnd("completed")
```

Expected EventOut sequence:
```
message_start (id: msg_*, content: [], usage: { input_tokens: 0, output_tokens: 0 })
content_block_start (index: 0, content_block: { type: "text", text: "" })
content_block_delta (index: 0, delta: { type: "text_delta", text: "The workspace is " })
content_block_delta (index: 0, delta: { type: "text_delta", text: "restored and " })
content_block_delta (index: 0, delta: { type: "text_delta", text: "ready." })
content_block_stop (index: 0)
message_delta (delta: { stop_reason: "end_turn" }, usage: null)
message_stop
[DONE]
```

### 6.2 Tool call turn

```
chunk(1, 0, "I will write the file.")
toolCall("bash", { command: "echo hello > greeting.txt" })
turnEnd("completed")
```

Expected EventOut sequence:
```
message_start
content_block_start (index: 0, text)
content_block_delta (text: "I will write the file.")
content_block_stop (index: 0)
content_block_start (index: 1, tool_use: { name: "bash", ... })
content_block_delta (index: 1, input_json_delta)
content_block_stop (index: 1)
message_delta (stop_reason: "end_turn")
message_stop
[DONE]
```