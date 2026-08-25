# SSE Protocol Adapter Layer — Design

This document is the normative design for the outbound protocol adaptation
layer: interfaces, data models, and mechanisms. Decisions and rationale live in
[ADR-0001](../adr/0001-generic-sse-protocol-adapter-layer.md),
[ADR-0002](../adr/0002-runner-config-file.md),
[ADR-0003](../adr/0003-openai-responses-protocol.md); this document specifies
*how* they are realized.

```
                    ┌────────────────────────────────────────────────────────┐
  channels          │ SseGateway (carrier — transport only)                  │
  (stdio/web) ──▶   │  publish(frame) ─▶ formatter.push ─▶ renderSseLine     │
                    │       │                          (EventOut)   │        │
                    │       │                                       ▼        │
                    │       ▼                            SSE subscribers     │
                    │  redaction, trace buffer, /status,/hitl       bus      │
                    └────────────────────────────────────────────────────────┘
                              formatter = registry.resolve(id).create(opts)
```

## 1. Data model

### 1.1 Internal frames (gateway input; produced by foreman from channel events)

The gateway input is a tagged frame object. The complete set:

| Frame | Shape | Origin |
|---|---|---|
| session event | `{ kind:'session.event', sessionId, seq, type, time, data }` | dsh session events (redacted via `redactJson` before entering the gateway) |
| status | `{ kind:'session.status', sessionId, status }` | channel status notifications |
| phase | `{ kind:'foreman.phase', phase, sessionId, ... }` | foreman lifecycle |
| approval | `{ kind:'approval/requested' \| 'approval/resolved', sessionId, approvalId, ... }` | web channel HITL |

Relevant dsh session event `type`s (the subset protocol adapters map; all other
types pass through `native` only):

| type | data shape |
|---|---|
| `assistant/chunk` | `{ turn, step, chunk: { type:'text-delta', index, text } }` |
| `assistant/message` | `{ turn, step, message: { content: [{ type:'text', text }, ...] } }` |
| `tool/call` | `{ name, arguments, ... }` |
| `tool/result` | `{ callId, meta: { diffs: [...] } }` |
| `turn/end` | `{ reason: { kind, ... } }` |

### 1.2 EventOut (adapter output; the stable adapter↔carrier contract)

```
type EventOut =
  | { type: 'data', payload: unknown }   // one outbound stream datum
  | { type: 'done' }                     // turn/stream termination marker
```

- `payload` is an opaque JSON value for the carrier; the SSE carrier renders it
  as `id: N\ndata: <json>\n\n`, the bus carrier publishes it as one message.
- `done` is a **carrier-level** turn separator: SSE renders `data: [DONE]\n\n`,
  the bus publishes a `stream.done` marker. It is uniform across protocols and
  independent of each protocol's own completion event
  (e.g. `response.completed`).
- Adapters must be deterministic given a frame sequence; all randomness (ids)
  is internal and stable within a turn.

## 2. Interface

### 2.1 Protocol adapter definition

```js
// src/events/protocols/<id>.js
export default {
  id: 'openai-chat',            // unique registry key
  aliases: ['codex'],           // optional alternate ids (resolution-level)
  title: 'OpenAI Chat Completions',
  description: 'chat.completion.chunk streaming protocol',
  create(options) {             // options: { model?, ...protocol-specific }
    return { push(frame) { /* → EventOut[] */ } }
  },
}
```

Rules:

- `push` returns **synchronously** an array of zero or more EventOut entries.
- Adapters hold per-turn state and reset it on `turn/end`; a gateway session
  may span many turns.
- Adapters never perform I/O, never redact (frames are already redacted),
  never touch subscribers/replay/bus.

### 2.2 Registry

```js
// src/events/protocols/registry.js
registerProtocol(definition)       // id/alias collision → throw
resolveProtocol(id)                // → definition (id or alias); unknown → throw (lists available)
listProtocols()                    // → [{ id, title, description, aliases }]
```

Built-ins register at registry module load: `native`, `openai-chat`,
`openai-responses` (alias `codex`). External registration happens before
gateway wiring.

### 2.3 Façade (backwards compatibility)

`src/events/formats.js` continues to export `createEventFormatter(format,
options)` (now registry-backed; `format` accepts any id or alias) and
`renderSseLine(entry, id)`.

### 2.4 Foreman wiring & configuration

`Foreman` accepts `events: { protocol?, format?, delivery?, model?, bus? }` and
an optional `configPath` (or `FOREMAN_CONFIG` env var) pointing at a
`foreman.config.json`:

```
effective.events.protocol = options.events.protocol
                          ?? options.events.format      // legacy alias
                          ?? configFile.events.protocol
                          ?? 'native'
```

(all `events.*` keys follow the same option > file > default precedence; the
config loader validates shape and fails loud on unknown keys — ADR-0002.)

Config schema (`foreman.config.json`):

```
{
  "events": {
    "protocol": <registry id or alias>,
    "delivery": "sse" | "bus" | "both",
    "model":    <string>,           // chunk/response model field
    "bus":      <createEventBus config>
  }
}
```

## 3. Mechanisms

### 3.1 Gateway publish path (unchanged by this design, restated as contract)

```
publish(frame):
  for entry of formatter.push(frame):
    id    = emitted.length
    line  = renderSseLine(entry, id)
    emitted.push({id, line})                 // replay buffer (wire lines)
    delivery ≠ 'bus'  → write line to every SSE subscriber
    delivery ≠ 'sse'  → bus.publish(entry.payload | stream.done)
```

Replay (Last-Event-ID) stores rendered wire lines, so resumption is
format-consistent by construction.

### 3.2 Turn lifecycle in adapted protocols

Both OpenAI dialects are turn-scoped: the first mapped event of a turn lazily
allocates the turn identity (`chatcmpl-*` / `resp_*`), `turn/end` closes it and
emits the carrier `done`. An empty turn still terminates properly (finish chunk
/ `response.completed` with empty output) so consumers always observe a validly
terminated stream.

### 3.3 `openai-responses` item model

One turn = one response; output items are allocated in encounter order:

- **message item** per `(turn, step)` that produces text (from deltas or the
  `assistant/message` fallback); text accumulates for the closing
  `output_text.done` / `output_item.done` / `response.completed` payloads.
- **function_call item** per `tool/call`, completed immediately (arguments are
  already complete in dsh; there is no arguments streaming phase to replay).
- `turn/end` closes all open items, then `response.completed`
  (or `response.failed` when `reason.kind !== 'completed'`).

### 3.4 Hermetic verification hooks

- Golden transcripts (`test/fixtures/transcripts.js`) are shared by conformance
  tests and the benchmark — the benchmark drives the exact same real frame
  sequences the tests assert on (ADR-0004).
- The wire tests subscribe through a real `fetch` client; assertions parse the
  SSE wire format themselves, so a carrier regression cannot hide behind the
  gateway's own rendering code.
