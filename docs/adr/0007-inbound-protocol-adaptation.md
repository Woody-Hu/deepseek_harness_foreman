# ADR-0007: Inbound protocol adaptation (generalized parse direction)

**Status:** Proposed
**Date:** 2026-08-25

## Context

The current protocol adapter layer (ADR-0001) is strictly outbound-only: adapter
modules implement `push(frame) → EventOut[]` to transform internal frames into
external wire formats. The gateway, replay buffer, and bus carrier all operate on
the EventOut stream.

However, with the introduction of the Codex app-server channel (ADR-0005) and the
Anthropic Messages adapter (ADR-0006), the system now has two distinct directions
of protocol concern:

1. **Outbound** (formatting): internal frames → external wire events (existing).
2. **Inbound** (parsing): external wire events → internal frames (needed).

The Codex channel receives JSON-RPC notifications from the `codex app-server`
subprocess and must convert them into the internal frame model. Today this
conversion logic lives inside the channel implementation. For a general "transparent
proxy" mode where foreman sits between a protocol-native client and the harness,
this parsing logic should be a reusable, registry-bound component — the mirror
image of the outbound adapter contract.

The Concrete requirement from the roadmap: "inbound protocol adaptation —
generalize the adapter contract with a `parse(wireChunk) → internal frames`
direction, starting with `openai-chat` and `openai-responses`, so the gateway can
accept protocol-native prompt submissions."

## Options considered

1. **Keep parsing logic inside each channel** — rejected: the Codex channel would
   parse Codex JSON-RPC notifications, a future WebSocket channel would parse its
   own wire format, and a transparent proxy mode would need to bridge two different
   protocols. No reuse, no registry, no single place to enumerate supported inbound
   protocols.

2. **Generalized adapter contract with a `parse` direction** (chosen) — each protocol
   adapter gains an optional `parse(wireChunk) → internal frames[]` method alongside
   the existing `push(frame) → EventOut[]`. The registry resolves adapters by id
   for both directions. A protocol module becomes a self-contained description of
   both the outbound shape and the inbound shape of a wire dialect.

3. **Separate parse-only modules** — rejected: the outbound and inbound mappings of
   a given protocol are inherently coupled (they share the same event model,
   the same mapping boundary rules). Splitting them into separate modules would
   create a maintenance burden without benefit.

## Decision

### 1. Extended adapter contract

```
{
  id, aliases?, title, description,
  create(options) → {
    push(frame) → EventOut[],         // outbound (existing, ADR-0001)
    parse?(wireChunk) → FrameIn[]     // inbound (new, optional)
  }
}
```

Where `FrameIn` is the internal frame type that the gateway can publish:

```
type FrameIn = {
  kind: 'session.event',
  sessionId: string,
  type: string,        // 'assistant/chunk' | 'assistant/message' | 'tool/call' | 'turn/end' | ...
  data: object,
}
```

### 2. parse() contract

- `parse(wireChunk)` receives a single wire-protocol datum (an SSE event payload,
  a JSON-RPC notification params, a WebSocket message, etc.) and returns zero or
  more internal frames.
- The method is synchronous (no I/O), same as `push()`.
- Unknown wire event types are silently skipped (the adapter documents which wire
  types it maps and which it drops).
- An adapter without `parse` (e.g. `native`) is inbound-only capable of receiving
  literal internal frames — which is correct for the native passthrough.

### 3. Registry integration

- `resolveProtocol(id)` continues to return the full adapter definition.
- `listProtocols()` gains a `directions` field: `['outbound']`, `['inbound']`,
  or `['outbound', 'inbound']` depending on which methods the adapter implements.
- A new convenience function `resolveInboundProtocol(id)` resolves by id/alias
  and throws if the adapter does not implement `parse`.

### 4. Gateway integration

The `SseGateway` accept an optional `inboundParser` (a `parse`-capable adapter
instance) alongside the existing `formatter` (outbound adapter). When an inbound
parser is configured, the gateway's `POST /events` or a new `POST /prompt`
endpoint can accept protocol-native payloads and route them through `parse()` →
`publish()` → outbound `push()`.

### 5. Initial inbound adapters

| Protocol | parse() implementation |
|---|---|
| `openai-chat` | Parse `chat.completion.chunk` delta → `assistant/chunk` frames; parse `{role:"user", content}` → user prompt frame |
| `openai-responses` (codex) | Parse `response.output_text.delta` → `assistant/chunk` frames; no user prompt frame (Codex channels use `turn/start` JSON-RPC, not the Responses API, for inbound) |
| `anthropic-messages` (claude) | Parse `content_block_delta` (text_delta) → `assistant/chunk` frames; parse user message → user prompt frame |

## Consequences

- The adapter contract becomes symmetric: each protocol module describes both how
  to speak the dialect (outbound) and how to listen to it (inbound).
- A transparent proxy mode becomes possible: an inbound parser on one protocol
  feeding into an outbound formatter on another protocol, with the gateway as the
  bidirectional bridge.
- Channel implementations can reuse inbound parsers instead of maintaining their
  own wire-to-frame conversion logic.
- The `parse` method is optional, so existing adapters (and third-party adapters
  that only need outbound formatting) continue to work unchanged.
- The adapter interface grows in complexity; the tradeoff is justified by the
  elimination of duplicated parsing logic across channels.