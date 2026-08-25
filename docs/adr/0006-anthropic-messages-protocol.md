# ADR-0006: Anthropic Messages streaming protocol adapter (Claude Code)

**Status:** Proposed
**Date:** 2026-08-25

## Context

ADR-0001 introduced the protocol adapter layer with a registry and a built-in set of
outbound adapters. ADR-0003 added the `openai-responses` (codex) dialect. The registry
is general, but only three adapters ship (`native`, `openai-chat`, `openai-responses`).

The Claude Code product line and the broader Anthropic ecosystem use the **Anthropic
Messages API** streaming protocol: a typed SSE event sequence (`message_start` →
`content_block_start` → `content_block_delta` → `content_block_stop` → `message_delta`
→ `message_stop`) delivered over `POST /v1/messages` with `stream: true`. This is a
well-defined, publicly documented wire protocol [docs.anthropic.com/en/build-with-claude/streaming].

The `NVIDIA-NeMo/Gym` project has demonstrated a working Claude Code integration
[PR #1603] that maps the Anthropic Messages protocol to the internal Responses API
format. Foreman can do the same at the protocol adapter layer, sharing the same
internal frame model that the existing adapters consume.

Adding an `anthropic-messages` adapter serves two purposes:
1. It proves the registry generalizes beyond OpenAI-shaped protocols.
2. It enables consumers that speak the Anthropic Messages protocol to subscribe to a
   foreman run directly, without an intermediate proxy or translation layer.

## Options considered

1. **No new adapter (Claude Code consumers use `native`)** — rejected: Claude Code
   clients expect the Anthropic SSE event sequence; forcing them to consume `native`
   frames and translate on their own defeats the purpose of the protocol adapter layer.

2. **Anthropic Messages streaming adapter (chosen)** — a new adapter module that maps
   internal foreman frames onto the Anthropic SSE event family, registered as
   `anthropic-messages` (alias `claude`). The adapter is a pure outbound transform,
   same shape as the existing adapters (ADR-0001 §2.1).

3. **Full-duplex adapter for Claude Code Remote (WebSocket protocol)** — the
   reverse-engineered Claude Code Remote protocol uses WebSocket for streaming and
   HTTP REST for session management. This is a separate transport concern and should
   be handled by a future carrier extension, not by the protocol adapter. Rejected for
   this ADR; tracked in the roadmap.

## Decision

### 1. Adapter identity

```
id:       'anthropic-messages'
aliases:  ['claude']
title:    'Anthropic Messages (Claude)'
description: 'Anthropic Messages API streaming events: message_start / content_block_start / content_block_delta / message_stop.'
```

### 2. One dsh turn = one Message

Each dsh turn maps to one Anthropic Message. The `content` array of the Message
contains content blocks in encounter order. Each turn gets a fresh `msg_*` id;
the carrier-level `[DONE]` marker closes the turn on the SSE stream (uniform carrier
contract, ADR-0001).

### 3. Mapping table (normative)

| Internal frame | Outbound events |
|---|---|
| first mapped item of a turn | `message_start` (Message with empty `content` array, `stop_reason: null`) |
| `assistant/chunk` (text-delta) | `content_block_start` (text block, index=0) on first delta of the turn, then `content_block_delta` (text_delta) per delta |
| `assistant/message` without a delta source (replay-derived) | `content_block_start` (text block) + one `content_block_delta` carrying the whole text |
| `tool/call` | `content_block_start` (tool_use block) + `content_block_delta` (input_json_delta) + `content_block_stop` (tool_use) |
| `turn/end` (reason.kind = `completed`) | `content_block_stop` for every open text block, then `message_delta` (stop_reason: `end_turn`) + `message_stop` |
| `turn/end` (other reasons) | `content_block_stop` for every open text block, then `message_delta` (stop_reason: `stop_sequence` or `max_tokens`) + `message_stop` |
| everything else (`tool/result`, approvals, status, phase frames) | no slot — not emitted (mapping boundary, below) |

### 4. Content block indexing

Content blocks are indexed in encounter order within a turn:
- Message text blocks (from `assistant/chunk` deltas) get the next available index.
- Tool use blocks (from `tool/call`) get the next available index after any preceding
  text blocks.
- The `content_block_stop` for a text block carries the accumulated text in the
  `content_block` payload (matching the Anthropic final block shape).

### 5. Mapping boundary (intentional)

- `tool/result` payloads, approval frames, status and phase frames have no
  representation as a Message content block; they are dropped, exactly as in
  `openai-chat` and `openai-responses`.
- `native` remains the lossless format.
- Non-text content blocks (e.g. thinking, signature) are not mapped in the initial
  implementation. The adapter silently drops `thinking_delta` and `signature_delta`
  event types.
- Token usage is not fabricated. `message_delta` carries `usage: null` (dsh does not
  emit usage on `turn/end`).

### 6. Config-driven selection

The adapter is selected by setting `events.protocol` to `'anthropic-messages'` or
`'claude'` in `foreman.config.json` (ADR-0002). No code changes are needed to select
it — the registry resolves the alias at wiring time.

## Consequences

- Consumers speaking the Anthropic Messages protocol (Claude Code compatible clients,
  Anthropic SDK consumers) can subscribe to a foreman run directly.
- The adapter provides the first proof that the registry generalizes beyond
  OpenAI-shaped protocols — a milestone for the roadmap ("Anthropic Messages
  streaming dialect").
- The adapter must accumulate per-block text to emit faithful `content_block_stop`
  payloads; state growth is bounded by one turn.
- Golden-transcript conformance tests (ADR-0004, extended by ADR-0008) pin the exact
  event sequence and payload invariants.
- The `claude` alias is short and recognizable; the canonical `anthropic-messages` id
  is descriptive and avoids ambiguity with any future Claude-specific protocol.