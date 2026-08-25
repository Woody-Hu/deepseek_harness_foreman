# ADR-0003: `openai-responses` protocol dialect (Codex)

**Status:** Accepted
**Date:** 2026-08-25

## Context

ADR-0001 introduces the protocol adapter layer; the first two consumers are the
OpenAI Chat Completions chunk protocol (already implemented) and the event
family used by the Codex product line. "Codex protocol" in the SSE/streaming
sense means the **OpenAI Responses API streaming events**: a typed event
sequence (`response.created` → `response.output_item.added` →
`response.content_part.added` → `response.output_text.delta` → … →
`response.completed`) carrying a structured `response` object with an `output`
array of typed items (`message`, `function_call`, …).

This is a well-defined, publicly documented wire protocol, and it maps far more
naturally onto an agent session than Chat Completions chunks do: it has first
class slots for multiple output items per response, for tool/function calls,
and for per-item lifecycle events.

## Options considered

1. **Chat Completions chunks with tool_calls deltas** — already exists as
   `openai-chat`; text-only in foreman's current mapping. Not the Codex family.
2. **Codex app-server JSON-RPC notifications** (`codex/event/*`) — rejected:
   that is a bidirectional JSON-RPC protocol over stdio, not an SSE streaming
   protocol; it does not fit the outbound gateway carrier.
3. **OpenAI Responses API streaming events** (chosen): SSE-native, typed,
   item-oriented, and the protocol the Codex product line consumes.

## Decision

1. **Id and alias** — protocol id `openai-responses`, alias `codex`. Both
   resolve in the registry; configs may use either.
2. **Unit of adaptation** — one dsh turn = one `response`. Each turn gets a
   fresh `resp_*` id; the carrier-level `[DONE]` marker closes the turn on the
   SSE stream (uniform carrier contract, see ADR-0001).
3. **Mapping table** (normative):

   | Internal frame | Outbound events |
   |---|---|
   | first mapped item of a turn | `response.created` |
   | `assistant/chunk` (text-delta), first of its (turn, step) | `response.output_item.added` (message item) + `response.content_part.added` (empty `output_text` part) |
   | `assistant/chunk` (text-delta) | `response.output_text.delta` |
   | `assistant/message` without a delta source (replay-derived) | item/part added (if needed) + one `response.output_text.delta` carrying the whole text |
   | `tool/call` | `response.output_item.added` (function_call item) + `response.function_call_arguments.delta` + `response.function_call_arguments.done` + `response.output_item.done` |
   | `turn/end` (reason.kind = `completed`) | `response.output_text.done` + `response.content_part.done` + `response.output_item.done` for every open message item, then `response.completed` |
   | `turn/end` (other reasons) | same closing sequence, then `response.failed` |
   | everything else (`tool/result`, approvals, status, phase frames) | no slot — not emitted (mapping boundary, below) |

   Output items are numbered (`output_index`) in encounter order; a turn may
   interleave message items and function_call items.
4. **Mapping boundary (intentional)** — `tool/result` payloads, approval
   frames, status and phase frames have no representation as a response output
   item; they are dropped, exactly as in `openai-chat`. `native` remains the
   lossless format. Non-text content blocks (e.g. thinking) are not mapped.
5. **Usage accounting** — dsh does not emit token usage on `turn/end`, so
   `response.completed` carries `usage: null`. We do not fabricate numbers.

## Consequences

- Consumers speaking the Responses event family can subscribe to a foreman run
  directly, including tool-call visibility (absent in `openai-chat`).
- The adapter must accumulate per-item text to emit faithful
  `output_text.done` / `output_item.done` / `response.completed` payloads —
  state growth is bounded by one turn.
- Golden-transcript conformance tests (ADR-0004) pin the exact event sequence
  and payload invariants, so the mapping above stays normative over time.
