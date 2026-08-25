# ADR-0001: Generic outbound SSE protocol adapter layer

**Status:** Accepted
**Date:** 2026-08-25
**Design detail:** [docs/design/sse-protocol-adapter.md](../design/sse-protocol-adapter.md)

## Context

Foreman's outbound gateway (`SseGateway`) adapts internal event frames into an
externally consumable SSE stream. Today the adaptation logic lives in a single
module (`src/events/formats.js`) with exactly two hardcoded formats — `native`
(passthrough) and `openai-chat` (Chat Completions chunk protocol) — selected by
a string in the `Foreman` constructor options.

The system must serve multiple downstream consumers, each speaking a different
SSE protocol (OpenAI Chat Completions, the Codex/Responses event family, and
later others). With the current design, every new protocol means editing a core
module and re-plumbing selection logic; there is no way to add a protocol
without touching foreman internals, and no single place that describes what
protocols exist.

The channel side already solved the analogous problem: `stdio` and `web`
channels implement one interface behind a selector. The event-format side
should be symmetric.

## Options considered

1. **Keep growing `formats.js`** with one class per protocol — rejected: core
   module churn, no registry, selection logic and protocol knowledge entangled.
2. **Pluggable protocol adapters behind a registry** (chosen) — each protocol is
   a self-contained module exporting metadata + a factory; a registry resolves
   protocol ids; selection is a string resolved at wiring time.
3. **Full middleware/pipeline engine** (ordered transform stages per config) —
   rejected as over-engineering: every current and foreseen protocol is a
   stateful *adapter*, not a composable pipeline; a registry of adapters is the
   minimum sufficient generalization.

## Decision

Introduce `src/events/protocols/` with three parts:

1. **Adapter contract** — every protocol implements
   `{ id, aliases?, title, description, create(options) }` where `create`
   returns `{ push(frame) → EventOut[] }`. `EventOut` remains
   `{ type:'data', payload } | { type:'done' }`. Adapters are pure stateful
   transforms: internal frames in, EventOut stream out. They never touch
   transport (HTTP, bus, replay) — that stays in the gateway (the "carrier").
2. **Registry** (`registry.js`) — `registerProtocol`, `resolveProtocol(id)`,
   `listProtocols()`. Built-ins (`native`, `openai-chat`, `openai-responses`)
   register at module load; external code can register more before wiring a
   gateway. Unknown ids fail loud, listing available protocols.
3. **Façade** — `src/events/formats.js` keeps exporting
   `createEventFormatter(format, options)` / `renderSseLine`, now delegating to
   the registry. Existing callers (foreman, tests) keep working; `format` is
   accepted as a legacy alias of `protocol`.

The gateway, replay buffer, SSE rendering, and bus delivery are unchanged —
they already operate on the EventOut stream, which becomes the stable internal
contract between protocol adapters and carriers.

## Consequences

- Adding a protocol = one new file + one registry entry; zero changes to
  foreman/gateway/carrier code.
- Protocol selection becomes a deployment concern that can move into a
  configuration file (ADR-0002).
- The mapping boundary of each protocol (which internal events have no slot in
  the outbound protocol) becomes explicit, per-adapter, documented behavior
  instead of an implicit implementation detail.
- `native` remains the only lossless format; adapted protocols are documented
  as projections. Consumers needing the full stream use `native`.
