# ADR-0005: Codex Harness app-server integration channel

**Status:** Proposed
**Date:** 2026-08-25

## Context

Foreman currently supports two harness channels — `stdio` (SDK JSON-RPC over NDJSON)
and `web` (dsh web apiproxy over HTTP+WebSocket) — both targeting the DeepSeek Harness
(dsh) runtime. The protocol adapter layer (ADR-0001, ADR-0003) already handles the
*outbound* event stream for the Codex product line (`openai-responses`/`codex` dialect),
but there is no *inbound* channel that can drive a Codex Harness agent session directly.

The [Codex Harness](https://github.com/openai/codex) was fully open-sourced on
2026-08-19 (Apache-2.0). Its `codex app-server` exposes a bidirectional JSON-RPC 2.0
protocol (with the `"jsonrpc":"2.0"` header omitted on the wire) over stdio, WebSocket,
or Unix socket. The protocol defines three core primitives — **Thread**, **Turn**, **Item**
— and a lifecycle handshake (`initialize` → `initialized` → `thread/start` → `turn/start`
→ streaming notifications → `turn/completed`).

Foreman must be able to drive a Codex Harness agent as a first-class channel, sharing
the same orchestrator (`prepare`/`start`/`prompt`/`collect`/`publish`), workspace
checkpointing, secret interception, and outbound event adaptation that the existing dsh
channels use.

## Options considered

1. **Map the Codex app-server protocol onto the existing `SdkChannel` abstraction** —
   the SDK channel already speaks NDJSON over stdio; the Codex app-server also speaks
   JSONL over stdio (`--stdio` flag, or the default Unix socket). The message schema and
   lifecycle differ significantly (dsh uses `session.create` / `agents.create` /
   `session.event` notifications; Codex uses `thread/start` / `turn/start` /
   `item/*` notifications), so a shared implementation would be tangled in conditionals.
   Rejected: the lifecycle differences are too deep.

2. **New channel class `CodexChannel` implementing the same interface as `SdkChannel`
   and `WebChannel`** (chosen) — the channel interface (`start`, `prompt`, `shutdown`,
   `kill`) is already abstract enough; a new channel class maps the Codex app-server
   handshake, turn lifecycle, and event stream onto the same `onEvent`/`onStatus`
   callbacks. The channel is selected by `channel: 'codex'`.

3. **Wrap the Codex SDK (`@openai/codex-sdk`)** — the SDK provides a higher-level
   `CodexAgent` class. This would add a non-trivial dependency (the SDK is published on
   npm). The raw JSON-RPC protocol is well-documented and stable, and embedding it
   directly avoids version coupling and SDK-specific error handling. Rejected: prefer
   the raw protocol for the same reasons the existing dsh channels use the raw SDK
   protocol rather than a higher-level wrapper.

## Decision

### 1. New channel: `src/channels/codex-channel.js`

A new channel class `CodexChannel` with the same interface as `SdkChannel`:

```
start({ onEvent, onStatus }) → Promise<{ sessionId: string, ... }>
prompt(sessionId, text, { timeoutMs }) → Promise<{ reason: { kind: string } }>
shutdown() → Promise<number>  // exit code
kill() → Promise<number>
```

The channel:

- Spawns `codex app-server --stdio` as a subprocess (or uses the path from
  configuration).
- Performs the initialization handshake: `initialize` request → `initialized`
  notification.
- Calls `thread/start` to create a new thread (or `thread/resume` when continuing
  a persisted session).
- On `prompt()`, calls `turn/start` with the user text and streams `item/*`
  notifications from stdout, converting them into the same internal frame model
  (`{ kind: 'session.event', type, data }`) that the outbound protocol adapters
  consume.
- On `turn/completed`, resolves the prompt promise.
- `shutdown()` sends no request (the subprocess exits when stdin closes); `kill()`
  sends SIGKILL.

### 2. Internal frame mapping

Codex app-server notifications are mapped to the internal frame model:

| Codex notification | Internal frame type | data shape |
|---|---|---|
| `item/agentMessage/delta` | `assistant/chunk` | `{ turn, step, chunk: { type:'text-delta', index, text } }` |
| `item/agentMessage/complete` (when no deltas arrived) | `assistant/message` | `{ turn, step, message: { content: [...] } }` |
| `item/toolUse/started` | `tool/call` | `{ name, arguments, callId }` |
| `item/toolResult/started` | `tool/result` | `{ callId, meta: { diffs: [] } }` |
| `turn/completed` | `turn/end` | `{ reason: { kind: 'completed' \| error \| cancelled } }` |
| `item/approval/requested` | `approval/requested` | `{ sessionId, approvalId, toolName, reason }` |
| `item/approval/resolved` | `approval/resolved` | `{ sessionId, approvalId, outcome }` |

### 3. Session identity and resume

The Codex `thread_id` doubles as the external `sessionId` (same pattern as the dsh
channels). `thread/resume` is called during `prepare()` when session logs exist for
the given sessionId.

### 4. Config-driven channel selection

The `channel` option in `Foreman` constructor accepts `'codex'` in addition to
`'stdio'` and `'web'`. The `foreman.config.json` schema (ADR-0002) is extended with
a `channel` key under a new `harness` namespace:

```json
{
  "harness": {
    "channel": "codex",
    "codex": {
      "binary": "codex",
      "args": ["app-server", "--stdio"],
      "model": "gpt-5.1-codex",
      "approvalPolicy": "never"
    }
  }
}
```

### 5. Outbound event adaptation

The Codex channel reuses the existing outbound protocol adapter layer. The same
`native` / `openai-chat` / `openai-responses` / `anthropic-messages` adapters are
available regardless of the inbound channel. The `openai-responses` (codex) adapter
is the natural choice for Codex consumers, but the decision is deployment-configurable
(ADR-0002).

## Consequences

- Foreman can drive a Codex Harness agent session as a first-class channel, with the
  same orchestrator lifecycle, workspace checkpointing, and secret interception.
- The new channel adds ~400 lines of implementation and ~200 lines of tests.
- The `codex` binary must be available in the sandbox PATH (or configured via
  `harness.codex.binary`). This is an operational dependency, not a code dependency.
- The Codex app-server protocol is evolving (experimental WebSocket transport, new
  item types); the channel implementation should be resilient to unknown item types
  (skip them with a warning, same as the protocol adapters handle unknown frame types).
- Session resume via `thread/resume` requires the Codex app-server to have access to
  the persisted thread store (shared `CODEX_HOME` across runs, stored in the workspace
  or session archive).