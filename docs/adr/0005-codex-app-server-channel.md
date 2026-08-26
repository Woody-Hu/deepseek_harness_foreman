# ADR-0005: Codex Harness app-server integration channel

**Status:** Accepted (supersedes the initial Proposed draft — the speculative protocol
shapes have been replaced with live-verified ones, see below)
**Date:** 2026-08-25 · revised 2026-08-26

## Context

Foreman supported two harness channels — `dsh-sdk` (SDK JSON-RPC over NDJSON stdio)
and `dsh-web` (dsh web apiproxy over HTTP+WebSocket) — both targeting the DeepSeek
Harness (dsh) runtime. The protocol adapter layer (ADR-0001, ADR-0003) already handles
the *outbound* event stream for the Codex product line (`openai-responses`/`codex`
dialect), but there was no *inbound* channel that can drive a Codex Harness agent
session directly.

The [Codex Harness](https://github.com/openai/codex) `codex app-server` exposes a
JSON-RPC 2.0-style protocol over stdio JSONL (`--stdio` flag). Foreman must be able to
drive a Codex Harness agent as a first-class channel, sharing the same orchestrator
(`prepare`/`start`/`prompt`/`collect`/`publish`), workspace checkpointing, secret
interception, and outbound event adaptation that the dsh channels use.

**Verification method.** The protocol was verified live against `codex-cli 0.149.1`
with a probe client plus a local Responses-API endpoint: initialize handshake,
thread/turn lifecycle, tool execution (`exec_command`), streaming notification shapes,
cross-process session resume, and multi-turn history growth. The normative shapes are
recorded in [docs/design/codex-channel.md](../design/codex-channel.md).

## Options considered

1. **Map the Codex app-server protocol onto the existing `SdkChannel` abstraction** —
   both speak JSONL over stdio, but the lifecycles differ deeply (dsh:
   `session.create`/`agents.create`/`session.event`; Codex: `thread/start`/`turn/start`
   + `item/*` notifications, plus a config-file-driven model provider model). A shared
   implementation would be tangled in conditionals. Rejected.
2. **New channel class `CodexChannel` implementing the same channel interface** (chosen) —
   the channel interface (`start`, `prompt`, `shutdown`, `kill`) is already abstract
   enough; a new class maps the Codex lifecycle onto the same `onEvent`/`onStatus`
   callbacks. Selected by `channel: 'codex'` (ADR-0009 names channels after their
   harness).
3. **Wrap the Codex SDK** — adds an npm dependency and version coupling; the raw
   protocol is small and verified. Rejected, consistent with why the dsh channels use
   the raw protocol.

## Decision

### 1. New channel: `src/channels/codex-channel.js`

`CodexChannel` implements the common channel interface and speaks the verified v2
app-server protocol:

- Spawns `codex app-server --stdio` (binary/args configurable).
- **Model wiring goes through `CODEX_HOME/config.toml`**, not through request params:
  the channel writes a `model_providers.foreman` entry (base URL + `wire_api =
  "responses"`; 0.149.1 rejects `wire_api = "chat"`) and reads the API key from a
  fixed env var — credentials stay env-injected, never on disk. Note this means the
  model endpoint must speak the OpenAI **Responses API** wire format (DeepSeek's
  chat-completions endpoint is therefore not directly usable behind codex ≥0.149
  without a translating gateway).
- Handshake: `initialize` → `initialized` → `thread/start {cwd, approvalPolicy,
  sandbox}` (or `thread/resume {threadId, cwd}` when the persisted thread store
  exists).
- `prompt()` sends `turn/start {threadId, input:[{type:'text',text}]}` and resolves on
  `turn/completed`.
- `shutdown()` closes stdin (graceful) with SIGTERM/SIGKILL fallbacks; `kill()` is an
  immediate SIGKILL.

### 2. Internal frame mapping (verified shapes)

| Codex notification | Internal frame | Notes |
|---|---|---|
| `item/agentMessage/delta` | `assistant/chunk` | `params.delta` is a plain string |
| `item/completed` (`item.type === 'agentMessage'`) | `assistant/message` | text in `item.text` |
| `item/started` (`item.type === 'commandExecution'`) | `tool/call` | `arguments.command` from `item.command` |
| `item/completed` (`item.type === 'commandExecution'`) | `tool/result` | `callId = item.id` |
| `turn/completed` | `turn/end` | `reason.kind` from `turn.status` |
| `thread/status/changed` | `onStatus` | active/idle |

Unknown notifications (e.g. `configWarning`, `account/rateLimits/updated`) are skipped
resiliently. Full shapes: [docs/design/codex-channel.md](../design/codex-channel.md).

### 3. Session identity and resume

Codex generates its own thread ids (UUIDs), so — unlike the dsh channels — the external
`sessionId` cannot double as the harness session id. The channel persists a
`sessionId → threadId` mapping inside `CODEX_HOME` (`threads-index.json`), which lives
under the foreman session root and is therefore archived/restored with the run's
session logs; `prepare()` → restore → `thread/resume` continues the conversation
across sandboxes (verified across process restarts).

### 4. Config-driven channel selection

`foreman.config.json` gains a `harness` namespace (ADR-0002 rules: unknown keys fail
loud; precedence constructor option > config file > default):

```json
{
  "harness": {
    "channel": "codex",
    "codex": {
      "binary": "codex",
      "model": "gpt-5.1-codex",
      "provider": { "name": "foreman", "baseUrl": "...", "envKey": "FOREMAN_CODEX_API_KEY" },
      "approvalPolicy": "never",
      "sandbox": "workspace-write"
    }
  }
}
```

### 5. Outbound event adaptation

Unchanged from the other channels: the internal frames flow into the existing
adapter registry (`native` / `openai-chat` / `openai-responses` / `anthropic-messages`),
selected per run (ADR-0002).

## Consequences

- Foreman drives a Codex Harness agent as a first-class channel with the same
  orchestrator lifecycle, workspace checkpointing, and secret interception.
- The `codex` binary must be available in the sandbox PATH (operational dependency).
- The channel requires a Responses-API-compatible model endpoint (codex ≥0.149 dropped
  chat-completions wire support); deployments aiming DeepSeek models at codex need a
  translating gateway — recorded as a capability boundary in the roadmap.
- Session resume requires the persisted `CODEX_HOME` (thread store + id mapping),
  carried by the existing `sessions.tar.gz` archive/restore path.
- HITL approval requests (server→client requests) are auto-accepted for now
  (`approvalPolicy: 'never'` in tests); full HITL forwarding is a roadmap item.
