# Codex Harness app-server channel — Design

Normative design for the Codex Harness integration channel: interfaces, data models,
mechanisms. Decisions and rationale live in [ADR-0005](../adr/0005-codex-app-server-channel.md).
All wire shapes below were **verified live against `codex-cli 0.149.1`** (probe client +
local Responses-API endpoint); they are observations, not aspirations.

## 1. Overview

```
                    ┌──────────────────────────────────────────────────┐
  Foreman           │ CodexChannel                                     │
  orchestrator ──▶  │  spawns `codex app-server --stdio`               │
                    │  JSONL JSON-RPC 2.0-style over stdin/stdout      │
                    │                                                  │
                    │  CODEX_HOME/config.toml  ← channel-written        │
                    │    (model + provider + wire_api="responses")    │
                    │                                                  │
                    │  initialize → initialized → thread/start|resume  │
                    │  → turn/start → item/* notifications              │
                    │  → turn/completed → (next turn or close)         │
                    │                                                  │
                    │  onEvent callback → internal frames               │
                    │  onStatus callback → thread status                │
                    └──────────────────────────────────────────────────┘
                              │
                              ▼
                    SseGateway (outbound adapters)
                    native | openai-chat | openai-responses | anthropic-messages
```

## 2. Transport

### 2.1 Subprocess launch

```
const proc = spawn(binary, args, {
  stdio: ['pipe', 'pipe', 'pipe'],
  cwd: workspaceDir,
  env: { ...process.env, CODEX_HOME, [envKey]: apiKey },  // key env-injected only
})
```

- **stdin**: client→server requests and notifications, one JSON object per line.
- **stdout**: server→client responses, notifications, and server-initiated *requests*
  (e.g. approvals — these carry an `id` and expect a response).
- **stderr**: diagnostics (kept as a tail for error messages; never parsed).

Messages omit the `"jsonrpc"` version field. Server→client requests are distinguished
from notifications by the presence of `id` + `method`.

### 2.2 `CODEX_HOME` and model wiring

`CODEX_HOME` is the foreman session root (`<workdir>/.codex`); it holds the thread
store (resume state) and `config.toml`, which the channel (re)writes at start:

```toml
model = "<configured model>"
model_provider = "foreman"

[model_providers.foreman]
name = "Foreman"
base_url = "<modelEnv base url>"
wire_api = "responses"
env_key = "FOREMAN_CODEX_API_KEY"   # read from the child env, never persisted
```

`wire_api` **must** be `responses`: 0.149.1 rejects `chat`
(`… 'wire_api = "chat"' is no longer supported`). The provider entry is only written
when an explicit base URL is configured; otherwise codex's built-in default provider
(OpenAI, `OPENAI_API_KEY`) is used as-is.

## 3. Protocol lifecycle (verified shapes)

### 3.1 Initialization handshake

```
→ {"id":1,"method":"initialize","params":{"clientInfo":{"name":"foreman","version":"…"},"capabilities":{}}}
← {"id":1,"result":{"userAgent":"…","codexHome":"…","platformFamily":"…","platformOs":"…"}}
→ {"method":"initialized","params":{}}
```

### 3.2 Thread creation / resume

```
→ {"id":2,"method":"thread/start","params":{"cwd":"…","approvalPolicy":"never","sandbox":"workspace-write"}}
← {"id":2,"result":{"thread":{"id":"01a03d3b-…"},"model":"…","modelProvider":"…", …}}
```

- Thread ids are **server-generated UUIDs** — the external `sessionId` cannot be reused
  as the thread id (unlike the dsh channels). The channel keeps a
  `sessionId → threadId` mapping at `CODEX_HOME/threads-index.json`, which the session
  archive/restore path carries across sandboxes.
- `sandbox` values: `read-only` | `workspace-write` | `danger-full-access` (kebab-case;
  camelCase is rejected).
- Resume: `{"method":"thread/resume","params":{"threadId":"…","cwd":"…"}}` — verified
  across process restarts with a persisted `CODEX_HOME`; the resumed turn's model
  request carries the full prior history.

### 3.3 Turn start

```
→ {"id":3,"method":"turn/start","params":{"threadId":"…","input":[{"type":"text","text":"…"}]}}
← {"id":3,"result":{"turn":{"id":"…","status":"inProgress"}}}
```

### 3.4 Streaming notifications (server → client, no `id`)

Observed sequence for a tool turn (in order):

```
thread/started
thread/status/changed        {"threadId":"…","status":{"type":"active"}}
turn/started                 {"threadId":"…","turn":{"id":"…","status":"inProgress"}}
item/started                 {"item":{"type":"userMessage", …},"threadId":"…","turnId":"…"}
item/completed               {"item":{"type":"userMessage", …}, …}
item/started                 {"item":{"type":"commandExecution","id":"call_1",
                              "command":"/usr/bin/zsh -lc 'echo probe > probe.txt'",
                              "cwd":"…","status":"inProgress", …}, …}
item/completed               {"item":{"type":"commandExecution","id":"call_1",
                              "status":"completed", …}, …}
item/started                 {"item":{"type":"agentMessage","id":"msg_1","text":"", …}, …}
item/agentMessage/delta      {"threadId":"…","turnId":"…","itemId":"msg_1","delta":"SHAPE…"}
item/completed               {"item":{"type":"agentMessage","id":"msg_1","text":"SHAPE…"}, …}
thread/status/changed        {"threadId":"…","status":{"type":"idle"}}
turn/completed               {"threadId":"…","turn":{"id":"…","status":"completed",
                              "items":[…],"error":null, …}}
```

Other observed notifications (skipped, no mapping): `configWarning`, `warning`,
`account/rateLimits/updated`, `remoteControl/status/changed`, `thread/goal/cleared`.

Item types observed: `userMessage`, `commandExecution`, `agentMessage`. Codex also
defines `fileChange`, `mcpToolCall`, `webSearch`, `todoList`, `reasoning`, `error` —
unmapped item types are skipped resiliently (registry-style forward compatibility).

### 3.5 Model endpoint contract

The endpoint must speak the OpenAI **Responses API** (`POST <base_url>/responses`,
SSE): `response.output_item.added` / `response.function_call_arguments.delta|done` /
`response.output_text.delta` / `response.output_item.done` / `response.completed`.
Tool calls reference codex tools by name (`exec_command`, `write_stdin`,
`update_plan`, `request_user_input`, `view_image`, …); the verified minimal tool
arguments for `exec_command` are `{cmd: string, timeout_ms: number}`.

## 4. Internal frame mapping

| Codex notification | Internal frame | data |
|---|---|---|
| `item/agentMessage/delta` | `assistant/chunk` | `{turn, step, chunk:{type:'text-delta', index:0, text: delta}}` (`delta` is a plain string) |
| `item/completed` (`agentMessage`) | `assistant/message` | `{turn, step, message:{content:[{type:'text', text: item.text}]}}` |
| `item/started` (`commandExecution`) | `tool/call` | `{name:'exec_command', arguments:{command: item.command}, callId: item.id}` |
| `item/completed` (`commandExecution`) | `tool/result` | `{callId: item.id, meta:{diffs:[]}}` |
| `turn/completed` | `turn/end` | `{reason:{kind: status→completed\|failed\|cancelled}}` |
| `thread/status/changed` | `onStatus` | `{sessionId, status: 'active'\|'idle'}` |

Turn/step numbers are channel-local counters (one `prompt()` = one turn).

## 5. Channel interface

Same contract as the dsh channels (consumed by foreman.js):

```
start({onEvent, onStatus}) → {threadId}          // handshake + thread start/resume
prompt(sessionId, text, {timeoutMs}) → {reason}  // turn/start … turn/completed
shutdown() → exitCode                            // stdin close → SIGTERM → SIGKILL
kill() → exitCode                                // immediate SIGKILL
sessionRoot → CODEX_HOME
```

Timeouts: `prompt()` rejects after `timeoutMs` and best-effort sends `turn/interrupt`.

## 6. Configuration

Selected via `foreman.config.json` → `harness.channel: "codex"` (or the `channel`
constructor option). Full option surface (ADR-0002 validation rules apply):

```json
{
  "harness": {
    "channel": "codex",
    "codex": {
      "binary": "codex",
      "args": ["app-server", "--stdio"],
      "model": "gpt-5.1-codex",
      "provider": { "name": "foreman", "baseUrl": "…", "envKey": "FOREMAN_CODEX_API_KEY" },
      "approvalPolicy": "never",
      "sandbox": "workspace-write",
      "timeoutMs": 300000
    }
  }
}
```

Runtime overrides (constructor options, same precedence rules): `modelEnv.CODEX_API_KEY`
/ `modelEnv.CODEX_BASE_URL` — env-injected into the child process only, never written
to disk (`config.toml` stores the env var *name*, not the value).

## 7. Error handling

- JSON-RPC error responses reject the pending request promise (code + message in the
  error string; `stderrTail` is appended for turn failures).
- Subprocess exit while a turn is pending rejects the pending prompt.
- `shutdown()` is a graceful drain (stdin close, 15 s) with SIGTERM (10 s) and SIGKILL
  fallbacks, matching the other channels' behavior.

## 8. Testing strategy

Per ADR-0008: the channel is tested end-to-end against the **real** `codex` binary
with a local Responses-API fixture endpoint (a scripted external dependency, same
pattern as the dsh tests' mock model) and a real local control plane. See
`test/e2e/codex.e2e.js` — cold start, tool execution, streaming frame mapping, session
resume across sandboxes, artifact publication. No part of foreman itself is mocked.
