# Codex Harness app-server channel — Design

This document is the normative design for the Codex Harness integration channel:
interfaces, data models, and mechanisms. Decisions and rationale live in
[ADR-0005](../adr/0005-codex-app-server-channel.md); this document specifies
*how* it is realized.

## 1. Overview

```
                    ┌────────────────────────────────────────────────────┐
  Foreman           │ CodexChannel                                      │
  orchestrator ──▶  │  spawns codex app-server --stdio                  │
                    │  JSON-RPC 2.0-lite over stdin/stdout (JSONL)      │
                    │                                                   │
                    │  initialize → initialized → thread/start          │
                    │  → turn/start → item/* notifications              │
                    │  → turn/completed → (next turn or close)          │
                    │                                                   │
                    │  onEvent callback → internal frames                │
                    │  onStatus callback → channel status updates        │
                    └────────────────────────────────────────────────────┘
                              │
                              ▼
                    SseGateway (outbound adapters)
                    native | openai-chat | openai-responses | anthropic-messages
```

## 2. Transport

### 2.1 Subprocess launch

The channel spawns `codex app-server --stdio` (or the configured binary path) as a
child process:

```
const proc = spawn(binary, args, {
  stdio: ['pipe', 'pipe', 'pipe'],
  cwd: workspaceDir,
  env: { ...process.env, CODEX_HOME, ...modelEnv },
})
```

- **stdin**: JSON-RPC requests (client → server), one JSON object per line.
- **stdout**: JSON-RPC responses and notifications (server → client), one JSON object
  per line.
- **stderr**: diagnostic logs (never parsed as protocol; forwarded to foreman's logger).

### 2.2 Line framing

Each message is a complete JSON object on a single line (JSONL). The channel reads
stdout line-by-line using a readline interface. The recommended max line size is 10 MB
(per the Codex app-server spec).

### 2.3 Transport options

| Option | Flag | Use case |
|---|---|---|
| stdio (default) | `--stdio` | Subprocess embedding (v0.136+) |
| WebSocket | `--listen ws://127.0.0.1:PORT` | Remote dashboard, multiple clients |
| Unix socket | (no flags) | TUI and internal CLI processes |

The initial implementation supports only `--stdio`. WebSocket and Unix socket are
deferred (tracked in the roadmap).

## 3. Protocol lifecycle

### 3.1 Initialization handshake

Required message order (every connection):

```
→ {"id": 0, "method": "initialize", "params": {
    "clientInfo": { "name": "foreman", "version": "0.1.0" },
    "capabilities": {}
  }}
← {"id": 0, "result": { "serverInfo": { ... } }}

→ {"method": "initialized", "params": {}}
```

The `initialized` notification is sent immediately after receiving the `initialize`
response. The server rejects any request before this handshake completes.

### 3.2 Thread creation

```
→ {"id": 1, "method": "thread/start", "params": {
    "model": "gpt-5.1-codex",
    "cwd": "/workspace",
    "approvalPolicy": "never"
  }}
← {"id": 1, "result": { "thread": { "id": "thr_..." } }}
```

On session resume (`thread/resume`):

```
→ {"id": 1, "method": "thread/resume", "params": {
    "threadId": "thr_...",
    "cwd": "/workspace"
  }}
← {"id": 1, "result": { "thread": { "id": "thr_..." } }}
```

### 3.3 Turn start

```
→ {"id": 2, "method": "turn/start", "params": {
    "threadId": "thr_...",
    "input": [{ "type": "text", "text": "do the task" }],
    "cwd": "/workspace"
  }}
← {"id": 2, "result": { "turn": { "id": "turn_..." } }}
```

### 3.4 Streaming notifications (server → client, no `id`)

After `turn/start`, the server emits zero or more notifications on stdout:

```
← {"method": "item/started", "params": { "item": { "id": "item_...", "type": "agentMessage", ... } }}
← {"method": "item/agentMessage/delta", "params": { "itemId": "item_...", "delta": { "type": "text", "text": "Hello" } }}
← {"method": "item/completed", "params": { "item": { "id": "item_...", "type": "agentMessage", ... } }}
← {"method": "turn/completed", "params": { "turn": { "id": "turn_...", "status": "completed" } }}
```

### 3.5 Turn lifecycle

```
turn/start
  → item/started (agentMessage)
  → item/agentMessage/delta (text)   [zero or more]
  → item/agentMessage/delta (text)
  → item/completed (agentMessage)
  → item/started (toolUse)
  → item/toolUse/delta
  → item/completed (toolUse)
  → item/started (toolResult)
  → item/completed (toolResult)
  → turn/completed
```

## 4. Internal frame mapping

Each Codex notification is mapped to the internal frame model that the outbound
protocol adapters consume.

### 4.1 Item types

| Codex notification | Internal frame | Notes |
|---|---|---|
| `item/agentMessage/delta` (text) | `assistant/chunk` | `data.chunk.text = delta.text` |
| `item/agentMessage/complete` (no prior deltas) | `assistant/message` | Fallback for replay-derived messages |
| `item/toolUse/started` | `tool/call` | `data.name`, `data.arguments`, `data.callId` |
| `item/toolResult/started` | `tool/result` | `data.callId`, `data.meta.diffs` |
| `item/approval/requested` | `approval/requested` | Web channel HITL equivalent |
| `item/approval/resolved` | `approval/resolved` | Web channel HITL equivalent |
| `turn/completed` | `turn/end` | `data.reason.kind` from `status` field |

### 4.2 Turn identity

The Codex `turn_id` maps to the internal `turn` number (monotonically increasing per
thread). The external `sessionId` = `thread_id` (same as the dsh channels use
`sessionId` as the dsh session id).

## 5. Session resume

The Codex channel supports session resume via `thread/resume`:

1. `prepare()` downloads the persisted session data (JSONL files from the Codex
   thread store) and restores them to `CODEX_HOME/threads/`.
2. `start()` calls `thread/resume` with the stored `threadId` instead of
   `thread/start`.
3. The Codex app-server reloads the thread history from the local store and
   continues the conversation.

This requires the `CODEX_HOME` directory to be persisted across runs (stored in
the workspace or session archive).

## 6. Configuration

The `CodexChannel` options are nested under `harness.codex` in the foreman config:

```json
{
  "harness": {
    "channel": "codex",
    "codex": {
      "binary": "codex",
      "args": ["app-server", "--stdio"],
      "model": "gpt-5.1-codex",
      "approvalPolicy": "never",
      "timeoutMs": 300000
    }
  }
}
```

The `Foreman` constructor passes these through to `CodexChannel` when
`channel === 'codex'`.

## 7. Error handling

### 7.1 Protocol errors

JSON-RPC error responses are mapped to channel errors:

```json
← { "id": 2, "error": { "code": -32603, "message": "Internal error" } }
```

- `code` -32603 (Internal error): retryable (turn is abandoned, channel is still valid).
- `code` -32000 (Not initialized): fatal (channel must be restarted).
- `code` -32601 (Method not found): fatal (wrong Codex version or binary).

### 7.2 Timeout

If `turn/start` does not receive a `turn/completed` within `timeoutMs`, the channel
sends `turn/interrupt` (if available) or kills the subprocess. The prompt promise
rejects with a timeout error.

### 7.3 Subprocess crash

If the `codex` subprocess exits unexpectedly, the channel sets the status to
`crashed` and rejects any pending prompt promise. `shutdown()` returns the exit code.
`publish()` still archives whatever state was collected.