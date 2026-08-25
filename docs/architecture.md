# Architecture

Foreman sits between a cloud control plane and a disposable sandbox. The control plane owns
configuration, object storage, and the message bus; foreman owns everything that happens inside
one sandbox lifetime. This document walks the run lifecycle, the integration channels, the event
flow, and the secret model.

```
            cloud                                sandbox (one foreman run)
┌──────────────────────────┐        ┌─────────────────────────────────────────────┐
│  object storage          │        │  Foreman                                    │
│   <agent>/<session>/     │◀──────▶│   prepare()  restore config+workspace+logs  │
│     cordis.yml           │        │   start()    launch dsh + SSE gateway       │
│     workspace.tar.gz     │        │   prompt()   task -> turn/end               │
│     sessions.tar.gz      │        │   collect()  answers, change sets, commits  │
│     checkpoints.json     │        │   publish()  redact+package+upload+reclaim  │
│     checkpoint-N.tar.gz  │        │                                             │
│  message bus             │        │  dsh runtime (stdio | web channel)          │
│  monitoring (OTLP)       │◀───────│  SSE gateway /hitl /events                  │
│  approval system         │───────▶│                                             │
└──────────────────────────┘        └─────────────────────────────────────────────┘
```

## Run lifecycle

The `Foreman` class is a state machine driven by five phases:

1. **`prepare()`** — downloads the composition config (`cordis.yml` for stdio,
   `web-patch.yml` for web) and the workspace seed from object storage, extracts the
   workspace, restores session logs (`sessions.tar.gz`, when present), and takes a manifest
   baseline of the workspace (the "before" state of the authoritative change set). With git
   enabled, it also creates the local workspace repository and a baseline commit; with
   checkpoints enabled, restore replays the incremental pack chain instead of the seed.
2. **`start()`** — spawns dsh over the configured channel with secrets injected via env
   only, opens the SSE gateway (formatter + optional bus delivery), and waits for the
   channel's readiness signal (SDK init handshake, or the `dsh web:` stdout URL line).
   When a trace shipper is configured, dsh's OTLP endpoint is repointed at the shipper's
   local receiver first.
3. **`prompt()`** — sends one task and resolves at `turn/end` (the completion signal).
   The web channel additionally returns whether the session had to be created (`created`
   flag: false on resumed runs).
4. **`collect()`** — gathers the final answer (last assistant message), the manifest
   change set, fs-tool content diffs, session log files, and — with git enabled — this
   turn's commit plus the authoritative `git diff` since baseline (the manifest diff is a
   fallback covering the bash blind spot).
5. **`publish()`** — redacts and packages the workspace (excluding sensitive paths,
   masking secret values), archives session logs and the trace buffer, uploads everything
   to object storage (optionally mirroring through the snapshot sink), syncs the checkpoint
   chain, then emits `run.completed` and `sandbox.reclaim-requested` on the bus. Asynchronous
   wiring (trace shipper, event bus) is flushed before the reclaim event, so nothing produced
   by the run is lost when the sandbox goes away.

Crash path: `kill()` (SIGKILL) skips all cleanup. Pending approvals stay dangling in the
session log; `collect()`/`publish()` still archive the crashed run's state, and the next run's
`prepare()` restores it.

## Channels

Both channels implement one interface — `start({onEvent, ...})`, `prompt(sessionId, text)`,
`respondApproval()`, `shutdown()`, `kill()` — and are selected by the `channel` option.

### SDK channel (`stdio`)

Launches the dsh JSON-RPC demo binary and speaks NDJSON over stdio. The narrow SDK protocol
has no session resume, so the runner bundles a `resume-adapter` plugin: it reroutes
`agents.create` calls for sessionIds with existing persisted logs to `agents.resume`,
enabling cross-sandbox resume. No HITL support.

### Web channel (`web`)

Launches `dsh web --patch <overlay>` and drives the apiproxy: unary calls over
`POST /api/<method>` (ClientRequest envelopes), approval answers over `POST /api/respond`,
and a WebSocket downlink (`/api/events.mux`) for session events and approval frames. Session
resume is native (prompting a cold persisted session triggers the api-remotes agent
resolver). Model-initiated sandbox escalations (`sandbox_permissions` on a tool call) surface
as `approval/requested` frames carrying a stable `rpcId`.

## Event flow

Internal session events fan out through one gateway (`SseGateway`):

```
dsh events ─▶ redactJson(secretValues) ─▶ formatter.push(frame) ─▶ rendered lines
                                                                    ├─ SSE subscribers (GET /events)
                                                                    ├─ message bus (publish)
                                                                    └─ replay buffer (Last-Event-ID)
```

- The **formatter** adapts internal frames into the outbound stream: `native` passes
  session events through; `openai-chat` renders OpenAI `chat.completion.chunk` protocol
  (role first chunk / content deltas / finish chunk / `data: [DONE]`).
- **Delivery** is `sse`, `bus`, or `both`; the bus receives the same adapted stream data
  as SSE subscribers.
- The **replay buffer** stores rendered wire lines, so Last-Event-ID resumption replays
  consistently regardless of format.
- Approval frames (`approval/requested` / `approval/resolved`) ride the same gateway.
  External systems answer via `POST /hitl`; the gateway resolves the pending approval by
  id and forwards the decision to dsh over the web channel.

## Telemetry

Two independent paths, both wired in configuration:

- **Path A** — dsh's native `session-telemetry-otel` plugin exports the session log as
  OTLP records (env-driven: `DSH_TELEMETRY_MODE`, `DSH_TELEMETRY_OTLP_URL`). The
  `telemetry-enrich` plugin (bundled by the runner, configured by the cloud) enriches every
  record through a rule table: flat env sources, literals, dot-path context fields from a
  JSON env variable, hash transforms (pseudonymization), event-type filters, and additivity
  protection against overriding dsh-owned keys. Exporter headers pass tenant identifiers
  through at the transport level.
- **Path B** — foreman's own event-stream forwarding (the SSE gateway + trace buffer),
  redacted before leaving the sandbox.

The optional **trace shipper** puts a local receiver in front of cloud monitoring: dsh
exports to the receiver, and a background queue retries delivery to the upstream with
backoff. Cloud-monitoring outages never block the run; records are delivered eventually,
and `publish()` flushes the queue before reclaim.

## Secret model

Secrets follow a strict "env-injected, never persisted" rule, enforced at three boundaries:

1. **Path exclusion** — sensitive paths (`.env`, credentials, keys, `.git`) are excluded
   from every packaged archive.
2. **Content masking** — `packageWorkspace` and the event gateway replace known secret
   values (exact substrings) with `[REDACTED]` in packaged files and forwarded event
   payloads (`redactJson`).
3. **Git interception** — before each turn commit, staged content is scanned for known
   secret values and secret shapes (regexes such as `sk-...` keys, AWS access keys).
   Matching files are unstaged: they stay out of git history and therefore out of every
   change pack (checkpoint packs are built from commit trees, so interception semantics
   are inherited along the chain). Interception never fails the turn; violations are
   reported in the publish result.

Snapshot-sink credentials are resolved dynamically from env on each call, so credential
rotation does not require restarts and credentials are never captured at construction time.

## Session identity

The external session id doubles as the dsh session id (both `session.create` and
`agents.create` accept caller-provided ids; the JSONL persistence layer escapes path-unsafe
characters). Resume therefore needs no mapping table — but it does need the workspace's
absolute path to match across runs, which is why cloud sandboxes mount workspaces at fixed
points and the tests reuse one `sandboxDir` across runs.

## Testing strategy

All tests are keyless and run against in-process mocks (`test/mocks/`): an
OpenAI-compatible mock model with scripted multi-step behavior, a mock control plane
(object storage + bus), and a mock OTLP collector. Four e2e scenarios drive the assembled
system end to end — basic (cold start + resume), web (HITL + crash recovery), cloud
(shipping/storage/bus/format), checkpoint (incremental chains) — asserting on real
transcripts, uploaded artifacts, and model requests rather than mocks' internals.
