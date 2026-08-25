# Foreman

Foreman is a cloud sandbox runner for [DeepSeek Harness (dsh)](../../README.md): it turns a
disposable sandbox into a durable agent execution environment. Every run restores a workspace
and session from object storage, launches dsh inside the sandbox, forwards events and traces
to the cloud in real time, intercepts secrets before anything leaves the sandbox, and publishes
the run's artifacts back to storage — so the system can reclaim the sandbox at any moment
without losing state.

## Feature highlights

- **Cross-sandbox session resume** — the external session id doubles as the dsh session id.
  Session logs are archived per run and restored on the next `prepare()`, so a new sandbox
  continues the conversation with full history (workspace absolute paths must match — cloud
  sandboxes use fixed mount points).
- **Workspace checkpoint chains** — all changes after initialization are packed as
  incremental change packs ("full first pack + incremental pack chain"). A skip-list-style
  tiered retention policy balances pack count against pack size, and periodic rebaselines
  bound the restore chain length. See [docs/checkpoint-design.md](docs/checkpoint-design.md).
- **Secret interception, three layers** — path exclusion (`.env` and friends never packaged),
  content masking (`[REDACTED]` replaces secret values in packaged files and forwarded event
  streams), and git pre-commit scanning (secret-looking files are unstaged — they stay out of
  git history and therefore out of every change pack). Secrets reach the sandbox via env
  injection only and are never written to disk.
- **Real-time event forwarding** — an outbound gateway exposes the run as SSE (`GET /events`,
  with `Last-Event-ID` resumption), can publish the same adapted stream onto a message bus,
  and renders it through a **generalized protocol adapter layer**: a registry of
  self-contained dialect adapters (`native`, OpenAI `chat.completion.chunk`,
  `openai-responses`/`codex`), selected per run via a config file and extendable without
  core changes. See [docs/design/sse-protocol-adapter.md](docs/design/sse-protocol-adapter.md).
- **HITL approvals over the wire** — the web channel forwards approval requests as SSE frames
  and accepts decisions via `POST /hitl`; a hard crash leaves the approval dangling in the log,
  and the resumed run synthesizes a `TOOL_OUTCOME_UNKNOWN` tool result instead of replaying it.
- **Dual trace paths** — path A: dsh's native OTLP telemetry exported to cloud monitoring;
  path B: foreman's own event-stream forwarding (redacted). An optional async trace shipper
  isolates cloud-monitoring failures with retry and eventual delivery.
- **Storage abstraction** — a snapshot sink interface (local / object-store) resolves
  credentials dynamically from env on each call, so token rotation never requires a restart.

## Channels

| Channel | Transport | Session resume | HITL |
|---|---|---|---|
| `stdio` | SDK JSON-RPC over NDJSON stdio | via the bundled resume-adapter plugin | — |
| `web` | dsh web apiproxy (HTTP + WebSocket) | native (cold persisted sessions) | full support |

Both channels share one `Foreman` orchestrator; the composition config (`cordis.yml` /
`web-patch.yml`) is owned by the cloud and delivered through object storage.

## Repository layout

```
solution/
  src/
    foreman.js            the orchestrator: prepare/start/prompt/collect/publish
    control-plane.js      control-plane client (artifact storage + message bus)
    core/
      checkpoint.js       change packs + skip-list retention (CheckpointKeeper)
      git-workspace.js    local workspace git + pre-commit secret interception
      workspace.js        manifests, packaging, archives, fs-change extraction
      redact.js           secret redaction utilities (text/buffer/json)
    channels/
      sdk-channel.js      dsh SDK JSON-RPC driver (stdio)
      web-channel.js      dsh web driver (HTTP + mux WebSocket)
    events/
      formats.js          outbound event formatter façade (delegates to the registry)
      protocols/          protocol adapter registry + built-in dialects
        registry.js         id/alias resolution; external adapters register here
        native.js           verbatim foreman frames
        openai-chat.js      OpenAI Chat Completions streaming chunks
        openai-responses.js OpenAI Responses API events (alias: codex)
      event-bus.js        event bus delivery (memory / http)
    observability/
      trace-shipper.js    async trace shipping with retry + failure isolation
    storage/
      snapshot-sink.js    snapshot storage abstraction (credentials via env)
    config.js             runner config file loader (protocol / delivery selection)
  plugins/
    resume-adapter.mjs    reroutes persisted sessionIds from agents.create to agents.resume
    telemetry-enrich.mjs  deployment-side telemetry attribute pipeline (rule table)
  bench/
    protocol.bench.js     protocol pipeline benchmark (real loopback HTTP, no mocks)
  test/
    unit.test.js          unit tests (retention, redaction, formats, packs, bus, sink)
    protocols.test.js     golden-transcript conformance tests for every adapter
    gateway-wire.test.js  real-HTTP wire tests for the SSE gateway
    e2e/                  end-to-end scenarios (see below)
    mocks/                mock control plane / model / OTLP collector
  cordis.yml              stdio-channel composition (cloud-owned)
  web-patch.yml           web-channel patch overlay (cloud-owned)
  ROADMAP.md              implemented boundary, capability boundary, TODOs
```

## Quick start

Requirements: Node.js >= 22.19, a built dsh repository (`pnpm install && pnpm run build` at the
repo root), and the `git` binary in PATH.

```sh
# from foreman/solution
pnpm test                 # unit + protocol conformance + wire tests
pnpm run bench            # protocol pipeline benchmark (real loopback HTTP)
pnpm run bench:quick      # same, smaller workload
pnpm test:e2e:basic       # cold start + session resume (stdio channel)
pnpm test:e2e:web         # HITL approvals + crash/dangling-approval recovery (web channel)
pnpm test:e2e:cloud       # trace shipping, snapshot sink, event bus, format adaptation
pnpm test:e2e:checkpoint  # 7-round incremental checkpoint chain + retention + rebase
```

All tests are keyless: they run against in-process mocks of the model endpoint, the control
plane, and the OTLP collector.

### Protocol selection via config file

The outbound SSE dialect is a config decision (ADR-0002). Point the runner at a
`foreman.config.json` (constructor `configPath` option or the `FOREMAN_CONFIG` env var) and
switch dialects without code changes — a typo'd protocol fails loud, it never silently
degrades to the native stream:

```json
{
  "events": {
    "protocol": "openai-responses",
    "delivery": "sse"
  }
}
```

`protocol` accepts any registered id or alias (`native`, `openai-chat`, `openai-responses` /
`codex`); precedence is constructor options > config file > defaults. New dialects are added
by registering an adapter module — see the
[protocol adapter design](docs/design/sse-protocol-adapter.md) and
[ROADMAP](ROADMAP.md#4-how-to-extend).

### Driving the runner programmatically

```js
import { Foreman } from '@deepseek-ai/foreman'

const foreman = new Foreman({
  repoRoot,                          // dsh repository root
  workdir,                           // isolated run directory (fixed path across runs)
  channel: 'web',                    // or 'stdio'
  agentId, sessionId,                // object-storage coordinates + session identity
  modelEnv: { DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL },  // env-injected only
  controlPlane: { baseUrl },         // artifact storage + message bus
  telemetry: { mode: 'FULL', otlpUrl },
  git: { enabled: true },            // workspace git + secret interception
  checkpoints: { recentKeep: 2, perLevel: 1, rebaseAfter: 6 },
})

await foreman.prepare()              // restore config + workspace + session logs
await foreman.start()                // launch dsh + open the SSE gateway
const { reason } = await foreman.prompt('do the task')
await foreman.shutdown()             // graceful (or foreman.kill() to simulate a crash)
await foreman.collect()              // final answer, change sets, session logs
await foreman.publish()              // redact + package + upload + reclaim event
```

## Documentation

- [Roadmap](ROADMAP.md) — implemented boundary, capability boundary, and TODOs.
- [Architecture](docs/architecture.md) — run lifecycle, channels, event flow, secret model.
- [Checkpoint design](docs/checkpoint-design.md) — change packs, skip-list retention, rebaseline.
- [SSE protocol adapter design](docs/design/sse-protocol-adapter.md) — adapter contract, data
  model, registry mechanics.
- Architecture Decision Records ([docs/adr/](docs/adr/)):
  - [ADR-0001](docs/adr/0001-generic-sse-protocol-adapter-layer.md) — generic SSE protocol
    adapter layer.
  - [ADR-0002](docs/adr/0002-runner-config-file.md) — runner config file and protocol
    selection.
  - [ADR-0003](docs/adr/0003-openai-responses-protocol.md) — the `openai-responses`
    (codex) dialect.
  - [ADR-0004](docs/adr/0004-hermetic-tests-and-benchmarks.md) — hermetic testing and
    benchmark strategy.

## License

MIT.
