# Foreman

Foreman is a cloud sandbox runner for [DeepSeek Harness (dsh)](../../README.md): it turns a
disposable sandbox into a durable agent execution environment. Every run restores a workspace
and session from object storage, launches the harness inside the sandbox, forwards events and
traces to the cloud in real time, intercepts secrets before anything leaves the sandbox, and
publishes the run's artifacts back to storage — so the system can reclaim the sandbox at any
moment without losing state.

Foreman supports multiple harness backends (DeepSeek Harness `dsh-sdk` / `dsh-web` channels,
**Codex Harness app-server** via ADR-0005, harness-scoped naming per ADR-0009) and multiple
outbound event protocols (native, OpenAI Chat Completions, OpenAI Responses/Codex,
**Anthropic Messages/Claude** via ADR-0006), all selected via configuration file with no
code changes.

## Feature highlights

- **Multi-harness channels** — drive DeepSeek Harness (`dsh-sdk`: SDK JSON-RPC over NDJSON
  stdio; `dsh-web`: web apiproxy) or Codex Harness (`codex`: app-server JSON-RPC over stdio)
  with the same orchestrator. The channel is selected per run via configuration; channel ids
  are harness-scoped (ADR-0009), with legacy `stdio`/`web` aliases accepted.
- **Cross-sandbox session resume** — the external session id doubles as the harness session id.
  Session logs are archived per run and restored on the next `prepare()`, so a new sandbox
  continues the conversation with full history (workspace absolute paths must match — cloud
  sandboxes use fixed mount points).
- **Workspace checkpoint chains** — all changes after initialization are packed as
  incremental change packs ("full first pack + incremental pack chain"). A skip-list-style
  tiered retention policy balances pack count against pack size, and periodic rebaselines
  bound the restore chain length. See [docs/checkpoint-design.md](docs/checkpoint-design.md).
- **Overlap scheduling (ADR-0011)** — run-lifecycle I/O overlaps at the session
  boundaries: `prepare()` downloads independent objects concurrently, and `publish()`
  runs one concurrent fan-out (artifact packaging ∥ checkpoint pack builds/uploads ∥
  the artifact upload batch; pack content comes from immutable git commits, so
  concurrency cannot tear a pack). The earlier per-turn background sync (ADR-0010 3a)
  was removed as disproportionate complexity. See
  [bench/run-pipeline.bench.js](bench/run-pipeline.bench.js).
- **Secret interception, three layers** — path exclusion (`.env` and friends never packaged),
  content masking (`[REDACTED]` replaces secret values in packaged files and forwarded event
  streams), and git pre-commit scanning (secret-looking files are unstaged — they stay out of
  git history and therefore out of every change pack). Secrets reach the sandbox via env
  injection only and are never written to disk.
- **Real-time event forwarding** — an outbound gateway exposes the run as SSE (`GET /events`,
  with `Last-Event-ID` resumption), can publish the same adapted stream onto a message bus,
  and renders it through a **generalized protocol adapter layer**: a registry of
  self-contained dialect adapters (`native`, OpenAI `chat.completion.chunk`,
  `openai-responses`/`codex`, `anthropic-messages`/`claude`), selected per run via a config
  file and extendable without core changes. See
  [docs/design/sse-protocol-adapter.md](docs/design/sse-protocol-adapter.md).
- **HITL approvals over the wire** — the web channel forwards approval requests as SSE frames
  and accepts decisions via `POST /hitl`; a hard crash leaves the approval dangling in the log,
  and the resumed run synthesizes a `TOOL_OUTCOME_UNKNOWN` tool result instead of replaying it.
- **Dual trace paths** — path A: harness's native OTLP telemetry exported to cloud monitoring;
  path B: foreman's own event-stream forwarding (redacted). An optional async trace shipper
  isolates cloud-monitoring failures with retry and eventual delivery.
- **Storage abstraction** — a snapshot sink interface (local / object-store) resolves
  credentials dynamically from env on each call, so token rotation never requires a restart.

## Channels

Canonical channel ids are harness-scoped (ADR-0009); legacy aliases `stdio` / `web` are
accepted and map to `dsh-sdk` / `dsh-web`.

| Channel | Transport | Harness | Session resume | HITL |
|---|---|---|---|---|
| `dsh-sdk` | SDK JSON-RPC over NDJSON stdio | DeepSeek Harness | via the bundled resume-adapter plugin | — |
| `dsh-web` | dsh web apiproxy (HTTP + WebSocket) | DeepSeek Harness | native (cold persisted sessions) | full support |
| `codex` | JSON-RPC 2.0-lite over stdio JSONL | Codex Harness app-server | via `thread/resume` (verified against codex-cli 0.149.1) | planned |

All channels share one `Foreman` orchestrator; the composition config (`cordis.yml` /
`web-patch.yml` / `foreman.config.json`) is owned by the cloud and delivered through object
storage.

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
      codex-channel.js    Codex Harness app-server driver (JSON-RPC over stdio)
    events/
      formats.js          outbound event formatter façade (delegates to the registry)
      protocols/          protocol adapter registry + built-in dialects
        registry.js         id/alias resolution; external adapters register here
        native.js           verbatim foreman frames
        openai-chat.js      OpenAI Chat Completions streaming chunks
        openai-responses.js OpenAI Responses API events (alias: codex)
        anthropic-messages.js Anthropic Messages API events (alias: claude)
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
    run-pipeline.bench.js session-boundary overlap A/B benchmark on a real run (ADR-0011)
  test/
    unit.test.js          unit tests (retention, redaction, formats, packs, bus, sink)
    protocols.test.js     golden-transcript conformance tests for every adapter
    gateway-wire.test.js  real-HTTP wire tests for the SSE gateway
    inbound-parse.test.js parse-direction conformance tests — proposed
    channels/             channel integration tests
      codex-channel.test.js
    e2e/                  end-to-end scenarios (see below)
    fixtures/             scripted harness fixtures (codex Responses endpoint)
    mocks/                mock control plane / model / OTLP collector
  cordis.yml              stdio-channel composition (cloud-owned)
  web-patch.yml           web-channel patch overlay (cloud-owned)
  ROADMAP.md              implemented boundary, capability boundary, TODOs
```

## Quick start

Requirements: Node.js >= 22.19, a built harness repository, and the `git` binary in PATH.

```sh
# from foreman/solution
pnpm test                 # unit + protocol conformance + wire tests
pnpm run bench            # protocol pipeline benchmark (real loopback HTTP)
pnpm run bench:quick      # same, smaller workload
pnpm run bench:pipeline   # session-boundary overlap A/B benchmark (ADR-0011; real codex channel)
pnpm test:e2e:basic       # cold start + session resume (dsh-sdk channel)
pnpm test:e2e:web         # HITL approvals + crash/dangling-approval recovery (dsh-web channel)
pnpm test:e2e:cloud       # trace shipping, snapshot sink, event bus, format adaptation
pnpm test:e2e:checkpoint  # 7-round incremental checkpoint chain + retention + rebase (dsh-sdk)
pnpm test:e2e:codex       # cold start + cross-sandbox resume (codex channel)
pnpm test:e2e:checkpoint:codex  # checkpoint chain stress on the codex channel (retention/rebase/restore)
pnpm test:e2e:config      # config-only channel switching acceptance (codex full run + dsh resolution)
```

All tests are keyless: they run against in-process mocks of the model endpoint, the control
plane, and the OTLP collector. The dsh e2e scenarios additionally require the harness
repository checkout one level above this one; the codex e2e only needs the `codex` binary
(codex-cli) in PATH and is otherwise self-contained.

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
`codex`, `anthropic-messages` / `claude`); precedence is constructor options > config file >
defaults. New dialects are added by registering an adapter module — see the
[protocol adapter design](docs/design/sse-protocol-adapter.md) and
[ROADMAP](ROADMAP.md#4-how-to-extend).

### Harness channel selection via config file

The inbound harness channel is selected per run via configuration (ADR-0005):

```json
{
  "harness": {
    "channel": "codex",
    "codex": {
      "binary": "codex",
      "model": "gpt-5.1-codex",
      "approvalPolicy": "never"
    }
  }
}
```

`harness.channel` accepts `'dsh-sdk'` (default, DeepSeek Harness SDK), `'dsh-web'`
(DeepSeek Harness web apiproxy), or `'codex'` (Codex Harness app-server); legacy aliases
`'stdio'` / `'web'` remain accepted (ADR-0009).

### Driving the runner programmatically

```js
import { Foreman } from '@deepseek-ai/foreman'

const foreman = new Foreman({
  repoRoot,                          // harness repository root
  workdir,                           // isolated run directory (fixed path across runs)
  channel: 'dsh-web',                // or 'dsh-sdk', 'codex' (legacy: 'stdio', 'web')
  agentId, sessionId,                // object-storage coordinates + session identity
  modelEnv: { DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL },  // env-injected only
  controlPlane: { baseUrl },         // artifact storage + message bus
  telemetry: { mode: 'FULL', otlpUrl },
  git: { enabled: true },            // workspace git + secret interception
  checkpoints: { recentKeep: 2, perLevel: 1, rebaseAfter: 6 },
})

await foreman.prepare()              // restore config + workspace + session logs
await foreman.start()                // launch harness + open the SSE gateway
const { reason } = await foreman.prompt('do the task')
await foreman.shutdown()             // graceful (or foreman.kill() to simulate a crash)
await foreman.collect()              // final answer, change sets, session logs
await foreman.publish()              // redact + package + upload + reclaim event
```

## Protocol adapters

Foreman ships with a protocol adapter layer (ADR-0001) that transforms internal event
frames into external wire formats. The registry-based design makes adding new protocols
a matter of writing a single module.

| Adapter | Id | Aliases | Protocol |
|---|---|---|---|
| Native | `native` | — | Verbatim foreman frames (lossless) |
| OpenAI Chat | `openai-chat` | — | Chat Completions streaming chunks |
| OpenAI Responses | `openai-responses` | `codex` | Responses API streaming events |
| Anthropic Messages | `anthropic-messages` | `claude` | Messages API streaming events |

## Documentation

- [Roadmap](ROADMAP.md) — implemented boundary, capability boundary, and TODOs.
- [Architecture](docs/architecture.md) — run lifecycle, channels, event flow, secret model.
- [Checkpoint design](docs/checkpoint-design.md) — change packs, skip-list retention, rebaseline.
- [SSE protocol adapter design](docs/design/sse-protocol-adapter.md) — adapter contract, data
  model, registry mechanics.
- [Codex channel design](docs/design/codex-channel.md) — Codex Harness app-server integration
  (ADR-0005), JSON-RPC lifecycle, frame mapping.
- [Anthropic Messages design](docs/design/anthropic-messages-protocol.md) — Claude Code
  protocol adapter (ADR-0006), event mapping, content block model.
- Architecture Decision Records ([docs/adr/](docs/adr/)):
  - [ADR-0001](docs/adr/0001-generic-sse-protocol-adapter-layer.md) — generic SSE protocol
    adapter layer.
  - [ADR-0002](docs/adr/0002-runner-config-file.md) — runner config file and protocol
    selection.
  - [ADR-0003](docs/adr/0003-openai-responses-protocol.md) — the `openai-responses`
    (codex) dialect.
  - [ADR-0004](docs/adr/0004-hermetic-tests-and-benchmarks.md) — hermetic testing and
    benchmark strategy.
  - [ADR-0005](docs/adr/0005-codex-app-server-channel.md) — Codex Harness app-server
    channel integration.
  - [ADR-0006](docs/adr/0006-anthropic-messages-protocol.md) — Anthropic Messages
    (Claude Code) protocol adapter.
  - [ADR-0007](docs/adr/0007-inbound-protocol-adaptation.md) — inbound protocol adaptation
    (generalized parse direction).
  - [ADR-0008](docs/adr/0008-harness-protocol-testing-and-benchmarks.md) — independent
    testing and benchmark strategy for harness protocols.
  - [ADR-0009](docs/adr/0009-channel-naming.md) — harness-scoped channel naming
    (`dsh-sdk` / `dsh-web` / `codex`).
  - [ADR-0010](docs/adr/0010-overlap-scheduling.md) — overlap scheduling for run-lifecycle
    I/O, with the critical-path model and measured verification.
  - [ADR-0011](docs/adr/0011-session-boundary-overlap-scheduling.md) — session-boundary
    overlap scheduling (simplification; supersedes ADR-0010's per-turn background sync).

## License

MIT.