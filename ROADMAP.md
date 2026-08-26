# Foreman Roadmap

This document tracks **what is implemented**, where the **capability boundaries** are, and
**what comes next**. It is updated as work lands; the date of the latest revision is kept at
the bottom. Companion documents: [README.md](README.md) (overview),
[docs/architecture.md](docs/architecture.md) (run lifecycle), ADRs in [docs/adr/](docs/adr/)
(decision records), [docs/design/sse-protocol-adapter.md](docs/design/sse-protocol-adapter.md)
(protocol adapter design), [docs/design/codex-channel.md](docs/design/codex-channel.md)
(Codex channel design), [docs/design/anthropic-messages-protocol.md](docs/design/anthropic-messages-protocol.md)
(Anthropic adapter design).

---

## 1. Implemented boundary — what works today

Everything below is exercised by the test suite and/or the e2e scenarios; nothing is claimed
without a test.

### Run lifecycle (orchestrator)

- `prepare()` — fetch run composition (config) + workspace snapshot from object storage,
  decompress, place files, restore session logs for cross-sandbox session resume.
- `start()` — launch dsh (`dsh-sdk` / `dsh-web`) or codex (`codex`), open the outbound
  SSE gateway.
- `prompt()` — drive a user turn; streaming events published to the gateway in real time.
- `collect()` — final answer, change sets, session logs.
- `shutdown()` / `kill()` — graceful vs. simulated-crash paths (dangling HITL approvals
  become `TOOL_OUTCOME_UNKNOWN` tool results on resume).
- `publish()` — redact, package (compress), upload the workspace snapshot, emit the
  sandbox-reclaimable notification.

### Overlap scheduling (ADR-0010)

- Per-turn background checkpoint sync: each turn's pack is built and uploaded while the
  next turn executes; `publish()` drains the chain before the retention sync (no reclaim
  with a sync in flight; the index is written only after the drain).
- Concurrent transfers: workspace packs and the session archive download in parallel
  during `prepare()`; the publish artifact batch uploads in parallel.
- Measured (`bench/run-pipeline.bench.js`, real codex channel, 5 turns × 8 MiB):
  2 065 ms saved per run (12.1% sandbox occupancy), 85% of the critical-path
  projection — see ADR-0010's verification section.

### Channels

Canonical ids per ADR-0009 (legacy aliases `stdio` / `web` accepted):

| Channel | State | Notes |
|---|---|---|
| `dsh-sdk` | implemented | dsh SDK JSON-RPC over NDJSON stdio; session resume via the bundled resume-adapter plugin |
| `dsh-web` | implemented | dsh web apiproxy (HTTP + WebSocket); native persisted sessions; full HITL |
| `codex` | **implemented** | Codex Harness app-server JSON-RPC over stdio; `CodexChannel` in `src/channels/codex-channel.js`; cross-sandbox resume via `thread/resume` (verified against codex-cli 0.149.1) |

### Outbound event layer (generalized protocol adaptation, ADR-0001)

- **Protocol registry** (`src/events/protocols/registry.js`) — adapters are self-contained
  modules `{ id, aliases, title, description, create(options) }`; external code can register
  more before wiring a gateway; unknown ids fail loud with the list of available protocols.
- **Built-in adapters**:
  - `native` — verbatim foreman frames (one wire data event per internal frame).
  - `openai-chat` — OpenAI Chat Completions streaming chunks (`chat.completion.chunk`),
    role-first chunk, text deltas, final finish chunk, `data: [DONE]`.
  - `openai-responses` (alias `codex`) — OpenAI Responses API streaming event sequence
    (`response.created` → `response.output_text.delta` → … → `response.completed`).
  - `anthropic-messages` (alias `claude`) — **implemented** (ADR-0006): Anthropic Messages API
    streaming events (`message_start` → `content_block_delta` → `message_stop`).
- **SSE gateway** — `GET /events`, per-event ids, `Last-Event-ID` resumption via replay
  buffer, graceful close that flushes subscribers before the server stops.
- **Event bus delivery** — same adapted stream publishable to a message bus
  (`memory` for tests, `http` relay with queue + retry backoff; publishing never throws).
- **Config-driven protocol switching** (ADR-0002) — `foreman.config.json` selects the
  protocol/dialect and delivery mode; precedence: constructor options > config file > defaults.

### Workspace & checkpointing

- Local git workspace with pre-commit secret interception (secret-looking files never enter
  history, therefore never enter packs); the scan streams files of any size in overlapping
  chunks — large files commit, secrets spanning chunk boundaries are still caught.
- Change packs: full first pack + incremental pack chain; skip-list retention
  (dense near, exponentially sparse far); periodic rebaseline bounds restore chain length;
  merged cross-round packs yield identical trees to stepwise application.
- Secret redaction: path exclusion, content masking (`[REDACTED]`) in packaged files and
  forwarded event streams; env-injected credentials only, never on disk.
- Session archives exclude harness transient scratch (`CODEX_HOME/.tmp`) — the harness is
  still alive at publish time and its in-flight plugin clones would tear the archive.

### Observability & storage

- Trace shipper: async OTLP forwarding with retry + failure isolation (4xx dropped, 5xx
  retried, queue overflow drops oldest).
- Snapshot sink abstraction (local / object-store), credentials resolved from env per call.

### Testing & benchmarking (ADR-0004, hermetic — no mocks in the measured path)

- **Golden-transcript conformance tests** (`test/protocols.test.js`) — every adapter must
  produce the exact expected wire sequence for a fixed transcript; unknown formats fail loud.
- **Real-HTTP wire tests** (`test/gateway-wire.test.js`) — real loopback server + real
  subscriber; asserts framing, ids, `Last-Event-ID` resumption, and close semantics on the
  actual wire bytes, not on formatter return values.
- **E2E scenarios** — basic resume, web HITL + crash recovery, cloud (trace/sink/bus/
  adaptation), 7-round checkpoint chain, codex channel (cold start + cross-sandbox
  `thread/resume` + tool execution + checkpoint restore, driven by the real codex binary
  against a scripted Responses-API endpoint; skips when the binary is absent).
- **Protocol pipeline benchmark** (`npm run bench`) — formatter-only throughput plus
  end-to-end throughput/latency (p50/p95/p99) per protocol over real loopback HTTP, driven
  by the same golden transcripts; results written to `bench/results/`. Integrity gate: the
  benchmark refuses to report numbers unless every emitted event is received (no benchmarking
  a broken pipeline, no derived numbers).
- **Run-pipeline benchmark** (`npm run bench:pipeline`, ADR-0010) — A/B overlap on/off on
  the real codex channel with real git/tar/uploads and a scripted model latency; integrity
  gates: payload presence, pack-sync phase placement, and bit-for-bit restore from the
  published index; verifies the critical-path performance model against the observed
  saving.

Reference figures (this sandbox, `--quick`, median of 3 runs — indicative, not a contract):

| Protocol | formatter-only | end-to-end | latency p50/p99 |
|---|---|---|---|
| `native` | ~14M frames/s | ~331K frames/s | 2.4 / 2.5 ms |
| `openai-chat` | ~1.5M frames/s | ~246K frames/s | 1.7 / 2.0 ms |
| `openai-responses` | ~1.0M frames/s | ~171K frames/s | 3.1 / 4.6 ms |

### Documentation

- ADRs 0001–0010 (adapter layer, config file, codex dialect, hermetic testing, Codex channel,
  Anthropic Messages, inbound adaptation, harness protocol testing, channel naming, overlap
  scheduling).
- Design docs: SSE protocol adapter, Codex channel, Anthropic Messages protocol.
- Architecture + checkpoint design docs; this roadmap.

---

## 2. Capability boundary — what the system does NOT do (yet)

Deliberate limits of the current implementation; each maps to a roadmap item below.

1. **Inbound adaptation is not generalized.** Only the *outbound* event stream is
   protocol-adapted. Inbound requests (prompt submission, HITL decisions) still speak the
   native/foreman HTTP shape per channel. ADR-0007 proposes the generalized parse direction.
2. **Protocol adapters cover chat-shaped text and tool calls only.** `openai-chat`/
   `openai-responses`/`anthropic-messages` emit text deltas, tool calls and turn/end events.
   Richer payloads (images, thinking blocks, structured outputs, reasoning summaries) pass
   through as native frames or are dropped per adapter rules — they are not translated.
3. **No Anthropic / Google / other dialects are implemented yet.** The registry is general,
   but only four adapters ship today. Google AI (Gemini) streaming protocol and other
   dialects are not yet covered.
4. **SSE gateway is single-node, in-memory.** Replay buffer lives in the process; a gateway
   restart loses resumability (events are still durable via trace/event-bus paths).
5. **Overlap scheduling is single-session only.** Overlaps happen within one run's
   lifecycle (pack sync during execution, concurrent transfers). There is no
   cross-session scheduler yet — no pre-restoring sandbox B's workspace while sandbox A
   executes (revisit when sandboxes are pooled; ADR-0010 option 2).
6. **Adapters are output-only modules.** There is no inbound parser (wire → internal frame);
   a "full duplex" protocol adapter (needed for a transparent proxy mode) does not exist.
   ADR-0007 proposes the generalized parse direction.
7. **Config file is read once at startup.** No hot reload / SIGHUP re-configuration.
8. **No auth on the SSE gateway.** The gateway assumes it sits on a trusted internal
   network (behind the platform's ingress); no per-subscriber tokens.
9. **No Claude Code Remote (WebSocket) protocol support.** The reverse-engineered
   dual-channel protocol (WebSocket streaming + HTTP REST) is not covered.

---

## 3. Roadmap

### Done (this phase)

- [x] Generic SSE protocol adapter layer + registry (ADR-0001, design doc)
- [x] Runner config file with protocol/dialect switching (ADR-0002)
- [x] `openai-chat` adapter (Chat Completions streaming chunks)
- [x] `openai-responses` / `codex` adapter (ADR-0003)
- [x] Hermetic conformance tests (golden transcripts, real-HTTP wire tests) (ADR-0004)
- [x] Protocol pipeline benchmark with integrity gate (ADR-0004)
- [x] Gateway graceful close (subscriber flush before server close)
- [x] ADR-0005: Codex Harness app-server channel design (design doc)
- [x] ADR-0006: Anthropic Messages protocol adapter design (design doc)
- [x] ADR-0007: Inbound protocol adaptation (generalized parse direction)
- [x] ADR-0008: Testing and benchmark strategy for harness protocols
- [x] **`CodexChannel` implementation** (ADR-0005) — new channel class for Codex Harness
      app-server JSON-RPC over stdio, with initialization handshake, thread/turn lifecycle,
      and internal frame mapping (`src/channels/codex-channel.js`).
- [x] **Codex channel verified end-to-end** — e2e scenario (real codex-cli 0.149.1 binary,
      scripted Responses endpoint): cold start, tool execution, cross-sandbox
      `thread/resume`, checkpoint chain restore, publish — `pnpm test:e2e:codex` (26/26).
- [x] **Harness-scoped channel naming** (ADR-0009) — `dsh-sdk` / `dsh-web` / `codex`
      canonical ids; legacy `stdio` / `web` aliases accepted; config surface validated
      against the canonical list.
- [x] **Overlap scheduling** (ADR-0010) — per-turn background checkpoint sync + concurrent
      prepare/publish transfers; measured on the real codex channel (5 turns × 8 MiB):
      2 065 ms saved per run (12.1% sandbox occupancy), 85% of the projected critical-path
      saving — `bench/run-pipeline.bench.js`.
- [x] **`anthropic-messages` adapter** (ADR-0006) — registered adapter, golden-transcript
      conformance tests, wire-level tests, and benchmark integration
      (`src/events/protocols/anthropic-messages.js`).
- [x] **SSE `event:` line support** — `renderSseLine` extended with optional named events
      for the Anthropic SSE protocol format.

### Next up (short term)

- [ ] **Inbound protocol adaptation** (ADR-0007) — generalize the adapter contract with a
      `parse(wireChunk) → internal frames` direction, starting with `openai-chat` and
      `anthropic-messages`, so the gateway can accept protocol-native prompt submissions.
- [ ] **Codex channel HITL** — approval requests are not yet forwarded as SSE frames with
      `POST /hitl` decisions (approval policy is currently set to a fixed value per run).
- [ ] **Gateway authentication** — per-subscriber token/auth on `GET /events` and
      `POST /hitl`, credentials injected via env like all other secrets.

### Later (medium term)

- [ ] **Transparent proxy mode** — full-duplex adaptation (inbound parse + outbound
      format) so foreman can sit invisibly between a protocol-native client and the harness.
- [ ] **Hot config reload** — re-read `foreman.config.json` between runs (or on SIGHUP)
      without restarting the harness.
- [ ] **Durable replay buffer** — move the SSE replay log to disk / object storage so a
      gateway restart can still honor `Last-Event-ID`.
- [ ] **Structured outputs & richer payloads in adapters** — images, tool-call arguments
      streaming, reasoning summaries across dialects.
- [ ] **Claude Code Remote (WebSocket) protocol** — support the reverse-engineered
      dual-channel protocol (WebSocket + HTTP REST) as an additional channel.
- [ ] **Benchmark regression gates in CI** — fail the build on throughput/latency
      regressions beyond tolerance (with environment-noise allowances).
- [ ] **Codex channel WebSocket transport** — support `--listen ws://` in addition to
      `--stdio` for the Codex channel.

### Under consideration

- Multi-subscriber fan-out benchmarks (N concurrent SSE subscribers per gateway).
- Backpressure policy for slow SSE subscribers (currently unbounded buffering per
  subscriber until socket-level flow control).
- gRPC / WebSocket outbound carriers in addition to SSE and the event bus.
- Google AI (Gemini) streaming protocol adapter.

---

## 4. How to extend

### Adding a new outbound protocol (no core changes required)

1. Write an adapter module `{ id, aliases, title, description, create(options) }`
   implementing `push(frame) → EventOut[]` and `flush() → EventOut[]`
   (see [docs/design/sse-protocol-adapter.md](docs/design/sse-protocol-adapter.md)).
2. Register it — `registerProtocol(adapter)` (or add to the built-in list).
3. Add a golden-transcript case in `test/protocols.test.js` and a wire case in
   `test/gateway-wire.test.js`.
4. Select it via `foreman.config.json` → `events.protocol`.

### Adding a new inbound channel (no core changes required)

1. Write a channel class implementing the channel interface
   (`start({onEvent, onStatus})`, `prompt(sessionId, text)`, `shutdown()`, `kill()`).
2. Add it to the `Foreman` constructor's channel selection logic.
3. Add tests in `test/channels/` and an e2e scenario in `test/e2e/`.
4. Select it via `foreman.config.json` → `harness.channel`.

### Adding an inbound parse direction to an existing protocol

1. Add a `parse(wireChunk) → FrameIn[]` method to the adapter's formatter.
2. The adapter definition automatically reports `directions: ['outbound', 'inbound']`.
3. Add parse conformance tests in `test/inbound-parse.test.js`.

---

*Last updated: 2026-08-26*