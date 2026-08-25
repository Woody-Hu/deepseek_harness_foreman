# ADR-0008: Independent testing and benchmark strategy for harness protocol integrations

**Status:** Proposed
**Date:** 2026-08-25

## Context

ADR-0004 established the hermetic testing and benchmarking strategy for the protocol
adapter layer. The existing suite covers outbound protocol adapters (`native`,
`openai-chat`, `openai-responses`) with golden-transcript conformance tests, wire-level
tests over real HTTP, and a protocol pipeline benchmark with integrity gates.

The new Codex Harness channel (ADR-0005) and the Anthropic Messages adapter (ADR-0006)
introduce new dimensions that the existing ADR-0004 strategy does not cover:

1. **Codex channel tests** — the `CodexChannel` communicates with a real `codex app-server`
   subprocess over stdio JSON-RPC. The existing tests assume the dsh binary is absent;
   the Codex channel tests must work with an actual `codex` binary on the test machine.
2. **Anthropic Messages conformance** — the new adapter needs golden transcripts and
   wire-level tests, following the same pattern as the existing adapters (ADR-0004).
3. **Inbound parse tests** — the `parse()` direction (ADR-0007) needs its own
   conformance tests: known wire chunks in, expected internal frames out.
4. **Benchmark coverage** — the existing benchmark covers the event pipeline only.
   The roadmap ("Next up") calls for benchmark coverage of the run pipeline
   (snapshot fetch/decompress/place, pack build/apply, compress/upload).

The requirement: no cheating, no mocks — tests must not pass by reimplementing or
faking the behavior they claim to verify, and benchmark numbers must come from real
measurements. This constraint applies to the new tests as well.

## Options considered

1. **Extend the existing conformance test suite** — the new `anthropic-messages` adapter
   follows the exact same pattern as the existing adapters: golden transcripts in,
   expected EventOut sequence out. The Codex channel is different: it needs a real
   `codex` binary. Handling both in the same test file would mix concerns.

2. **Separate test files per concern** (chosen) — following the existing layout
   (`test/protocols.test.js`, `test/gateway-wire.test.js`, `test/e2e/`), the new
   tests get their own files:
   - `test/protocols.test.js` — extended with `anthropic-messages` conformance cases.
   - `test/gateway-wire.test.js` — extended with `anthropic-messages` wire cases.
   - `test/inbound-parse.test.js` — new file for inbound `parse()` conformance.
   - `test/channels/codex-channel.test.js` — new file for Codex channel integration.
   - `test/e2e/codex.e2e.js` — new e2e scenario for the Codex channel.

3. **No new tests (rely on the existing e2e suites)** — rejected: the existing e2e
   suites require a built dsh repository; the Codex channel does not use dsh. The
   Codex channel must be testable independently, with only the `codex` binary
   available.

## Decision

### 1. Anthropic Messages conformance (extends existing test files)

**Golden-transcript tests** (`test/protocols.test.js`):
- new transcript fixtures in `test/fixtures/transcripts.js`: `anthropicTextOnlyTurn`,
  `anthropicToolTurn`, `anthropicEmptyTurn`, `anthropicFailedTurn`.
- Conformance assertions: every adapter must produce the exact expected EventOut
  sequence for the given transcript.
- The `claude` alias resolves to the same adapter as `anthropic-messages`.

**Wire-level tests** (`test/gateway-wire.test.js`):
- Real `SseGateway` + real `fetch` subscriber for the `anthropic-messages` adapter.
- Assert on SSE event names (`message_start`, `content_block_start`,
  `content_block_delta`, `message_delta`, `message_stop`), payload invariants,
  and `[DONE]` carrier termination.

### 2. Inbound parse conformance (new file: `test/inbound-parse.test.js`)

**Golden-wire-to-frame tests**:
- Known wire chunks (SSE event payloads) for each inbound-capable adapter
  (`openai-chat`, `openai-responses`, `anthropic-messages`).
- Assert the exact internal frame sequence produced by `parse()`.
- Unknown wire events are skipped (produce zero frames).
- An adapter without `parse()` (e.g. `native`) throws when accessed via
  `resolveInboundProtocol()`.

### 3. Codex channel integration tests (new file: `test/channels/codex-channel.test.js`)

**Hermetic Codex channel tests**:
- Require the `codex` binary in PATH (skip with a clear message when absent).
- Start `codex app-server --stdio` as a subprocess.
- Perform the initialization handshake, create a thread, start a turn, and assert
  on the streaming event sequence.
- Test session resume (`thread/resume`).
- Test graceful shutdown and kill paths.
- Test timeout behavior (turn that exceeds `timeoutMs`).

**Not hermetic in the ADR-0004 sense** — the Codex channel tests require the real
`codex` binary. They are still keyless and require no network. The test file must
document this dependency and skip gracefully when the binary is absent.

### 4. Codex e2e scenario (new file: `test/e2e/codex.e2e.js`)

End-to-end scenario that exercises the full `Foreman` orchestrator with the
`codex` channel:
- `prepare()` → `start()` → `prompt()` → `collect()` → `publish()`.
- Workspace checkpointing with the Codex channel.
- Outbound event adaptation (the `openai-responses` adapter) on the Codex channel.
- Published artifacts are accessible and correct.

### 5. Benchmark extension

The existing benchmark (`bench/protocol.bench.js`) is extended with:
- `anthropic-messages` adapter throughput and latency (same methodology as the
  existing adapters).
- A new benchmark for the inbound parse direction: `parse()` throughput for each
  inbound-capable adapter.

The roadmap item "benchmark coverage for the run pipeline" (snapshot
compress/upload, pack application, full run latency) remains a separate effort
tracked in the roadmap.

### 6. Boundary honesty

The hermetic suite does not cover:
- The Codex channel integration (requires the `codex` binary).
- Full-run e2e with the Codex harness (requires the `codex` binary).
- Benchmark coverage for the run pipeline (snapshot, checkpoints).

These are stated explicitly in the ROADMAP. The hermetic suite never pretends to
substitute for them.

## Consequences

- The Anthropic Messages adapter has the same conformance guarantees as the existing
  adapters: golden transcripts, wire-level assertions, and benchmark baselines.
- The inbound parse direction has its own conformance suite, independent of any
  channel implementation.
- The Codex channel tests are independent of dsh and require only the `codex` binary.
- The overall test suite grows from ~43 tests to ~65 tests (estimated).
- The benchmark reports numbers for 4 protocols instead of 3.