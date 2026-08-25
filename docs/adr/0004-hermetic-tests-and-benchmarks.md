# ADR-0004: Hermetic testing and benchmarking strategy

**Status:** Accepted
**Date:** 2026-08-25

## Context

The existing e2e suites are keyless and run against in-process mocks of the
*external* world (model endpoint, control plane, OTLP collector) — the system
under test (foreman + dsh) is real. Those suites require a built dsh repository
on disk, which is not always available (e.g. CI images that only check out the
foreman solution tree).

The new protocol adapter layer (ADR-0001) and config mechanism (ADR-0002) must
be developed test-first, in environments where dsh is absent, without
weakening the guarantee that **tests exercise real code paths**. The explicit
constraint from the requirements: no cheating, no mocks — tests must not pass
by reimplementing or faking the behavior they claim to verify, and benchmark
numbers must come from real measurements, not estimates.

## Definitions

- **Hermetic** — the suite is fully self-contained: no network beyond loopback,
  no credentials, no external services, no dependence on the dsh repository.
- **No mocking** — nothing that the assertion observes is produced by a test
  double of the system under test. Test doubles of *external dependencies*
  (upstream HTTP endpoints) are boundary scaffolding, not observations; when
  even that can be avoided (pure transforms, real loopback servers), it is.
- **No cheating** — assertions are made against wire bytes / observable
  outcomes, never against a parallel reimplementation of the SUT logic inside
  the test; benchmarks read real timers, run real workloads, and report what
  happened, including unfavorable numbers.

## Decision

1. **Golden-transcript conformance tests** (`test/protocols.test.js`) — each
   protocol is driven with recorded frame transcripts (real internal frame
   shapes and orderings, captured from live runs) and must produce the exact
   expected EventOut sequences. Transcripts are *fixtures* (input data), not
   mocks: the adapter under test is the real implementation.
2. **Wire-level tests over real HTTP** (`test/gateway-wire.test.js`) — a real
   `SseGateway` listens on a real loopback socket; the test subscribes with a
   real `fetch` SSE client and asserts on the parsed wire stream (ids, data
   payloads, `[DONE]`, Last-Event-ID replay) for every built-in protocol, plus
   registry resolution and config-file selection (ADR-0002) through the real
   `Foreman` wiring path. No HTTP interception of any kind.
3. **Benchmarks** (`bench/protocol.bench.js`, `npm run bench`) — measure the
   real adaptation + gateway pipeline end to end over loopback HTTP:
   per-protocol throughput (frames/s, MB/s), publish→subscriber latency
   percentiles (p50/p95/p99), and formatter-only throughput. Warmup runs,
   repeated measured runs, median reported; results are written to
   `bench/results/` as JSON and printed as a table. Nothing is mocked and no
   number is derived — every figure comes from `performance.now()` around real
   work.
4. **Boundary honesty** — what the hermetic suite does *not* cover is stated
   explicitly in the ROADMAP: full-run e2e and end-to-end benchmarks still
   require the dsh repository and keep using the existing keyless e2e suites.
   The hermetic suite never pretends to substitute for them.

## Consequences

- Protocol and config regressions are caught in any environment, in seconds,
  without dsh or credentials.
- The benchmark establishes a baseline for the optimization loop the
  requirements ask for ("gradually optimize the system"); improvements and
  regressions are quantifiable per protocol.
- Golden transcripts must be maintained when the internal frame model changes;
  the conformance tests make such changes loud instead of silent.
