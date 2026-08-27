# ADR-0013: Run profiling and the task-throughput performance model

**Status:** Accepted
**Date:** 2026-08-27

## Context

Foreman's run lifecycle is already instrumented with a flat `timings` map
(`prepareMs`, `bootMs`, `turnMs`, `packagingMs`, `checkpointSyncMs`,
`publishMs`). That map answers "how long did each phase take" for a single
run, but it cannot answer the questions a scheduler of pooled agent sandboxes
needs:

- `turnMs` is **overwritten every turn** — there is no per-turn history, so
  turn-time distribution (variance, drift, tool-heavy vs. model-heavy turns)
  is invisible.
- Phases are recorded as single durations with **no sub-phase spans** —
  `prepareMs` cannot be split into config download / workspace restore /
  session restore / git baseline; `publishMs` cannot be split into packaging /
  checkpoint sync / uploads / bus events.
- There are **no stream metrics** — event counts per type, first-event
  latency after a prompt, or event rate — so the cost of the streaming path
  itself is unmeasured.
- There is **no derived throughput view**: what fraction of a sandbox's wall
  clock is the agent actually executing (useful work) vs. orchestration
  overhead (restore, boot, commit, package, upload)? Without that ratio,
  scheduling policies (when to reclaim, when to pre-restore the next sandbox,
  how many turns to batch per sandbox) have no quantitative basis.

ADR-0010/0011 built an explicit critical-path *model* for one decision
(publish overlap) and verified it with a bespoke benchmark. The pattern works:
model the run as composed costs, measure the constants in the real path, let
derived quantities guide scheduling. What is missing is a *general* mechanism
that produces those measurements on every run, plus the written-down model
that interprets them.

## Options considered

1. **Extend the flat `timings` map** (more keys: `turn1Ms`, `turn2Ms`, …).
   Rejected: flat keys don't compose (per-turn × per-sub-phase explodes the
   key space), and consumers still have to know which numbers to divide by
   which.
2. **Adopt a full tracing stack** (OpenTelemetry spans exported via the
   existing OTLP path). Rejected for now: the profiling consumer here is the
   control plane's *scheduler*, not a human in a trace UI; the run profile
   must ride the existing artifact path (result.json + the artifact batch)
   rather than a second telemetry pipeline. The harness's own telemetry is
   already exported separately (path A); duplicating foreman's lifecycle
   spans there couples two unrelated failure domains.
3. **A structured span profiler + derived metrics report** (chosen): one
   in-process recorder, hierarchical spans, counters, and a derived
   throughput section computed once at publish; emitted as (a) the
   `profiling` field of `result.json` and (b) a standalone `profile.json`
   artifact uploaded with the batch.

## Decision

### 1. RunProfiler (`src/observability/profiler.js`)

A span-based recorder with a deliberately small API:

- `span(name, fn)` — wraps a sync or async operation, recording
  `{ name, start, end, durationMs }` on a monotonic clock
  (`performance.now()`), nesting inferred from dotted names
  (`prepare.workspace.extract` nests under `prepare.workspace` conceptually;
  the report renders a tree).
- `count(name, n = 1)` — cumulative counters (event types, uploads, packs).
- `gauge(name, value)` — last-write-wins values (payload bytes, pack count).
- `report()` — the immutable snapshot: spans (sorted, with wall-clock
  offsets), counters, gauges, and the derived metrics below.

Profiling is **always on** (the overhead is one object per span; the spans
are the run's own structure) and **additive to `timings`** — existing
consumers (tests, benchmarks, `result.json`) are untouched; `timings` remains
the compatibility surface, the profiler is the analysis surface.

### 2. Instrumentation points (the run, spanned)

| Span | Covers |
|---|---|
| `prepare.config.download` | composition config fetch |
| `prepare.workspace.download` / `.extract` | seed / checkpoint-index + pack downloads and restore |
| `prepare.sessions.download` / `.extract` | session archive restore |
| `prepare.git.baseline` / `prepare.checkpoint.restore` | git init/baseline or pack-chain replay |
| `start.channel` | harness process boot + handshake (per channel's `bootMs`) |
| `turn.N.execute` | one prompt → turn/end (the channel's measured turn) |
| `turn.N.commit` | per-turn git commit (secret scan + commit) |
| `collect` | manifest diff + change extraction |
| `publish.packaging.workspace` / `.sessions` / `.trace` | the three archive builds |
| `publish.checkpointSync` | retention plan + pack builds + pack uploads + index write |
| `publish.uploads` | artifact batch upload |
| `publish.bus` | reclaim events |

Stream counters ride the existing `onEvent` fan-out: per-type event counts,
per-turn first-event latency (prompt dispatch → first event of the turn),
and total event count (already surfaced as `eventCount`).

### 3. Derived metrics — the task-throughput view

The model (detailed in `docs/design/performance-modeling.md`) decomposes one
sandbox run's wall clock as:

```
T_run  = P + B + ΣEᵢ + Σ(commitᵢ) + C + U + O
          ├──── overhead ────────────────┤ ├─ useful ─┤├─ overhead ─┤
```

- `P` prepare (restore), `B` boot, `Eᵢ` turn i execution, `commitᵢ` per-turn
  commit, `C` collect, `U` publish, `O` inter-phase gaps.
- Derived: `usefulWorkRatio = ΣE / T_run`, `turnThroughput = turns / T_run`,
  `eventRate = events / ΣE`, per-turn `firstEventLatency`, and the publish
  critical-path split already established by ADR-0011
  (`max(K, ΣC) + U` shape).

These are exactly the inputs a sandbox-pool scheduler needs: the ratio tells
whether reclaiming + re-preparing a sandbox pays off vs. keeping it warm;
`P` and `B` bound the pre-restore saving (ADR-0010 option 2 revisited);
turn-time distribution feeds batching decisions.

### 4. Emission

- `profile.json` joins the artifact batch (`<session>/profile.json`) — the
  scheduler-consumable form; `result.json` gains a `profiling` section with
  the derived metrics (spans stay in the artifact to keep result.json
  readable).
- No secrets: spans carry names and numbers only; payload content is never
  profiled (redaction rules unchanged).

## Consequences

- Every run self-reports its cost structure; `bench/run-pipeline.bench.js`
  can (later) source its constants from the profiler instead of the flat
  timings, but keeps its own gates.
- The flat `timings` map stays as-is (compatibility); new analysis code
  should read the profiler. A future ADR may retire `timings` once all
  consumers migrate.
- The performance model becomes a living document
  (`docs/design/performance-modeling.md`) that must be updated when the
  lifecycle changes — it is the contract between profiling data and
  scheduling policy.
- Multi-run aggregation (fleet-level throughput) is deliberately out of
  scope for this ADR: per-run profiles are the primitive; aggregation
  belongs to the control plane that sees many runs.
