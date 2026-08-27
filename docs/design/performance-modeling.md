# Performance model and profiling — how fast is one agent task, and where does the time go

This document is the single written-down performance model of Foreman: what a
run's wall clock is composed of, how each component is measured (the
profiling mechanism, ADR-0013), which derived quantities matter for
scheduling, and what the current best strategy is. It is written for a
reviewer who has seen operation names in the code (`publish`, `prepare`,
`syncCheckpoints`) and wants to know what they *cost* and *why they exist*.

Companion documents: [ADR-0013](../adr/0013-run-profiling-and-throughput-model.md)
(the decision), [architecture.md](../architecture.md) (the lifecycle),
[checkpoint-design.md](../checkpoint-design.md) (the pack chain),
[ADR-0011](../adr/0011-session-boundary-overlap-scheduling.md) (the publish
overlap model and its measured verification).

---

## 1. What each lifecycle operation is (the vocabulary)

A run is five operations on one disposable sandbox (`Foreman`):

| Operation | What it actually does | Why it exists |
|---|---|---|
| `prepare()` | download config + workspace snapshot (+ checkpoint packs) + session logs from object storage; extract; git baseline or pack-chain replay | the sandbox is *disposable*: nothing survives unless it was restored |
| `start()` | spawn the harness process (dsh or codex), wait for its readiness handshake, open the SSE gateway | the harness is a separate process speaking its own protocol over stdio/HTTP |
| `prompt()` | send one task, stream events until `turn/end`; with checkpoints, immediately git-commit the turn | the turn commit is the immutable input that checkpoint packs are built from |
| `collect()` | final answer, manifest diff, fs diffs, session log list | assemble the run's products |
| `publish()` | **the session-end save**: redact + package the workspace, archive session logs, build & upload checkpoint packs, upload the artifact batch, emit `run.completed` and `sandbox.reclaim-requested` | this is the operation that makes the sandbox *reclaimable* — after publish, killing the sandbox loses nothing |

**What "publish" is, in one paragraph:** publish is the terminal save-and-
hand-off. It compresses the workspace (with secret redaction), archives the
harness's session logs (the conversation history), syncs the incremental
checkpoint chain (the workspace's git-history-derived change packs), writes
`result.json` + `profile.json`, uploads all of it to object storage, and only
then tells the message bus "this sandbox can be reclaimed". Everything a
future run needs to continue this session must be inside those artifacts —
that is the durability contract.

---

## 2. The run cost model

One run's wall clock decomposes as:

```
T_run  =  P  +  B  +  Σᵢ (Eᵢ + commitᵢ)  +  C  +  U  +  O
          │    │         │                   │    │    │
          │    │         │                   │    │    └ inter-phase gaps (small)
          │    │         │                   │    └ publish (save + hand-off)
          │    │         │                   └ collect
          │    │         └ per-turn: execution + turn commit   ← the useful work
          │    └ harness boot + handshake
          └ prepare (restore config/workspace/session)
```

Published (publish = U) itself decomposes — this is the ADR-0011 critical
path — as:

```
U  ≈  max(K, ΣC) + Ubatch        (overlap mode, the default)
U  ≈  K + ΣC + Ubatch            (serialized mode, the benchmark baseline)

K       packaging the workspace + sessions + trace archives
ΣC      checkpoint pack builds + uploads (one per retained turn)
Ubatch  the artifact batch upload (result/workspace/sessions/trace/profile)
```

### Derived quantities (what scheduling actually consumes)

| Quantity | Formula | Question it answers |
|---|---|---|
| `usefulWorkRatio` | `ΣE / T_run` | is the sandbox mostly doing agent work, or mostly orchestration overhead? |
| `turnThroughput` | `turns / T_run` | tasks per unit wall clock for this sandbox |
| `warmupCost` | `P + B` | what a cold sandbox pays before the first useful token; the upper bound on what pre-restoring the next sandbox (ADR-0010 option 2) could save |
| `saveCost` | `U` | what one extra turn in this session costs in terminal save time |
| `eventRate` | `events / ΣE` | streaming-path load (feeds the gateway/backpressure roadmap) |
| `firstEventLatency` (per turn) | prompt dispatch → first event of the turn | perceived responsiveness of the harness |

A scheduler deciding *reclaim vs. keep-warm* compares `warmupCost` against
the expected time to the next task; a scheduler deciding *how many turns to
batch per sandbox* weighs `saveCost` (paid once per run) against the
per-turn `commit` cost (paid every turn).

---

## 3. The profiling mechanism (how the numbers are produced)

`src/observability/profiler.js` implements a span recorder (ADR-0013):

- **Spans** — `{ name, start, end, durationMs }` on a monotonic clock,
  hierarchical dotted names (`prepare.workspace.extract`,
  `turn.3.execute`, `publish.checkpointSync`). Every lifecycle operation in
  `Foreman` is wrapped; one span per sub-phase, one per turn, one per
  publish phase.
- **Counters** — event-type counts off the stream fan-out, upload counts,
  pack counts.
- **The derived section** — computed once at `publish()` from the spans:
  everything in the table above.

Emission (both ride the existing artifact path — no new telemetry
pipeline):

- `profile.json` in the artifact batch — full spans + counters + derived
  metrics; the machine-readable form for the control plane.
- `result.json` → `profiling` — the derived metrics summary (no span list;
  result.json stays readable).

Reference shape of `profile.json`:

```json
{
  "schema": 1,
  "startedAt": "…", "endedAt": "…",
  "run": { "channel": "codex", "turns": 2, "events": 41 },
  "derived": {
    "runWallMs": 18411,
    "executionMs": 14320,
    "usefulWorkRatio": 0.778,
    "turnThroughputPerSec": 0.109,
    "warmupMs": 2780,
    "saveCostMs": 612,
    "eventRatePerSec": 2.86,
    "turns": [{ "turn": 1, "executeMs": 6210, "commitMs": 88, "firstEventLatencyMs": 1412, "events": 22 }]
  },
  "spans": [{ "name": "prepare.workspace.extract", "start": 12.3, "end": 44.1, "durationMs": 31.8 }, "..."],
  "counters": { "event.assistant/chunk": 14, "event.tool/call": 2, "upload": 5 }
}
```

The profiler never records payload content — names and numbers only — so no
redaction surface is added.

---

## 4. Current best strategy (and why)

The measured constants below come from the existing benchmarks
(`bench/run-pipeline.bench.js`, real codex channel; e2e timings in this
sandbox) and shape today's defaults:

1. **Session-boundary overlap (ADR-0011) stays the default.** Publish
   fan-out (`max(K, ΣC) + Ubatch` instead of `K + ΣC + Ubatch`) is
   correctness-cheap (pack content comes from immutable commits) and saves
   the min(K, ΣC)-shaped term; measured ~320 ms on 5 turns × 8 MiB with CPU
   contention accounted for.
2. **Per-turn commits, publish-time pack builds.** The turn commit (a git
   commit) is cheap (~tens of ms) and must be immediate (it is the immutable
   pack input); pack build+upload is deferred to publish (the removed
   per-turn background sync of ADR-0010 was disproportionate complexity for
   the saving).
3. **Prepare downloads concurrently** (session archive ∥ workspace packs ∥
   seed) — independent objects, sequential application (the replay chain is
   ordered).
4. **One sandbox = one session** for the `usefulWorkRatio` accounting;
   multi-turn batching amortizes `warmupCost` — the dominant overhead term
   (boot alone is ~1 s on the dsh channels, ~2.6 s on codex in e2e
   conditions; prepare is tens of ms with local object storage, grows with
   real network distance).

What is deliberately *not* done yet: cross-session pre-restore (needs a
sandbox pool + push-model prepare), publish work overlapped with the *next*
run's prepare (needs the reclaim to be advisory rather than terminal), and
fleet-level aggregation of profiles (control-plane concern).

---

## 5. Using the model to evaluate a change

The discipline that ADR-0011 established and this document generalizes:

1. Write the change's effect as a formula in terms of the model's constants
   (e.g. "overlap saves min(K, ΣC)").
2. Implement it.
3. Read the constants from the profiler / benchmark (real path, no mocks —
   ADR-0004).
4. Compare predicted vs. observed; a mismatch is a model bug or a hidden
   contention term — either way it goes back into this document.

*Last updated: 2026-08-27*
