# ADR-0010: Overlap scheduling for run-lifecycle I/O

**Status:** Accepted
**Date:** 2026-08-26

## Context

Foreman's run lifecycle is strictly sequential per session:

```
prepare (download packs + session logs) → start → prompt₁ … prompt_N
→ collect → publish (commit → build packs → upload packs → upload artifacts → reclaim)
```

All storage I/O sits on the critical path: checkpoint packs for turn *i* are built and
uploaded only at `publish()`, *after* execution ends; the four publish artifacts are
uploaded one-by-one; in `prepare()` the workspace packs and the session archive are
downloaded sequentially. In multi-agent / multi-session deployments the sandbox is the
scarce resource: every second of serialized I/O is a second the sandbox occupies
without executing.

Observations that make overlap attractive:

- **Execution and storage I/O use disjoint resources.** A turn is model round-trips +
  tool execution (network + harness process); checkpoint sync is git/tar/gzip (CPU,
  mostly in child processes) + HTTP upload (network I/O). Node's event loop is free
  while the harness executes.
- **The checkpoint index is already a commit point.** `checkpoints.json` is the single
  source of truth for restore; uploading pack objects *before* writing the index
  changes no semantics (an orphaned pack without an index entry is invisible to
  restore and is overwritten or garbage-collected by the next sync).
- **Multi-turn-per-sandbox is a real scenario** (one sandbox serving several prompts
  before reclaim), even though earlier e2e tests used one prompt per sandbox.

The open question: *which* overlaps actually pay off, by how much, and at what
correctness risk — to be answered quantitatively (benchmark + critical-path model), not
by intuition.

## Options considered

1. **Do nothing (fully serialized baseline).** Safe, but leaves sandbox occupancy on
   the table and the question unanswered.
2. **Cross-session scheduler with prefetch** (orchestrator pre-restores sandbox B's
   workspace while sandbox A executes). Rejected for now: in the current architecture
   `prepare()` runs *inside* the new sandbox (pull model); an orchestrator-side
   pre-restorer duplicates restore logic in a second place. Revisit when sandboxes are
   pooled rather than created per session.
3. **Intra-session overlap (chosen):**
   a. **Per-turn background checkpoint sync** — after each `prompt()` returns, commit
      the turn and build+upload its pack in the background while the next turn
      executes; `publish()` drains before the final retention sync.
   b. **Concurrent downloads in `prepare()`** — session archive and workspace
      packs download in parallel (independent resources).
   c. **Concurrent uploads in `publish()`** — the artifact batch uploads in
      parallel; packaging and uploading overlap where data dependencies allow.
4. **Aggressive pipelining inside a turn** (upload partial workspace while the model
   streams). Rejected: mid-turn workspace state is mutating; snapshotting it
   concurrently introduces torn-state restores — unacceptable for a system whose core
   promise is restore correctness.

## Decision

Implement 3a/3b/3c, each behind the existing semantics (3a requires
`checkpoints` enabled; 3b/3c are unconditional — they have no ordering constraints).

### Mechanism (3a)

- After a successful `prompt()` turn: `ckpt.turn += 1`; `git.commitTurn()`; register
  the oid; enqueue a background sync on a **serialized promise chain** (one pack
  build/upload at a time — git index operations stay ordered).
- The background sync uploads pack(prevKeptTurn → turn) using the *current* index for
  the anchor, recording `(turn, from)` in an in-memory `preloaded` map.
- `publish()` **drains the chain first**, then runs the authoritative
  `syncCheckpoints()` retention pass; desired entries matching a preloaded
  `(turn, from)` are counted as already uploaded and skipped (no re-upload, no
  double-count). A preloaded pack that retention no longer wants (e.g. a rebase turn
  chooses `from=null`) is simply overwritten by the authoritative pack — same object
  key, last-write-wins, index still decides.
- Turn commits move from `collect()` to `prompt()` **only when checkpoints are
  enabled**; the non-checkpoint flow (commit at `collect()`) is unchanged.

### Correctness invariants

1. The index (`checkpoints.json`) is written only by the final `syncCheckpoints()`
   after the drain — restore never sees a half-synced chain.
2. `publish()` never completes (and no reclaim event fires) with a pending background
   sync — the graceful-shutdown delivery guarantee is preserved.
3. A crash mid-background-sync leaves at most an orphaned pack object; restore
   correctness is unaffected (index untouched).

### Performance model

Critical path for a session with N turns, execution `Eᵢ`, sync cost `Cᵢ`, final
publish `P`:

```
serial   T = Σᵢ(Eᵢ + Cᵢ) + P
overlap  T' = ΣᵢEᵢ + max(C_last, 0) + P'      (Cᵢ hides under Eᵢ₊₁ when Cᵢ ≤ Eᵢ₊₁)
saved    ≈ Σᵢ₌₁^{N-1} min(Cᵢ, Eᵢ₊₁) + min(C_N, collect) − contention
```

(`C_N` cannot hide under a next turn, but it overlaps `collect()` — publish drains
whatever is still in flight.)

3b/3c shave `max(0, D_ws − D_sess)`-style terms off prepare/publish (independent
transfers run concurrently). The measured constants come from
`bench/run-pipeline.bench.js` (below); the model decides whether a mechanism is worth
its complexity, and the benchmark verifies the projection on real work.

### Verification (measured, `bench/run-pipeline.bench.js`)

A/B on the real codex channel: 5 turns × 8 MiB incompressible payload per sandbox,
600 ms scripted model latency, 1 warmup + 3 measured pairs, mode order alternated;
real git/tar/gzip/uploads against a disk-backed object store, only the model endpoint
scripted (ADR-0004). Integrity gates per run: every payload file present at the
expected size, pack-sync phases match the mode (all at publish vs all in
background), and a fresh restore from the published index reproduces the final
workspace bit-for-bit. Median across runs (this sandbox, 3 cores):

| Constant / metric | serial | overlap |
|---|---|---|
| execution ΣE | 11 412 ms | 11 377 ms |
| pack sync ΣC (location) | 2 429 ms (publish, on critical path) | 2 367 ms (background, 0 ms drained at publish) |
| run total prepare→publish | 17 119 ms | 15 054 ms |

- projected saving `Σ min(Cᵢ, Eᵢ₊₁) + min(C_N, collect)` = **2 429 ms**
- observed saving `T_serial − T_overlap` = **2 065 ms** (12.1% of the serial run)
- fidelity **85%** — the shortfall is CPU contention (background tar/gzip/git
  shares cores with the harness's turn-commit git work), which the model counts as
  the negative `contention` term.

Conclusion: with multi-turn sandboxes the mechanism hides effectively the whole
checkpoint-sync cost; single-turn flows degenerate to the serial order. The
benchmark also surfaced and now guards two correctness issues: harness-transient
scratch (`CODEX_HOME/.tmp`) must be excluded from session archives, and the git
secret scan must not drop large files (see Consequences).

## Consequences

- Multi-turn sandboxes overlap checkpoint upload with execution; single-turn flows are
  unaffected (drain degenerates to the serial order, verified by the existing
  checkpoint e2e passing unchanged).
- `prepare`/`publish` wall time drops by the non-dominant transfer term even for
  single-turn sessions.
- The background sync adds one in-flight-promise chain to foreman's state; failure
  isolation: a background sync error propagates to `publish()` (fail loud rather than
  silently losing a checkpoint).
- Benchmark + model: `bench/run-pipeline.bench.js` A/B (overlap on/off) on the real
  codex channel with real git/tar/uploads and an integrity gate (final restore from
  the produced index must equal the workspace) — no mocks in the measured path,
  consistent with ADR-0004/0008.
- Two correctness issues surfaced by the benchmark's integrity gates (fixed with it):
  - **Session archives must exclude harness transient scratch.** The harness process
    is still alive at `publish()`, and codex keeps mutating `CODEX_HOME/.tmp`
    (in-flight plugin clones), failing the archive with "file changed as we read it".
    `.tmp` never carries durable session state and is regenerated on demand.
  - **The git secret scan must not drop large files.** The pre-commit scan's size
    ceiling used to unstage files over 2 MiB as `oversize` violations — such files
    never entered git history, hence never entered any checkpoint pack, and restores
    silently lost them. The scan now streams files of any size in overlapping
    chunks, so secret interception (including secrets spanning chunk boundaries) is
    preserved without data loss.
- Not done (recorded): mid-turn snapshotting (torn-state risk), orchestrator-side
  prefetch (pull-model duplication) — revisit under a pooled-sandbox architecture.
