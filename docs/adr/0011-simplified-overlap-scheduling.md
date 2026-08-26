# ADR-0011: Simplified overlap scheduling

**Status:** Accepted
**Date:** 2026-08-26

## Context

ADR-0010 introduced three overlap mechanisms for run-lifecycle I/O:

- **3a — per-turn background checkpoint sync.** After each `prompt()`, the turn's
  checkpoint pack is built and uploaded on a serialized background promise chain
  while the next turn executes. This required: a retention-anchor recomputation at
  prompt time (`#checkpointAnchor`, duplicating `CheckpointKeeper.planPacks`
  logic), a `ckptPreloaded` map consulted by `publish()`, a drain-before-publish
  invariant, and a third state field (`ckptSyncChain`) on the orchestrator.
- **3b — concurrent downloads in `prepare()`** (session archive ∥ workspace packs).
- **3c — concurrent uploads/packaging in `publish()`** (artifact batch ∥ packaging).

3a is the dominant source of scheduling complexity in `foreman.js`, and its measured
benefit varies widely with the environment:

| Measurement | Workload | Observed saving | Fidelity vs model |
|---|---|---|---|
| ADR-0010 (original sandbox, 3 cores) | 5 turns × 8 MiB | 2 065 ms (12.1%) | 85% |
| 2026-08-26 re-baseline (this sandbox) | 3 turns × 2 MiB (`--quick`) | 254 ms (4.3%) | 48% |
| 2026-08-26 re-baseline (this sandbox) | 5 turns × 8 MiB | 3 009 ms (16.3%) | 131% (execution-time noise inflates the delta) |

The benefit only materializes when (a) runs are multi-turn and (b) pack-sync cost is
large relative to execution. It costs: duplicated retention logic on the hot path, an
async in-flight chain whose failure semantics must be reasoned about at every exit
point of the run lifecycle, and a wider state surface to test.

Session-boundary concurrency (3b/3c) has none of these costs: it is a plain
`Promise.all` over independent objects with no ordering constraints, and it benefits
every run including single-turn ones.

## Options considered

1. **Keep 3a as is.** Rejected: the complexity/benefit ratio is unfavorable and
   environment-dependent (48–85% fidelity; 4–12% occupancy).
2. **Simplify 3a's anchor logic only** (always upload adjacent packs `turn-1 → turn`
   in the background; let publish's retention rebuild merged packs). Rejected: keeps
   the promise chain, the drain invariant, and the preload map — the bulk of the
   complexity — while making some background uploads wasted work when retention
   merges packs.
3. **Remove 3a; keep 3b/3c (chosen).** Overlap lives only at session boundaries
   (prepare/publish), where transfers are independent objects with no ordering
   constraints. Checkpoint packs build/upload at `publish()` under the single
   authoritative retention pass — one planner, one code path, no background state.
4. **Cross-session overlap** (pre-restore sandbox B while sandbox A executes).
   Rejected for the same reason as in ADR-0010: `prepare()` runs inside the new
   sandbox (pull model); revisit under a pooled-sandbox architecture.

## Decision

1. Remove the per-turn background checkpoint sync entirely: no `#enqueueCheckpointSync`,
   no `#checkpointAnchor`, no `ckptPreloaded`/`ckptSyncChain`, no drain step, and the
   `checkpoints.overlap` option is withdrawn.
2. Keep per-turn **commits** at `prompt()` (synchronous, local git): they preserve
   per-turn pack granularity for the chain; only the build+upload moves back to
   `publish()`.
3. Keep 3b/3c unchanged (concurrent prepare downloads, concurrent publish uploads
   and packaging).
4. `publish()`'s `syncCheckpoints()` remains the single authoritative planner: it
   computes the desired pack list from retention, builds and uploads every pack,
   writes the index last. Per-pack timings are still recorded for observability.
5. `bench/run-pipeline.bench.js` is reworked from an A/B (overlap on/off) into a
   single-mode critical-path benchmark (prepare / execution / collect / publish /
   pack-sync breakdown) with the same integrity gates: payload presence, pack phase
   placement, and bit-for-bit restore from the published index.

## Consequences

- `foreman.js` loses its only background-mutation machinery: run-lifecycle state is
  synchronous between public method boundaries again, and every checkpoint write
  happens inside `publish()` — easier to test, audit, and reason about.
- Multi-turn sandbox occupancy regresses by the pack-sync cost that 3a used to hide
  (measured 254 ms – 3 009 ms per multi-turn run depending on workload and
  environment; see the table above). This is accepted in exchange for the state
  and code-path reduction; single-turn flows are unaffected. ADR-0010's numbers
  remain the record of what a background-sync design can achieve; reintroduce one
  only with a measured occupancy need, and prefer designing it as a separate
  component rather than inline orchestrator state.
- Mid-run crash loses the current run's checkpoint progress (restore returns to the
  previous run's index). This matches the session-archive semantics (sessions also
  upload only at publish), so checkpoint durability and session durability stay
  aligned — no partial-durability window to reason about.
- Mid-turn snapshotting and orchestrator-side prefetch remain rejected (unchanged
  from ADR-0010).
- ADR-0010 is superseded **in part** (mechanism 3a); its 3b/3c mechanisms, critical-path
  model, and verification method remain in force and are inherited by this ADR.
