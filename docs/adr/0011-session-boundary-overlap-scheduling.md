# ADR-0011: Session-boundary overlap scheduling

**Status:** Accepted
**Date:** 2026-08-27

## Context

ADR-0010 introduced intra-session overlap scheduling with three mechanisms:

- **3a** per-turn background checkpoint sync — after each `prompt()`, the turn's
  checkpoint pack is built and uploaded on a serialized background promise chain
  while the next turn executes;
- **3b** concurrent downloads in `prepare()`;
- **3c** concurrent uploads in `publish()`.

3a measured well (it hid ~85% of the projected checkpoint-sync cost on
multi-turn runs) but its mechanism is disproportionately complex:

- a serialized in-flight promise chain on the orchestrator (`ckptSyncChain`)
  plus a drain step that `publish()` must run before anything else;
- a `ckptPreloaded` (turn → anchor) map duplicating upload state;
- `#checkpointAnchor()` re-implements the retention planning *at the time of
  the background sync* and must stay perfectly consistent with the
  authoritative `syncCheckpoints()` planning *at publish time* — a subtle
  implicit invariant that is easy to break when the retention policy changes;
- an extra `checkpoints.overlap` toggle and per-phase bookkeeping
  (`phase: 'background' | 'publish'`) threaded through the sync records.

The overlap benefit also only materializes for multi-turn sandboxes; single-turn
runs (a common case) degenerate to the serial order while still carrying the
full mechanism. On review the complexity/benefit ratio is not favorable:
correctness reasoning about the run lifecycle should not require simulating a
background interleaving.

## Options considered

1. **Keep 3a as is.** Rejected: the complexity is permanent while the benefit is
   limited to multi-turn sandboxes and already largely dominated by the
   terminal `publish()` work it cannot hide.
2. **Cross-session overlap** (pre-restore sandbox B while sandbox A executes).
   Still rejected for the same reason as in ADR-0010: `prepare()` runs inside
   the new sandbox (pull model); an orchestrator-side pre-restorer duplicates
   restore logic. Revisit under a pooled-sandbox architecture.
3. **Session-boundary overlap (chosen):** keep the two trivially-correct
   boundary overlaps (3b/3c) and make the *session end* — `publish()` — one
   concurrent fan-out: checkpoint pack builds/uploads overlap the artifact
   packaging phase and each other, instead of overlapping turn execution.

## Decision

Remove ADR-0010 mechanism 3a entirely (background chain, preload map, anchor
computation, drain invariant). Overlap now happens only at session boundaries:

1. **Session start (`prepare()`)** — unchanged (ADR-0010 3b): the session
   archive, workspace packs, and seed download concurrently (independent
   objects); pack application stays sequential (the replay chain is ordered).
2. **Session end (`publish()`)** — one fan-out: workspace packaging, session
   archive, trace file, and *all* checkpoint pack builds run concurrently, then
   the artifact batch and pack uploads upload concurrently; finally the
   retention pass deletes dropped packs and writes the index. `checkpoints.overlap`
   retains its name and default (`true`) but now selects *concurrent vs
   serialized publish fan-out* — kept as a one-line toggle so the benchmark can
   run a real A/B (below), not as a mechanism fork.

Why concurrent pack builds are safe where the old background sync was risky:

- pack content comes from **immutable git commits** (never the working tree),
  and each pack stages into its own directory — concurrent builds cannot tear
  each other;
- the checkpoint index is still written **only after** every pack upload in the
  same pass completes (ADR-0010 invariant 1 preserved);
- no state survives between turns — everything happens inside one `publish()`
  call, so there is no interleaving with harness execution to reason about.

### Correctness invariants (carried over from ADR-0010)

1. `checkpoints.json` is written only after all pack uploads of the same sync
   have completed — restore never sees a half-synced chain.
2. No reclaim event fires while any upload is in flight — the fan-out is fully
   awaited before the sandbox-reclaim notification (ADR-0010 invariant 2
   becomes trivial: there is no cross-turn background work left).
3. A crash mid-publish leaves at most orphaned pack objects; restore correctness
   is unaffected (index untouched).

### Verification

`bench/run-pipeline.bench.js` is reworked to A/B the new mechanism on the real
codex channel (real git/tar/gzip/uploads, scripted model endpoint only,
ADR-0004):

- **serial mode** (`overlap: false`): packaging, pack builds/uploads, and the
  artifact batch run sequentially;
- **overlap mode** (`overlap: true`): the publish fan-out described above.

Integrity gates (unchanged in spirit): every turn's payload present at the
expected size; a fresh restore from the published index reproduces the final
workspace bit-for-bit; and a **phase-overlap gate** — the overlap mode's publish
wall time must be less than the sum of its packaging and checkpoint-sync
durations (concurrency must actually happen), while the serial mode's must not.
The critical-path model becomes `publish_serial ≈ K + ΣC + U` vs
`publish_overlap ≈ max(K, ΣC) + U` (K = packaging, C = pack build/upload,
U = artifact uploads); the projected saving `min(K, ΣC)` is computed from the
serial run's own measured constants and checked against the observed
`T_serial − T_overlap`.

Measured (full workload, 5 turns × 8 MiB, 1 warmup + 3 measured pairs,
mode order alternated; all integrity gates green — including the
publish-concurrency gate and bit-for-bit restore):

| Constant / metric (median, ms) | serial | overlap |
|---|---|---|
| execution ΣE | 10 939 | 11 041 |
| packaging K | 1 700 | 2 583 |
| checkpoint sync ΣC | 1 198 | 1 482 |
| publish wall time | 3 150 | 2 828 |
| run total prepare→publish | 15 670 | 15 380 |

Reading: the publish fan-out saves 322 ms of publish wall time. The naive
projection `min(K, ΣC) = 1 198 ms` over-states the gain because concurrent
tar/gzip inflates both phases (K 1 700→2 583, ΣC 1 198→1 482 — CPU contention);
the contention-aware publish-level projection `(K + ΣC) − max(K′, ΣC′) =
2 898 − 2 583 = 315 ms` matches the observed 322 ms almost exactly. Run-total
saving (290 ms) additionally absorbs execution-time drift between modes.

## Consequences

- Foreman loses the ability to hide checkpoint cost under *turn execution*; it
  now hides it under the *terminal packaging work* of `publish()`. For
  multi-turn sandboxes the terminal saving is smaller than 3a's; for
  single-turn runs it is strictly better (3a degenerated to serial there).
- Orchestrator state shrinks: no background chain, no preload map, no anchor
  duplication, no drain step; per-turn `prompt()` keeps only the turn commit.
- `ckptSyncRecords` phases collapse to `'publish'`; `timings.checkpointDrainMs`
  is removed, `timings.packagingMs` / `timings.checkpointSyncMs` are added as
  the benchmark's phase-overlap evidence.
- ADR-0010 remains Accepted for 3b/3c and its recorded measurements; this ADR
  supersedes its mechanism 3a. Its two benchmark-surfaced correctness fixes
  (session-archive `.tmp` exclusion, streaming secret scan) are untouched.
- Cross-session overlap stays out of scope until sandboxes are pooled
  (revisit ADR-0010 option 2 then).
