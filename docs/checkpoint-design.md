# Checkpoint design

The checkpoint chain turns workspace persistence into an incremental synchronization problem:
all changes made after initialization are packed into incremental change packs, and restoring
a sandbox means unpacking the chain into the workspace before the agent starts. A
skip-list-style tiered retention policy decides how many checkpoints to keep, balancing pack
count against pack size; periodic rebaselines bound the restore chain length.

This document describes the model implemented in [src/core/checkpoint.js](../src/core/checkpoint.js)
and wired by `Foreman.prepare()`/`Foreman.publish()` (both in [src/foreman.js](../src/foreman.js)).

## Model

- **Workspace history** is a sequence of checkpoints, one git commit per collected turn
  (git is required: `checkpoints` configuration requires `git.enabled`).
- **Change pack** = the file-level change set between two commits: full content for added
  and modified files plus deletion markers for removed files, packed as
  `manifest.json + files/<path>` inside `checkpoint-<turn>.tar.gz`.
  `from=null` marks a **full pack** (the first pack, and every rebaseline pack).
- **Index** = `checkpoints.json`, the ordered pack list
  (`packs[].turn/from/object`, `format: 1`, `rebasedAt`).

Object-store layout for one session:

```
<agentId>/<sessionId>/
  checkpoints.json           the index
  checkpoint-<turn>.tar.gz   one pack per retained checkpoint
```

## Retention: skip-list tiers

Checkpoint `i`'s tier is `v2(i)` — the number of trailing zero bits of `i` (the 2-adic
valuation):

- **Level 0** (odd turns) keeps only the most recent `recentKeep` checkpoints.
- **Every level >= 1** keeps only the most recent `perLevel` checkpoints.

Newer checkpoints are therefore dense (adjacent step 1) while older ones are exponentially
sparse (a level-`L` checkpoint spans `2^L` turns), for a total of
`O(recentKeep + perLevel·log n)` packs — the skip-list balance between checkpoint count and
per-pack size. Turn 0 is a chain-head sentinel (kept, produces no pack); the latest turn is
always kept.

Example with `recentKeep=2, perLevel=1` (the values the e2e test uses), turns 1..7:

```
turn:    1  2  3  4  5  6  7
level:   0  1  0  2  0  1  0
kept:       ✓     ✓     ✓  ✓        (per-level most-recent rule)
```

Level 0 keeps {5, 7}; level 1 keeps {6}; level 2 keeps {4} — retention set {4, 5, 6, 7}.

## Pack planning and merging

Each retained checkpoint's `from` is its predecessor **within the retained set**. When
retention drops a checkpoint, the packs after it get a drifted anchor (`from` no longer the
nearest retained predecessor) and are rebuilt from local git history as **merged packs**
spanning multiple turns — LSM-compaction semantics: adjacent small packs merge into one.

Rebuilds happen in-place under the same object name; dropped checkpoints have their pack
objects deleted from object storage. The index is written back atomically as the last step
of every publish, so a restore always replays a consistent chain.

## Rebaseline

`rebaseAfter` bounds the restore chain length: when `turn - rebasedAt >= rebaseAfter`, the
current tree is packed as a `from=null` full pack and the chain head resets (`rebasedAt =
turn`). The rebase pack is kept with the chain as the restore's starting point; subsequent
packs are relative to it. `rebaseAfter: 0` disables rebaselining.

## Restore

`prepare()` detects a `checkpoints.json` index and rebuilds the workspace by replaying the
packs in order — each pack applied to the workspace directory, with one git commit per pack
replaying history (so the local repository, the authoritative change sets, and the next round's
pack diffs all keep working). The seed `workspace.tar.gz` is only used when no index exists
(first round). This round's change baseline becomes the restore end state (the last commit on
the chain).

## Secret interception inheritance

Packs are built from git commit trees, never from the working directory. Files intercepted by
the pre-commit secret scan never entered a commit, so they never appear in any pack and never
come back on restore — interception semantics are inherited along the whole chain with no
extra mechanism.

## Configuration

```js
new Foreman({
  git: { enabled: true },                        // required
  checkpoints: {
    recentKeep: 4,     // level-0 (odd turns) retention, default 4
    perLevel: 2,       // level>=1 retention per tier, default 2
    rebaseAfter: 0,    // chain-length bound before a rebaseline, 0 = never
  },
})
```

`publish()` returns per-round sync stats (`{ turn, rebasedAt, kept, uploaded, rebuilt,
deleted, packs }`) and merges them into the run result uploaded to object storage.

## Verification

`test/e2e/checkpoint.e2e.js` runs a 7-round scenario (each round simulates a destroyed
sandbox by wiping the workspace directory before restore) asserting: cumulative restore
correctness, per-round pack increments containing only that round's changes (A/M), merged
packs after retention drops, deletion of dropped pack objects (404), rebaseline at round 6
followed by chain continuation at round 7, and secret-interception inheritance. Unit tests
cover retention sets, pack planning, and pack round-trips.
