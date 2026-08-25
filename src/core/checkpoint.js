/**
 * Checkpoint — incremental workspace synchronization chain.
 *
 * Goal: all changes made after initialization are packed into incremental
 * change packs; restoring means unpacking and installing them into the sandbox
 * workspace before the agent starts. Multiple checkpoints are retained with a
 * skip-list-style tiered policy that balances pack count against pack size.
 *
 * Model:
 *   workspace history = a sequence of checkpoints (turns 1..n, one git commit per collected turn)
 *   change pack       = the file-level change set between two commits (or empty-tree -> commit,
 *                       from=null): full content for A/M plus a deletion marker for D, tar.gz'd
 *                       as manifest.json + files/<path>
 *   index             = checkpoints.json, the pack list (packs[].turn/from/object);
 *                       from=null marks a full pack (the first pack and every rebaseline pack)
 *
 * Retention policy (CheckpointKeeper, skip-list-style tiers):
 *   checkpoint i's level = v2(i) (the number of trailing zero bits; 0 is the chain-head sentinel)
 *   level 0 (odd turns) keeps only the most recent `recentKeep`; every level >= 1 keeps only
 *   the most recent `perLevel` -> newer checkpoints are dense (adjacent step 1) while older ones
 *   are exponentially sparse (step 2^L), for a total of O(recentKeep + perLevel·log n) — a
 *   skip-list-style balance between checkpoint count and per-pack size.
 *   After each publish the retention set is recomputed: packs of dropped checkpoints are
 *   deleted; packs whose anchor drifted (from no longer the nearest retained predecessor) are
 *   rebuilt from local git history as merged packs (LSM-compaction semantics: adjacent small
 *   packs merge into one pack spanning multiple turns).
 */
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { archiveDirectory, extractArchive } from './workspace.js'

/** Object-store key of a checkpoint pack (the <sessionId>/ prefix is added by the caller). */
export const packKey = (turn) => `checkpoint-${turn}.tar.gz`

/** Object-store key of the checkpoint index. */
export const INDEX_KEY = 'checkpoints.json'

/** v2(i): the number of trailing zero bits of i (the 2-adic valuation of i). */
function trailingZeros(i) {
  let level = 0
  let value = i
  while (value % 2 === 0 && value > 0) { level += 1; value /= 2 }
  return level
}

/**
 * Skip-list-style checkpoint retention policy (pure functions).
 *
 * @param {object} options
 * @param {number} [options.recentKeep=4] how many recent checkpoints level 0 (odd turns) keeps
 * @param {number} [options.perLevel=2] how many recent checkpoints every level >= 1 keeps
 */
export class CheckpointKeeper {
  constructor({ recentKeep = 4, perLevel = 2 } = {}) {
    this.recentKeep = recentKeep
    this.perLevel = perLevel
  }

  /** The tier of checkpoint i (0 = chain-head sentinel; i > 0 is v2(i)). */
  levelOf(i) {
    return i === 0 ? Number.POSITIVE_INFINITY : trailingZeros(i)
  }

  /**
   * Compute the retention set over [0, n] (ascending). 0 is the chain-head sentinel (produces no
   * pack); n is the latest turn (always kept).
   * @param {number} n number of turns since the latest rebaseline (>= 1)
   * @returns {number[]}
   */
  keepIndices(n) {
    if (n < 1) throw new Error(`checkpoint: n must be >= 1 (got ${String(n)})`)
    const byLevel = new Map() // level -> ascending index list
    for (let i = 1; i <= n; i += 1) {
      const level = trailingZeros(i)
      if (!byLevel.has(level)) byLevel.set(level, [])
      byLevel.get(level).push(i)
    }
    const kept = new Set([0, n])
    for (const [level, indices] of byLevel) {
      const limit = level === 0 ? this.recentKeep : this.perLevel
      for (const index of indices.slice(-limit)) kept.add(index)
    }
    return [...kept].sort((a, b) => a - b)
  }

  /**
   * Derive the required pack list from the retention set: each retained checkpoint's from is
   * its predecessor within the retained set (from=null when the predecessor is 0, i.e. a full
   * pack starting from the empty tree).
   * @param {number} n
   * @returns {Array<{turn: number, from: number|null}>}
   */
  planPacks(n) {
    const kept = this.keepIndices(n)
    const packs = []
    for (let index = 1; index < kept.length; index += 1) {
      const turn = kept[index]
      const prev = kept[index - 1]
      packs.push({ turn, from: prev === 0 ? null : prev })
    }
    return packs
  }
}

/**
 * Build a change pack: staging directory (manifest.json + files/<path>) -> tar.gz.
 *
 * Pack content comes from git commit history (the tree of toRef), never from
 * the working directory — intercepted secret files never entered a commit, so
 * they naturally never appear in a pack (interception semantics are inherited
 * along the chain).
 *
 * @param {import('../core/git-workspace.js').GitWorkspace} gitWs
 * @param {object} options
 * @param {string|null} options.fromRef start commit (null = full pack from the empty tree)
 * @param {string} options.toRef end commit
 * @param {number|null} options.fromTurn start turn number (manifest metadata)
 * @param {number} options.toTurn end turn number
 * @param {string} options.outPath output tar.gz path
 * @returns {Promise<{ path: string, changes: Array<{status: string, path: string}>, fileCount: number }>}
 */
export async function buildChangePack(gitWs, { fromRef, toRef, fromTurn, toTurn, outPath }) {
  const changes = fromRef === null
    ? (await gitWs.lsTree(toRef)).map((path) => ({ status: 'A', path }))
    : await gitWs.diffFiles(fromRef, toRef)
  const staging = `${outPath}.staging`
  await rm(staging, { recursive: true, force: true })
  await mkdir(staging, { recursive: true })
  for (const change of changes) {
    if (change.status === 'D') continue // deletions carry no content
    const content = await gitWs.readFileAt(toRef, change.path)
    if (content === null) throw new Error(`checkpoint: ${change.path} missing in ${toRef}`)
    const target = join(staging, 'files', change.path)
    await mkdir(dirname(target), { recursive: true })
    await writeFile(target, content)
  }
  const manifest = { format: 1, fromTurn, toTurn, changes }
  await writeFile(join(staging, 'manifest.json'), JSON.stringify(manifest, null, 2))
  await archiveDirectory(staging, outPath)
  await rm(staging, { recursive: true, force: true })
  return { path: outPath, changes, fileCount: changes.filter((c) => c.status !== 'D').length }
}

/**
 * Apply a change pack to a workspace (restore path): delete D entries, write
 * A/M files per the manifest, then commit. The commit goes through commitAll
 * (the secret scan still runs — defense in depth).
 *
 * @param {import('../core/git-workspace.js').GitWorkspace} gitWs
 * @param {string} packDir unpacked pack directory (manifest.json + files/)
 * @param {string} message commit message
 * @returns {Promise<{committed: boolean, oid?: string, files: string[]}>}
 */
export async function applyChangePack(gitWs, packDir, message) {
  const manifest = JSON.parse(await readFile(join(packDir, 'manifest.json'), 'utf8'))
  for (const change of manifest.changes) {
    const target = join(gitWs.options.cwd, change.path)
    if (change.status === 'D') {
      await rm(target, { force: true })
      continue
    }
    const content = await readFile(join(packDir, 'files', change.path))
    await mkdir(dirname(target), { recursive: true })
    await writeFile(target, content)
  }
  const result = await gitWs.commitAll(message)
  if (result.oid === undefined && result.committed) throw new Error('checkpoint: apply produced no commit oid')
  return result
}

/** Unpack a change pack into a temporary directory (restore prerequisite). */
export async function extractChangePack(archivePath, destDir) {
  await extractArchive(archivePath, destDir)
  return destDir
}
