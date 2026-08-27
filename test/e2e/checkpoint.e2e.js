/**
 * Foreman checkpoint-chain e2e test: workspace incremental sync + skip-list
 * multi-checkpoint retention.
 *
 * Scenario (7 rounds, one external sessionId, one sandboxDir absolute path;
 * between rounds the entire sandboxDir is rm -rf'd, simulating "k8s destroying
 * the Pod and rescheduling" — the workspace can only be restored from the
 * incremental pack chain in object storage):
 *   Rounds 1-4  CHECKPOINT n: the model writes turns/turn-n.txt (A) and
 *               overwrites journal.txt via bash append (M)
 *   Round 5     WRITE_SECRET: writes the clean output notes.md + the
 *               secret-looking leak.txt (git interception)
 *   Round 6     CHECKPOINT 6: also triggers a rebaseline (rebaseAfter=6)
 *   Round 7     CHECKPOINT 7: the chain continues after the rebase (full pack
 *               6 + delta pack 6->7)
 *
 * Verifies:
 *   - Restore: after each round's prepare the workspace equals the
 *     accumulation of all previous rounds (full first pack + delta packs
 *     replayed in order)
 *   - Delta: a single round's pack contains only that round's changes (A/M),
 *     no historical files
 *   - Merge: after retention drops old checkpoints, packs whose anchor
 *     drifted are rebuilt as cross-round merged packs
 *   - Delete: dropped packs disappear from object storage (404)
 *   - Rebase: when the chain exceeds its bound the current tree is packed as
 *     a from=null full pack resetting the chain head (bounded restore chain)
 *   - Interception inheritance: files intercepted by git secret scanning
 *     never enter commits -> never enter any pack -> do not exist after
 *     restore
 *
 * Usage: node test/e2e/checkpoint.e2e.js [--keep]. Requires the dsh npm
 * distribution on PATH (ADR-0012; see README "Prerequisites"); a missing
 * binary fails loud.
 */
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { downloadArtifact, uploadArtifact } from '../../src/control-plane.js'
import { startMockControlPlane } from '../mocks/control-plane.js'
import { startMockModel } from '../mocks/model.js'
import { archiveDirectory, extractArchive } from '../../src/core/workspace.js'
import { requireBinary } from '../require-bin.js'

const repoDir = new URL('../../', import.meta.url).pathname
const keep = process.argv.includes('--keep')

// The dsh distribution binary is a hard prerequisite (ADR-0012) — never a skip
await requireBinary('dsh-jsonrpc-agent', [], 'npm install -g @deepseek-ai/dsh-sdk-jsonrpc-demo (see README Prerequisites)')

const t0 = Date.now()
const log = (...args) => { console.log(`[+${((Date.now() - t0) / 1000).toFixed(1)}s]`, ...args) }

const results = []
function assert(name, condition, detail = '') {
  const pass = Boolean(condition)
  results.push({ name, pass })
  console.log(`  ${pass ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`)
}

const fileExists = async (path) => { try { await access(path); return true } catch { return false } }

const ENV_SECRET = 'sk-foreman-ckpt-secret-31d7c0'
const agentId = 'agent-ckpt'
const sessionId = 'sess-e2e-ckpt-001'
const ROUNDS = 7
// Skip-list retention policy (recentKeep=2 / perLevel=1 has a pronounced
// effect); rebaseAfter=6 -> round 6 triggers the reset
const CHECKPOINT_OPTIONS = { recentKeep: 2, perLevel: 1, rebaseAfter: 6 }

const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-ckpt-'))
const sandboxDir = join(base, 'sandbox') // shared by all 7 rounds -> identical workspace absolute paths (session resumable)
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
log('mock control plane (object storage + bus) port:', controlPlane.port)

// ---- Seed: config and workspace present in object storage before the first round ----
const seedDir = join(base, 'seed-workspace')
await mkdir(join(seedDir, 'src'), { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# seed workspace\n\ncheckpoint chain e2e.\n')
await writeFile(join(seedDir, 'src', 'app.js'), 'export function main() { return 1 }\n')
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
await uploadArtifact(controlPlane, agentId, `${sessionId}/cordis.yml`, await readFile(join(repoDir, 'cordis.yml')))
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(seedArchive))
log('object storage seeded (cordis.yml + workspace.tar.gz)')

const model = await startMockModel() // shared by all 7 rounds (a stateless scripted model)
const modelRequestsAtTurn = [] // request count before each round's first request (for history assertions)

/** Read the checkpoint index from object storage. */
async function readIndex() {
  try {
    return JSON.parse((await downloadArtifact(controlPlane, agentId, `${sessionId}/checkpoints.json`)).toString('utf8'))
  } catch { return null }
}

/** Download and extract checkpoint-<turn>.tar.gz; return its manifest. */
async function readPackManifest(turn) {
  const buffer = await downloadArtifact(controlPlane, agentId, `${sessionId}/checkpoint-${turn}.tar.gz`)
  const file = join(base, `inspect-${turn}.tar.gz`)
  const dir = join(base, `inspect-${turn}`)
  await writeFile(file, buffer)
  await rm(dir, { recursive: true, force: true })
  await extractArchive(file, dir)
  return JSON.parse(await readFile(join(dir, 'manifest.json'), 'utf8'))
}

/** Whether a key exists in object storage. */
async function objectExists(key) {
  try { await downloadArtifact(controlPlane, agentId, key); return true } catch { return false }
}

// ---- Deterministic expectations for the index after each round
// (recentKeep=2 / perLevel=1 / rebaseAfter=6) ----
const EXPECTED_INDEX = {
  1: [{ turn: 1, from: null }],
  2: [{ turn: 1, from: null }, { turn: 2, from: 1 }],
  3: [{ turn: 1, from: null }, { turn: 2, from: 1 }, { turn: 3, from: 2 }],
  4: [{ turn: 1, from: null }, { turn: 2, from: 1 }, { turn: 3, from: 2 }, { turn: 4, from: 3 }],
  // Round 5 (WRITE_SECRET): K(5)={0,2,3,4,5}, drop 1 -> checkpoint-1 deleted,
  // checkpoint-2 rebuilt as a full pack (0->2)
  5: [{ turn: 2, from: null }, { turn: 3, from: 2 }, { turn: 4, from: 3 }, { turn: 5, from: 4 }],
  // Round 6: rebase (6-0 >= 6) -> the chain head resets to a full pack of the
  // current tree; all old packs deleted
  6: [{ turn: 6, from: null }],
  7: [{ turn: 6, from: null }, { turn: 7, from: 6 }],
}

const packList = (index) => index?.packs.map(({ turn, from }) => ({ turn, from }))

for (let round = 1; round <= ROUNDS; round += 1) {
  console.log(`\n=== RUN ${round}: ${round === 5 ? 'WRITE_SECRET (secret-interception round)' : `CHECKPOINT ${round}${round === 6 ? ' (also triggers rebaseline)' : ''}`} ===`)

  // Simulate sandbox destruction: the whole sandboxDir is wiped; workspace and
  // session can only be restored from object storage
  await rm(sandboxDir, { recursive: true, force: true })
  await mkdir(sandboxDir, { recursive: true })

  modelRequestsAtTurn[round] = model.requests.length
  const foreman = new Foreman({
    workdir: sandboxDir,
    pluginsDir: join(repoDir, 'plugins'),
    agentId,
    sessionId,
    modelEnv: {
      DEEPSEEK_API_KEY: ENV_SECRET,
      DEEPSEEK_BASE_URL: `http://127.0.0.1:${model.port}`,
    },
    controlPlane,
    telemetry: { mode: 'DISABLED' }, // this e2e focuses on the incremental workspace chain; the dual trace paths are covered by the cloud/web e2e
    secretValues: [ENV_SECRET],
    git: { enabled: true },
    checkpoints: CHECKPOINT_OPTIONS,
  })
  await foreman.prepare()

  // ---- Restore correctness: accumulated outputs of all previous rounds
  // (turns/*.txt accumulated + journal + seed folded in) ----
  if (round === 1) {
    assert('R1 first round has no index -> seed state (README.md in place)', (await fileExists(join(sandboxDir, 'workspace', 'README.md')))
      && foreman.external.gitBaseline.oid !== undefined)
  } else {
    let restoredOk = true
    for (let prev = 1; prev < round; prev += 1) {
      if (prev === 5) continue // round 5 is WRITE_SECRET (no turns/turn-5.txt)
      restoredOk = restoredOk && await fileExists(join(sandboxDir, 'workspace', 'turns', `turn-${prev}.txt`))
    }
    restoredOk = restoredOk && await fileExists(join(sandboxDir, 'workspace', 'journal.txt'))
    restoredOk = restoredOk && await fileExists(join(sandboxDir, 'workspace', 'README.md')) // the seed is folded into the delta chain
    assert(`R${round} workspace restored from the incremental pack chain (accumulated outputs + seed folded in)`, restoredOk)
    assert(`R${round} intercepted files are not restored along the chain (leak.txt absent)`,
      !(await fileExists(join(sandboxDir, 'workspace', 'leak.txt'))))
    if (round >= 6) {
      assert(`R${round} round 5's clean output restored along the chain (notes.md present)`,
        await fileExists(join(sandboxDir, 'workspace', 'notes.md')))
    }
  }

  await foreman.start()
  const prompt = round === 5
    ? 'please run the cloud task: WRITE_SECRET'
    : `please record the checkpoint progress: CHECKPOINT ${round}`
  const { reason } = await foreman.prompt(prompt)
  assert(`R${round} turn completed`, reason?.kind === 'completed', JSON.stringify(reason))

  // Session resume must be evaluated after model requests happen: this
  // round's first request must carry the previous round's history (the
  // sandbox was destroyed and rebuilt; history can only come from the
  // sessions.tar.gz restore + resume)
  if (round > 1) {
    const prevMarker = round === 6 ? 'WRITE_SECRET' : `CHECKPOINT ${round - 1}`
    const textOf = (message) => typeof message?.content === 'string'
      ? message.content
      : Array.isArray(message?.content) ? message.content.map((block) => block?.text ?? '').join(' ') : ''
    const requests = model.requests.slice(modelRequestsAtTurn[round])
    assert(`R${round} session logs restored in sync (model request carries the previous round's history)`, requests.some((request) => (request.messages ?? []).some(
      (message) => textOf(message).includes(prevMarker),
    )))
  }

  await foreman.collect()
  const published = await foreman.publish()
  await foreman.shutdown()

  // ---- Index evolution: deterministic assertions ----
  const index = await readIndex()
  assert(`R${round} index evolves per the skip-list retention policy`, JSON.stringify(packList(index)) === JSON.stringify(EXPECTED_INDEX[round]),
    JSON.stringify(packList(index)))
  assert(`R${round} rebasedAt correct`, index.rebasedAt === (round >= 6 ? 6 : 0), `rebasedAt=${String(index.rebasedAt)}`)

  // ---- Per-round deep dives ----
  if (round === 3) {
    const manifest = await readPackManifest(3)
    assert('R3 delta semantics: pack(2->3) contains only this round\'s changes (journal.txt M + turn-3.txt A)',
      JSON.stringify(manifest.changes) === JSON.stringify([
        { status: 'M', path: 'journal.txt' },
        { status: 'A', path: 'turns/turn-3.txt' },
      ]), JSON.stringify(manifest.changes))
  }
  if (round === 5) {
    const stats = published.external.checkpoints
    assert('R5 retention stats: 1 old pack dropped + 1 merged pack rebuilt + 1 new pack uploaded',
      stats.deleted === 1 && stats.rebuilt === 1 && stats.uploaded === 1 && stats.kept === 2,
      `deleted=${String(stats.deleted)} rebuilt=${String(stats.rebuilt)} uploaded=${String(stats.uploaded)} kept=${String(stats.kept)}`)
    assert('R5 evicted checkpoint-1 deleted from object storage', !(await objectExists(`${sessionId}/checkpoint-1.tar.gz`)))
    const manifest2 = await readPackManifest(2)
    assert('R5 merged pack: checkpoint-2 rebuilt as a full pack (0->2, contains turn-1.txt)',
      manifest2.fromTurn === null && manifest2.changes.some((c) => c.path === 'turns/turn-1.txt'))
    const manifest5 = await readPackManifest(5)
    assert('R5 secret interception inherited along the chain: the pack excludes leak.txt and includes the clean notes.md',
      !manifest5.changes.some((c) => c.path === 'leak.txt')
      && manifest5.changes.some((c) => c.path === 'notes.md'),
      JSON.stringify(manifest5.changes))
  }
  if (round === 6) {
    const stats = published.external.checkpoints
    assert('R6 rebaseline: all 4 old-chain packs deleted, 1 full pack resets the chain head',
      stats.deleted === 4 && stats.uploaded === 1 && stats.kept === 0,
      `deleted=${String(stats.deleted)} uploaded=${String(stats.uploaded)} kept=${String(stats.kept)}`)
    for (const stale of [2, 3, 4, 5]) {
      assert(`R6 old pack checkpoint-${String(stale)} deleted`, !(await objectExists(`${sessionId}/checkpoint-${String(stale)}.tar.gz`)))
    }
    const manifest6 = await readPackManifest(6)
    assert('R6 full pack: from=null and covers the complete workspace (seed/accumulated outputs/notes)',
      manifest6.fromTurn === null
      && manifest6.changes.some((c) => c.path === 'README.md')
      && manifest6.changes.some((c) => c.path === 'turns/turn-4.txt')
      && manifest6.changes.some((c) => c.path === 'notes.md'))
  }
  if (round === 7) {
    const manifest7 = await readPackManifest(7)
    assert('R7 delta continues after the rebase: pack(6->7) contains only this round\'s changes',
      JSON.stringify(manifest7.changes) === JSON.stringify([
        { status: 'M', path: 'journal.txt' },
        { status: 'A', path: 'turns/turn-7.txt' },
      ]), JSON.stringify(manifest7.changes))
    // Final state: every round's outputs in place (journal contains the lines
    // of 1-4/6-7 — round 5 was WRITE_SECRET with no append)
    let finalOk = true
    for (const turn of [1, 2, 3, 4, 6, 7]) {
      finalOk = finalOk && await fileExists(join(sandboxDir, 'workspace', 'turns', `turn-${String(turn)}.txt`))
    }
    const journal = await readFile(join(sandboxDir, 'workspace', 'journal.txt'), 'utf8')
    finalOk = finalOk && journal === 'round 1\nround 2\nround 3\nround 4\nround 6\nround 7\n'
    finalOk = finalOk && await fileExists(join(sandboxDir, 'workspace', 'notes.md'))
    finalOk = finalOk && await fileExists(join(sandboxDir, 'workspace', 'src', 'app.js'))
    assert('R7 final state: all 7 rounds\' outputs + seed files in place (incremental-chain restore integrity)', finalOk)
    const manifests = await readPackManifest(6)
    // Quantify the "delta" value: the full pack carries the complete
    // workspace while the delta pack carries only this round's changes
    assert('R7 chain scale: full pack carries everything, delta pack only this round (a direct expression of the count-size balance)',
      manifests.changes.length >= 8 && manifest7.changes.length === 2,
      `full=${String(manifests.changes.length)} delta=${String(manifest7.changes.length)}`)
  }
}

// ---- Summary ----
console.log(`\n===== Summary: ${results.filter((r) => r.pass).length}/${results.length} PASS =====`)
if (!keep) await rm(base, { recursive: true, force: true })
// Close mock servers (otherwise the event loop hangs and the process never exits on all-green)
await model.close()
await controlPlane.close()
if (results.some((r) => !r.pass)) process.exitCode = 1
