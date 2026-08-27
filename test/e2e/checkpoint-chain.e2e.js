/**
 * Checkpoint-chain e2e on the real codex channel (ADR-0011): drives a
 * multi-round chain with aggressive retention so the concurrent publish
 * fan-out exercises every sync branch on real work —
 *
 *   - new-pack uploads (each round's pack built+uploaded concurrently with
 *     the artifact packaging)
 *   - retention drops (objects deleted from object storage)
 *   - anchor-drift rebuilds (merged packs rebuilt under the same key)
 *   - rebase (rebaseAfter: the full-pack chain reset)
 *   - cross-round restore integrity (each round's workspace must restore
 *     bit-for-bit from the published chain before the next round starts)
 *
 * Companion of test/e2e/checkpoint.e2e.js (which additionally requires the dsh
 * harness repository checkout); this scenario only needs the `codex` binary.
 * The model endpoint is a local scripted Responses-API fixture: the network,
 * codex binary, git, tar and HTTP uploads are all real; only the model is
 * scripted (ADR-0004). A missing binary fails loud.
 *
 * Usage: node test/e2e/checkpoint-chain.e2e.js [--keep]
 */
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { downloadArtifact, uploadArtifact } from '../../src/control-plane.js'
import { startMockControlPlane } from '../mocks/control-plane.js'
import { startCodexResponsesFixture } from '../fixtures/codex-responses.js'
import { archiveDirectory, fileManifest } from '../../src/core/workspace.js'
import { requireBinary } from '../require-bin.js'

const keep = process.argv.includes('--keep')
const t0 = Date.now()
const log = (...args) => { console.log(`[+${((Date.now() - t0) / 1000).toFixed(1)}s]`, ...args) }

const results = []
function assert(name, condition, detail = '') {
  const pass = Boolean(condition)
  results.push({ name, pass })
  console.log(`  ${pass ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`)
}

// The codex binary is a hard prerequisite — never a skip
await requireBinary('codex', ['--version'], 'npm install -g @openai/codex')

const agentId = 'agent-ckpt-codex'
const sessionId = 'sess-e2e-ckpt-codex-001'
const ROUNDS = 4
// Aggressive retention: recentKeep=1 drops fast, perLevel=1 forces frequent
// anchor-drift rebuilds; rebaseAfter=3 -> round 3 resets the chain
const CHECKPOINT_OPTIONS = { recentKeep: 1, perLevel: 1, rebaseAfter: 3 }

const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-ckpt-codex-'))
const sandboxDir = join(base, 'sandbox') // shared by all rounds (identical workspace paths)
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
const model = await startCodexResponsesFixture({
  commandFor: (text) => {
    const round = /CHAIN ROUND (\d+)/.exec(text)?.[1] ?? '0'
    return `echo chain-round-${round} >> journal.txt`
  },
  finalTextFor: (text) => `CHAIN_ROUND_${/CHAIN ROUND (\d+)/.exec(text)?.[1] ?? '0'}_OK`,
})
log('mock control plane port:', controlPlane.port, '/ scripted Responses model port:', model.port)

// ---- Seed ----
const seedDir = join(base, 'seed-workspace')
await mkdir(seedDir, { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# checkpoint-chain e2e\n')
await writeFile(join(seedDir, 'base.txt'), 'seed\n')
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(seedArchive))

/** Path -> {size, sha256} for every non-.git file (content identity). */
async function contentManifest(dir) {
  const manifest = {}
  for (const [rel, info] of await fileManifest(dir)) {
    if (rel === '.git' || rel.startsWith('.git/')) continue
    manifest[rel] = { size: info.size, sha256: info.sha256 }
  }
  return manifest
}

async function readIndex() {
  try {
    return JSON.parse((await downloadArtifact(controlPlane, agentId, `${sessionId}/checkpoints.json`)).toString('utf8'))
  } catch { return null }
}

/** Simulate sandbox destruction: the whole sandboxDir is wiped and the workspace must restore from the chain. */
const wipeSandbox = async () => {
  await rm(sandboxDir, { recursive: true, force: true, maxRetries: 20, retryDelay: 250 })
  await mkdir(sandboxDir, { recursive: true })
}

for (let round = 1; round <= ROUNDS; round += 1) {
  console.log(`\n=== ROUND ${round} (retention ${JSON.stringify(CHECKPOINT_OPTIONS)}) ===`)
  await wipeSandbox()

  const foreman = new Foreman({
    workdir: sandboxDir,
    agentId,
    sessionId,
    channel: 'codex',
    codex: { baseUrl: `${model.baseUrl}/v1`, apiKey: 'test-key' },
    controlPlane,
    secretValues: ['test-key'],
    git: { enabled: true },
    checkpoints: CHECKPOINT_OPTIONS,
  })
  await foreman.prepare()
  await foreman.start()
  const { reason } = await foreman.prompt(`please record the progress: CHAIN ROUND ${round}`, { timeoutMs: 180_000 })
  assert(`R${round} turn completed`, reason?.kind === 'completed', JSON.stringify(reason))
  await foreman.collect()
  const published = await foreman.publish()
  await foreman.shutdown()

  const stats = published.result.checkpoints
  const index = await readIndex()
  assert(`R${round} index written (turn=${String(stats.turn)}, packs=${String(index.packs.length)})`,
    index !== null && stats.turn === round)
  assert(`R${round} concurrent publish produced pack records (${String(foreman.ckptSyncRecords.length)} pack(s))`,
    foreman.ckptSyncRecords.length > 0)

  if (round === 3) {
    assert('R3 rebaseline reset the chain (single from=null full pack)',
      stats.rebasedAt === 3 && index.packs.length === 1 && index.packs[0].from === null,
      JSON.stringify({ rebasedAt: stats.rebasedAt, packs: index.packs }))
  }

  // Restore integrity: a fresh sandbox must reproduce this round's final
  // workspace bit-for-bit from the published chain
  const finalManifest = await contentManifest(join(sandboxDir, 'workspace'))
  await wipeSandbox()
  const restore = new Foreman({
    workdir: sandboxDir,
    agentId,
    sessionId,
    channel: 'codex',
    controlPlane,
    git: { enabled: true },
    checkpoints: CHECKPOINT_OPTIONS,
  })
  await restore.prepare()
  assert(`R${round} restore reproduces the workspace bit-for-bit (${String(Object.keys(finalManifest).length)} files)`,
    JSON.stringify(await contentManifest(join(sandboxDir, 'workspace'))) === JSON.stringify(finalManifest))
  assert(`R${round} journal accumulates all rounds`,
    (await readFile(join(sandboxDir, 'workspace', 'journal.txt'), 'utf8'))
      === Array.from({ length: round }, (_, i) => `chain-round-${i + 1}`).join('\n') + '\n')
}

// ---- Summary ----
console.log(`\n===== Summary: ${results.filter((r) => r.pass).length}/${results.length} PASS =====`)
if (!keep) await rm(base, { recursive: true, force: true, maxRetries: 20, retryDelay: 250 })
await model.close()
await controlPlane.close()
if (results.some((r) => !r.pass)) process.exitCode = 1
