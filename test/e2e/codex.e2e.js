/**
 * Codex channel e2e (ADR-0005, ADR-0009): the REAL codex binary
 * (`codex app-server --stdio`, codex-cli 0.149.1) driven end to end through
 * foreman over the canonical `codex` channel:
 *
 *   Round 1  cold start: prepare (workspace seed — no dsh composition config
 *            needed), start (subprocess + initialize/thread/start), prompt
 *            (a tool-calling turn: the harness runs exec_command), collect,
 *            publish (workspace + CODEX_HOME session archive + checkpoint
 *            pack), shutdown
 *   Round 2  cross-sandbox resume: sandboxDir wiped, same sessionId — the
 *            workspace restores from the checkpoint chain and the codex
 *            thread resumes via the restored sessionId->threadId index (the
 *            model request must carry round 1's prompt text)
 *
 * The model endpoint is a local scripted Responses-API fixture: the network,
 * codex binary, git, tar and HTTP uploads are all real; only the model is
 * scripted (ADR-0004). Requires the `codex` binary on PATH; a missing binary
 * fails loud.
 *
 * Usage: node test/e2e/codex.e2e.js [--keep]
 */
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { downloadArtifact, uploadArtifact } from '../../src/control-plane.js'
import { startMockControlPlane } from '../mocks/control-plane.js'
import { startCodexResponsesFixture } from '../fixtures/codex-responses.js'
import { archiveDirectory } from '../../src/core/workspace.js'
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

const fileExists = async (path) => { try { await access(path); return true } catch { return false } }

// The codex binary is a hard prerequisite — never a skip
await requireBinary('codex', ['--version'], 'npm install -g @openai/codex')

const agentId = 'agent-codex'
const sessionId = 'sess-e2e-codex-001'
const ROUNDS = 2
const CHECKPOINT_OPTIONS = { recentKeep: 2, perLevel: 1, rebaseAfter: 0 }

const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-codex-'))
const sandboxDir = join(base, 'sandbox') // shared by both rounds -> identical workspace paths (session resume requirement)
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
log('mock control plane (object storage + bus) port:', controlPlane.port)

const model = await startCodexResponsesFixture({
  commandFor: (text) => text.includes('ROUND 2')
    ? 'mkdir -p turns && echo codex-round-2 > turns/turn-2.txt && echo codex-round-2 >> journal.txt'
    : 'mkdir -p turns && echo codex-round-1 > out.txt && echo codex-round-1 >> journal.txt',
  finalTextFor: (text) => text.includes('ROUND 2') ? 'CODEX_ROUND_2_OK' : 'CODEX_ROUND_1_OK',
})
log('scripted Responses model endpoint port:', model.port)

// ---- Seed: workspace only (the codex channel needs no dsh composition config) ----
const seedDir = join(base, 'seed-workspace')
await mkdir(join(seedDir, 'src'), { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# seed workspace\n\ncodex channel e2e.\n')
await writeFile(join(seedDir, 'src', 'app.js'), 'export function main() { return 1 }\n')
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(seedArchive))
log('object storage seeded (workspace.tar.gz)')

for (let round = 1; round <= ROUNDS; round += 1) {
  console.log(`\n=== RUN ${round}: ${round === 1 ? 'cold start (thread/start)' : 'cross-sandbox resume (thread/resume)'} ===`)

  // Simulate sandbox destruction: the whole sandboxDir is wiped; workspace and
  // session can only be restored from object storage
  await rm(sandboxDir, { recursive: true, force: true })
  await mkdir(sandboxDir, { recursive: true })

  const requestsAtRound = model.requests.length
  const foreman = new Foreman({
    workdir: sandboxDir,
    agentId,
    sessionId,
    channel: 'codex', // canonical id (ADR-0009)
    codex: { baseUrl: `${model.baseUrl}/v1`, apiKey: 'test-key' },
    controlPlane,
    secretValues: ['test-key'],
    git: { enabled: true },
    checkpoints: CHECKPOINT_OPTIONS,
  })
  await foreman.prepare()

  if (round > 1) {
    assert(`R${round} workspace restored from the checkpoint chain (round 1's out.txt + journal.txt present)`,
      await fileExists(join(sandboxDir, 'workspace', 'out.txt'))
      && (await readFile(join(sandboxDir, 'workspace', 'journal.txt'), 'utf8')) === 'codex-round-1\n')
  }

  const init = await foreman.start()
  assert(`R${round} codex app-server started (threadId assigned)`, typeof init.threadId === 'string' && init.threadId.length > 0)
  assert(`R${round} start returns resumed=${round > 1}`, init.resumed === (round > 1), `resumed=${String(init.resumed)}`)

  const { reason } = await foreman.prompt(`please record the progress: CODEX ROUND ${round}`, { timeoutMs: 180_000 })
  assert(`R${round} turn completed`, reason?.kind === 'completed', JSON.stringify(reason))
  if (process.env.DEBUG_DUMP) {
    for (const [i, request] of model.requests.slice(requestsAtRound).entries()) {
      console.log(`    [R${round} req ${i}]`, (request.input ?? []).map((item) => `${item.type}${item.role ? `:${item.role}` : ''}`).join(' | '))
      for (const item of request.input ?? []) {
        if (item?.type === 'function_call') console.log(`      [call]`, item.arguments)
        if (item?.type === 'function_call_output') console.log(`      [output]`, JSON.stringify(item.output).slice(0, 300))
      }
    }
  }

  // Internal frames: the codex notifications mapped onto the generic inbound model
  const eventTypes = foreman.events.map((event) => event.type)
  assert(`R${round} tool-calling turn produced tool/call + tool/result frames`,
    eventTypes.includes('tool/call') && eventTypes.includes('tool/result'),
    eventTypes.join(','))
  const toolCall = foreman.events.find((event) => event.type === 'tool/call')
  assert(`R${round} tool/call carries the exec_command name and the command line`,
    toolCall?.data?.name === 'exec_command' && typeof toolCall?.data?.arguments?.command === 'string')
  assert(`R${round} assistant/message frames carry the scripted final text`,
    foreman.events.some((event) => event.type === 'assistant/message'
      && event.data.message.content.some((block) => block.text === `CODEX_ROUND_${String(round)}_OK`)))

  // Cross-sandbox session resume: this round's first model request must carry
  // the previous round's prompt text (history can only come from the restored
  // CODEX_HOME thread store + thread/resume)
  if (round > 1) {
    const inputText = (request) => JSON.stringify(request.input ?? [])
    assert(`R${round} thread resumed with history (model request carries round 1's prompt text)`,
      model.requests.slice(requestsAtRound).some((request) => inputText(request).includes('CODEX ROUND 1')))
  }

  const collected = await foreman.collect()
  const published = await foreman.publish()
  await foreman.shutdown()

  assert(`R${round} final answer extracted from the assistant message`,
    collected.finalAnswer === `CODEX_ROUND_${String(round)}_OK`, collected.finalAnswer)
  assert(`R${round} harness file changes visible in the manifest diff`,
    collected.manifestDiff.added.length + collected.manifestDiff.modified.length > 0)
  assert(`R${round} publish status ok`, published.result.status === 'ok')
  assert(`R${round} checkpoint pack synced to object storage`,
    published.result.checkpoints.packs.length > 0
      && published.result.checkpoints.packs.some((pack) => pack.turn === round))

  if (round === 1) {
    assert('R1 exec_command wrote files inside the sandboxed workspace',
      (await readFile(join(sandboxDir, 'workspace', 'out.txt'), 'utf8')).trim() === 'codex-round-1')
    const sessionIndex = JSON.parse(await downloadArtifact(controlPlane, agentId, `${sessionId}/checkpoints.json`))
    assert('R1 checkpoint index written (skip-list plan for turn 1)', JSON.stringify(
      sessionIndex.packs.map(({ turn, from }) => ({ turn, from })),
    ) === JSON.stringify([{ turn: 1, from: null }]))
  }
  if (round === 2) {
    const journal = await readFile(join(sandboxDir, 'workspace', 'journal.txt'), 'utf8')
    assert('R2 final workspace accumulates both rounds (chain restore + this round)', journal === 'codex-round-1\ncodex-round-2\n')
    assert('R2 turn-2 file written by the resumed thread',
      await fileExists(join(sandboxDir, 'workspace', 'turns', 'turn-2.txt')))
  }
}

// ---- Summary ----
console.log(`\n===== Summary: ${results.filter((r) => r.pass).length}/${results.length} PASS =====`)
if (!keep) await rm(base, { recursive: true, force: true })
await model.close()
await controlPlane.close()
if (results.some((r) => !r.pass)) process.exitCode = 1
