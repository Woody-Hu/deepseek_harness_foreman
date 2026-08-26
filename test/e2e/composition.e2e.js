/**
 * Composition acceptance e2e (ADR-0012): the SAME orchestrator starts
 * DIFFERENT harnesses with zero code changes and zero channel constructor
 * options — the channel selection (and the codex wiring) comes entirely from
 * `foreman.config.json`.
 *
 *   Part A (always runs)  legacy alias via config: harness.channel='stdio'
 *                         resolves to the canonical 'dsh-sdk' entry and the
 *                         composition config is fetched from object storage
 *                         and materialized (registry-level acceptance; no dsh
 *                         repository needed).
 *   Part B (dsh repo)     full dsh-sdk run driven purely by config: real dsh
 *                         binary against the scripted mock model, one
 *                         REPLY_DIRECTLY turn, collect + publish. Skips
 *                         loudly when the harness repository is absent.
 *   Part C (codex bin)    full codex run driven purely by config: the REAL
 *                         codex app-server binary (codex-cli 0.149.1) with
 *                         model/provider wiring from harness.codex; only the
 *                         apiKey arrives via the constructor (env-injected
 *                         secrets never live in the config file). Skips when
 *                         the binary is absent.
 *
 * Usage: node test/e2e/composition.e2e.js [--keep]
 */
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { execFile } from 'node:child_process'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { uploadArtifact } from '../../src/control-plane.js'
import { startMockControlPlane } from '../mocks/control-plane.js'
import { startMockModel } from '../mocks/model.js'
import { startCodexResponsesFixture } from '../fixtures/codex-responses.js'
import { archiveDirectory } from '../../src/core/workspace.js'

const repoRoot = new URL('../../../../', import.meta.url).pathname
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

const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-composition-'))
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
log('mock control plane (object storage + bus) port:', controlPlane.port)

/** Seed a minimal workspace snapshot for agentId+sessionId. */
async function seedWorkspace(agentId, sessionId, extra = {}) {
  const seedDir = join(base, `seed-${sessionId}`)
  await mkdir(join(seedDir, 'src'), { recursive: true })
  await writeFile(join(seedDir, 'README.md'), '# seed workspace\n\ncomposition acceptance e2e.\n')
  await writeFile(join(seedDir, 'src', 'app.js'), 'export function main() { return 1 }\n')
  for (const [path, content] of Object.entries(extra)) await writeFile(join(seedDir, path), content)
  const archive = join(base, `seed-${sessionId}.tar.gz`)
  await archiveDirectory(seedDir, archive)
  await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(archive))
}

// ================= Part A: legacy alias resolution via config (always runs) =================
console.log('\n=== PART A: config harness.channel=stdio (legacy alias) -> canonical dsh-sdk ===')
{
  const agentId = 'agent-comp-a'
  const sessionId = 'sess-e2e-comp-a'
  await seedWorkspace(agentId, sessionId)
  // The composition config is cloud-owned: any valid cordis.yml content works
  // (it is materialized, not parsed, at prepare time)
  await uploadArtifact(controlPlane, agentId, `${sessionId}/cordis.yml`, Buffer.from('version: 1\nplugins: []\n'))

  const configPath = join(base, 'alias.config.json')
  await writeFile(configPath, JSON.stringify({ harness: { channel: 'stdio' } }))

  const sandboxDir = join(base, 'sandbox-a')
  const foreman = new Foreman({
    workdir: sandboxDir,
    agentId,
    sessionId,
    controlPlane,
    configPath, // the ONLY channel selection input
  })
  await foreman.prepare()
  const entry = foreman.channelEntry()
  assert('A: legacy alias resolved to the canonical dsh-sdk entry', entry.id === 'dsh-sdk' && foreman.channelId === 'dsh-sdk')
  assert('A: composition config fetched from object storage and materialized',
    foreman.configPath === join(sandboxDir, 'cordis.yml') && await fileExists(foreman.configPath))
  assert('A: dsh-sdk entry declares the cordis.yml composition file', entry.compositionFile === 'cordis.yml')
}

// ================= Part B: full dsh-sdk run driven purely by config =================
const dshAvailable = await fileExists(join(repoRoot, 'foreman', 'solution', 'cordis.yml'))
if (!dshAvailable) {
  console.log('\n=== PART B: SKIP (dsh repository not found at', join(repoRoot, 'foreman', 'solution'), ') ===')
} else {
  console.log('\n=== PART B: config harness.channel=dsh-sdk -> full dsh run ===')
  const agentId = 'agent-comp-b'
  const sessionId = 'sess-e2e-comp-b'
  await seedWorkspace(agentId, sessionId)
  await uploadArtifact(controlPlane, agentId, `${sessionId}/cordis.yml`,
    await readFile(join(repoRoot, 'foreman', 'solution', 'cordis.yml')))

  const configPath = join(base, 'dsh.config.json')
  await writeFile(configPath, JSON.stringify({ harness: { channel: 'dsh-sdk' } }))

  const model = await startMockModel()
  log('scripted mock model port:', model.port)

  const sandboxDir = join(base, 'sandbox-b')
  const foreman = new Foreman({
    repoRoot,
    workdir: sandboxDir,
    pluginsDir: join(repoRoot, 'foreman/solution/plugins'),
    agentId,
    sessionId,
    modelEnv: { DEEPSEEK_API_KEY: 'sk-comp-env-only', DEEPSEEK_BASE_URL: `http://127.0.0.1:${model.port}` },
    controlPlane,
    configPath, // the ONLY channel selection input
  })
  await foreman.prepare()
  assert('B: channel resolved from config (dsh-sdk)', foreman.channelId === 'dsh-sdk')

  await foreman.start()
  const { reason } = await foreman.prompt('REPLY_DIRECTLY: confirm you are running', { timeoutMs: 120_000 })
  assert('B: turn completed', reason?.kind === 'completed', JSON.stringify(reason))
  const collected = await foreman.collect()
  const published = await foreman.publish()
  await foreman.shutdown()
  assert('B: final answer collected', typeof collected.finalAnswer === 'string' && collected.finalAnswer.length > 0, collected.finalAnswer)
  assert('B: publish status ok', published.result.status === 'ok')
  await model.close()
}

// ================= Part C: full codex run driven purely by config =================
const codexAvailable = await new Promise((resolve) => {
  execFile('codex', ['--version'], (error) => { resolve(error === null) })
})
if (!codexAvailable) {
  console.log('\n=== PART C: SKIP (codex binary not found on PATH) ===')
} else {
  console.log('\n=== PART C: config harness.channel=codex -> full codex app-server run ===')
  const agentId = 'agent-comp-c'
  const sessionId = 'sess-e2e-comp-c'
  await seedWorkspace(agentId, sessionId)

  const model = await startCodexResponsesFixture({
    commandFor: () => 'mkdir -p turns && echo composition-ok > out.txt',
    finalTextFor: () => 'COMPOSITION_CODEX_OK',
  })
  log('scripted Responses model endpoint port:', model.port)

  // Everything except the secret lives in the config file: channel selection,
  // binary, model, provider endpoint (ADR-0012: config-only composition)
  const configPath = join(base, 'codex.config.json')
  await writeFile(configPath, JSON.stringify({
    harness: {
      channel: 'codex',
      codex: {
        model: 'gpt-5.1-codex',
        approvalPolicy: 'never',
        provider: { name: 'foreman-fixture', baseUrl: `${model.baseUrl}/v1` },
      },
    },
  }))

  const sandboxDir = join(base, 'sandbox-c')
  const foreman = new Foreman({
    workdir: sandboxDir,
    agentId,
    sessionId,
    controlPlane,
    secretValues: ['test-key'],
    git: { enabled: true },
    configPath, // the ONLY channel selection input
    codex: { apiKey: 'test-key' }, // env-injected secret — never in the config file
  })
  await foreman.prepare()
  const entry = foreman.channelEntry()
  assert('C: channel resolved from config (codex registry entry)', foreman.channelId === 'codex' && entry.compositionFile === undefined)

  const init = await foreman.start()
  assert('C: codex app-server started from config wiring (threadId assigned)',
    typeof init.threadId === 'string' && init.threadId.length > 0)
  const { reason } = await foreman.prompt('please write the composition proof file', { timeoutMs: 180_000 })
  assert('C: turn completed', reason?.kind === 'completed', JSON.stringify(reason))

  const collected = await foreman.collect()
  const published = await foreman.publish()
  await foreman.shutdown()
  assert('C: final answer extracted', collected.finalAnswer === 'COMPOSITION_CODEX_OK', collected.finalAnswer)
  assert('C: harness wrote files inside the workspace',
    (await readFile(join(sandboxDir, 'workspace', 'out.txt'), 'utf8')).trim() === 'composition-ok')
  assert('C: publish status ok', published.result.status === 'ok')
  await model.close()
}

// ---- Summary ----
console.log(`\n===== Summary: ${results.filter((r) => r.pass).length}/${results.length} PASS =====`)
if (!keep) await rm(base, { recursive: true, force: true })
await controlPlane.close()
if (results.some((r) => !r.pass)) process.exitCode = 1
