/**
 * Config-only channel selection e2e (ADR-0002 / ADR-0005 / ADR-0009
 * acceptance): the harness channel must be switchable by changing ONLY the
 * runner configuration file — no constructor-level channel/codex options.
 *
 *   Part 1 (full run, codex)   a foreman.config.json selecting the codex
 *                              channel (model + provider.baseUrl entirely from
 *                              the config file) drives the REAL codex binary
 *                              end to end: prepare -> start -> prompt ->
 *                              collect -> publish. No API key is needed by the
 *                              scripted endpoint, so nothing harness-related
 *                              comes from the constructor.
 *   Part 2 (resolution, dsh)   config files selecting dsh-sdk / dsh-web
 *                              resolve to the right channel through prepare();
 *                              a full dsh start additionally requires the
 *                              harness repository checkout (not present in
 *                              this sandbox) and is covered by the basic/web
 *                              e2e scenarios.
 *
 * The model endpoint is a local scripted Responses-API fixture: the network,
 * codex binary, git, tar and HTTP uploads are all real; only the model is
 * scripted (ADR-0004). Requires the `codex` binary on PATH; a missing binary
 * fails loud.
 *
 * Usage: node test/e2e/config-channel.e2e.js [--keep]
 */
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { uploadArtifact } from '../../src/control-plane.js'
import { CodexChannel } from '../../src/channels/codex-channel.js'
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

const agentId = 'agent-config'
const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-config-'))
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
const model = await startCodexResponsesFixture({
  commandFor: () => 'echo config-only > out.txt',
  finalTextFor: () => 'CONFIG_ONLY_OK',
})
log('mock control plane port:', controlPlane.port, '/ scripted Responses model port:', model.port)

// ---- Seed: workspace only (the codex channel needs no dsh composition config) ----
const seedDir = join(base, 'seed-workspace')
await mkdir(seedDir, { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# config-channel e2e\n')
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
const seedBuffer = await readFile(seedArchive)

// ======================================================================
// Part 1 — full run, channel + model wiring entirely from the config file
// ======================================================================
console.log('\n=== Part 1: config-only codex run (no constructor channel/codex options) ===')

const configPath = join(base, 'foreman.config.json')
await writeFile(configPath, JSON.stringify({
  harness: {
    channel: 'codex',
    codex: {
      model: 'gpt-5.1-codex',
      approvalPolicy: 'never',
      sandbox: 'workspace-write',
      provider: { name: 'Foreman', baseUrl: `${model.baseUrl}/v1` },
    },
  },
}, null, 2))

const sessionId = 'sess-e2e-config-codex-001'
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, seedBuffer)
const sandboxDir = join(base, 'sandbox')
await mkdir(sandboxDir, { recursive: true })

const foreman = new Foreman({
  workdir: sandboxDir,
  agentId,
  sessionId,
  controlPlane,
  configPath, // the ONLY harness-related input
  secretValues: [],
  git: { enabled: true },
  checkpoints: { recentKeep: 2, perLevel: 1, rebaseAfter: 0 },
})
await foreman.prepare()
assert('config file selects the codex channel', foreman.channelId === 'codex', foreman.channelId)
const init = await foreman.start()
assert('config-only wiring launches the real CodexChannel', foreman.channel instanceof CodexChannel)
assert('codex app-server started from config (threadId assigned)', typeof init.threadId === 'string' && init.threadId.length > 0)

const { reason } = await foreman.prompt('please write the marker file: CONFIG ONLY', { timeoutMs: 180_000 })
assert('turn completed', reason?.kind === 'completed', JSON.stringify(reason))
assert('config provider.baseUrl reached the harness (model requests observed)', model.requests.length > 0)
assert('exec_command ran inside the workspace', await fileExists(join(sandboxDir, 'workspace', 'out.txt')))

const collected = await foreman.collect()
const published = await foreman.publish()
await foreman.shutdown()
assert('final answer extracted', collected.finalAnswer === 'CONFIG_ONLY_OK', collected.finalAnswer)
assert('publish status ok', published.result.status === 'ok')
assert('checkpoint pack synced from the config-only run',
  published.result.checkpoints.packs.some((pack) => pack.turn === 1))

// ======================================================================
// Part 2 — dsh channels resolve from the config file alone
// ======================================================================
console.log('\n=== Part 2: config-only dsh-sdk / dsh-web channel resolution ===')

for (const [channel, configKey] of [['dsh-sdk', 'cordis.yml'], ['dsh-web', 'web-patch.yml']]) {
  const dshConfigPath = join(base, `foreman.${channel}.config.json`)
  await writeFile(dshConfigPath, JSON.stringify({ harness: { channel } }, null, 2))
  const dshSessionId = `sess-e2e-config-${channel}`
  await uploadArtifact(controlPlane, agentId, `${dshSessionId}/${configKey}`, Buffer.from('# composition\n'))
  await uploadArtifact(controlPlane, agentId, `${dshSessionId}/workspace.tar.gz`, seedBuffer)
  const dshDir = join(base, `sandbox-${channel}`)
  await mkdir(dshDir, { recursive: true })
  const f = new Foreman({
    workdir: dshDir,
    agentId,
    sessionId: dshSessionId,
    controlPlane,
    configPath: dshConfigPath,
  })
  await f.prepare()
  assert(`config file selects the ${channel} channel`, f.channelId === channel, f.channelId)
}

// ---- Summary ----
console.log(`\n===== Summary: ${results.filter((r) => r.pass).length}/${results.length} PASS =====`)
if (!keep) await rm(base, { recursive: true, force: true })
await model.close()
await controlPlane.close()
if (results.some((r) => !r.pass)) process.exitCode = 1
