/**
 * Example: Codex Harness app-server channel driven end to end against the
 * REAL DeepSeek API — no scripted model, no mocks.
 *
 * What is real here:
 *   - codex: the npm distribution binary (codex-cli 0.150.x, `codex app-server
 *     --stdio`), not a source checkout
 *   - the model: DeepSeek's Responses-API endpoint
 *     (https://api.deepseek.com/v1/responses — codex 0.150 removed
 *     wire_api="chat", the Responses wire is required)
 *   - the turn: real tool-calling (codex exec_command runs in the workspace)
 *   - git, tar packaging, checkpoint pack build/upload, thread resume index
 *
 * Local mode: the control plane (object storage + message bus) runs as a real
 * local server on 127.0.0.1 (examples/local-control-plane.js) — every artifact
 * and bus event is persisted to disk, nothing is mocked. The only network
 * egress is the model API call itself. The API key is injected via env only
 * (never written to disk; CODEX_HOME/config.toml carries only the endpoint)
 * and is additionally registered as a secret value for masking.
 *
 * Usage:
 *   DEEPSEEK_API_KEY=sk-... node examples/codex-real.js [--keep]
 * (reads DEEPSEEK_API_KEY or deepseek_key from the environment)
 */
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../src/foreman.js'
import { archiveDirectory } from '../src/core/workspace.js'
import { downloadArtifact, uploadArtifact } from '../src/control-plane.js'
import { startLocalControlPlane } from './local-control-plane.js'
import { requireBinary } from '../test/require-bin.js'

const keep = process.argv.includes('--keep')

const API_KEY = process.env.DEEPSEEK_API_KEY ?? process.env.deepseek_key
if (API_KEY === undefined || API_KEY === '') {
  console.error('error: DEEPSEEK_API_KEY (or deepseek_key) is required — a real run needs a real key')
  process.exit(1)
}

// The codex distribution binary is a hard prerequisite — never a skip
await requireBinary('codex', ['--version'], 'npm install -g @openai/codex')

const t0 = Date.now()
const log = (...args) => { console.log(`[+${((Date.now() - t0) / 1000).toFixed(1)}s]`, ...args) }
const fileExists = async (path) => { try { await access(path); return true } catch { return false } }

const agentId = 'agent-example-codex'
const sessionId = `sess-example-codex-${Date.now()}`
const base = await mkdtemp(join(tmpdir(), 'foreman-example-codex-'))
const sandboxDir = join(base, 'sandbox')

// Local mode: a real control-plane server on 127.0.0.1, backed by disk
// (storage/ + bus-events.jsonl under the run's control-plane/ directory)
const controlPlane = await startLocalControlPlane({ dir: join(base, 'control-plane') })
log('local control plane port:', controlPlane.port)

// ---- Seed object storage: workspace only (the codex channel needs no dsh composition config) ----
const seedDir = join(base, 'seed-workspace')
await mkdir(join(seedDir, 'src'), { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# seed workspace\n\ncodex channel real-API example.\n')
await writeFile(join(seedDir, 'src', 'app.js'), 'export function main() { return 1 }\n')
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(seedArchive))
log('object storage seeded (workspace.tar.gz)')

// ---- The run: prepare -> start -> prompt -> collect -> publish -> shutdown ----
const foreman = new Foreman({
  workdir: sandboxDir,
  agentId,
  sessionId,
  channel: 'codex',
  codex: {
    // DeepSeek's Responses-API endpoint; the channel writes wire_api="responses"
    // into CODEX_HOME/config.toml and injects the key via env only
    baseUrl: 'https://api.deepseek.com/v1',
    apiKey: API_KEY,
    model: 'deepseek-chat',
    timeoutMs: 300_000,
  },
  controlPlane,
  secretValues: [API_KEY], // exact-value masking in forwarded events + packaged artifacts
  git: { enabled: true },
  checkpoints: { recentKeep: 2, perLevel: 1, rebaseAfter: 0 },
})

await foreman.prepare()
log('prepared (workspace restored from object storage)')

const init = await foreman.start()
log('codex app-server up (threadId:', init.threadId, ', resumed:', init.resumed, ')')

const { reason } = await foreman.prompt(
  'Inspect this workspace, then create a file named notes.md with a one-line summary of what this project does, '
  + 'and reply with a single sentence describing what you did.',
  { timeoutMs: 300_000 },
)
log('turn/end reason:', JSON.stringify(reason))

const collected = await foreman.collect()
const published = await foreman.publish()
await foreman.shutdown()

// ---- Report ----
console.log('\n===== run result =====')
console.log('final answer:', collected.finalAnswer)
console.log('tool calls:', collected.fsChanges.toolCalls.map((call) => call.name).join(', ') || '(none)')
console.log('changed files:', JSON.stringify(collected.manifestDiff))
console.log('notes.md written by the real turn:', await fileExists(join(sandboxDir, 'workspace', 'notes.md')))
console.log('publish status:', published.result.status, '| checkpoint packs:', JSON.stringify(published.result.checkpoints.packs))

const profile = JSON.parse((await downloadArtifact(controlPlane, agentId, `${sessionId}/profile.json`)).toString('utf8'))
console.log('\n===== profile (ADR-0013 throughput view) =====')
const d = profile.derived
console.log(`wall=${Math.round(d.runWallMs)}ms  prepare=${Math.round(d.prepareMs)}ms  boot=${Math.round(d.bootMs)}ms  `
  + `execution=${Math.round(d.executionMs)}ms  commit=${Math.round(d.commitMs)}ms  collect=${Math.round(d.collectMs)}ms  `
  + `publish=${Math.round(d.publishMs)}ms`)
console.log(`usefulWorkRatio=${(d.usefulWorkRatio * 100).toFixed(1)}%  turnThroughput=${d.turnThroughputPerSec.toFixed(4)}/s  turns=${d.turns}`)

// Secret hygiene: the key must not appear in any uploaded artifact
let leaked = ''
for (const name of ['result.json', 'trace.jsonl', 'workspace.tar.gz', 'sessions.tar.gz', 'profile.json']) {
  const buffer = await downloadArtifact(controlPlane, agentId, `${sessionId}/${name}`)
  if (buffer.includes(API_KEY)) leaked = name
}
console.log('\nsecret hygiene: API key never persisted in artifacts —', leaked === '' ? 'clean' : `LEAK IN ${leaked}`)
if (leaked !== '') process.exitCode = 1

console.log(keep ? `\nrun directory preserved at: ${base}` : '')
if (!keep) await rm(base, { recursive: true, force: true })
await controlPlane.close()
