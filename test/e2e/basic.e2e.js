/**
 * Foreman end-to-end test: simulates the full "cloud control plane + sandbox
 * runner" loop in one process (SDK stdio channel).
 *
 * Scenario (two runs, verifying cold start and cross-sandbox session resume):
 *   RUN 1 (cold start): seed object storage -> foreman restores config and
 *     workspace -> launches dsh -> a multi-step tool task (bash writes
 *     greeting.txt + fs write writes report.md containing a secret-looking
 *     string) -> SSE forwarding (including Last-Event-ID resumption) -> OTLP
 *     trace export (path A) -> collect/redact/package/upload -> message bus
 *     reclaim events.
 *   RUN 2 (session resume): a fresh mock model, the same agentId+sessionId and
 *     the same workspace mount path -> foreman restores workspace + session
 *     logs from object storage -> dsh answers with history (REPLY_DIRECTLY).
 *
 * Assertion coverage: completion detection, artifact collection, manifest
 * change set (bash blind-spot fallback), fs content-level diffs, SSE frame
 * completeness and event-stream redaction, OTLP record counts, packaging
 * exclusion + masking, secrets never persisted, bus events, session history
 * resume.
 *
 * Usage: node test/e2e/basic.e2e.js [--keep] (any cwd; --keep preserves the
 * run directory). Requires the dsh npm distribution on PATH (ADR-0012; see
 * README "Prerequisites"); skips when the binary is absent.
 */
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { execFile } from 'node:child_process'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { extractArchive, archiveDirectory } from '../../src/core/workspace.js'
import { uploadArtifact, downloadArtifact } from '../../src/control-plane.js'
import { startMockControlPlane } from '../mocks/control-plane.js'
import { startMockModel } from '../mocks/model.js'
import { startMockOtlpCollector } from '../mocks/otlp.js'

const repoDir = new URL('../../', import.meta.url).pathname // foreman repository root
const keep = process.argv.includes('--keep')

// Skip when the dsh distribution binary is unavailable (ADR-0012)
const dshAvailable = await new Promise((resolve) => {
  execFile('dsh-jsonrpc-agent', [], (error) => { resolve(error?.code !== 'ENOENT') })
})
if (!dshAvailable) {
  console.log('SKIP: dsh-jsonrpc-agent not found on PATH (install: npm install -g @deepseek-ai/dsh-sdk-jsonrpc-demo — see README Prerequisites)')
  process.exit(0)
}

const t0 = Date.now()
const log = (...args) => { console.log(`[+${((Date.now() - t0) / 1000).toFixed(1)}s]`, ...args) }

const results = []
function assert(name, condition, detail = '') {
  const pass = Boolean(condition)
  results.push({ name, pass })
  console.log(`  ${pass ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`)
}

/** Subscribe to the foreman SSE gateway (simulating a cloud-side consumer). */
async function subscribeSse(port, onFrame, lastEventId) {
  const controller = new AbortController()
  const headers = lastEventId === undefined ? {} : { 'last-event-id': String(lastEventId) }
  const response = await fetch(`http://127.0.0.1:${port}/events`, { signal: controller.signal, headers })
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  const pump = (async () => {
    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const parts = buffer.split('\n\n')
        buffer = parts.pop() ?? ''
        for (const part of parts) {
          const dataLine = part.split('\n').find((line) => line.startsWith('data: '))
          if (dataLine !== undefined) onFrame(JSON.parse(dataLine.slice(6)))
        }
      }
    } catch { /* reads interrupted by abort are a normal close */ }
  })()
  return { abort: () => controller.abort(), settled: pump }
}

const ENV_SECRET = 'sk-foreman-env-secret-9f3ab2' // a secret injected via env, never persisted
const MODEL_SECRET = 'sk-test-12345' // the secret-looking string the mock model writes into report.md
const TENANT_ID = 'tenant-7f2a' // tenant identifier (attribute pipeline: flat env source)
// Run context (the value-arrival contract of the attribute pipeline): one env
// JSON variable, fields picked by dot-path from the spec
const RUN_CONTEXT = { costCenter: 'cc-8842', quota: { tier: 'gold' } }
const agentId = 'agent-7f2a'
const sessionId = 'sess-e2e-basic-001'

const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-basic-'))
const sandboxDir = join(base, 'sandbox') // shared by both runs -> identical workspace absolute paths -> the session can resume
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
log('mock control plane (object storage + bus) port:', controlPlane.port)

// ---- Seed: config and workspace present in object storage before the first
// run (simulating a remote k8s object store) ----
const seedDir = join(base, 'seed-workspace')
await mkdir(join(seedDir, 'src'), { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# seed workspace\n\nrestored from object storage.\n')
await writeFile(join(seedDir, 'src', 'app.js'), 'export function main() { return 1 }\n')
await writeFile(join(seedDir, '.env'), `API_KEY=sk-seed-env-secret-77c1\n`) // must be excluded from packaging
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
await uploadArtifact(controlPlane, agentId, `${sessionId}/cordis.yml`, await readFile(join(repoDir, 'cordis.yml')))
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(seedArchive))
log('object storage seeded (cordis.yml + workspace.tar.gz containing .env)')

// ================= RUN 1: cold start =================
console.log('\n=== RUN 1: cold start (multi-step tool task) ===')
const model1 = await startMockModel()
const otlp = await startMockOtlpCollector()
const foreman1 = new Foreman({
  workdir: sandboxDir,
  pluginsDir: join(repoDir, 'plugins'), // runner-bundled adapter plugins (session resume + attribute pipeline)
  agentId,
  sessionId,
  modelEnv: {
    DEEPSEEK_API_KEY: ENV_SECRET, // env-injected only, never persisted
    DEEPSEEK_BASE_URL: `http://127.0.0.1:${model1.port}`,
  },
  envExtra: { // the value-arrival contract of the attribute pipeline (same plugin, same mechanism as the web channel)
    FOREMAN_TENANT_ID: TENANT_ID,
    FOREMAN_RUN_CONTEXT: JSON.stringify(RUN_CONTEXT),
  },
  controlPlane,
  telemetry: { mode: 'FULL', otlpUrl: `http://127.0.0.1:${otlp.port}/v1/logs` }, // trace path A
  secretValues: [ENV_SECRET], // exact-value masking of known secrets (shared by path B/packaging)
})

await foreman1.prepare()
assert('R1 config and workspace restored from object storage (with manifest baseline)', foreman1.phase === 'prepared'
  && foreman1.baselineManifest.has('README.md') && foreman1.baselineManifest.has('.env'))

const init1 = await foreman1.start()
assert('R1 dsh initialize handshake succeeded', init1?.serverInfo?.name === 'deepseek-harness-sdk-runtime')
log('dsh runtime:', init1.serverInfo.name, init1.serverInfo.version, '| SSE port:', foreman1.ssePort)

const sseFrames = []
const sse = await subscribeSse(foreman1.ssePort, (frame) => { sseFrames.push(frame) })
const { reason } = await foreman1.prompt('please do the foreman task: write greeting.txt via bash and report.md via fs write')
log('turn/end reason:', JSON.stringify(reason), '| model requests:', model1.requests.length)

// SSE frames were forwarded in real time: the event count must match the dsh
// session events (more if status frames are included)
await new Promise((resolve) => setTimeout(resolve, 200))
const sseEventFrames = sseFrames.filter((frame) => frame.kind === 'session.event')
assert('R1 SSE realtime forwarding complete (frame count === session event count)', sseEventFrames.length === foreman1.events.length,
  `${sseEventFrames.length} / ${foreman1.events.length}`)
assert('R1 SSE event stream redacted (no secret-looking strings)', !sseFrames.some((frame) => JSON.stringify(frame).includes(MODEL_SECRET)
  || JSON.stringify(frame).includes(ENV_SECRET)))

// Last-Event-ID resumption: replay after frame id=2 (wait for the replay to
// finish before disconnecting)
const replayed = []
const replaySub = await subscribeSse(foreman1.ssePort, (frame) => { replayed.push(frame) }, 2)
await new Promise((resolve) => setTimeout(resolve, 300))
replaySub.abort()
await replaySub.settled
const expectedReplay = sseFrames.length - 3
assert('R1 SSE Last-Event-ID resumption (replays frames with id>2)', replayed.length === expectedReplay,
  `${replayed.length} / ${expectedReplay}`)

const exit1 = await foreman1.shutdown()
assert('R1 graceful shutdown (shutdown -> exit 0)', exit1 === 0, `exit=${exit1}`)

const collected1 = await foreman1.collect()
if (process.env.FOREMAN_DEBUG) {
  await writeFile(join(base, 'debug-events.json'), JSON.stringify(foreman1.events, null, 2))
  log('DEBUG: events dumped ->', join(base, 'debug-events.json'))
}
assert('R1 completion detection: turn/end reason.kind === completed', reason.kind === 'completed')
assert('R1 final answer extractable', collected1.finalAnswer.includes('TASK_COMPLETE'), JSON.stringify(collected1.finalAnswer))
assert('R1 manifest change set captures the bash blind-spot file (greeting.txt)', collected1.manifestDiff.added.includes('greeting.txt'))
assert('R1 manifest change set captures the fs-written file (report.md)', collected1.manifestDiff.added.includes('report.md'))
assert('R1 manifest change set captures the fs-modified file (README.md)', collected1.manifestDiff.modified.includes('README.md'))
const readmeDiff = collected1.fsChanges.diffs.find((diff) => diff.path === 'README.md')
assert('R1 fs tool content-level diff (README.md edit: oldText/newText carry the secret-looking string — raw log; redacted later at packaging/forwarding)',
  readmeDiff !== undefined && readmeDiff.oldText.includes('restored from object storage.')
  && readmeDiff.newText.includes(MODEL_SECRET))
assert('R1 fs write of a new file yields no content-level diff (known dsh behavior; the manifest covers it)',
  !collected1.fsChanges.diffs.some((diff) => diff.path === 'report.md'))
assert('R1 bash path yields no content-level diff (known blind spot; the manifest covers it)',
  !collected1.fsChanges.diffs.some((diff) => diff.path === 'greeting.txt'))
assert('R1 session logs persisted (.jsonl.zstd)', collected1.sessionLogFiles.some((file) => file.endsWith('.jsonl.zstd')),
  JSON.stringify(collected1.sessionLogFiles))

// OTLP path A: FULL mode mirrors session events (chunk projection: only the
// first chunk per (turn,step); full content lives in assistant/message;
// record count >= event count - dropped follow-up chunks)
await new Promise((resolve) => setTimeout(resolve, 300))
const chunkSeenKeys = new Set()
let droppedChunks = 0
for (const event of foreman1.events) {
  if (event.type !== 'assistant/chunk') continue
  const key = `${event.data.turn}:${event.data.step}`
  if (chunkSeenKeys.has(key)) droppedChunks += 1
  else chunkSeenKeys.add(key)
}
assert('R1 OTLP traces exported progressively (record count >= event count - dropped follow-up chunks)',
  otlp.records.length >= foreman1.events.length - droppedChunks,
  `${otlp.records.length} records / ${foreman1.events.length} events - ${droppedChunks} chunks`)
assert('R1 OTLP records contain turn/end payloads', otlp.records.some((record) => JSON.stringify(record).includes('turn/end')))
assert('R1 OTLP records contain tool-result payloads (the write path)', otlp.records.some((record) => JSON.stringify(record).includes('report.md')))
assert('R1 attribute pipeline is channel-agnostic (stdio channel, same engine + spec: tenant.id + cost.center + quota.tier)',
  otlp.records.length > 0 && otlp.records.every((record) =>
    record.attributes['tenant.id'] === TENANT_ID
    && record.attributes['cost.center'] === RUN_CONTEXT.costCenter
    && record.attributes['quota.tier'] === RUN_CONTEXT.quota.tier),
  `${otlp.records.filter((record) => record.attributes['tenant.id'] === TENANT_ID).length}/${otlp.records.length} records`)

const published1 = await foreman1.publish()
assert('R1 packaging excludes sensitive files (.env)', published1.excluded.includes('.env'), JSON.stringify(published1.excluded))
assert('R1 packaging masks content (README.md and report.md -> [REDACTED])',
  published1.masked.includes('report.md') && published1.masked.includes('README.md'),
  JSON.stringify(published1.masked))

// Fetch the uploaded workspace archive from object storage and verify redaction
const verifyDir = join(base, 'verify-run1')
await extractArchive(join(sandboxDir, 'artifacts', 'workspace.tar.gz'), verifyDir)
const readmeText = await readFile(join(verifyDir, 'README.md'), 'utf8')
assert('R1 uploaded README.md redacted ([REDACTED] replaces the secret)', readmeText.includes('[REDACTED]') && !readmeText.includes(MODEL_SECRET))
const reportText = await readFile(join(verifyDir, 'report.md'), 'utf8')
assert('R1 uploaded report.md redacted', reportText.includes('[REDACTED]') && !reportText.includes(MODEL_SECRET))
assert('R1 uploaded archive contains no .env', await readdir(verifyDir).then((entries) => !entries.includes('.env')))
assert('R1 uploaded archive keeps the bash-written greeting.txt',
  await readFile(join(verifyDir, 'greeting.txt'), 'utf8').then((text) => text.includes('hello from dsh')))

// Secrets never persisted: the env-injected secret must not appear in any
// uploaded artifact
const artifactNames = ['result.json', 'trace.jsonl', 'workspace.tar.gz', 'sessions.tar.gz']
let secretLeak = ''
for (const name of artifactNames) {
  const buffer = await downloadArtifact(controlPlane, agentId, `${sessionId}/${name}`)
  if (buffer.includes(ENV_SECRET)) secretLeak = name
}
assert('R1 secrets never persisted (no uploaded artifact contains the env-injected secret)', secretLeak === '', secretLeak || 'clean')

const busTypes = controlPlane.events.map((event) => event.type)
assert('R1 bus event: run.completed', busTypes.includes('run.completed'))
assert('R1 bus event: sandbox.reclaim-requested (triggers sandbox reclaim)', busTypes.includes('sandbox.reclaim-requested'))
const reclaim = controlPlane.events.find((event) => event.type === 'sandbox.reclaim-requested')
assert('R1 reclaim event carries the full artifact list', reclaim !== undefined && reclaim.artifacts.length === 4,
  JSON.stringify(reclaim?.artifacts))
sse.abort()

// ================= RUN 2: cross-sandbox session resume =================
console.log('\n=== RUN 2: cross-sandbox session resume (same agentId+sessionId+mount path) ===')
const model2 = await startMockModel() // a fresh model endpoint in the new "sandbox"
const foreman2 = new Foreman({
  workdir: sandboxDir,
  pluginsDir: join(repoDir, 'plugins'),
  agentId,
  sessionId,
  modelEnv: {
    DEEPSEEK_API_KEY: ENV_SECRET,
    DEEPSEEK_BASE_URL: `http://127.0.0.1:${model2.port}`,
  },
  controlPlane,
  telemetry: { mode: 'DISABLED', otlpUrl: '' }, // configuration-driven: path A disabled for this run
  secretValues: [ENV_SECRET],
})
await foreman2.prepare()
const restored = await readdir(join(sandboxDir, 'workspace'))
assert('R2 workspace restored from object storage (greeting.txt/report.md/README.md)',
  restored.includes('greeting.txt') && restored.includes('report.md') && restored.includes('README.md'))

await foreman2.start()
const { reason: reason2 } = await foreman2.prompt('REPLY_DIRECTLY: summarize what you did before')
log('R2 turn/end reason:', JSON.stringify(reason2), '| stderr tail:', (foreman2.stderrTail ?? '').slice(-500))
await foreman2.shutdown()
if (process.env.FOREMAN_DEBUG) {
  await writeFile(join(base, 'debug-events-run2.json'), JSON.stringify(foreman2.events, null, 2))
  log('DEBUG: R2 events dumped ->', join(base, 'debug-events-run2.json'))
}
const collected2 = await foreman2.collect()
await foreman2.publish()

const historyCount = model2.requests[0]?.messages?.length ?? 0
assert('R2 session history restored (model request carries multi-turn history)', historyCount > 2, `messages=${historyCount}`)
assert('R2 resumed run completes and answers with history', reason2.kind === 'completed' && collected2.finalAnswer.includes('RESUMED_OK'),
  JSON.stringify(collected2.finalAnswer))
assert('R2 the model was requested exactly once (history comes from session logs, not replays)', model2.requests.length === 1)

// ================= Summary =================
const failed = results.filter((item) => !item.pass)
console.log(`\n=== Summary: ${results.length - failed.length}/${results.length} passed ===`)
for (const item of failed) console.log('  FAILED:', item.name)
console.log('run 1 timings (ms):', JSON.stringify(foreman1.timings), '| run 2 timings (ms):', JSON.stringify(foreman2.timings))

if (!keep) await rm(base, { recursive: true, force: true })
else console.log('run directory preserved at:', base)
process.exit(failed.length > 0 ? 1 : 0)
