/**
 * Foreman end-to-end test: web channel (dsh web apiproxy HTTP+WS) full loop.
 *
 * Four runs sharing one external sessionId and one workspace absolute path:
 *   RUN 1 (cold start + HITL allow): dsh web launched (patch overlay + env
 *     secret injection) -> the mock model requests a bash sandbox escalation
 *     (sandbox_permissions=danger-full-access) -> approval/requested forwarded
 *     through the foreman SSE gateway -> POST /hitl answers allowed-once ->
 *     the escalated write succeeds (outside the workspace) -> turn/end -> OTLP
 *     path A -> collect/package/upload/reclaim.
 *   RUN 2 (native session resume + HITL reject): a fresh process cold-resumes
 *     the persisted session by the external sessionId (session.list contains
 *     it -> created:false) -> the escalation approval is rejected -> the tool
 *     result is an error, the file stays unwritten -> history-carrying request
 *     verified.
 *   RUN 3 (crash + dangling approval): an escalation approval hangs (never
 *     answered) -> SIGKILL -> the session log keeps a dangling approval/asked
 *     without approval/decided -> collect/publish archives the crashed run.
 *   RUN 4 (post-crash resume): dsh web restarted (cold-resume of the dangling
 *     log) -> the pending approval does not reappear (the mux never replays
 *     it, no new frame) -> a REPLY_DIRECTLY prompt -> the model request
 *     carries the crash-repair synthetic TOOL_OUTCOME_UNKNOWN tool result.
 *
 * Assertion coverage: SSE gateway approval forwarding and redaction, HITL
 * answer receipts and resolution broadcast, escalation allow/deny effects,
 * event-stream audit pairs, tenant attribute pipeline + exporter header
 * passthrough, secrets never persisted, native cold-session resume (created
 * flag + history), dangling approvals surviving crashes without replay, and
 * the crash-repair synthetic tool outcome.
 *
 * Usage: node test/e2e/web.e2e.js [--keep] (any cwd; --keep preserves the run
 * directory). Requires the dsh npm distribution on PATH (ADR-0012; see README
 * "Prerequisites"); a missing binary fails loud.
 */
import { createHash } from 'node:crypto'
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { archiveDirectory } from '../../src/core/workspace.js'
import { downloadArtifact, uploadArtifact } from '../../src/control-plane.js'
import { startMockControlPlane } from '../mocks/control-plane.js'
import { startMockModel } from '../mocks/model.js'
import { startMockOtlpCollector } from '../mocks/otlp.js'
import { requireBinary } from '../require-bin.js'

const repoDir = new URL('../../', import.meta.url).pathname
const keep = process.argv.includes('--keep')

// The dsh distribution binary is a hard prerequisite (ADR-0012) — never a skip
await requireBinary('dsh', ['--version'], 'npm install -g @deepseek-ai/dsh (see README Prerequisites)')

const t0 = Date.now()
const log = (...args) => { console.log(`[+${((Date.now() - t0) / 1000).toFixed(1)}s]`, ...args) }

const results = []
function assert(name, condition, detail = '') {
  const pass = Boolean(condition)
  results.push({ name, pass })
  console.log(`  ${pass ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`)
}

/** Subscribe to the foreman SSE gateway (simulating a cloud-side consumer). */
async function subscribeSse(port, onFrame) {
  const controller = new AbortController()
  const response = await fetch(`http://127.0.0.1:${port}/events`, { signal: controller.signal })
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

/** Poll until the predicate holds (default 20s; SSE frames arrive asynchronously). */
async function waitFor(predicate, { timeoutMs = 20_000, intervalMs = 50 } = {}) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const value = predicate()
    if (value !== undefined && value !== null && value !== false) return value
    await new Promise((resolve) => { setTimeout(resolve, intervalMs) })
  }
  return undefined
}

const fileExists = async (path) => { try { await access(path); return true } catch { return false } }

/** Answer a pending HITL approval through the foreman SSE gateway (simulating a cloud approval callback). */
async function respondHitl(port, approvalId, outcome) {
  const response = await fetch(`http://127.0.0.1:${port}/hitl`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ approvalId, outcome }),
  })
  return await response.json()
}

const ENV_SECRET = 'sk-foreman-web-secret-4c91de' // injected via env, never persisted
const TENANT_ID = 'tenant-7f2a' // tenant identifier (env -> telemetry attributes / OTLP headers)
const W3C_TRACE_ID = '4bf92f3577b34da6a3ce929d0e0e4736' // outer distributed-tracing id (32 hex)
// Run context (the value-arrival contract of the attribute pipeline): control
// plane -> foreman -> one env JSON variable; the spec picks fields by dot path
// (costCenter / quota.tier / user.id / sandbox.profile)
const RUN_CONTEXT = {
  costCenter: 'cc-8842',
  quota: { tier: 'gold' },
  user: { id: 'user-42' },
  sandbox: { profile: 'workspace-write' },
}
const agentId = 'agent-7f2a'
const sessionId = 'sess-e2e-web-001' // the external session id doubles as the dsh session id (reuse across runs = session resume)

const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-web-'))
const sandboxDir = join(base, 'sandbox') // shared by all runs -> identical workspace absolute paths -> the session can resume
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
log('mock control plane (object storage + bus) port:', controlPlane.port)

// ---- Seed: the web patch overlay and the initial workspace in object storage ----
const seedDir = join(base, 'seed-workspace')
await mkdir(seedDir, { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# web e2e workspace\n\nrestored from object storage.\n')
await writeFile(join(seedDir, '.env'), 'API_KEY=sk-web-seed-secret-31aa\n') // must be excluded from packaging
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
await uploadArtifact(controlPlane, agentId, `${sessionId}/web-patch.yml`, await readFile(join(repoDir, 'web-patch.yml')))
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(seedArchive))
log('object storage seeded (web-patch.yml + workspace.tar.gz containing .env)')

/** Build a web-channel foreman (shared options; the model endpoint and telemetry differ per run). */
function makeForeman(modelPort, otlp) {
  return new Foreman({
    workdir: sandboxDir,
    channel: 'web', // the web primary channel (apiproxy HTTP+WS + native HITL/resume)
    agentId,
    sessionId,
    modelEnv: {
      DEEPSEEK_API_KEY: ENV_SECRET,
      DEEPSEEK_BASE_URL: `http://127.0.0.1:${modelPort}`,
    },
    controlPlane,
    telemetry: otlp,
    secretValues: [ENV_SECRET],
    pluginsDir: join(repoDir, 'plugins'), // telemetry-enrich (the generalized attribute pipeline engine)
    envExtra: {
      FOREMAN_TENANT_ID: TENANT_ID,
      FOREMAN_W3C_TRACE_ID: W3C_TRACE_ID,
      FOREMAN_RUN_CONTEXT: JSON.stringify(RUN_CONTEXT),
    },
  })
}

// ================= RUN 1: cold start + HITL allow =================
console.log('\n=== RUN 1: web cold start + HITL approval allowed (escalated write outside the workspace) ===')
const model1 = await startMockModel()
const otlp = await startMockOtlpCollector()
const foreman1 = makeForeman(model1.port, { mode: 'FULL', otlpUrl: `http://127.0.0.1:${otlp.port}/v1/logs` })

await foreman1.prepare()
assert('R1 config and workspace restored from object storage (web-patch.yml + manifest baseline)',
  foreman1.phase === 'prepared' && foreman1.baselineManifest.has('README.md') && foreman1.baselineManifest.has('.env'))

const info1 = await foreman1.start()
assert('R1 dsh web ready (binding URL parsed from stdout)', /^http:\/\/127\.0\.0\.1:\d+$/.test(info1.url), info1.url)
log('dsh web:', info1.url, '| foreman SSE gateway port:', foreman1.ssePort)

const frames1 = []
const sse1 = await subscribeSse(foreman1.ssePort, (frame) => { frames1.push(frame) })
await new Promise((resolve) => { setTimeout(resolve, 200) })

const turn1 = foreman1.prompt('ESCALATE_ALLOW: write ../escalation-proof.txt outside the workspace, it needs sandbox escalation')
const approvalFrame1 = await waitFor(() => frames1.find((frame) => frame.kind === 'approval/requested'))
assert('R1 HITL approval request forwarded through the SSE gateway (approval/requested frame)', approvalFrame1 !== undefined,
  approvalFrame1 ? `approvalId=${approvalFrame1.approvalId} tool=${approvalFrame1.toolName}` : 'not received')
assert('R1 approval request frame redacted (no env-injected secret)',
  !JSON.stringify(approvalFrame1 ?? {}).includes(ENV_SECRET))
assert('R1 approval request carries the escalation reason (reason mentions the sandbox escalation)',
  JSON.stringify(approvalFrame1?.reason ?? '').includes('escalate sandbox'))

const receipt1 = await respondHitl(foreman1.ssePort, approvalFrame1.approvalId, 'allowed-once')
assert('R1 POST /hitl answer receipt accepted', receipt1.accepted === true, JSON.stringify(receipt1))
const resolved1 = await waitFor(() => frames1.find((frame) => frame.kind === 'approval/resolved'))
assert('R1 approval resolution broadcast (approval/resolved allowed-once)', resolved1?.outcome === 'allowed-once')

const { reason: reason1, created: created1 } = await turn1
log('R1 turn/end reason:', JSON.stringify(reason1), '| model requests:', model1.requests.length)
assert('R1 the external session id doubles as the dsh session id (first run created)', created1 === true)
assert('R1 completion detection: turn/end completed', reason1.kind === 'completed')
assert('R1 the escalated write outside the workspace succeeded after the approval (escalation-proof.txt)',
  await fileExists(join(sandboxDir, 'escalation-proof.txt')), 'danger-full-access applies to that call only')

const asked1 = foreman1.events.filter((event) => event.type === 'approval/asked')
const decided1 = foreman1.events.filter((event) => event.type === 'approval/decided')
assert('R1 session event stream carries the approval audit pair (approval/asked + approval/decided allowed-once)',
  asked1.length === 1 && decided1.length === 1 && decided1[0].data.outcome === 'allowed-once')

await new Promise((resolve) => { setTimeout(resolve, 300) })
const eventFrames1 = frames1.filter((frame) => frame.kind === 'session.event')
assert('R1 SSE realtime forwarding complete (frame count === session event count)', eventFrames1.length === foreman1.events.length,
  `${eventFrames1.length} / ${foreman1.events.length}`)
assert('R1 SSE event stream redacted (no env secret)', !JSON.stringify(frames1).includes(ENV_SECRET))

await new Promise((resolve) => { setTimeout(resolve, 400) })
assert('R1 OTLP trace path A active (web mode FULL mirrors session events)', otlp.records.length > 0,
  `${otlp.records.length} records`)
assert('R1 deployment-side attributes injected (the waterfall plugin attaches tenant.id / w3c.trace_id to every record)',
  otlp.records.length > 0 && otlp.records.every((record) =>
    record.attributes['tenant.id'] === TENANT_ID && record.attributes['w3c.trace_id'] === W3C_TRACE_ID),
  `${otlp.records.filter((record) => record.attributes['tenant.id'] === TENANT_ID).length}/${otlp.records.length} records carry tenant.id`)
assert('R1 OTLP export requests carry the tenant header (exporter.headers passes x-tenant-id through)',
  otlp.requests.length > 0 && otlp.requests.every((request) => request.headers['x-tenant-id'] === TENANT_ID),
  `${otlp.requests.length} export requests`)
assert('R1 attribute pipeline: literal/context/hash sources active (region, costCenter, quota.tier on every record; pseudonym=sha256(user.id))',
  otlp.records.length > 0 && otlp.records.every((record) =>
    record.attributes['deployment.region'] === 'cn-east-1'
    && record.attributes['cost.center'] === RUN_CONTEXT.costCenter
    && record.attributes['quota.tier'] === RUN_CONTEXT.quota.tier
    && record.attributes['user.pseudonym'] === createHash('sha256').update(RUN_CONTEXT.user.id).digest('hex').slice(0, 16)),
  `${otlp.records.length}/${otlp.records.length} records carry region/cost/quota/pseudonym`)
const toolRecords = otlp.records.filter((record) => String(record.attributes['event.type'] ?? '').startsWith('tool/'))
const nonToolRecords = otlp.records.filter((record) => !String(record.attributes['event.type'] ?? '').startsWith('tool/'))
assert('R1 attribute pipeline: event-type filter active (tool/* records carry tool.sandbox_profile, others do not)',
  toolRecords.length > 0 && nonToolRecords.length > 0
  && toolRecords.every((record) => record.attributes['tool.sandbox_profile'] === RUN_CONTEXT.sandbox.profile)
  && nonToolRecords.every((record) => record.attributes['tool.sandbox_profile'] === undefined),
  `${toolRecords.length} tool records carry it / ${nonToolRecords.length} other records do not`)
assert('R1 attribute pipeline: injection is additive (a rule named like a dsh-owned key never overrides it; session.id keeps its value)',
  otlp.records.every((record) => record.attributes['session.id'] === sessionId),
  `session.id is ${sessionId} on every record`)

const exit1 = await foreman1.shutdown()
assert('R1 graceful shutdown (SIGTERM -> exit 0)', exit1 === 0, `exit=${exit1}`)

const collected1 = await foreman1.collect()
assert('R1 final answer extractable', collected1.finalAnswer.includes('TASK_COMPLETE_ESCALATION'),
  JSON.stringify(collected1.finalAnswer))
assert('R1 session logs persisted ($DSH_HOME/sessions)', collected1.sessionLogFiles.length > 0,
  JSON.stringify(collected1.sessionLogFiles))
const published1 = await foreman1.publish()
assert('R1 packaging excludes sensitive files (.env)', published1.excluded.includes('.env'))
assert('R1 secrets never persisted (no uploaded artifact contains the env secret)', (await Promise.all(
  ['result.json', 'trace.jsonl', 'workspace.tar.gz', 'sessions.tar.gz'].map(async (name) =>
    (await downloadArtifact(controlPlane, agentId, `${sessionId}/${name}`)).includes(ENV_SECRET)))).every((leaked) => !leaked))
sse1.abort()
await sse1.settled

// ================= RUN 2: native session resume + HITL reject =================
console.log('\n=== RUN 2: cross-process native session resume + HITL approval rejected ===')
const model2 = await startMockModel()
const foreman2 = makeForeman(model2.port, { mode: 'DISABLED', otlpUrl: '' })
await foreman2.prepare()
await foreman2.start()
const frames2 = []
const sse2 = await subscribeSse(foreman2.ssePort, (frame) => { frames2.push(frame) })
await new Promise((resolve) => { setTimeout(resolve, 200) })

const turn2 = foreman2.prompt('ESCALATE_REJECT: try writing ../rejected-proof.txt, the human will reject it')
const approvalFrame2 = await waitFor(() => frames2.find((frame) => frame.kind === 'approval/requested'))
assert('R2 the resumed run forwards its approval request normally', approvalFrame2 !== undefined)
const receipt2 = await respondHitl(foreman2.ssePort, approvalFrame2.approvalId, 'rejected')
assert('R2 POST /hitl rejection receipt accepted', receipt2.accepted === true, JSON.stringify(receipt2))
const resolved2 = await waitFor(() => frames2.find((frame) => frame.kind === 'approval/resolved'))
assert('R2 approval resolution broadcast (approval/resolved rejected)', resolved2?.outcome === 'rejected')

const { reason: reason2, created: created2 } = await turn2
log('R2 turn/end reason:', JSON.stringify(reason2), '| model requests:', model2.requests.length)
assert('R2 the external session id reuses the cold persisted session (created:false = not re-created)', created2 === false)
assert('R2 completion detection: turn/end completed', reason2.kind === 'completed')
assert('R2 the file outside the workspace stays unwritten after the rejection (rejected-proof.txt absent)',
  !(await fileExists(join(sandboxDir, 'rejected-proof.txt'))))
const decided2 = foreman2.events.filter((event) => event.type === 'approval/decided')
assert('R2 audit event approval/decided rejected', decided2.at(-1)?.data.outcome === 'rejected')
assert('R2 native resume active (the model request carries history messages>2)', (model2.requests[0]?.messages?.length ?? 0) > 2,
  `messages=${model2.requests[0]?.messages?.length}`)

await foreman2.shutdown()
const collected2 = await foreman2.collect()
assert('R2 the final answer after a rejected escalation is extractable', collected2.finalAnswer.includes('TASK_COMPLETE_ESCALATION'))
await foreman2.publish()
sse2.abort()
await sse2.settled

// ================= RUN 3: crash + dangling approval =================
console.log('\n=== RUN 3: hard kill crash (SIGKILL while an approval hangs) ===')
const model3 = await startMockModel()
const foreman3 = makeForeman(model3.port, { mode: 'DISABLED', otlpUrl: '' })
await foreman3.prepare()
await foreman3.start()
const frames3 = []
const sse3 = await subscribeSse(foreman3.ssePort, (frame) => { frames3.push(frame) })
await new Promise((resolve) => { setTimeout(resolve, 200) })

const turn3Promise = foreman3.prompt('ESCALATE_HANG: request escalation and wait for the human forever')
// kill() rejects the pending prompt — the runner main loop must consume that
// rejection: attach a handler first to avoid an unhandled rejection (it fires
// while kill() awaits process exit); the outcome is read below
turn3Promise.catch(() => {})
const approvalFrame3 = await waitFor(() => frames3.find((frame) => frame.kind === 'approval/requested'))
assert('R3 the hanging approval request was forwarded (never answered)', approvalFrame3 !== undefined,
  approvalFrame3 ? `approvalId=${approvalFrame3.approvalId}` : 'not received')

const exit3 = await foreman3.kill() // SIGKILL: no cleanup, no approval settlement
log('R3 kill exit:', exit3)
const turn3Error = await turn3Promise.then(() => undefined, (error) => error)
assert('R3 the pending prompt was rejected (abandoned; no 120s timer keeps the process alive)', turn3Error !== undefined)

const asked3 = foreman3.events.filter((event) => event.type === 'approval/asked')
const decided3 = foreman3.events.filter((event) => event.type === 'approval/decided')
assert('R3 the event stream keeps the dangling approval (asked without decided)', asked3.length === 1 && decided3.length === 0)
const collected3 = await foreman3.collect()
await foreman3.publish() // crashed runs are archived too: sessions.tar.gz carries the dangling log (the resume run's input)
assert('R3 the crashed run reports status error (no turn/end)', (await downloadArtifact(controlPlane, agentId, `${sessionId}/result.json`))
  .toString('utf8').includes('"status": "error"'))
sse3.abort()
await sse3.settled

// ================= RUN 4: post-crash resume (no dangling-approval replay) =================
console.log('\n=== RUN 4: post-crash resume (dangling approval not replayed + TOOL_OUTCOME_UNKNOWN repair) ===')
const model4 = await startMockModel()
const foreman4 = makeForeman(model4.port, { mode: 'DISABLED', otlpUrl: '' })
await foreman4.prepare()
await foreman4.start()
const frames4 = []
const sse4 = await subscribeSse(foreman4.ssePort, (frame) => { frames4.push(frame) })

// If the mux replayed a pending approval it would appear now — wait a moment
// to confirm the baseline is empty
await new Promise((resolve) => { setTimeout(resolve, 1500) })
assert('R4 the dangling approval does not reappear (no approval/requested replay frame) — pending approvals live only in-process',
  !frames4.some((frame) => frame.kind === 'approval/requested'))

const { reason: reason4, created: created4 } = await foreman4.prompt('REPLY_DIRECTLY: summarize what happened before the crash')
log('R4 turn/end reason:', JSON.stringify(reason4), '| model requests:', model4.requests.length)
assert('R4 the dangling session resumes by the external id (created:false)', created4 === false)
assert('R4 the resumed run raises no new approval request', !frames4.some((frame) => frame.kind === 'approval/requested'))

const lastRequest4 = model4.requests.at(-1)
const requestText4 = JSON.stringify(lastRequest4?.messages ?? [])
assert('R4 crash repair active (the model request contains the synthetic TOOL_OUTCOME_UNKNOWN tool result)',
  requestText4.includes('outcome is unknown'), 'the interruptedTurnClosers synthetic tool result')
assert('R4 session history restored (the request carries the pre-crash multi-turn history)', (lastRequest4?.messages?.length ?? 0) > 2,
  `messages=${lastRequest4?.messages?.length}`)

const collected4 = await foreman4.collect()
assert('R4 the resumed run completes and answers with history', reason4.kind === 'completed'
  && collected4.finalAnswer.includes('RESUMED_OK'), JSON.stringify(collected4.finalAnswer))
await foreman4.shutdown()
await foreman4.publish()
sse4.abort()
await sse4.settled

// ================= Summary =================
const failed = results.filter((item) => !item.pass)
console.log(`\n=== Summary: ${results.length - failed.length}/${results.length} passed ===`)
for (const item of failed) console.log('  FAILED:', item.name)
console.log('timings (ms):', JSON.stringify({
  r1: foreman1.timings, r2: foreman2.timings, r3: foreman3.timings, r4: foreman4.timings,
}))

for (const closeable of [model1, model2, model3, model4, otlp, controlPlane]) await closeable.close()
if (results.some((item) => !item.pass)) process.exitCode = 1
if (!keep) await rm(base, { recursive: true, force: true })
else console.log('run directory preserved at:', base)
