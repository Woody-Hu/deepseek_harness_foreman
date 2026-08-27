#!/usr/bin/env node
/**
 * Cloud external-wiring e2e test (keyless): one web-channel run with all five
 * outbound mechanisms enabled simultaneously:
 *   1. Async trace shipping: dsh OTLP -> foreman TraceShipper (local receiver
 *      + queue) -> cloud monitoring mock. While cloud monitoring returns 500
 *      the turn still completes normally (failure isolation); after recovery
 *      the flush delivers everything (eventual consistency).
 *   2. Workspace snapshot storage abstraction: an object-store sink whose
 *      Bearer token is injected dynamically via env (the token is rotated
 *      before publish, verifying per-call resolution — credentials are not
 *      captured at construction time).
 *   3. Event stream onto the message bus: delivery 'both' — SSE and the bus
 *      consume the same adapted EventOut stream.
 *   4. Outbound SSE format adaptation: format 'openai-chat' — the
 *      chat.completion.chunk protocol (role-first chunk / content delta
 *      chunks / finish-stop final chunk / data: [DONE] termination).
 *   5. Local workspace git: baseline commit -> the model writes notes.md
 *      (clean) + leak.txt (secret-looking) -> turn-commit secret interception
 *      (leak.txt stays out of commits/history) + the authoritative change set.
 *
 * Prerequisite: the dsh npm distribution on PATH (ADR-0012; see README
 * "Prerequisites"). Usage: node test/e2e/cloud.e2e.js
 */
import { createServer } from 'node:http'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { execFile } from 'node:child_process'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Foreman } from '../../src/foreman.js'
import { startMockModel } from '../mocks/model.js'
import { startMockControlPlane } from '../mocks/control-plane.js'
import { uploadArtifact } from '../../src/control-plane.js'
import { parseOtlpLogs } from '../mocks/otlp.js'
import { archiveDirectory } from '../../src/core/workspace.js'

const repoDir = new URL('../../', import.meta.url).pathname
const t0 = Date.now()

// Skip when the dsh distribution binary is unavailable (ADR-0012)
const dshAvailable = await new Promise((resolve) => {
  execFile('dsh', ['--version'], (error) => { resolve(error?.code !== 'ENOENT') })
})
if (!dshAvailable) {
  console.log('SKIP: dsh not found on PATH (install: npm install -g @deepseek-ai/dsh — see README Prerequisites)')
  process.exit(0)
}
const log = (...args) => { console.log(`[+${((Date.now() - t0) / 1000).toFixed(1)}s]`, ...args) }

const results = []
function assert(name, condition, detail = '') {
  const pass = Boolean(condition)
  results.push({ name, pass })
  console.log(`  ${pass ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`)
}

/** Subscribe to SSE and collect raw data payload strings (JSON and [DONE] alike, format-agnostic). */
async function subscribeSseRaw(port, onData) {
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
          if (dataLine !== undefined) onData(dataLine.slice(6))
        }
      }
    } catch { /* closed by abort */ }
  })()
  return { abort: () => controller.abort(), settled: pump }
}

/** Cloud monitoring mock (OTLP): switchable fail mode; records arriving requests and parsed LogRecords. */
function startCloudMonitoring() {
  const requests = []
  const records = []
  let failMode = true
  const server = createServer((request, response) => {
    const chunks = []
    request.on('data', (chunk) => { chunks.push(chunk) })
    request.on('end', () => {
      if (failMode) {
        response.writeHead(500).end()
        return
      }
      requests.push({ at: Date.now(), size: Buffer.concat(chunks).length })
      try { records.push(...parseOtlpLogs(JSON.parse(Buffer.concat(chunks).toString('utf8')))) } catch { /* ignore invalid bodies */ }
      response.writeHead(200).end()
    })
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({
        url: `http://127.0.0.1:${server.address().port}/v1/logs`,
        records,
        requestCount: () => requests.length,
        setHealthy: () => { failMode = false },
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}

/** Object store mock: PUT/GET validating the Bearer token (the verifier of the snapshot sink's dynamic credentials). */
function startObjectStore() {
  const uploads = []
  const store = new Map()
  const server = createServer((request, response) => {
    const url = new URL(request.url, 'http://localhost')
    if (request.method === 'PUT') {
      const chunks = []
      request.on('data', (chunk) => { chunks.push(chunk) })
      request.on('end', () => {
        const auth = request.headers.authorization ?? ''
        const token = auth.replace(/^Bearer /, '')
        if (token !== process.env.FOREMAN_SNAPSHOT_TOKEN) {
          response.writeHead(401).end(JSON.stringify({ error: 'bad token' }))
          return
        }
        const buffer = Buffer.concat(chunks)
        store.set(url.pathname, buffer)
        uploads.push({ key: url.pathname, token, size: buffer.length })
        response.writeHead(200).end()
      })
      return
    }
    if (request.method === 'GET' && store.has(url.pathname)) {
      response.writeHead(200, { 'content-type': 'application/octet-stream' })
      response.end(store.get(url.pathname))
      return
    }
    response.writeHead(404).end()
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({
        url: `http://127.0.0.1:${server.address().port}`,
        uploads,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}

/** Message bus relay mock: one POST per message (the verifier of event-bus delivery). */
function startBusRelay() {
  const messages = []
  const server = createServer((request, response) => {
    const chunks = []
    request.on('data', (chunk) => { chunks.push(chunk) })
    request.on('end', () => {
      try { messages.push(JSON.parse(Buffer.concat(chunks).toString('utf8'))) } catch { /* ignore */ }
      response.writeHead(200).end()
    })
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({
        url: `http://127.0.0.1:${server.address().port}/relay`,
        messages,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}

const ENV_SECRET = 'sk-foreman-cloud-secret-77c1' // env-injected, never persisted
const agentId = 'agent-cloud'
const sessionId = 'sess-e2e-cloud-001'

console.log('=== Cloud external wiring e2e (async trace / snapshot sink / event bus / OpenAI format / git interception) ===')
const base = await mkdtemp(join(tmpdir(), 'foreman-e2e-cloud-'))
const sandboxDir = join(base, 'sandbox')
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
const model = await startMockModel()
const monitoring = await startCloudMonitoring() // starts in the failing state (failure window)
const objectStore = await startObjectStore()
const relay = await startBusRelay()
log('mocks ready: control plane / model / cloud monitoring (failing) / object store / bus relay')

// ---- Seed: config + initial workspace ----
const seedDir = join(base, 'seed-workspace')
await mkdir(seedDir, { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# cloud e2e workspace\n\nrestored from object storage.\n')
const seedArchive = join(base, 'seed.tar.gz')
await archiveDirectory(seedDir, seedArchive)
await uploadArtifact(controlPlane, agentId, `${sessionId}/web-patch.yml`, await readFile(join(repoDir, 'web-patch.yml')))
await uploadArtifact(controlPlane, agentId, `${sessionId}/workspace.tar.gz`, await readFile(seedArchive))

// Dynamic credentials (injected by the cloud per task; the snapshot token
// will be rotated before publish to verify per-call resolution)
process.env.FOREMAN_SNAPSHOT_TOKEN = 'snap-token-A'
process.env.FOREMAN_BUS_TOKEN = 'bus-token-cloud'

const foreman = new Foreman({
  workdir: sandboxDir,
  channel: 'web',
  agentId,
  sessionId,
  modelEnv: { DEEPSEEK_API_KEY: ENV_SECRET, DEEPSEEK_BASE_URL: `http://127.0.0.1:${model.port}` },
  controlPlane,
  telemetry: { mode: 'FULL', otlpUrl: monitoring.url }, // taken over by the shipper (local receiver)
  secretValues: [ENV_SECRET],
  pluginsDir: join(repoDir, 'plugins'),
  envExtra: { FOREMAN_TENANT_ID: 'tenant-cloud', FOREMAN_RUN_CONTEXT: JSON.stringify({ costCenter: 'cc-1' }) },
  // 1. Async trace shipping (cloud-monitoring failures never affect the main flow; flush delivers after recovery)
  traceShipper: { upstreamUrl: monitoring.url, retries: 8, retryBaseMs: 50 },
  // 2. Snapshot storage abstraction (credentials injected dynamically via env)
  snapshot: { kind: 'object-store', endpoint: objectStore.url, bucket: agentId, tokenEnv: 'FOREMAN_SNAPSHOT_TOKEN' },
  // 3+4. Event stream: SSE + bus dual delivery, OpenAI chunk format
  events: {
    delivery: 'both',
    format: 'openai-chat',
    bus: { kind: 'http', url: relay.url, headersEnv: { authorization: 'FOREMAN_BUS_TOKEN' } },
  },
  // 5. Local workspace git (secret interception)
  git: { enabled: true },
})

await foreman.prepare()
assert('git baseline commit done (restored workspace = the before state)', foreman.external.gitBaseline !== undefined
  && typeof foreman.external.gitBaseline.oid === 'string' && foreman.external.gitBaseline.files >= 1,
  `oid=${foreman.external.gitBaseline?.oid?.slice(0, 8)}`)

await foreman.start()
const sseData = [] // raw data payloads (JSON strings or [DONE])
const sse = await subscribeSseRaw(foreman.ssePort, (data) => { sseData.push(data) })
log('dsh web ready; SSE (OpenAI format) subscribed; shipper receiver:', foreman.shipper.endpoint)

// ---- Turn: WRITE_SECRET scenario (cloud monitoring 500 throughout) ----
const turn = await foreman.prompt('please do the cloud task. WRITE_SECRET')
assert('turn completes while cloud monitoring 500s throughout (trace-export failures isolated)',
  turn.reason.kind === 'completed', `reason=${turn.reason.kind}`)
assert('async proof: zero records delivered to cloud monitoring at turn end (dsh batch-flush lag + failure retries never affect the main flow)',
  foreman.shipper.stats.forwarded === 0,
  `received=${foreman.shipper.stats.received}, forwarded=${foreman.shipper.stats.forwarded}`)

// ---- Git closure (cloud monitoring still failing — collect depends on no external wiring) ----
const collected = await foreman.collect()
assert('git secret interception: leak.txt (secret-looking) kept out of the commit', collected.git.violations.some(
  (v) => v.file === 'leak.txt' && v.rules.includes('pattern:openai-key')),
  JSON.stringify(collected.git.violations))
assert('git turn commit contains only clean output (notes.md)', collected.git.turnCommit?.files.includes('notes.md')
  && !collected.git.turnCommit.files.includes('leak.txt'), JSON.stringify(collected.git.turnCommit?.files))
assert('git authoritative change set (since baseline) = notes.md added', JSON.stringify(collected.git.changedSinceBaseline)
  === JSON.stringify([{ status: 'A', path: 'notes.md' }]), JSON.stringify(collected.git.changedSinceBaseline))
assert('intercepted file left on disk uncommitted (uncommitted residue is reportable)',
  collected.git.uncommitted.some((line) => line.includes('leak.txt')))

// ---- Cloud monitoring recovery + credential rotation + publish (flush + snapshot upload + reclaim events) ----
monitoring.setHealthy()
process.env.FOREMAN_SNAPSHOT_TOKEN = 'snap-token-B' // rotation: the sink resolves per call, proving no construction-time capture
await new Promise((resolve) => { setTimeout(resolve, 300) }) // leave an in-flight window for failure-window retries
const published = await foreman.publish()

assert('trace shipper delivers everything after the flush (received == forwarded, nothing dropped)',
  published.result.traceShipper.received === published.result.traceShipper.forwarded
  && published.result.traceShipper.forwarded > 0 && published.result.traceShipper.droppedRetries === 0,
  JSON.stringify(published.result.traceShipper))
assert('records delivered to cloud monitoring carry tenant attributes (the attribute pipeline crosses the shipper)',
  monitoring.records.length > 0 && monitoring.records.every((record) => record.attributes['tenant.id'] === 'tenant-cloud'),
  `${monitoring.records.length} records`)
assert('cloud monitoring received records only after the turn ended (async: main flow first)',
  monitoring.records.length > 0 && monitoring.requestCount() > 0)

const artifactNames = ['result.json', 'workspace.tar.gz', 'sessions.tar.gz', 'trace.jsonl']
assert('snapshot sink: 4 artifacts uploaded via object storage (rotated Bearer credential accepted)',
  objectStore.uploads.length === 4 && objectStore.uploads.every((upload) => upload.token === 'snap-token-B'),
  objectStore.uploads.map((upload) => `${upload.key.split('/').at(-1)}@${upload.token}`).join(', '))
assert('snapshot sink upload keys = <agentId>/<sessionId>/<artifact>', objectStore.uploads.every(
  (upload) => upload.key.startsWith(`/${agentId}/${sessionId}/`)))
assert('publish result carries git interception records (cloud-observable)', Array.isArray(published.result.git?.violations)
  && published.result.git.violations.length > 0)
assert('publish result carries shipper/bus stats (cloud-observable)', published.result.traceShipper !== undefined
  && published.result.eventBus !== undefined && published.result.eventBus.published > 0,
  `bus=${JSON.stringify(published.result.eventBus)}`)

await foreman.shutdown()

// ---- SSE (OpenAI format) consumption verification ----
await new Promise((resolve) => { setTimeout(resolve, 300) })
sse.abort()
const chunks = []
let sawDone = false
for (const data of sseData) {
  if (data === '[DONE]') { sawDone = true; continue }
  try { chunks.push(JSON.parse(data)) } catch { /* ignore non-JSON lines */ }
}
assert('OpenAI SSE: stream terminated by data: [DONE]', sawDone, `${sseData.length} frames`)
assert('OpenAI SSE: all JSON frames are chat.completion.chunk', chunks.length > 0
  && chunks.every((chunk) => chunk.object === 'chat.completion.chunk'))
assert('OpenAI SSE: first chunk delta.role=assistant', chunks[0]?.choices?.[0]?.delta?.role === 'assistant')
const text = chunks.map((chunk) => chunk.choices[0]?.delta?.content ?? '').join('')
assert('OpenAI SSE: content deltas concatenated rebuild the final answer', text.includes('CLOUD_POC_DONE'), text.slice(0, 60))
assert('OpenAI SSE: final chunk finish_reason=stop', chunks.at(-1)?.choices?.[0]?.finish_reason === 'stop')
assert('OpenAI SSE: stable completion id within a turn', new Set(chunks.map((chunk) => chunk.id)).size === 1)
assert('OpenAI SSE: tool/status events absent from the text stream (format boundary)', !chunks.some(
  (chunk) => chunk.choices?.[0]?.delta?.tool_calls !== undefined))

// ---- Bus (the same EventOut stream) verification ----
const busChunks = relay.messages.filter((message) => message?.object === 'chat.completion.chunk')
const busDone = relay.messages.filter((message) => message?.kind === 'stream.done')
assert('event bus: received the same OpenAI chunk stream (same source as SSE)', busChunks.length === chunks.length
  && busChunks[0]?.choices?.[0]?.delta?.role === 'assistant', `${busChunks.length} chunks`)
assert('event bus: stream termination marker stream.done published', busDone.length === 1)
assert('event bus: no native frames leaked (format adaptation applies to the bus equally)', relay.messages.every(
  (message) => message?.object === 'chat.completion.chunk' || message?.kind === 'stream.done'))

// ---- Cleanup and summary ----
for (const closeable of [model, controlPlane, monitoring, objectStore, relay]) await closeable.close()
await rm(base, { recursive: true, force: true })

const failed = results.filter((entry) => !entry.pass)
console.log(`\n=== Result: ${results.length - failed.length}/${results.length} passed, ${failed.length} failed, ${((Date.now() - t0) / 1000).toFixed(1)}s ===`)
if (failed.length > 0) {
  for (const entry of failed) console.log(`  FAIL  ${entry.name}`)
  process.exitCode = 1
}
