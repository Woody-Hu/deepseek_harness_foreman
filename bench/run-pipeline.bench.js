/**
 * Run-pipeline benchmark (ADR-0010): A/B measures overlap scheduling on REAL
 * work — the real codex binary driving a multi-turn sandbox, real git
 * commits, real tar/gzip pack builds, real HTTP uploads/downloads against a
 * disk-backed object store. Only the model endpoint is scripted (a local
 * Responses-API fixture with a per-request delay — the workload generator;
 * external-dependency rule per ADR-0004, no foreman code is mocked).
 *
 * Workload per run: one sandbox, N turns. Each turn's exec_command writes
 * PAYLOAD_MB of incompressible data (dd /dev/urandom) — checkpoint pack cost
 * C is real compression + upload work; DELAY_MS makes execution time E
 * configurable through the scripted model latency.
 *
 * Modes (same work, only `checkpoints.overlap` differs):
 *   serial   overlap=false — every pack is built+uploaded inside publish(),
 *            fully on the critical path
 *   overlap  overlap=true  — each turn's pack is built+uploaded on the
 *            background chain while the next turn executes (ADR-0010 3a)
 *
 * Performance model (ADR-0010): saved ≈ Σᵢ₌₁^{N-1} min(Cᵢ, Eᵢ₊₁) − contention.
 * The projection is computed from the serial run's own measured constants
 * (E from per-turn wall time, C from per-pack sync records — real timers in
 * the real path) and verified against the observed T_serial − T_overlap.
 *
 * Integrity gates (no benchmarking a broken pipeline, no derived numbers):
 *   - every turn's payload file exists with the expected size (the dd workload
 *     actually ran inside the sandbox)
 *   - a fresh restore from the published checkpoint index reproduces the
 *     final workspace bit-for-bit (content manifest equality)
 *   - the A/B is what it claims: serial runs upload all N packs at publish,
 *     overlap runs upload all N packs in the background
 *
 * Run: node bench/run-pipeline.bench.js [--quick] [--keep]
 *        [--turns N] [--payload-mb N] [--delay-ms N] [--runs N]
 */
import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { randomBytes } from 'node:crypto'
import { Foreman } from '../src/foreman.js'
import { uploadArtifact } from '../src/control-plane.js'
import { fileManifest } from '../src/core/workspace.js'
import { startMockControlPlane } from '../test/mocks/control-plane.js'
import { startCodexResponsesFixture } from '../test/fixtures/codex-responses.js'

const argv = process.argv.slice(2)
const arg = (name, fallback) => {
  const index = argv.indexOf(`--${name}`)
  return index !== -1 ? Number(argv[index + 1]) : fallback
}
const quick = argv.includes('--quick')
const keep = argv.includes('--keep')

const TURNS = arg('turns', quick ? 3 : 5)
const PAYLOAD_MB = arg('payload-mb', quick ? 2 : 8)
const DELAY_MS = arg('delay-ms', quick ? 300 : 600)
const MEASURED_RUNS = arg('runs', quick ? 1 : 3)
const WARMUP_PAIRS = quick ? 0 : 1
const SEED_MB = quick ? 1 : 4
const PAYLOAD_BYTES = PAYLOAD_MB * 1024 * 1024

const t0 = Date.now()
const log = (...args) => { console.log(`[+${((Date.now() - t0) / 1000).toFixed(1)}s]`, ...args) }

// ---- Skip when the codex binary is unavailable ----
const codexAvailable = await new Promise((resolve) => {
  execFile('codex', ['--version'], (error) => { resolve(error === null) })
})
if (!codexAvailable) {
  console.log('SKIP: codex binary not found on PATH (install: npm install -g @openai/codex)')
  process.exit(0)
}

/** Fail-loud gate helper. */
function gate(name, condition, detail = '') {
  if (!condition) throw new Error(`bench/run-pipeline: gate failed — ${name}${detail ? ` (${detail})` : ''}`)
}

/**
 * Wipe a directory, tolerating transient concurrent writers: the harness's
 * orphaned plugin-clone children may still be writing into CODEX_HOME/.tmp
 * for a short while after the parent process exited (rm retries ENOTEMPTY).
 */
const wipeDir = async (dir) => {
  await rm(dir, { recursive: true, force: true, maxRetries: 20, retryDelay: 250 })
}

/** Path -> {size, sha256} for every non-.git file (content identity; mtimes are restore-irrelevant). */
async function contentManifest(dir) {
  const manifest = {}
  for (const [rel, info] of await fileManifest(dir)) {
    if (rel === '.git' || rel.startsWith('.git/')) continue
    manifest[rel] = { size: info.size, sha256: info.sha256 }
  }
  return manifest
}

/**
 * One benchmark run: fresh sessionId, cold sandbox, TURNS prompts, publish,
 * then a restore-integrity pass against the produced checkpoint chain.
 *
 * @returns per-run measurements { mode, runMs, prepareMs, bootMs, turnMs[], collectMs,
 *          publishMs, drainMs, packSyncs[], restoredBytes }
 */
async function runOnce({ mode, sessionId, sandboxDir, controlPlane, model, seedBuffer }) {
  await uploadArtifact(controlPlane, 'bench-run-pipeline', `${sessionId}/workspace.tar.gz`, seedBuffer)
  await wipeDir(sandboxDir)
  await mkdir(sandboxDir, { recursive: true })
  const foreman = new Foreman({
    workdir: sandboxDir,
    agentId: 'bench-run-pipeline',
    sessionId,
    channel: 'codex',
    codex: { baseUrl: `${model.baseUrl}/v1`, apiKey: 'bench-key' },
    controlPlane,
    secretValues: ['bench-key'],
    git: { enabled: true },
    checkpoints: { recentKeep: TURNS + 1, perLevel: 1, rebaseAfter: 0, overlap: mode === 'overlap' },
  })

  const runStarted = Date.now()
  await foreman.prepare()
  await foreman.start()
  const turnMs = []
  for (let turn = 1; turn <= TURNS; turn += 1) {
    const promptStarted = Date.now()
    const { reason } = await foreman.prompt(`please record the benchmark progress: BENCH TURN ${turn}`, { timeoutMs: 300_000 })
    turnMs.push(Date.now() - promptStarted)
    gate(`turn ${turn} completed (${mode})`, reason?.kind === 'completed', JSON.stringify(reason))
  }
  const collectStarted = Date.now()
  await foreman.collect()
  const collectMs = Date.now() - collectStarted
  await foreman.publish()
  const runMs = Date.now() - runStarted
  const packSyncs = foreman.ckptSyncRecords.map(({ turn, from, ms, phase }) => ({ turn, from, ms, phase }))
  await foreman.shutdown()

  // Gate: the workload actually ran — every turn's payload is in place with
  // the expected size (a silently-failed exec_command must fail the benchmark)
  for (let turn = 1; turn <= TURNS; turn += 1) {
    const info = await stat(join(sandboxDir, 'workspace', 'turns', `turn-${turn}.bin`))
    gate(`turn ${turn} payload size`, info.size === PAYLOAD_BYTES, `${info.size} != ${PAYLOAD_BYTES}`)
  }
  // Gate: the A/B is what it claims — serial uploads every pack at publish,
  // overlap uploads every pack in the background
  gate(`pack sync phases (${mode})`,
    packSyncs.length === TURNS && packSyncs.every((entry) => entry.phase === (mode === 'overlap' ? 'background' : 'publish')),
    JSON.stringify(packSyncs.map(({ turn, phase }) => ({ turn, phase }))))

  // Gate: restore integrity — a fresh sandbox restores the workspace
  // bit-for-bit from the published checkpoint chain
  const finalManifest = await contentManifest(join(sandboxDir, 'workspace'))
  await wipeDir(sandboxDir)
  await mkdir(sandboxDir, { recursive: true })
  const restore = new Foreman({
    workdir: sandboxDir,
    agentId: 'bench-run-pipeline',
    sessionId,
    channel: 'codex',
    controlPlane,
    git: { enabled: true },
    checkpoints: { recentKeep: TURNS + 1, perLevel: 1, rebaseAfter: 0 },
  })
  await restore.prepare()
  const restoredManifest = await contentManifest(join(sandboxDir, 'workspace'))
  gate('restore reproduces the final workspace bit-for-bit',
    JSON.stringify(restoredManifest) === JSON.stringify(finalManifest),
    `${Object.keys(restoredManifest).length} vs ${Object.keys(finalManifest).length} files`)

  return {
    mode,
    runMs,
    prepareMs: foreman.timings.prepareMs,
    bootMs: foreman.timings.bootMs,
    turnMs,
    collectMs,
    publishMs: foreman.timings.publishMs ?? null,
    drainMs: foreman.timings.checkpointDrainMs ?? 0,
    packSyncs,
    workspaceFiles: Object.keys(finalManifest).length,
  }
}

// ---------------------------------------------------------------- setup

const base = await mkdtemp(join(tmpdir(), 'foreman-bench-run-'))
const sandboxDir = join(base, 'sandbox')
const controlPlane = await startMockControlPlane({ dir: join(base, 'control-plane') })
const model = await startCodexResponsesFixture({
  delayMs: DELAY_MS,
  commandFor: (text) => {
    const turn = /BENCH TURN (\d+)/.exec(text)?.[1] ?? '0'
    return `mkdir -p turns && dd if=/dev/urandom of=turns/turn-${turn}.bin bs=${1024 * 1024} count=${PAYLOAD_MB} 2>/dev/null && echo turn-${turn} >> journal.txt`
  },
  finalTextFor: () => 'BENCH_TURN_DONE',
})
log(`object store port ${controlPlane.port}, scripted model port ${model.port} (delay ${DELAY_MS}ms/request)`)

// Seed workspace: SEED_MB of incompressible base data (real cold-start I/O)
const seedDir = join(base, 'seed')
await mkdir(join(seedDir, 'src'), { recursive: true })
await writeFile(join(seedDir, 'README.md'), '# run-pipeline benchmark seed\n')
for (let index = 0; index < SEED_MB; index += 1) {
  await writeFile(join(seedDir, 'src', `base-${index}.bin`), randomBytes(1024 * 1024))
}
const seedArchive = join(base, 'seed.tar.gz')
await import('../src/core/workspace.js').then(({ archiveDirectory }) => archiveDirectory(seedDir, seedArchive))
const seedBuffer = await readFile(seedArchive)
log(`seed workspace: ${SEED_MB} MiB`)

console.log('\nforeman run-pipeline benchmark (ADR-0010 overlap scheduling, A/B on real work)')
console.log(`workload: ${TURNS} turns x ${PAYLOAD_MB} MiB incompressible payload, ${DELAY_MS}ms scripted model latency per request`)
console.log(`method: ${WARMUP_PAIRS} warmup pair(s) + ${MEASURED_RUNS} measured pair(s), median reported\n`)

const runDetails = []
let runCounter = 0
const pairs = WARMUP_PAIRS + MEASURED_RUNS
for (let pair = 0; pair < pairs; pair += 1) {
  const measured = pair >= WARMUP_PAIRS
  // Alternate the mode order across pairs (serial first on odd pairs) so
  // disk-cache/system drift cannot bias one mode systematically
  const modes = pair % 2 === 0 ? ['serial', 'overlap'] : ['overlap', 'serial']
  for (const mode of modes) {
    runCounter += 1
    log(`pair ${pair + 1}/${pairs} [${measured ? 'measured' : 'warmup'}] ${mode} run`)
    const result = await runOnce({ mode, sessionId: `bench-${runCounter}-${mode}`, sandboxDir, controlPlane, model, seedBuffer })
    if (measured) runDetails.push(result)
  }
}

// ---------------------------------------------------------------- model + report

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}
const round = (ms) => Number(ms.toFixed(0))

const byMode = (mode) => runDetails.filter((run) => run.mode === mode)
const summarise = (mode) => {
  const runs = byMode(mode)
  return {
    runMs: median(runs.map((run) => run.runMs)),
    executionMs: median(runs.map((run) => run.turnMs.reduce((sum, ms) => sum + ms, 0))),
    packSyncMs: median(runs.map((run) => run.packSyncs.reduce((sum, entry) => sum + entry.ms, 0))),
    backgroundPackMs: median(runs.map((run) => run.packSyncs.filter((entry) => entry.phase === 'background').reduce((sum, entry) => sum + entry.ms, 0))),
    prepareMs: median(runs.map((run) => run.prepareMs)),
    collectMs: median(runs.map((run) => run.collectMs)),
    drainMs: median(runs.map((run) => run.drainMs)),
  }
}
const serial = summarise('serial')
const overlap = summarise('overlap')

// Model projection from the serial runs' own constants: E_i = per-turn wall
// time, C_i = per-pack sync cost; the projection is what overlap can hide:
// turns 1..N-1 under the next turn's execution, the last turn's pack under
// collect() (publish() drains whatever is still in flight)
const projections = byMode('serial').map((run) => {
  let projection = 0
  for (let index = 0; index < run.turnMs.length - 1; index += 1) {
    const packMs = run.packSyncs.find((entry) => entry.turn === index + 1)?.ms ?? 0
    projection += Math.min(packMs, run.turnMs[index + 1])
  }
  projection += Math.min(run.packSyncs.at(-1)?.ms ?? 0, run.collectMs)
  return projection
})
const projectedSavingMs = median(projections)
const observedSavingMs = serial.runMs - overlap.runMs

console.log('── measured constants (median, ms)')
console.log(`   execution ΣE            serial ${round(serial.executionMs)}  overlap ${round(overlap.executionMs)}`)
console.log(`   pack sync ΣC (publish)  serial ${round(serial.packSyncMs)}`)
console.log(`   pack sync ΣC (hidden)   overlap ${round(overlap.backgroundPackMs)}  (drained at publish: ${round(overlap.drainMs)})`)
console.log('── wall time (median, ms)')
console.log(`   prepare                 serial ${round(serial.prepareMs)}  overlap ${round(overlap.prepareMs)}`)
console.log(`   run total (prepare->publish) serial ${round(serial.runMs)}  overlap ${round(overlap.runMs)}`)
console.log('── ADR-0010 model check')
console.log(`   projected saving  Σ min(Ci, Ei+1) = ${round(projectedSavingMs)} ms`)
console.log(`   observed saving  T_serial - T_overlap = ${round(observedSavingMs)} ms`)
console.log(`   fidelity          observed/projected = ${projectedSavingMs > 0 ? Math.round((observedSavingMs / projectedSavingMs) * 100) : 'n/a'}%`)
console.log(`   sandbox occupancy reduction = ${serial.runMs > 0 ? ((observedSavingMs / serial.runMs) * 100).toFixed(1) : 'n/a'}% of the run\n`)

const report = {
  timestamp: new Date().toISOString(),
  node: process.version,
  codex: 'codex-cli 0.149.1',
  workload: { turns: TURNS, payloadMb: PAYLOAD_MB, seedMb: SEED_MB, modelDelayMs: DELAY_MS, measuredRuns: MEASURED_RUNS },
  serial,
  overlap,
  model: { projectedSavingMs: round(projectedSavingMs), observedSavingMs: round(observedSavingMs) },
  runs: runDetails.map((run) => ({
    mode: run.mode,
    runMs: round(run.runMs),
    turnMs: run.turnMs.map(round),
    packSyncs: run.packSyncs.map((entry) => ({ ...entry, ms: round(entry.ms) })),
    prepareMs: round(run.prepareMs),
    collectMs: round(run.collectMs),
    drainMs: round(run.drainMs),
  })),
}
const resultsDir = join(fileURLToPath(new URL('.', import.meta.url)), 'results')
await mkdir(resultsDir, { recursive: true })
const resultsPath = join(resultsDir, `${report.timestamp.replace(/[:.]/g, '-')}-run-pipeline.json`)
await writeFile(resultsPath, JSON.stringify(report, null, 2))
console.log(`results written to ${resultsPath}`)

await model.close()
await controlPlane.close()
if (!keep) await rm(base, { recursive: true, force: true })
