/**
 * Run-pipeline benchmark (ADR-0011): measures the run lifecycle critical path
 * on REAL work — the real codex binary driving a multi-turn sandbox, real git
 * commits, real tar/gzip pack builds, real HTTP uploads/downloads against a
 * disk-backed object store. Only the model endpoint is scripted (a local
 * Responses-API fixture with a per-request delay — the workload generator;
 * external-dependency rule per ADR-0004, no foreman code is mocked).
 *
 * Workload per run: one sandbox, N turns. Each turn's exec_command writes
 * PAYLOAD_MB of incompressible data (dd /dev/urandom) — checkpoint pack cost
 * is real compression + upload work; DELAY_MS makes execution time
 * configurable through the scripted model latency.
 *
 * Measured critical-path breakdown (per run, median over MEASURED_RUNS):
 *   prepare / boot / per-turn execution / collect / publish, plus the
 *   per-pack checkpoint sync timings recorded inside publish()'s retention
 *   pass (foreman.ckptSyncRecords, real timers in the real path). Historical
 *   A/B context: ADR-0010 measured a background-sync design hiding ΣC under
 *   execution; ADR-0011 removed it for simplicity — this benchmark now tracks
 *   the occupancy cost of that decision.
 *
 * Integrity gates (no benchmarking a broken pipeline, no derived numbers):
 *   - every turn's payload file exists with the expected size (the dd workload
 *     actually ran inside the sandbox)
 *   - every pack is built+uploaded inside publish()'s retention pass (the
 *     ADR-0011 scheduling claim)
 *   - a fresh restore from the published checkpoint index reproduces the
 *     final workspace bit-for-bit (content manifest equality)
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
const WARMUP_RUNS = quick ? 0 : 1
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
 * @returns per-run measurements { runMs, prepareMs, bootMs, turnMs[], collectMs,
 *          publishMs, packSyncs[], restoredBytes }
 */
async function runOnce({ sessionId, sandboxDir, controlPlane, model, seedBuffer }) {
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
    // recentKeep=TURNS+1 keeps every turn -> one pack per turn (uniform sync cost)
    checkpoints: { recentKeep: TURNS + 1, perLevel: 1, rebaseAfter: 0 },
  })

  const runStarted = Date.now()
  await foreman.prepare()
  await foreman.start()
  const turnMs = []
  for (let turn = 1; turn <= TURNS; turn += 1) {
    const promptStarted = Date.now()
    const { reason } = await foreman.prompt(`please record the benchmark progress: BENCH TURN ${turn}`, { timeoutMs: 300_000 })
    turnMs.push(Date.now() - promptStarted)
    gate(`turn ${turn} completed`, reason?.kind === 'completed', JSON.stringify(reason))
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
  // Gate: the scheduling claim — every pack is built+uploaded inside
  // publish()'s retention pass (ADR-0011: no background syncs)
  gate('pack sync phases',
    packSyncs.length === TURNS && packSyncs.every((entry) => entry.phase === 'publish'),
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
    runMs,
    prepareMs: foreman.timings.prepareMs,
    bootMs: foreman.timings.bootMs,
    turnMs,
    collectMs,
    publishMs: foreman.timings.publishMs ?? null,
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

console.log('\nforeman run-pipeline benchmark (ADR-0011 critical-path breakdown, real work)')
console.log(`workload: ${TURNS} turns x ${PAYLOAD_MB} MiB incompressible payload, ${DELAY_MS}ms scripted model latency per request`)
console.log(`method: ${WARMUP_RUNS} warmup run(s) + ${MEASURED_RUNS} measured run(s), median reported\n`)

const runDetails = []
let runCounter = 0
for (let run = 0; run < WARMUP_RUNS + MEASURED_RUNS; run += 1) {
  const measured = run >= WARMUP_RUNS
  runCounter += 1
  log(`run ${run + 1}/${WARMUP_RUNS + MEASURED_RUNS} [${measured ? 'measured' : 'warmup'}]`)
  const result = await runOnce({ sessionId: `bench-${runCounter}`, sandboxDir, controlPlane, model, seedBuffer })
  if (measured) runDetails.push(result)
}

// ---------------------------------------------------------------- report

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}
const round = (ms) => Number(ms.toFixed(0))

const summary = {
  runMs: median(runDetails.map((run) => run.runMs)),
  executionMs: median(runDetails.map((run) => run.turnMs.reduce((sum, ms) => sum + ms, 0))),
  prepareMs: median(runDetails.map((run) => run.prepareMs)),
  bootMs: median(runDetails.map((run) => run.bootMs)),
  collectMs: median(runDetails.map((run) => run.collectMs)),
  publishMs: median(runDetails.map((run) => run.publishMs ?? 0)),
  packSyncMs: median(runDetails.map((run) => run.packSyncs.reduce((sum, entry) => sum + entry.ms, 0))),
}

console.log('── critical path (median, ms)')
console.log(`   prepare                ${round(summary.prepareMs)}`)
console.log(`   boot                   ${round(summary.bootMs)}`)
console.log(`   execution ΣE           ${round(summary.executionMs)}  (${runDetails.at(-1)?.turnMs.map(round).join(' + ')} per turn)`)
console.log(`   collect                ${round(summary.collectMs)}`)
console.log(`   publish                ${round(summary.publishMs)}  (incl. pack sync ΣC ${round(summary.packSyncMs)} — on the critical path per ADR-0011)`)
console.log(`   run total              ${round(summary.runMs)}`)
console.log('── ADR-0011 context: a background-sync design could hide up to Σ min(Ci, Ei+1) of the')
console.log('   pack-sync cost under execution; ADR-0010 measured 48–85% fidelity of that projection.')
console.log('   This benchmark tracks the accepted occupancy cost of the simpler design.\n')

const report = {
  timestamp: new Date().toISOString(),
  node: process.version,
  codex: 'codex-cli 0.149.1',
  workload: { turns: TURNS, payloadMb: PAYLOAD_MB, seedMb: SEED_MB, modelDelayMs: DELAY_MS, measuredRuns: MEASURED_RUNS },
  summary,
  runs: runDetails.map((run) => ({
    runMs: round(run.runMs),
    turnMs: run.turnMs.map(round),
    packSyncs: run.packSyncs.map((entry) => ({ ...entry, ms: round(entry.ms) })),
    prepareMs: round(run.prepareMs),
    bootMs: round(run.bootMs),
    collectMs: round(run.collectMs),
    publishMs: round(run.publishMs ?? 0),
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
