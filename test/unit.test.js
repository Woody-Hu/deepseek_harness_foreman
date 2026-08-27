/**
 * Unit tests for the foreman extension modules (node --test, keyless).
 *
 * Covers the five externally wired modules:
 *   events/formats, observability/trace-shipper, storage/snapshot-sink,
 *   events/event-bus, core/git-workspace, core/checkpoint
 * Run: npm test (node --test test/unit.test.js)
 */
import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, mkdir, rm, writeFile, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createEventFormatter, renderSseLine } from '../src/events/formats.js'
import { TraceShipper } from '../src/observability/trace-shipper.js'
import { RunProfiler } from '../src/observability/profiler.js'
import { createSnapshotSink } from '../src/storage/snapshot-sink.js'
import { createEventBus } from '../src/events/event-bus.js'
import { GitWorkspace } from '../src/core/git-workspace.js'
import { CheckpointKeeper, applyChangePack, buildChangePack, extractChangePack } from '../src/core/checkpoint.js'

/** Start a controllable upstream mock (records bodies/headers; failFirst 500s, constant status, delay). */
function startUpstream({ failFirst = 0, status = 200, delayMs = 0 } = {}) {
  const bodies = []
  const headers = []
  let failuresLeft = failFirst
  const server = createServer((request, response) => {
    const chunks = []
    request.on('data', (chunk) => { chunks.push(chunk) })
    request.on('end', () => {
      setTimeout(() => {
        if (failuresLeft > 0) {
          failuresLeft -= 1
          response.writeHead(500).end()
          return
        }
        if (status !== 200) {
          response.writeHead(status).end()
          return
        }
        bodies.push(Buffer.concat(chunks).toString('utf8'))
        headers.push({ ...request.headers })
        response.writeHead(200).end()
      }, delayMs)
    })
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({
        url: `http://127.0.0.1:${server.address().port}/v1/logs`,
        bodies,
        headers,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}

const sessionEvent = (type, data) => ({ kind: 'session.event', sessionId: 's1', seq: 1, type, time: 0, data })

// ---------------------------------------------------------------- event formats

test('formats: native passthrough (exactly one data output per internal frame)', () => {
  const formatter = createEventFormatter('native')
  const out = formatter.push(sessionEvent('turn/end', {}))
  assert.deepEqual(out, [{ type: 'data', payload: sessionEvent('turn/end', {}) }])
})

test('formats: openai-chat role-first chunk, text delta chunks, turn/end final chunk and DONE', () => {
  const formatter = createEventFormatter('openai-chat', { model: 'test-model' })
  const out = [
    ...formatter.push(sessionEvent('assistant/chunk', { turn: 1, step: 0, chunk: { type: 'text-delta', index: 0, text: 'Hello' } })),
    ...formatter.push(sessionEvent('assistant/chunk', { turn: 1, step: 0, chunk: { type: 'text-delta', index: 0, text: ' world' } })),
    ...formatter.push(sessionEvent('turn/end', { reason: { kind: 'completed' } })),
  ]
  const datas = out.filter((entry) => entry.type === 'data').map((entry) => entry.payload)
  assert.equal(datas.length, 4) // role + 2 content + finish
  assert.deepEqual(datas[0].choices[0].delta, { role: 'assistant', content: '' })
  assert.equal(datas[0].model, 'test-model')
  assert.equal(datas[0].object, 'chat.completion.chunk')
  assert.equal(datas[1].choices[0].delta.content, 'Hello')
  assert.equal(datas[2].choices[0].delta.content, ' world')
  const ids = new Set(datas.map((chunk) => chunk.id))
  assert.equal(ids.size, 1) // stable id within one turn
  assert.equal(datas[3].choices[0].finish_reason, 'stop')
  assert.deepEqual(datas[3].choices[0].delta, {})
  assert.equal(out.at(-1).type, 'done')
  assert.equal(renderSseLine({ type: 'done' }), 'data: [DONE]\n\n')
  assert.match(renderSseLine({ type: 'data', payload: datas[0] }, 3), /^id: 3\ndata: \{.*\}\n\n$/)
})

test('formats: openai-chat fallback for assistant/message without delta source; streamed steps not repeated', () => {
  const formatter = createEventFormatter('openai-chat')
  const fallback = formatter.push(sessionEvent('assistant/message', {
    turn: 1, step: 0, message: { content: [{ type: 'text', text: 'fallback text' }] },
  }))
  assert.equal(fallback.filter((e) => e.type === 'data').at(-1).payload.choices[0].delta.content, 'fallback text')
  // The same step already streamed: the message is not emitted again (no duplicates)
  const formatter2 = createEventFormatter('openai-chat')
  formatter2.push(sessionEvent('assistant/chunk', { turn: 1, step: 0, chunk: { type: 'text-delta', index: 0, text: 'x' } }))
  const dup = formatter2.push(sessionEvent('assistant/message', {
    turn: 1, step: 0, message: { content: [{ type: 'text', text: 'x' }] },
  }))
  assert.equal(dup.length, 0)
})

test('formats: openai-chat ignores non-text events; multiple turns each terminate with distinct ids', () => {
  const formatter = createEventFormatter('openai-chat')
  assert.equal(formatter.push(sessionEvent('tool/result', { callId: 'c1' })).length, 0)
  assert.equal(formatter.push({ kind: 'foreman.phase', phase: 'running' }).length, 0)
  const t1 = formatter.push(sessionEvent('assistant/chunk', { turn: 1, step: 0, chunk: { type: 'text-delta', index: 0, text: 'a' } }))
  const end1 = formatter.push(sessionEvent('turn/end', {}))
  const t2 = formatter.push(sessionEvent('assistant/chunk', { turn: 2, step: 0, chunk: { type: 'text-delta', index: 0, text: 'b' } }))
  const end2 = formatter.push(sessionEvent('turn/end', {}))
  assert.equal(end1.at(-1).type, 'done')
  assert.equal(end2.at(-1).type, 'done')
  assert.notEqual(t1[0].payload.id, t2[0].payload.id) // an independent completion id per turn
})

test('formats: unknown format throws (fail loud)', () => {
  assert.throws(() => createEventFormatter('anthropic'), /unknown format/)
})

// ---------------------------------------------------------------- trace-shipper

test('trace-shipper: async forward to upstream + instant 200 from the receiver (does not block dsh)', async () => {
  const upstream = await startUpstream()
  const shipper = await new TraceShipper({ upstreamUrl: upstream.url }).start()
  const t0 = Date.now()
  const response = await fetch(shipper.endpoint, { method: 'POST', body: '{"records":1}' })
  assert.equal(response.status, 200)
  assert.ok(Date.now() - t0 < 50) // enqueue answers immediately, without waiting for the upstream
  const stats = await shipper.stop({ flushMs: 2000 })
  assert.equal(stats.received, 1)
  assert.equal(stats.forwarded, 1)
  assert.equal(upstream.bodies[0], '{"records":1}')
  await upstream.close()
})

test('trace-shipper: upstream 500 retried to success; stats counting', async () => {
  const upstream = await startUpstream({ failFirst: 2 })
  const shipper = await new TraceShipper({ upstreamUrl: upstream.url, retryBaseMs: 5 }).start()
  await fetch(shipper.endpoint, { method: 'POST', body: 'x' })
  const stats = await shipper.stop({ flushMs: 3000 })
  assert.equal(stats.forwarded, 1)
  assert.equal(stats.retries, 2)
  assert.equal(stats.droppedRetries, 0)
  await upstream.close()
})

test('trace-shipper: 4xx not retried, dropped directly', async () => {
  const upstream = await startUpstream({ status: 400 })
  const shipper = await new TraceShipper({ upstreamUrl: upstream.url, retryBaseMs: 5 }).start()
  await fetch(shipper.endpoint, { method: 'POST', body: 'x' })
  const stats = await shipper.stop({ flushMs: 1000 })
  assert.equal(stats.droppedRetries, 1)
  assert.equal(stats.retries, 0) // 4xx is not retryable: no backoff at all
  assert.equal(stats.forwarded, 0)
  await upstream.close()
})

test('trace-shipper: full queue drops the oldest to keep the newest (overflow counting)', async () => {
  const upstream = await startUpstream()
  const shipper = await new TraceShipper({ upstreamUrl: upstream.url, maxQueue: 2, retryBaseMs: 1 }).start()
  shipper.enqueue('a'); shipper.enqueue('b'); shipper.enqueue('c'); shipper.enqueue('d')
  assert.equal(shipper.queue.length, 2)
  assert.equal(shipper.stats.droppedOverflow, 2)
  assert.deepEqual(shipper.queue, ['c', 'd'])
  await shipper.stop({ flushMs: 3000 })
  assert.equal(upstream.bodies.join(''), 'cd')
  await upstream.close()
})

test('trace-shipper: dynamic credential headers resolved per delivery (env read at drain time)', async () => {
  const upstream = await startUpstream()
  process.env.FOREMAN_TEST_TRACE_TOKEN = 'token-1'
  const shipper = await new TraceShipper({
    upstreamUrl: upstream.url, headersEnv: { authorization: 'FOREMAN_TEST_TRACE_TOKEN' },
  }).start()
  await fetch(shipper.endpoint, { method: 'POST', body: 'p1' })
  await new Promise((resolve) => { setTimeout(resolve, 80) })
  process.env.FOREMAN_TEST_TRACE_TOKEN = 'token-2' // rotation
  await fetch(shipper.endpoint, { method: 'POST', body: 'p2' })
  await shipper.stop({ flushMs: 2000 })
  assert.equal(upstream.headers[0].authorization, 'token-1')
  assert.equal(upstream.headers[1].authorization, 'token-2')
  delete process.env.FOREMAN_TEST_TRACE_TOKEN
  await upstream.close()
})

// ---------------------------------------------------------------- snapshot-sink

test('snapshot-sink: local put/get roundtrip', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'sink-local-'))
  const sink = createSnapshotSink({ kind: 'local', dir, bucket: 'agent-1' })
  await sink.put('sess/1/workspace.tar.gz', Buffer.from('tar-bytes'))
  assert.equal((await sink.get('sess/1/workspace.tar.gz')).toString(), 'tar-bytes')
})

test('snapshot-sink: object-store PUT carries per-call dynamic credentials; missing fails loud', async () => {
  const uploads = []
  const server = createServer((request, response) => {
    const chunks = []
    request.on('data', (chunk) => { chunks.push(chunk) })
    request.on('end', () => {
      uploads.push({ url: request.url, auth: request.headers.authorization, body: Buffer.concat(chunks).toString() })
      response.writeHead(200).end()
    })
  })
  await new Promise((resolve) => { server.listen(0, '127.0.0.1', resolve) })
  const endpoint = `http://127.0.0.1:${server.address().port}`
  const sink = createSnapshotSink({ kind: 'object-store', endpoint, bucket: 'agent-7', prefix: 'sess-9/', tokenEnv: 'FOREMAN_TEST_SNAP_TOKEN' })

  await assert.rejects(() => sink.put('a.txt', Buffer.from('x')), /FOREMAN_TEST_SNAP_TOKEN/) // missing credential fails loud
  process.env.FOREMAN_TEST_SNAP_TOKEN = 'snap-token-1'
  await sink.put('a.txt', Buffer.from('content-1'))
  process.env.FOREMAN_TEST_SNAP_TOKEN = 'snap-token-2' // rotation takes effect per call
  await sink.put('b.txt', Buffer.from('content-2'))
  assert.equal(uploads[0].url, '/agent-7/sess-9/a.txt')
  assert.equal(uploads[0].auth, 'Bearer snap-token-1')
  assert.equal(uploads[1].auth, 'Bearer snap-token-2')
  delete process.env.FOREMAN_TEST_SNAP_TOKEN
  await new Promise((resolve) => { server.close(resolve) })
})

// ---------------------------------------------------------------- event-bus

test('event-bus: memory records in order', async () => {
  const bus = createEventBus({ kind: 'memory' })
  await bus.publish({ n: 1 })
  await bus.publish({ n: 2 })
  assert.deepEqual(bus.messages.map((m) => m.n), [1, 2])
  assert.deepEqual(await bus.stop(), { published: 2 })
})

test('event-bus: http per-message POST + failure retry + publish never throws', async () => {
  const received = []
  let failFirst = 1
  const server = createServer((request, response) => {
    const chunks = []
    request.on('data', (chunk) => { chunks.push(chunk) })
    request.on('end', () => {
      if (failFirst > 0) { failFirst -= 1; response.writeHead(500).end(); return }
      received.push({ auth: request.headers.authorization, body: JSON.parse(Buffer.concat(chunks).toString()) })
      response.writeHead(200).end()
    })
  })
  await new Promise((resolve) => { server.listen(0, '127.0.0.1', resolve) })
  process.env.FOREMAN_TEST_BUS_TOKEN = 'bus-token'
  const bus = await createEventBus({ kind: 'http', url: `http://127.0.0.1:${server.address().port}/relay`, headersEnv: { authorization: 'FOREMAN_TEST_BUS_TOKEN' }, retryBaseMs: 5 }).start()
  await bus.publish({ n: 1 }) // first delivery gets a 500 -> retried successfully
  await bus.publish({ n: 2 })
  const stats = await bus.stop({ flushMs: 3000 })
  assert.equal(stats.published, 2)
  assert.equal(stats.retries, 1)
  assert.equal(received[0].auth, 'bus-token')
  assert.deepEqual(received.map((r) => r.body.n), [1, 2])
  // Publishing after the bus died does not throw (isolation)
  const dead = await createEventBus({ kind: 'http', url: 'http://127.0.0.1:1/relay', retries: 0 }).start()
  await dead.publish({ n: 3 })
  const deadStats = await dead.stop({ flushMs: 100 })
  assert.equal(deadStats.droppedRetries, 1)
  delete process.env.FOREMAN_TEST_BUS_TOKEN
  await new Promise((resolve) => { server.close(resolve) })
})

// ---------------------------------------------------------------- git-workspace

test('git-workspace: baseline -> turn commit -> change set -> secret interception (kept out of commits)', async () => {
  const cwd = await mkdtemp(join(tmpdir(), 'git-ws-'))
  await mkdir(cwd, { recursive: true })
  await writeFile(join(cwd, 'README.md'), 'seed\n')
  const git = new GitWorkspace({ cwd, secretValues: ['sk-known-secret-value'] })
  await git.ensureRepo()
  const baseline = await git.commitBaseline()
  assert.equal(baseline.committed, true)

  await writeFile(join(cwd, 'notes.md'), 'clean\n')
  await writeFile(join(cwd, 'leak.txt'), 'dump:\nsk-leak-abcdef123456\n')
  await writeFile(join(cwd, 'known.txt'), 'value=sk-known-secret-value\n')
  const turn = await git.commitTurn('sess-1')
  assert.equal(turn.committed, true)
  assert.deepEqual(turn.files, ['notes.md']) // the clean commit after interception contains only notes

  // Interception: leak (pattern) and known (known value + pattern, dual hit)
  // are unstaged; the actual commit contains only notes
  assert.deepEqual(turn.violations.map((v) => v.file).sort(), ['known.txt', 'leak.txt'])
  const leak = turn.violations.find((v) => v.file === 'leak.txt')
  const known = turn.violations.find((v) => v.file === 'known.txt')
  assert.deepEqual(leak.rules, ['pattern:openai-key'])
  assert.deepEqual(known.rules.sort(), ['known-secret', 'pattern:openai-key']) // one file aggregates multiple rules
  assert.deepEqual(git.commits.at(-1).files, ['notes.md'])

  const changed = await git.changedSinceBaseline()
  assert.deepEqual(changed, [{ status: 'A', path: 'notes.md' }]) // the authoritative change set excludes intercepted files

  // Intercepted files remain on disk but uncommitted (visible as uncommitted
  // residue, for reporting)
  const uncommitted = await git.uncommitted()
  assert.ok(uncommitted.some((line) => line.includes('leak.txt')))
  assert.ok(uncommitted.some((line) => line.includes('known.txt')))
  assert.ok((await readFile(join(cwd, 'leak.txt'))).toString().includes('sk-leak'))
})

test('git-workspace: large files commit (streaming scan, no silent drop) and boundary-spanning secrets are caught', async () => {
  const cwd = await mkdtemp(join(tmpdir(), 'git-ws-large-'))
  await mkdir(cwd, { recursive: true })
  // Small chunk size so the streaming path (multiple chunks + carry-over) is exercised
  const git = new GitWorkspace({ cwd, secretValues: ['sk-known-secret-value'], maxScanBytes: 1024 })
  await git.ensureRepo()
  await git.commitBaseline()

  // A clean file larger than the chunk size must COMMIT (regression: the old
  // "oversize -> unstage" rule silently dropped it from history and from
  // every checkpoint pack — restores lost the file)
  await writeFile(join(cwd, 'large.bin'), Buffer.alloc(8 * 1024 * 1024, 7))
  // A secret split across a chunk boundary must still be intercepted (it
  // starts 4 bytes before the 1024-byte boundary — only the carry-over can
  // reassemble the match)
  await writeFile(join(cwd, 'span.txt'), `${'x'.repeat(1020)}sk-boundary-abcdef123456${'x'.repeat(1500)}`)
  const turn = await git.commitTurn('sess-large')
  assert.deepEqual(turn.files.sort(), ['large.bin']) // span.txt intercepted, large.bin committed
  assert.deepEqual(turn.violations.map((v) => v.file), ['span.txt'])
  assert.deepEqual(turn.violations[0].rules, ['pattern:openai-key'])

  const tree = await git.lsTree('HEAD')
  assert.ok(tree.includes('large.bin')) // in the tree -> enters checkpoint packs
  assert.ok(!tree.includes('span.txt'))
})

test('git-workspace: commitAll returns committed:false when nothing changed', async () => {
  const cwd = await mkdtemp(join(tmpdir(), 'git-ws-empty-'))
  await writeFile(join(cwd, 'a.txt'), 'a\n')
  const git = new GitWorkspace({ cwd })
  await git.ensureRepo()
  await git.commitBaseline()
  const again = await git.commitTurn('no-change')
  assert.equal(again.committed, false)
  assert.deepEqual(again.violations, [])
})

// ---------------------------------------------------------------- checkpoint

test('checkpoint: skip-list retention policy — deterministic distribution (dense near, exponentially sparse far, includes 0 and n)', () => {
  const keeper = new CheckpointKeeper({ recentKeep: 2, perLevel: 1 })
  // Small n keeps everything (each level has fewer members than its cap)
  assert.deepEqual(keeper.keepIndices(1), [0, 1])
  assert.deepEqual(keeper.keepIndices(3), [0, 1, 2, 3])
  // n=5: L0(odd)={1,3,5} keeps the recent 2 -> {3,5}; L1={2} -> {2}; L2={4} -> {4}
  assert.deepEqual(keeper.keepIndices(5), [0, 2, 3, 4, 5])
  // n=7: L0={1,3,5,7}->{5,7}; L1={2,6}->{6}; L2={4}->{4}
  assert.deepEqual(keeper.keepIndices(7), [0, 4, 5, 6, 7])
  // Default parameters (recentKeep=4, perLevel=2): n=20 keeps 11 points,
  // chain step 1 near the end and 4/8 far away
  const kept = new CheckpointKeeper().keepIndices(20)
  assert.equal(kept.length, 11)
  assert.deepEqual(kept, [0, 8, 12, 13, 14, 15, 16, 17, 18, 19, 20])
  const gaps = kept.slice(1).map((value, index) => value - kept[index])
  assert.deepEqual(gaps.slice(-8), [1, 1, 1, 1, 1, 1, 1, 1]) // near (tail) step 1 (dense)
  assert.equal(Math.max(...gaps), 8) // far step grows exponentially (sparse)
  // levelOf semantics: v2(i); 0 is the chain-head sentinel
  assert.equal(keeper.levelOf(0), Number.POSITIVE_INFINITY)
  assert.equal(keeper.levelOf(4), 2)
  assert.equal(keeper.levelOf(5), 0)
})

test('checkpoint: planPacks from = predecessor in the kept set (0 -> null, the full-pack start)', () => {
  const keeper = new CheckpointKeeper({ recentKeep: 2, perLevel: 1 })
  assert.deepEqual(keeper.planPacks(5), [
    { turn: 2, from: null }, // prev=0 -> full pack from the empty tree
    { turn: 3, from: 2 },
    { turn: 4, from: 3 },
    { turn: 5, from: 4 },
  ])
  assert.deepEqual(keeper.planPacks(1), [{ turn: 1, from: null }])
})

/** Build a multi-round git repository and produce per-round oids (helper: seed + per-round file operations + commitTurn). */
async function buildRepo(cwd, rounds) {
  const git = new GitWorkspace({ cwd, secretValues: ['sk-known-secret-value'] })
  await git.ensureRepo()
  const oids = new Map()
  const baseline = await git.commitBaseline()
  oids.set(0, baseline.oid)
  for (let turn = 1; turn <= rounds; turn += 1) {
    await writeFile(join(cwd, `turn-${turn}.txt`), `round ${turn}\n`)
    await writeFile(join(cwd, 'latest.txt'), `n=${turn}\n`)
    if (turn === 2) await rm(join(cwd, 'obsolete.txt'), { force: true }) // the D case
    const result = await git.commitTurn(`#${turn}`)
    oids.set(turn, result.oid)
  }
  return { git, oids }
}

/** Pack the source repository and extract it into an isolated temp directory (simulating an object-storage roundtrip without polluting the source repo). */
async function roundtripPack(gitWs, fromRef, toRef, fromTurn, toTurn) {
  const scratch = await mkdtemp(join(tmpdir(), 'ckpt-pack-'))
  const outPath = join(scratch, `pack-${toTurn}.tar.gz`)
  const built = await buildChangePack(gitWs, { fromRef, toRef, fromTurn, toTurn, outPath })
  const packDir = join(scratch, `pack-dir-${toTurn}`)
  await extractChangePack(outPath, packDir)
  return { built, packDir }
}

test('checkpoint: full/delta pack build and apply roundtrip (A/M/D; renames split into D+A)', async () => {
  const cwd = await mkdtemp(join(tmpdir(), 'ckpt-src-'))
  await writeFile(join(cwd, 'obsolete.txt'), 'to be deleted\n')
  await writeFile(join(cwd, 'seed.md'), 'seed\n')
  const { git, oids } = await buildRepo(cwd, 3)

  // Full pack (from=null): every file of the target tree is an A
  const full = await roundtripPack(git, null, oids.get(3), null, 3)
  assert.deepEqual(full.built.changes.map((c) => c.status), ['A', 'A', 'A', 'A', 'A']) // latest/seed/turn-1..3 (obsolete deleted)
  assert.ok(full.built.changes.some((c) => c.path === 'turn-3.txt'))

  // Delta pack: turn2->turn3 contains only that interval's changes (latest.txt
  // modified + turn-3.txt added)
  const delta = await roundtripPack(git, oids.get(2), oids.get(3), 2, 3)
  assert.deepEqual(delta.built.changes, [
    { status: 'M', path: 'latest.txt' },
    { status: 'A', path: 'turn-3.txt' },
  ])

  // Applied to a fresh empty repository: the full pack yields a tree identical
  // to the source repo (file set + byte-exact contents)
  const fresh = await mkdtemp(join(tmpdir(), 'ckpt-dst-'))
  const git2 = new GitWorkspace({ cwd: fresh })
  await git2.ensureRepo()
  await applyChangePack(git2, full.packDir, 'foreman: checkpoint 3 (replayed)')
  const paths = (await git2.lsTree('HEAD')).sort()
  assert.deepEqual(paths, ['latest.txt', 'seed.md', 'turn-1.txt', 'turn-2.txt', 'turn-3.txt'])
  assert.equal(await readFile(join(fresh, 'latest.txt'), 'utf8'), 'n=3\n')
  // Delta stacked: apply turn2->turn3 onto the turn2 state
  const fresh2 = await mkdtemp(join(tmpdir(), 'ckpt-dst2-'))
  const partial = await roundtripPack(git, null, oids.get(2), null, 2)
  const git3 = new GitWorkspace({ cwd: fresh2 })
  await git3.ensureRepo()
  await applyChangePack(git3, partial.packDir, 'foreman: checkpoint 2 (replayed)')
  await applyChangePack(git3, delta.packDir, 'foreman: checkpoint 3 (replayed)')
  assert.equal(await readFile(join(fresh2, 'latest.txt'), 'utf8'), 'n=3\n')
  assert.equal(await readFile(join(fresh2, 'turn-3.txt'), 'utf8'), 'round 3\n')
  // D semantics: obsolete.txt was deleted in turn2; the restored tree does not contain it
  assert.ok(!(await git3.lsTree('HEAD')).includes('obsolete.txt'))
})

test('checkpoint: merged-pack equivalence — a cross-round pack (skipping dropped intermediate checkpoints) yields the same tree as stepwise application', async () => {
  const cwd = await mkdtemp(join(tmpdir(), 'ckpt-merge-'))
  await writeFile(join(cwd, 'seed.md'), 'seed\n')
  const { git, oids } = await buildRepo(cwd, 4) // retention drops turn1/turn2 -> a merged pack 0->3 is required

  const stepwise = await mkdtemp(join(tmpdir(), 'ckpt-merge-step-'))
  const gitStep = new GitWorkspace({ cwd: stepwise })
  await gitStep.ensureRepo()
  for (let turn = 1; turn <= 4; turn += 1) {
    const { packDir } = await roundtripPack(git, oids.get(turn - 1), oids.get(turn), turn - 1, turn)
    await applyChangePack(gitStep, packDir, `foreman: checkpoint ${turn} (replayed)`)
  }

  const merged = await mkdtemp(join(tmpdir(), 'ckpt-merge-jump-'))
  const gitJump = new GitWorkspace({ cwd: merged })
  await gitJump.ensureRepo()
  // Merged packs: 0->3 (spanning 3 rounds) + 3->4
  const pack03 = await roundtripPack(git, oids.get(0), oids.get(3), 0, 3)
  const pack34 = await roundtripPack(git, oids.get(3), oids.get(4), 3, 4)
  await applyChangePack(gitJump, pack03.packDir, 'foreman: checkpoint 3 (replayed)')
  await applyChangePack(gitJump, pack34.packDir, 'foreman: checkpoint 4 (replayed)')

  // Trees are identical (file set + per-file contents)
  const treeStep = await gitStep.lsTree('HEAD')
  const treeJump = await gitJump.lsTree('HEAD')
  assert.deepEqual(treeJump.sort(), treeStep.sort())
  for (const path of treeStep) {
    assert.deepEqual(await readFile(join(merged, path)), await readFile(join(stepwise, path)))
  }
})

test('checkpoint: secret interception is inherited along the chain — intercepted files never enter commits, nor packs', async () => {
  const cwd = await mkdtemp(join(tmpdir(), 'ckpt-secret-'))
  await writeFile(join(cwd, 'seed.md'), 'seed\n')
  const git = new GitWorkspace({ cwd, secretValues: ['sk-known-secret-value'] })
  await git.ensureRepo()
  const baseline = await git.commitBaseline()

  await writeFile(join(cwd, 'clean.txt'), 'clean output\n')
  await writeFile(join(cwd, 'leak.txt'), 'dump:\nsk-leak-abcdef123456\n')
  const turn = await git.commitTurn('#1')
  assert.deepEqual(turn.files, ['clean.txt']) // interception works: leak stays out of the commit

  const { built, packDir } = await roundtripPack(git, baseline.oid, turn.oid, 0, 1)
  assert.ok(!built.changes.some((change) => change.path === 'leak.txt')) // the pack excludes intercepted files
  assert.ok(built.changes.some((change) => change.path === 'clean.txt'))

  // Restore side: the tree after applying the pack also excludes the secret file
  const fresh = await mkdtemp(join(tmpdir(), 'ckpt-secret-dst-'))
  const git2 = new GitWorkspace({ cwd: fresh })
  await git2.ensureRepo()
  await applyChangePack(git2, packDir, 'foreman: checkpoint 1 (replayed)')
  assert.ok(!(await git2.lsTree('HEAD')).includes('leak.txt'))
})

test('profiler: spans, counters, gauges; error paths still record the span', async () => {
  const profiler = new RunProfiler({ channel: 'codex', sessionId: 's1', agentId: 'a1' })
  const value = await profiler.span('prepare.config.download', async () => {
    await new Promise((resolve) => { setTimeout(resolve, 5) })
    return 42
  })
  assert.equal(value, 42) // the span wrapper is transparent
  assert.equal(profiler.span('publish.packaging.trace', () => 'ok'), 'ok') // sync spans work too
  await assert.rejects(profiler.span('turn.1.execute', async () => { throw new Error('boom') }), /boom/)
  const span = profiler.spans.find((entry) => entry.name === 'turn.1.execute')
  assert.ok(span !== undefined && span.durationMs >= 0) // the failed span is still recorded
  assert.ok(profiler.spans.find((entry) => entry.name === 'prepare.config.download').durationMs >= 5)
})

test('profiler: derived metrics match the performance model (P + B + ΣE + C + U decomposition)', async () => {
  const profiler = new RunProfiler({ sessionId: 's1' })
  await profiler.span('prepare.workspace.download', () => new Promise((resolve) => { setTimeout(resolve, 10) }))
  await profiler.span('start.channel', () => new Promise((resolve) => { setTimeout(resolve, 10) }))
  const turn = profiler.beginTurn(1)
  await profiler.span('turn.1.execute', () => new Promise((resolve) => { setTimeout(resolve, 20) }))
  profiler.noteEvent('assistant/chunk')
  profiler.noteEvent('assistant/chunk')
  profiler.noteEvent('tool/call')
  profiler.endTurn(turn, { executeMs: 20, commitMs: 4 })
  await profiler.span('turn.1.commit', () => new Promise((resolve) => { setTimeout(resolve, 4) }))
  await profiler.span('collect', () => new Promise((resolve) => { setTimeout(resolve, 3) }))
  await profiler.span('publish.packaging.workspace', () => new Promise((resolve) => { setTimeout(resolve, 7) }))
  profiler.count('upload')
  profiler.gauge('checkpoint.packs', 2)
  profiler.end()

  const derived = profiler.derived()
  const approx = (actual, expected, slack = 2) =>
    assert.ok(Math.abs(actual - expected) <= slack, `${actual} ≉ ${expected}±${slack}`)
  assert.equal(derived.turns, 1)
  assert.equal(derived.events, 3) // event.* counters only
  assert.equal(derived.executionMs, 20)
  assert.equal(derived.commitMs, 4)
  approx(derived.prepareMs, 10)
  approx(derived.bootMs, 10)
  approx(derived.collectMs, 3)
  approx(derived.publishMs, 7)
  approx(derived.warmupMs, 20) // P + B
  approx(derived.saveCostMs, 7) // U
  assert.ok(derived.runWallMs >= 54) // at least the sum of the (serial) spans
  assert.ok(derived.usefulWorkRatio > 0 && derived.usefulWorkRatio < 1)
  assert.ok(derived.turnThroughputPerSec > 0)
  assert.ok(derived.eventRatePerSec > 0)
  assert.equal(derived.turnDetails[0].firstEventLatencyMs >= 20, true) // first event after the execute span
  assert.equal(derived.turnDetails[0].events, 3)

  const report = profiler.report()
  assert.equal(report.schema, 1)
  assert.equal(report.counters['event.assistant/chunk'], 2)
  assert.equal(report.counters['upload'], 1)
  assert.equal(report.gauges['checkpoint.packs'], 2)
  assert.ok(report.spans.length >= 5)
  assert.equal(report.endedAt <= new Date().toISOString(), true)
})

test('profiler: spanFrom records externally measured boundaries', () => {
  const profiler = new RunProfiler({ sessionId: 's1' })
  profiler.spanFrom('publish.uploads', 100, 250)
  assert.equal(profiler.derived().publishMs, 150)
  assert.equal(profiler.derived().saveCostMs, 150)
})

// The derived publish decomposition: max(K, ΣC) + Ubatch is the ADR-0011
// critical path; the profiler's span sums are the raw material for it.
test('profiler: publish spans support the ADR-0011 critical-path computation', async () => {
  const profiler = new RunProfiler({ sessionId: 's1' })
  await profiler.span('publish.packaging.workspace', () => new Promise((resolve) => { setTimeout(resolve, 30) })) // K
  await profiler.span('publish.checkpointSync', () => new Promise((resolve) => { setTimeout(resolve, 20) })) // ΣC
  await profiler.span('publish.uploads', () => new Promise((resolve) => { setTimeout(resolve, 10) })) // Ubatch
  const derived = profiler.derived()
  const K = 30, sumC = 20, Ubatch = 10
  assert.ok(Math.abs(derived.publishMs - (K + sumC + Ubatch)) <= 2) // serialized wall time
  const projectedSerial = K + sumC + Ubatch
  const projectedOverlap = Math.max(K, sumC) + Ubatch
  assert.ok(projectedOverlap < projectedSerial) // the model predicts the saving
})
