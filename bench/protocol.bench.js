/**
 * Protocol pipeline benchmark (ADR-0004): measures the REAL adaptation +
 * gateway pipeline end to end over real loopback HTTP. Nothing is mocked and
 * no number is derived — every figure comes from performance.now() around
 * real work, driven by the same golden transcripts the conformance tests
 * assert on (test/fixtures/transcripts.js).
 *
 * Per protocol (native | openai-chat | openai-responses):
 *   formatter-only   push() throughput without any transport (frames/s, EventOut/s)
 *   end-to-end       real SseGateway + real fetch SSE subscriber
 *                    - throughput: internal frames/s and wire bytes/s
 *                    - latency: p50/p95/p99 of publish -> subscriber parse
 *                      per wire event (burst publishing, the realistic
 *                      gateway fan-out workload)
 *
 * Methodology: 1 warmup + N measured runs per protocol; the table reports the
 * median run. Results are also written to bench/results/<timestamp>.json.
 *
 * Run: npm run bench   (node bench/protocol.bench.js [--quick])
 */
import { mkdir, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { performance } from 'node:perf_hooks'
import { get as httpGet } from 'node:http'
import { SseGateway } from '../src/foreman.js'
import { createEventFormatter } from '../src/events/formats.js'
import { listProtocols } from '../src/events/protocols/registry.js'
import { bulkTranscript } from '../test/fixtures/transcripts.js'

const root = dirname(fileURLToPath(import.meta.url))
const quick = process.argv.includes('--quick')
const TURNS = quick ? 50 : 200
const DELTAS_PER_TURN = quick ? 5 : 10
const MEASURED_RUNS = quick ? 3 : 5

const transcript = bulkTranscript(TURNS, DELTAS_PER_TURN)

const percentile = (sortedValues, p) => {
  if (sortedValues.length === 0) return 0
  const index = Math.min(sortedValues.length - 1, Math.ceil((p / 100) * sortedValues.length) - 1)
  return sortedValues[index]
}

const median = (values) => percentile([...values].sort((a, b) => a - b), 50)

/** Push every frame through a formatter; returns { ms, eventOutCount }. */
function measureFormatterOnly(protocolId) {
  const formatter = createEventFormatter(protocolId)
  const t0 = performance.now()
  let eventOutCount = 0
  for (const frame of transcript) eventOutCount += formatter.push(frame).length
  return { ms: performance.now() - t0, eventOutCount }
}

/**
 * Full pipeline: real gateway server + real fetch SSE subscriber.
 * Wire-event ids correlate publish timestamps with subscriber parse timestamps.
 * No mid-stream aborts (aborting undici responses poisons the connection
 * pool): the subscriber runs to the natural end of the stream, which the
 * gateway's close() produces.
 */
async function measureEndToEnd(protocolId) {
  const gateway = new SseGateway({ formatter: createEventFormatter(protocolId) })
  const port = await gateway.listen()
  try {
    // Deterministic expected wire totals (dry run on a separate formatter)
    const expected = (() => {
      const formatter = createEventFormatter(protocolId)
      let data = 0
      let done = 0
      for (const frame of transcript) {
        for (const entry of formatter.push(frame)) {
          if (entry.type === 'done') done += 1
          else data += 1
        }
      }
      return { data, done }
    })()
    const receiptTimes = new Map() // wire id -> parse timestamp (data events)
    let doneCount = 0 // data: [DONE] wire events (no id on the wire)
    // Raw node:http SSE subscriber: global fetch may be patched/dispatched by
    // the environment (proxies), which interferes with loopback streaming.
    const pump = new Promise((resolve, reject) => {
      const request = httpGet(`http://127.0.0.1:${port}/events`, (response) => {
        if (response.statusCode !== 200) {
          reject(new Error(`bench/${protocolId}: SSE subscribe failed with ${response.statusCode}`))
          return
        }
        response.setEncoding('utf8')
        let buffer = ''
        response.on('data', (chunk) => {
          buffer += chunk
          const parts = buffer.split('\n\n')
          buffer = parts.pop() ?? ''
          const now = performance.now()
          for (const part of parts) {
            const lines = part.split('\n')
            const idLine = lines.find((line) => line.startsWith('id: '))
            const dataLine = lines.find((line) => line.startsWith('data: '))
            if (dataLine === undefined) continue
            if (dataLine === 'data: [DONE]') doneCount += 1
            else if (idLine !== undefined) receiptTimes.set(Number(idLine.slice(4)), now)
          }
        })
        response.on('end', () => resolve())
        response.on('error', (error) => reject(error))
      })
      request.on('error', (error) => reject(error))
    })
    await new Promise((resolve) => setTimeout(resolve, 20)) // let the subscriber attach

    const publishTimes = new Map() // wire id -> publish timestamp
    const t0 = performance.now()
    for (const frame of transcript) {
      const firstId = gateway.emitted.length
      gateway.publish(frame)
      const now = performance.now()
      for (let id = firstId; id < gateway.emitted.length; id += 1) publishTimes.set(id, now)
    }
    const publishMs = performance.now() - t0
    await gateway.close() // ends the SSE stream -> the subscriber finishes
    await pump

    // Integrity gate (no benchmarking a broken pipeline): exact counts of data
    // events and DONE markers, and every received wire id is a real emitted
    // id (note: DONE entries consume emitted ids too, so data ids are not
    // contiguous on the wire).
    if (receiptTimes.size !== expected.data || doneCount !== expected.done) {
      throw new Error(`bench/${protocolId}: received ${receiptTimes.size}/${expected.data} data events and ${doneCount}/${expected.done} DONE markers`)
    }
    const totalEmitted = expected.data + expected.done
    for (const id of receiptTimes.keys()) {
      if (id < 0 || id >= totalEmitted) throw new Error(`bench/${protocolId}: phantom wire id ${id}`)
    }

    const wireBytes = gateway.emitted.reduce((sum, entry) => sum + entry.line.length, 0)
    const latencies = []
    for (const [id, receipt] of receiptTimes) {
      const published = publishTimes.get(id)
      if (published !== undefined) latencies.push(receipt - published)
    }
    latencies.sort((a, b) => a - b)
    return {
      totalMs: publishMs,
      framesPerSecond: Math.round(transcript.length / (publishMs / 1000)),
      wireBytesPerSecond: Math.round(wireBytes / (publishMs / 1000)),
      wireEventCount: expected.data + expected.done,
      wireBytes,
      latency: {
        p50: Number(percentile(latencies, 50).toFixed(3)),
        p95: Number(percentile(latencies, 95).toFixed(3)),
        p99: Number(percentile(latencies, 99).toFixed(3)),
      },
    }
  } finally {
    await gateway.close()
  }
}

// ---------------------------------------------------------------- run

console.log(`foreman protocol pipeline benchmark`)
console.log(`workload: ${TURNS} turns x ${DELTAS_PER_TURN} deltas + tool call + turn end = ${transcript.length} internal frames`)
console.log(`method: 1 warmup + ${MEASURED_RUNS} measured runs per protocol, median reported\n`)

const report = {
  timestamp: new Date().toISOString(),
  node: process.version,
  workload: { turns: TURNS, deltasPerTurn: DELTAS_PER_TURN, frames: transcript.length, measuredRuns: MEASURED_RUNS },
  protocols: [],
}

for (const { id } of listProtocols()) {
  await measureEndToEnd(id) // warmup (result discarded)
  const formatterRuns = []
  const e2eRuns = []
  for (let run = 0; run < MEASURED_RUNS; run += 1) {
    formatterRuns.push(measureFormatterOnly(id))
    e2eRuns.push(await measureEndToEnd(id))
  }
  const fmt = median(formatterRuns.map((entry) => entry.ms))
  const fmtOut = median(formatterRuns.map((entry) => entry.eventOutCount))
  const e2e = median(e2eRuns)
  const entry = {
    protocol: id,
    formatterOnly: {
      ms: Number(fmt.toFixed(3)),
      framesPerSecond: Math.round(transcript.length / (fmt / 1000)),
      eventOutPerSecond: Math.round(fmtOut / (fmt / 1000)),
    },
    endToEnd: {
      framesPerSecond: e2e.framesPerSecond,
      wireBytesPerSecond: e2e.wireBytesPerSecond,
      wireEventCount: e2e.wireEventCount,
      wireKb: Number((e2e.wireBytes / 1024).toFixed(1)),
      latencyMs: e2e.latency,
    },
  }
  report.protocols.push(entry)
  console.log(`── ${id}`)
  console.log(`   formatter-only   ${entry.formatterOnly.framesPerSecond.toLocaleString()} frames/s (${entry.formatterOnly.eventOutPerSecond.toLocaleString()} EventOut/s)`)
  console.log(`   end-to-end       ${entry.endToEnd.framesPerSecond.toLocaleString()} frames/s, ${(entry.endToEnd.wireBytesPerSecond / 1024).toFixed(0)} KiB/s on the wire (${entry.endToEnd.wireKb} KiB total, ${entry.endToEnd.wireEventCount} wire events)`)
  console.log(`   latency (ms)     p50 ${entry.endToEnd.latencyMs.p50}  p95 ${entry.endToEnd.latencyMs.p95}  p99 ${entry.endToEnd.latencyMs.p99}`)
  console.log()
}

const resultsPath = join(root, 'results', `${report.timestamp.replace(/[:.]/g, '-')}-protocol.json`)
await mkdir(dirname(resultsPath), { recursive: true })
await writeFile(resultsPath, JSON.stringify(report, null, 2))
console.log(`results written to ${resultsPath}`)
