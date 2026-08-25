/**
 * Wire-level gateway tests (hermetic, ADR-0004): a real SseGateway listens on
 * a real loopback socket; the test subscribes with a real fetch SSE client and
 * asserts on the parsed wire stream for every built-in protocol, plus
 * Last-Event-ID replay and bus delivery of the same adapted stream.
 *
 * No HTTP interception of any kind: every byte observed by the assertions
 * traveled through the real HTTP server, the real formatter pipeline, and the
 * real replay buffer.
 *
 * Run: node --test test/gateway-wire.test.js
 */
import test from 'node:test'
import assert from 'node:assert/strict'
import { SseGateway } from '../src/foreman.js'
import { createEventFormatter } from '../src/events/formats.js'
import { createEventBus } from '../src/events/event-bus.js'
import { textOnlyTurn, multiStepToolTurn } from './fixtures/transcripts.js'

/**
 * Subscribe to a gateway's /events endpoint with a real fetch client and
 * collect parsed SSE events ({id, data}) until `expected` events arrived.
 * `data: [DONE]` lines are recorded as {id, done: true}.
 */
async function subscribe(port, { expected, lastEventId } = {}) {
  const events = []
  const controller = new AbortController()
  const headers = lastEventId === undefined ? {} : { 'last-event-id': String(lastEventId) }
  const response = await fetch(`http://127.0.0.1:${port}/events`, { signal: controller.signal, headers })
  assert.equal(response.status, 200)
  assert.match(response.headers.get('content-type'), /text\/event-stream/)
  const pump = (async () => {
    try {
      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const parts = buffer.split('\n\n')
        buffer = parts.pop() ?? ''
        for (const part of parts) {
          const lines = part.split('\n')
          const idLine = lines.find((line) => line.startsWith('id: '))
          const dataLine = lines.find((line) => line.startsWith('data: '))
          if (dataLine === undefined) continue
          if (dataLine === 'data: [DONE]') events.push({ id: idLine === undefined ? undefined : Number(idLine.slice(4)), done: true })
          else events.push({ id: idLine === undefined ? undefined : Number(idLine.slice(4)), data: JSON.parse(dataLine.slice(6)) })
        }
        if (expected !== undefined && events.length >= expected) {
          controller.abort()
          return
        }
      }
    } catch { /* reads interrupted by abort are a normal close */ }
  })()
  return { events, abort: () => controller.abort(), settled: pump }
}

async function withGateway({ formatter, bus, delivery }, run) {
  const gateway = new SseGateway({ formatter, bus, delivery })
  const port = await gateway.listen()
  try {
    return await run(gateway, port)
  } finally {
    await gateway.close()
  }
}

const pushAll = (gateway, frames) => { for (const frame of frames) gateway.publish(frame) }

// ---------------------------------------------------------------- SSE carrier, per protocol

test('wire/native: every internal frame is one numbered SSE data event on the wire', async () => {
  await withGateway({ formatter: createEventFormatter('native') }, async (gateway, port) => {
    const sub = await subscribe(port, { expected: multiStepToolTurn.length })
    pushAll(gateway, multiStepToolTurn)
    await sub.settled
    // The frames on the wire are byte-identical to the internal frames
    assert.deepEqual(sub.events.map((event) => event.data), multiStepToolTurn)
    assert.deepEqual(sub.events.map((event) => event.id), multiStepToolTurn.map((_, index) => index))
  })
})

test('wire/openai-chat: chat.completion.chunk stream with [DONE] terminator', async () => {
  await withGateway({ formatter: createEventFormatter('openai-chat', { model: 'wire-model' }) }, async (gateway, port) => {
    // role + 3 deltas + finish + DONE = 6 events
    const sub = await subscribe(port, { expected: 6 })
    pushAll(gateway, textOnlyTurn)
    await sub.settled
    const chunks = sub.events.filter((event) => !event.done).map((event) => event.data)
    assert.ok(chunks.every((chunk) => chunk.object === 'chat.completion.chunk' && chunk.model === 'wire-model'))
    assert.deepEqual(chunks[0].choices[0].delta, { role: 'assistant', content: '' })
    assert.equal(chunks.at(-1).choices[0].finish_reason, 'stop')
    const done = sub.events.at(-1)
    assert.equal(done.done, true)
    // The [DONE] wire line has no id (carrier termination marker)
    assert.equal(done.id, undefined)
  })
})

test('wire/openai-responses: Responses event stream over real HTTP; the codex alias is selectable', async () => {
  await withGateway({ formatter: createEventFormatter('codex') }, async (gateway, port) => {
    // created + item added + part added + 3 deltas + text done + part done + item done + completed + DONE
    const sub = await subscribe(port, { expected: 11 })
    pushAll(gateway, textOnlyTurn)
    await sub.settled
    const types = sub.events.filter((event) => !event.done).map((event) => event.data.type)
    assert.deepEqual(types, [
      'response.created',
      'response.output_item.added',
      'response.content_part.added',
      'response.output_text.delta',
      'response.output_text.delta',
      'response.output_text.delta',
      'response.output_text.done',
      'response.content_part.done',
      'response.output_item.done',
      'response.completed',
    ])
    assert.equal(sub.events.at(-1).done, true)
  })
})

// ---------------------------------------------------------------- replay (Last-Event-ID)

test('wire/replay: Last-Event-ID resumption replays rendered wire lines (format-consistent)', async () => {
  await withGateway({ formatter: createEventFormatter('openai-responses') }, async (gateway, port) => {
    pushAll(gateway, multiStepToolTurn)
    await new Promise((resolve) => setTimeout(resolve, 50))
    const full = await subscribe(port)
    await new Promise((resolve) => setTimeout(resolve, 50))
    full.abort()
    await full.settled.catch(() => {})
    // resume after wire event id 3: every later wire line replays, ids intact
    const resumed = await subscribe(port, { lastEventId: 3 })
    await new Promise((resolve) => setTimeout(resolve, 50))
    resumed.abort()
    await resumed.settled.catch(() => {})
    const firstReplayed = full.events.findIndex((event) => event.id > 3)
    assert.deepEqual(resumed.events, full.events.slice(firstReplayed))
  })
})

// ---------------------------------------------------------------- bus carrier (same adapted stream)

test('wire/bus: delivery "both" publishes the same adapted stream onto the bus', async () => {
  const bus = createEventBus({ kind: 'memory' })
  await withGateway({ formatter: createEventFormatter('openai-chat'), bus, delivery: 'both' }, async (gateway, port) => {
    const sub = await subscribe(port, { expected: 6 })
    pushAll(gateway, textOnlyTurn)
    await sub.settled
    // bus payloads are exactly the SSE payloads (same adapted EventOut stream)
    const ssePayloads = sub.events.filter((event) => !event.done).map((event) => event.data)
    const busPayloads = bus.messages.filter((message) => message?.kind !== 'stream.done')
    assert.deepEqual(busPayloads, ssePayloads)
    // the done marker rides the bus as stream.done
    assert.ok(bus.messages.some((message) => message?.kind === 'stream.done'))
  })
})

test('wire/bus: delivery "bus" does not require SSE subscribers', async () => {
  const bus = createEventBus({ kind: 'memory' })
  await withGateway({ formatter: createEventFormatter('native'), bus, delivery: 'bus' }, async (gateway) => {
    pushAll(gateway, textOnlyTurn)
    // native is passthrough: one bus message per frame, no done marker
    assert.deepEqual(bus.messages, textOnlyTurn)
  })
})
