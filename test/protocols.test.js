/**
 * Protocol adapter conformance tests (hermetic, ADR-0004): drive every
 * built-in protocol with golden transcripts and assert the exact EventOut
 * sequences / payload invariants per the normative mapping tables
 * (ADR-0001, ADR-0003; docs/design/sse-protocol-adapter.md §3).
 *
 * Run: node --test test/protocols.test.js
 */
import test from 'node:test'
import assert from 'node:assert/strict'
import { createEventFormatter } from '../src/events/formats.js'
import { listProtocols, registerProtocol, resolveProtocol } from '../src/events/protocols/registry.js'
import { loadForemanConfig } from '../src/config.js'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  textOnlyTurn, multiStepToolTurn, emptyTurn, failedTurn, twoTurns, messageFallbackTurn,
} from './fixtures/transcripts.js'

const run = (formatter, frames) => frames.flatMap((frame) => formatter.push(frame))
const payloads = (out) => out.filter((entry) => entry.type === 'data').map((entry) => entry.payload)
const eventTypes = (out) => payloads(out).map((payload) => payload.type)

// ---------------------------------------------------------------- registry

test('registry: built-ins are listed with ids, aliases, and metadata', () => {
  const protocols = listProtocols()
  assert.deepEqual(protocols.map((protocol) => protocol.id), ['native', 'openai-chat', 'openai-responses'])
  const responses = protocols.find((protocol) => protocol.id === 'openai-responses')
  assert.deepEqual(responses.aliases, ['codex'])
})

test('registry: aliases resolve to the same definition as the canonical id', () => {
  assert.equal(resolveProtocol('codex'), resolveProtocol('openai-responses'))
})

test('registry: unknown protocol fails loud and lists what is available', () => {
  assert.throws(() => resolveProtocol('anthropic'), /unknown protocol 'anthropic'.*native.*openai-chat.*openai-responses/s)
  // The formats façade preserves its historical wording (backwards compatibility)
  assert.throws(() => createEventFormatter('anthropic'), /unknown format/)
})

test('registry: external protocol registration works; id collisions fail loud', () => {
  registerProtocol({
    id: 'test-dialect',
    title: 'Test dialect',
    description: 'registered by the conformance test',
    create: () => ({ push: () => [{ type: 'data', payload: { dialect: true } }] }),
  })
  const formatter = createEventFormatter('test-dialect')
  assert.deepEqual(formatter.push({ kind: 'session.event' }), [{ type: 'data', payload: { dialect: true } }])
  assert.throws(() => registerProtocol({ id: 'test-dialect', title: 'x', description: 'x', create: () => {} }), /already registered/)
  assert.throws(() => registerProtocol({ id: 'native', title: 'x', description: 'x', create: () => {} }), /already registered/)
})

// ---------------------------------------------------------------- native

test('native: lossless passthrough — every internal frame becomes exactly one data entry', () => {
  const out = run(createEventFormatter('native'), multiStepToolTurn)
  assert.deepEqual(out, multiStepToolTurn.map((frame) => ({ type: 'data', payload: frame })))
})

// ---------------------------------------------------------------- openai-chat (registry path)

test('openai-chat: one completion per turn with role-first, deltas, finish and DONE', () => {
  const out = run(createEventFormatter('openai-chat', { model: 'm1' }), textOnlyTurn)
  const chunks = payloads(out)
  assert.equal(out.at(-1).type, 'done')
  assert.equal(chunks.length, 5) // role + 3 deltas + finish
  assert.deepEqual(chunks[0].choices[0].delta, { role: 'assistant', content: '' })
  assert.equal(chunks[1].choices[0].delta.content, 'The workspace is ')
  assert.equal(chunks[2].choices[0].delta.content, 'restored and ')
  assert.equal(chunks[3].choices[0].delta.content, 'ready.')
  assert.equal(chunks[4].choices[0].finish_reason, 'stop')
  // assistant/message after deltas must NOT duplicate text (dedupe by turn:step)
  assert.ok(!out.some((entry) => JSON.stringify(entry.payload ?? null).includes('restored and ready.')))
  // stable completion id within the turn
  assert.equal(new Set(chunks.map((chunk) => chunk.id)).size, 1)
  assert.ok(chunks.every((chunk) => chunk.model === 'm1' && chunk.object === 'chat.completion.chunk'))
})

// ---------------------------------------------------------------- openai-responses (codex)

test('openai-responses: text-only turn produces the full Responses event sequence', () => {
  const out = run(createEventFormatter('openai-responses', { model: 'm2' }), textOnlyTurn)
  assert.deepEqual(eventTypes(out), [
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
  assert.equal(out.at(-1).type, 'done') // carrier-level turn separator
  const [created, , , d1, d2, d3, textDone, , , completed] = payloads(out)
  const responseId = created.response.id
  assert.equal(created.response.status, 'in_progress')
  assert.equal(created.response.model, 'm2')
  assert.equal(d1.delta, 'The workspace is ')
  assert.equal(d2.delta, 'restored and ')
  assert.equal(d3.delta, 'ready.')
  assert.equal(textDone.text, 'The workspace is restored and ready.')
  assert.equal(completed.type, 'response.completed')
  assert.equal(completed.response.id, responseId) // one response per turn
  assert.equal(completed.response.status, 'completed')
  assert.equal(completed.response.usage, null) // no fabricated usage numbers
  const [item] = completed.response.output
  assert.equal(item.type, 'message')
  assert.equal(item.role, 'assistant')
  assert.equal(item.content[0].type, 'output_text')
  assert.equal(item.content[0].text, 'The workspace is restored and ready.')
})

test('openai-responses: tool call becomes a function_call output item with argument lifecycle', () => {
  const out = run(createEventFormatter('openai-responses'), [
    ...multiStepToolTurn.slice(0, 3), // chunk, message, tool/call
  ])
  assert.deepEqual(eventTypes(out), [
    'response.created',
    'response.output_item.added', // message item
    'response.content_part.added',
    'response.output_text.delta',
    'response.output_item.added', // function_call item
    'response.function_call_arguments.delta',
    'response.function_call_arguments.done',
    'response.output_item.done',
  ])
  const fcAdded = payloads(out)[4]
  assert.equal(fcAdded.item.type, 'function_call')
  assert.equal(fcAdded.item.name, 'bash')
  assert.equal(fcAdded.item.status, 'in_progress')
  assert.equal(fcAdded.item.arguments, '')
  assert.equal(fcAdded.output_index, 1) // after the message item
  const argsDone = payloads(out)[6]
  assert.equal(argsDone.arguments, JSON.stringify({ command: "printf 'hello from dsh\n' > greeting.txt", description: 'write greeting file into workspace' }))
  const itemDone = payloads(out)[7]
  assert.equal(itemDone.item.status, 'completed')
  assert.equal(itemDone.item.call_id, fcAdded.item.call_id)
})

test('openai-responses: turn/end closes open message items and completes the response with the full output', () => {
  const out = run(createEventFormatter('openai-responses'), multiStepToolTurn)
  const completed = payloads(out).at(-1)
  assert.equal(completed.type, 'response.completed')
  // output: message (step 0) at index 0, function_call at index 1, message (step 1) at index 2
  assert.equal(completed.response.output.length, 3)
  assert.equal(completed.response.output[0].type, 'message')
  assert.equal(completed.response.output[1].type, 'function_call')
  assert.equal(completed.response.output[2].type, 'message')
  assert.equal(completed.response.output[2].content[0].text, 'greeting.txt written; task complete.')
  // every item was closed exactly once
  const itemDoneCount = eventTypes(out).filter((type) => type === 'response.output_item.done').length
  assert.equal(itemDoneCount, 3)
  // tool/result has no slot in the response output — it must not appear
  assert.ok(!JSON.stringify(out).includes('"tool/result"'))
})

test('openai-responses: empty turn still yields a validly terminated empty response', () => {
  const out = run(createEventFormatter('openai-responses'), emptyTurn)
  assert.deepEqual(eventTypes(out), ['response.created', 'response.completed'])
  assert.equal(payloads(out)[1].response.output.length, 0)
  assert.equal(out.at(-1).type, 'done')
})

test('openai-responses: non-completed turn/end maps to response.failed with the reason', () => {
  const out = run(createEventFormatter('openai-responses'), failedTurn)
  const last = payloads(out).at(-1)
  assert.equal(last.type, 'response.failed')
  assert.equal(last.response.status, 'failed')
  assert.equal(last.response.error.code, 'turn_end')
  assert.equal(last.response.error.message, 'aborted')
})

test('openai-responses: two turns produce two distinct responses', () => {
  const out = run(createEventFormatter('openai-responses'), twoTurns)
  const completed = payloads(out).filter((payload) => payload.type === 'response.completed')
  assert.equal(completed.length, 2)
  assert.notEqual(completed[0].response.id, completed[1].response.id)
  assert.equal(out.filter((entry) => entry.type === 'done').length, 2)
})

test('openai-responses: assistant/message without a delta source is a whole-text delta fallback', () => {
  const out = run(createEventFormatter('openai-responses'), messageFallbackTurn)
  assert.deepEqual(eventTypes(out), [
    'response.created',
    'response.output_item.added',
    'response.content_part.added',
    'response.output_text.delta',
    'response.output_text.done',
    'response.content_part.done',
    'response.output_item.done',
    'response.completed',
  ])
  assert.equal(payloads(out)[3].delta, 'text that never streamed')
})

test('openai-responses: the codex alias resolves the same adapter', () => {
  const out = run(createEventFormatter('codex'), textOnlyTurn)
  assert.ok(eventTypes(out).includes('response.output_text.delta'))
})

// ---------------------------------------------------------------- config file (ADR-0002)

test('config: valid file loads; precedence is option > file > default', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'foreman-config-'))
  try {
    const path = join(dir, 'foreman.config.json')
    await writeFile(path, JSON.stringify({ events: { protocol: 'codex', delivery: 'both', model: 'cfg-model' } }))
    const config = await loadForemanConfig(path)
    assert.deepEqual(config, { events: { protocol: 'codex', delivery: 'both', model: 'cfg-model' } })
    // merge semantics as implemented by Foreman.start (option > file > default)
    const merged = (optionEvents) => ({ ...config.events, ...optionEvents })
    assert.equal(merged(undefined).protocol, 'codex')
    assert.equal(merged({ protocol: 'openai-chat' }).protocol, 'openai-chat') // option wins
    assert.equal(merged({}).delivery, 'both')
    assert.equal(merged({}).model, 'cfg-model')
    // unknown protocol in config fails loud at formatter creation (registry)
    await writeFile(path, JSON.stringify({ events: { protocol: 'nope' } }))
    const bad = await loadForemanConfig(path)
    assert.throws(() => createEventFormatter(bad.events.protocol), /unknown format/)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test('config: no path is a valid empty config; bad files fail loud', async () => {
  assert.deepEqual(await loadForemanConfig(undefined), {})
  const dir = await mkdtemp(join(tmpdir(), 'foreman-config-'))
  try {
    await assert.rejects(() => loadForemanConfig(join(dir, 'missing.json')), /cannot read/)
    const malformed = join(dir, 'bad.json')
    await writeFile(malformed, '{ not json')
    await assert.rejects(() => loadForemanConfig(malformed), /invalid JSON/)
    const unknownKey = join(dir, 'unknown.json')
    await writeFile(unknownKey, JSON.stringify({ events: { protocol: 'native', wat: 1 } }))
    await assert.rejects(() => loadForemanConfig(unknownKey), /unknown key\(s\) under 'events'/)
    const unknownTop = join(dir, 'top.json')
    await writeFile(unknownTop, JSON.stringify({ secretValues: ['x'] }))
    await assert.rejects(() => loadForemanConfig(unknownTop), /unknown top-level key/)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})
