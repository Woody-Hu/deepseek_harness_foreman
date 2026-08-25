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
  assert.deepEqual(protocols.map((protocol) => protocol.id), ['native', 'openai-chat', 'openai-responses', 'anthropic-messages'])
  const responses = protocols.find((protocol) => protocol.id === 'openai-responses')
  assert.deepEqual(responses.aliases, ['codex'])
  const anthropic = protocols.find((protocol) => protocol.id === 'anthropic-messages')
  assert.deepEqual(anthropic.aliases, ['claude'])
})

test('registry: aliases resolve to the same definition as the canonical id', () => {
  assert.equal(resolveProtocol('codex'), resolveProtocol('openai-responses'))
})

test('registry: unknown protocol fails loud and lists what is available', () => {
  assert.throws(() => resolveProtocol('anthropic'), /unknown protocol 'anthropic'.*native.*openai-chat.*openai-responses.*anthropic-messages/s)
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

// ---------------------------------------------------------------- anthropic-messages (claude)

test('anthropic-messages: text-only turn produces the full Anthropic event sequence', () => {
  const out = run(createEventFormatter('anthropic-messages', { model: 'm3' }), textOnlyTurn)
  const events = out.filter((entry) => entry.type === 'data').map((entry) => entry.event)
  assert.deepEqual(events, [
    'message_start',
    'content_block_start',
    'content_block_delta',
    'content_block_delta',
    'content_block_delta',
    'content_block_stop',
    'message_delta',
    'message_stop',
  ])
  assert.equal(out.at(-1).type, 'done') // carrier-level turn separator
  const [msgStart, blockStart, d1, d2, d3, blockStop, msgDelta, msgStop] = payloads(out)
  // message_start envelope
  assert.equal(msgStart.type, 'message_start')
  assert.equal(msgStart.message.role, 'assistant')
  assert.equal(msgStart.message.model, 'm3')
  assert.equal(msgStart.message.stop_reason, null)
  assert.deepEqual(msgStart.message.usage, { input_tokens: 0, output_tokens: 0 })
  // content block lifecycle
  assert.equal(blockStart.type, 'content_block_start')
  assert.equal(blockStart.index, 0)
  assert.equal(blockStart.content_block.type, 'text')
  assert.equal(blockStart.content_block.text, '')
  // text deltas
  assert.equal(d1.delta.text, 'The workspace is ')
  assert.equal(d2.delta.text, 'restored and ')
  assert.equal(d3.delta.text, 'ready.')
  // block stop
  assert.equal(blockStop.type, 'content_block_stop')
  assert.equal(blockStop.index, 0)
  // message delta
  assert.equal(msgDelta.type, 'message_delta')
  assert.equal(msgDelta.delta.stop_reason, 'end_turn')
  assert.equal(msgDelta.usage, null)
  // message stop
  assert.equal(msgStop.type, 'message_stop')
  // stable message id within the turn
  assert.equal(new Set([msgStart.message.id]).size, 1)
})

test('anthropic-messages: tool call becomes a tool_use content block', () => {
  // Use the multiStepToolTurn first 3 frames (chunk + message + toolCall)
  const partial = multiStepToolTurn.slice(0, 3)
  const out = run(createEventFormatter('anthropic-messages'), partial)
  const events = out.filter((entry) => entry.type === 'data').map((entry) => entry.event)
  assert.deepEqual(events, [
    'message_start',
    'content_block_start',
    'content_block_delta',
    'content_block_stop',
    'content_block_start',
    'content_block_delta',
    'content_block_stop',
  ])
  const textBlock = payloads(out)[1]
  assert.equal(textBlock.content_block.type, 'text')
  assert.equal(textBlock.content_block.text, '')
  const toolBlock = payloads(out)[4]
  assert.equal(toolBlock.content_block.type, 'tool_use')
  assert.equal(toolBlock.content_block.name, 'bash')
  assert.ok(toolBlock.content_block.id.startsWith('toolu_'))
  assert.deepEqual(toolBlock.content_block.input, {})
  const toolDelta = payloads(out)[5]
  assert.equal(toolDelta.delta.type, 'input_json_delta')
  assert.ok(toolDelta.delta.partial_json.includes('printf'))
})

test('anthropic-messages: turn/end with completed reason emits end_turn stop_reason', () => {
  const out = run(createEventFormatter('anthropic-messages'), multiStepToolTurn)
  const msgDelta = payloads(out).filter((p) => p.type === 'message_delta').at(-1)
  assert.equal(msgDelta.delta.stop_reason, 'end_turn')
  assert.equal(msgDelta.usage, null)
})

test('anthropic-messages: empty turn still yields a validly terminated empty message', () => {
  const out = run(createEventFormatter('anthropic-messages'), emptyTurn)
  const events = out.filter((entry) => entry.type === 'data').map((entry) => entry.event)
  assert.deepEqual(events, ['message_start', 'message_delta', 'message_stop'])
  assert.equal(out.at(-1).type, 'done')
  const msgStart = payloads(out)[0]
  assert.equal(msgStart.message.content.length, 0)
})

test('anthropic-messages: non-completed turn/end maps to the given stop_reason', () => {
  const out = run(createEventFormatter('anthropic-messages'), failedTurn)
  const msgDelta = payloads(out).filter((p) => p.type === 'message_delta').at(-1)
  assert.equal(msgDelta.delta.stop_reason, 'aborted')
})

test('anthropic-messages: two turns produce two distinct message ids', () => {
  const out = run(createEventFormatter('anthropic-messages'), twoTurns)
  const msgStarts = payloads(out).filter((p) => p.type === 'message_start')
  assert.equal(msgStarts.length, 2)
  assert.notEqual(msgStarts[0].message.id, msgStarts[1].message.id)
  assert.equal(out.filter((entry) => entry.type === 'done').length, 2)
})

test('anthropic-messages: the claude alias resolves the same adapter as anthropic-messages', () => {
  const a = createEventFormatter('anthropic-messages')
  const b = createEventFormatter('claude')
  const outA = a.push(textOnlyTurn[0])
  const outB = b.push(textOnlyTurn[0])
  // Length and event types match (ids differ due to randomUUID per instance)
  assert.equal(outA.length, outB.length)
  assert.equal(outA[0].type, outB[0].type)
  assert.equal(outA[0].event, outB[0].event)
  assert.equal(outA[0].payload.type, outB[0].payload.type)
  assert.equal(outA[0].payload.message.role, outB[0].payload.message.role)
  assert.equal(outA[0].payload.message.model, outB[0].payload.message.model)
})

test('anthropic-messages: assistant/message without a delta source is a whole-text delta fallback', () => {
  const out = run(createEventFormatter('anthropic-messages'), messageFallbackTurn)
  const events = out.filter((entry) => entry.type === 'data').map((entry) => entry.event)
  // message_start -> content_block_start -> content_block_delta -> content_block_stop -> message_delta -> message_stop
  assert.ok(events.includes('content_block_delta'))
  const deltas = payloads(out).filter((p) => p.type === 'content_block_delta')
  assert.ok(deltas.some((d) => d.delta?.text === 'text that never streamed'))
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
