/**
 * Golden transcripts for protocol conformance tests and benchmarks
 * (ADR-0004). These are recorded internal-frame sequences — real gateway
 * frame shapes and realistic orderings (text streaming, tool calls, turn
 * ends) — used as *input data*, never as mocks of the system under test.
 *
 * Frame model (docs/design/sse-protocol-adapter.md §1):
 *   { kind:'session.event', sessionId, seq, type, time, data }
 */

const sessionId = 'sess-transcript-001'
let seq = 0
const frame = (type, data) => ({ kind: 'session.event', sessionId, seq: ++seq, type, time: seq, data })

const chunk = (turn, step, text) => frame('assistant/chunk', { turn, step, chunk: { type: 'text-delta', index: 0, text } })
const message = (turn, step, text) => frame('assistant/message', { turn, step, message: { content: [{ type: 'text', text }] } })
const toolCall = (name, args) => frame('tool/call', { name, arguments: args })
const toolResult = (callId) => frame('tool/result', { callId, meta: { diffs: [] } })
const turnEnd = (kind = 'completed') => frame('turn/end', { reason: { kind } })

/** One turn: streamed text only. */
export const textOnlyTurn = [
  chunk(1, 0, 'The workspace is '),
  chunk(1, 0, 'restored and '),
  chunk(1, 0, 'ready.'),
  message(1, 0, 'The workspace is restored and ready.'),
  turnEnd(),
]

/** One turn: text step -> tool call -> tool result -> final text step. */
export const multiStepToolTurn = [
  chunk(1, 0, 'I will write the greeting file.'),
  message(1, 0, 'I will write the greeting file.'),
  toolCall('bash', { command: "printf 'hello from dsh\n' > greeting.txt", description: 'write greeting file into workspace' }),
  toolResult('call_1'),
  chunk(1, 1, 'greeting.txt written; task complete.'),
  message(1, 1, 'greeting.txt written; task complete.'),
  turnEnd(),
]

/** A turn that ends without any mapped event (empty response). */
export const emptyTurn = [turnEnd()]

/** A turn that does not complete (reason.kind !== 'completed'). */
export const failedTurn = [
  chunk(1, 0, 'partial work'),
  message(1, 0, 'partial work'),
  turnEnd('aborted'),
]

/** Two consecutive turns (fresh response/completion identity per turn). */
export const twoTurns = [
  ...multiStepToolTurn,
  chunk(2, 0, 'Second turn text.'),
  message(2, 0, 'Second turn text.'),
  turnEnd(),
]

/** A step that arrives only as assistant/message (replay-derived, no deltas). */
export const messageFallbackTurn = [
  message(1, 0, 'text that never streamed'),
  turnEnd(),
]

/**
 * Bulk transcript for the benchmark: `turns` turns, each with `deltasPerTurn`
 * text deltas, a tool call, and a turn end — the realistic mixed workload.
 */
export function bulkTranscript(turns = 200, deltasPerTurn = 10) {
  const frames = []
  let n = 0
  for (let turn = 1; turn <= turns; turn += 1) {
    for (let step = 0; step < deltasPerTurn; step += 1) {
      frames.push(chunk(turn, 0, `delta ${++n} of the benchmark workload stream. `))
    }
    frames.push(toolCall('bash', { command: `echo ${turn}`, description: `benchmark tool call ${turn}` }))
    frames.push(turnEnd())
  }
  return frames
}
