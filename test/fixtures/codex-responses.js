/**
 * Local OpenAI Responses-API endpoint for driving the REAL codex app-server in
 * tests/benchmarks (ADR-0004 hermetic harness testing; ADR-0005).
 *
 * The wire shapes below were live-verified against codex-cli 0.149.1 (SSE
 * response.* events; tool calls via function_call items). Behavior is scripted
 * per turn — codex always sends the full conversation as `input`, so the
 * fixture only inspects the LAST input item:
 *   last item is user text            -> function_call (exec_command) running
 *                                        commandFor(lastUserText)
 *   last item is function_call_output -> assistant message (the turn's final
 *                                        text: finalTextFor(lastUserText))
 * lastUserText is the last user-role message in input (codex prepends its own
 * environment-context user message, so position-based round counting is
 * unreliable — drive behavior from the prompt text itself). Every request body
 * is recorded for assertions (e.g. cross-sandbox session resume must carry the
 * previous rounds' history).
 */
import { createServer } from 'node:http'

/**
 * @param {object} [options]
 * @param {number} [options.delayMs] artificial per-response latency (workload
 *   generator for benchmarks; 0 in tests)
 * @param {(lastUserText: string) => string} [options.commandFor] shell command
 *   the scripted exec_command tool call runs
 * @param {(lastUserText: string) => string} [options.finalTextFor] final
 *   assistant text after the tool result
 * @returns {Promise<{port: number, baseUrl: string, requests: object[], close(): Promise<void>}>}
 */
export function startCodexResponsesFixture({
  delayMs = 0,
  commandFor = () => 'echo fixture-default > fixture-default.txt',
  finalTextFor = () => 'TURN_DONE',
} = {}) {
  const requests = []
  const server = createServer((request, response) => {
    const chunks = []
    request.setEncoding('utf8')
    request.on('data', (chunk) => { chunks.push(chunk) })
    request.on('end', async () => {
      const parsed = chunks.length > 0 ? JSON.parse(chunks.join('')) : {}
      requests.push(parsed)
      const input = Array.isArray(parsed.input) ? parsed.input : []
      const last = input.at(-1)
      const lastUser = [...input].reverse().find((item) => item?.type === 'message' && item?.role === 'user')
      const lastUserText = (lastUser?.content ?? []).map((part) => part?.text ?? '').join(' ')
      const sse = (event) => { response.write(`data: ${JSON.stringify(event)}\n\n`) }
      response.writeHead(200, { 'content-type': 'text/event-stream' })
      if (delayMs > 0) await new Promise((resolve) => { setTimeout(resolve, delayMs) })
      const output = []
      const seq = requests.length // unique item ids per response
      if (last?.type === 'function_call_output') {
        const text = finalTextFor(lastUserText)
        const message = { type: 'message', id: `msg_${String(seq)}`, role: 'assistant', content: [{ type: 'output_text', text }], status: 'completed' }
        sse({ type: 'response.output_item.added', output_index: 0, item: { type: 'message', id: `msg_${String(seq)}`, role: 'assistant', content: [], status: 'in_progress' } })
        sse({ type: 'response.output_text.delta', item_id: `msg_${String(seq)}`, output_index: 0, content_index: 0, delta: text })
        sse({ type: 'response.output_item.done', output_index: 0, item: message })
        output.push(message)
      } else {
        const argumentsJson = JSON.stringify({ cmd: commandFor(lastUserText), timeout_ms: 120_000 })
        const call = { type: 'function_call', id: `fc_${String(seq)}`, call_id: `call_${String(seq)}`, name: 'exec_command', arguments: argumentsJson, status: 'completed' }
        sse({ type: 'response.output_item.added', output_index: 0, item: { ...call, arguments: '', status: 'in_progress' } })
        sse({ type: 'response.function_call_arguments.delta', item_id: `fc_${String(seq)}`, output_index: 0, delta: argumentsJson })
        sse({ type: 'response.function_call_arguments.done', item_id: `fc_${String(seq)}`, output_index: 0, arguments: argumentsJson })
        sse({ type: 'response.output_item.done', output_index: 0, item: call })
        output.push(call)
      }
      sse({ type: 'response.completed', response: { id: `resp_${String(requests.length)}`, status: 'completed', model: parsed.model, output } })
      response.end()
    })
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      resolve({
        port,
        baseUrl: `http://127.0.0.1:${port}`,
        requests,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}
