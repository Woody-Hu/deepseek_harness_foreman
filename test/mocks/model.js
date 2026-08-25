/**
 * Mock OpenAI-compatible chat-completions server (for keyless tests).
 *
 * Behavior script (talks to dsh's DeepSeek adapter, simulating a realistic
 * multi-step task):
 *  1. New user message (no tool results)  -> bash tool call: write greeting.txt
 *                                 (demonstrates the workspace-change blind
 *                                 spot of the bash path; the bash tool requires
 *                                 a non-empty description parameter)
 *  2. Exactly 1 tool result                -> read README.md (fs-observation-policy
 *                                 requires read-before-edit)
 *  3. Exactly 2 tool results               -> edit README.md: replace one line and
 *                                 write a secret-looking string (demonstrates
 *                                 content-level diff persistence of fs tools +
 *                                 content redaction during packaging/forwarding)
 *  4. Exactly 3 tool results               -> write report.md (a new file: dsh's
 *                                 write yields empty diffs for new files — known
 *                                 behavior; the manifest covers content-level
 *                                 changes)
 *  5. >= 4 tool results                    -> final text, finish_reason=stop
 *  Special case: a user message containing "REPLY_DIRECTLY" -> answer directly
 *  without tools (the session-resume verification round)
 *
 *  Web-test behavior (dispatched by "the last user message containing a
 *  behavior marker" — the web composition injects runtime-context/skill-reminder
 *  user messages after the real user message, and resumed-round history carries
 *  old markers, so only the newest marker survives the injected messages and
 *  new instructions take precedence over historical ones; the tool-result count
 *  of a web turn only counts tool messages "after the last user message" —
 *  resumed-round history carries the previous round's tool results, so a global
 *  count would misjudge):
 *  - contains "ESCALATE_ALLOW"   -> bash with sandbox_permissions=danger-full-access
 *                            + justification writing ../escalation-proof.txt
 *                            (outside the workspace; only possible after an
 *                            approved escalation); answers after the tool result
 *  - contains "ESCALATE_REJECT"  -> same, writing ../rejected-proof.txt (expected
 *                            to be denied by a human; the file must not appear)
 *  - contains "ESCALATE_HANG"    -> same, writing ../hanging-proof.txt (the test
 *                            does not answer and SIGKILLs instead)
 *
 *  Every request body is recorded for assertions (e.g. whether round two's
 *  session carries round one's history = session resume verification).
 */
import { createServer } from 'node:http'

const FINAL_TEXT = 'TASK_COMPLETE: greeting.txt (bash), README.md (edit) and report.md (write) are all done.'
const SECRET_LIKE = 'sk-test-12345'
const REPORT_CONTENT = `# Run Report\n\nwritten by dsh agent.\n\nref: ${SECRET_LIKE}\n`

/** Write one OpenAI SSE stream frame. */
function sseFrame(response, payload) {
  response.write(`data: ${JSON.stringify(payload)}\n\n`)
}

/** Respond with a complete chat.completions stream. */
function respondSse(response, deltas, finishReason) {
  response.writeHead(200, { 'content-type': 'text/event-stream' })
  for (const delta of deltas) sseFrame(response, { choices: [{ delta }] })
  sseFrame(response, {
    choices: [{ delta: {}, finish_reason: finishReason }],
    usage: { prompt_tokens: 10, completion_tokens: 5 },
  })
  response.end('data: [DONE]\n\n')
}

function toolCallDelta(id, name, args) {
  return {
    role: 'assistant',
    content: null,
    tool_calls: [{ index: 0, id, type: 'function', function: { name, arguments: JSON.stringify(args) } }],
  }
}

/**
 * Start the mock model server.
 * @returns {Promise<{server: import('node:http').Server, port: number, requests: object[], close(): Promise<void>}>}
 */
export function startMockModel() {
  const requests = []
  const server = createServer((request, response) => {
    let body = ''
    request.setEncoding('utf8')
    request.on('data', (chunk) => { body += chunk })
    request.on('end', () => {
      const parsed = body.length > 0 ? JSON.parse(body) : {}
      requests.push(parsed)
      const messages = parsed.messages ?? []
      const last = messages[messages.length - 1]
      const toolCount = messages.filter((message) => message.role === 'tool').length
      const userTextOf = (message) => typeof message?.content === 'string'
        ? message.content
        : Array.isArray(message?.content)
          ? message.content.map((block) => block.text ?? '').join(' ')
          : ''
      // The web composition injects extra user-role context (runtime context /
      // skill reminders) after the latest user message, and resumed-round
      // history carries earlier marker messages — the behavior marker must be
      // taken from "the last user message containing a marker" (new
      // instructions take precedence over historical ones)
      const MARKERS = ['ESCALATE_ALLOW', 'ESCALATE_REJECT', 'ESCALATE_HANG', 'REPLY_DIRECTLY', 'WRITE_SECRET', 'CHECKPOINT']
      const lastMarkedUser = [...messages].reverse().find(
        (message) => message.role === 'user' && MARKERS.some((marker) => userTextOf(message).includes(marker)),
      )
      const userText = userTextOf(lastMarkedUser ?? messages.findLast((message) => message.role === 'user'))

      if (userText.includes('REPLY_DIRECTLY')) {
        respondSse(response, [
          { role: 'assistant', content: null },
          { content: `RESUMED_OK: saw ${messages.length} messages in history.` },
        ], 'stop')
        return
      }
      // Web tests: the escalation-approval behavior family. turnToolCount only
      // counts tool results after the latest user message, so resumed rounds
      // (history carrying the previous round's tool results) and fresh rounds
      // share one dispatch.
      const lastUserIndex = messages.findLastIndex((message) => message.role === 'user')
      const turnToolCount = messages.slice(lastUserIndex + 1).filter((message) => message.role === 'tool').length

      // Checkpoint tests: each "CHECKPOINT <n>" round writes
      // turns/turn-<n>.txt (a new file, via fs write) + appends to journal.txt
      // via bash (a modification; bash bypasses fs-observation-policy's
      // read-before-overwrite requirement) — driving the incremental pack
      // chain's evolution
      const checkpointRound = /CHECKPOINT (\d+)/.exec(userText)?.[1]
      if (checkpointRound !== undefined) {
        if (turnToolCount === 0) {
          respondSse(response, [
            toolCallDelta('call_checkpoint_turn', 'write', {
              file_path: `turns/turn-${checkpointRound}.txt`,
              content: `round ${checkpointRound}\n`,
            }),
          ], 'tool_calls')
          return
        }
        if (turnToolCount === 1) {
          respondSse(response, [
            toolCallDelta('call_checkpoint_journal', 'bash', {
              command: `printf 'round ${checkpointRound}\\n' >> journal.txt`,
              description: 'append this round marker to the workspace journal',
            }),
          ], 'tool_calls')
          return
        }
        respondSse(response, [
          { role: 'assistant', content: null },
          { content: `CHECKPOINT_DONE ${checkpointRound}` },
        ], 'stop')
        return
      }

      // Cloud tests: write one clean file + one file containing a
      // secret-looking leak, verifying git-layer secret interception (the
      // leak file stays out of commits) while normal output is committed
      if (userText.includes('WRITE_SECRET')) {
        if (turnToolCount === 0) {
          respondSse(response, [
            toolCallDelta('call_cloud_notes', 'write', {
              file_path: 'notes.md',
              content: '# Notes\n\nclean content written by dsh agent.\n',
            }),
          ], 'tool_calls')
          return
        }
        if (turnToolCount === 1) {
          respondSse(response, [
            toolCallDelta('call_cloud_leak', 'write', {
              file_path: 'leak.txt',
              content: 'credential dump:\nsk-leak-abcdef123456\n',
            }),
          ], 'tool_calls')
          return
        }
        respondSse(response, [
          { role: 'assistant', content: null },
          { content: 'CLOUD_POC_DONE: notes.md committed, leak.txt intercepted by foreman git scan.' },
        ], 'stop')
        return
      }
      const escalation = userText.includes('ESCALATE_ALLOW')
        ? { marker: 'ESCALATE_ALLOW', file: '../escalation-proof.txt' }
        : userText.includes('ESCALATE_REJECT')
          ? { marker: 'ESCALATE_REJECT', file: '../rejected-proof.txt' }
          : userText.includes('ESCALATE_HANG')
            ? { marker: 'ESCALATE_HANG', file: '../hanging-proof.txt' }
            : undefined
      if (escalation !== undefined) {
        if (turnToolCount === 0) {
          respondSse(response, [
            toolCallDelta(`call_${escalation.marker.toLowerCase()}`, 'bash', {
              command: `printf 'escalated write\\n' > ${escalation.file}`,
              description: 'write outside the workspace (needs sandbox escalation)',
              sandbox_permissions: 'danger-full-access',
              justification: `the task requires writing ${escalation.file} outside the workspace sandbox.`,
            }),
          ], 'tool_calls')
          return
        }
        respondSse(response, [
          { role: 'assistant', content: null },
          { content: `TASK_COMPLETE_ESCALATION: attempted ${escalation.file}.` },
        ], 'stop')
        return
      }
      if (last?.role === 'tool' && toolCount >= 4) {
        respondSse(response, [
          { role: 'assistant', content: null },
          { content: FINAL_TEXT },
        ], 'stop')
        return
      }
      if (last?.role === 'tool' && toolCount === 3) {
        respondSse(response, [
          toolCallDelta('call_foreman_write', 'write', { file_path: 'report.md', content: REPORT_CONTENT }),
        ], 'tool_calls')
        return
      }
      if (last?.role === 'tool' && toolCount === 2) {
        respondSse(response, [
          toolCallDelta('call_foreman_edit', 'edit', {
            file_path: 'README.md',
            old_string: 'restored from object storage.',
            new_string: `edited by dsh agent. ref: ${SECRET_LIKE}`,
          }),
        ], 'tool_calls')
        return
      }
      if (last?.role === 'tool' && toolCount === 1) {
        respondSse(response, [
          toolCallDelta('call_foreman_read', 'read', { file_path: 'README.md' }),
        ], 'tool_calls')
        return
      }
      respondSse(response, [
        toolCallDelta('call_foreman_bash', 'bash', {
          command: "printf 'hello from dsh\\n' > greeting.txt",
          description: 'write greeting file into workspace',
        }),
      ], 'tool_calls')
    })
  })
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      resolve({
        server,
        port,
        requests,
        close: () => new Promise((done) => { server.close(() => { done() }) }),
      })
    })
  })
}
