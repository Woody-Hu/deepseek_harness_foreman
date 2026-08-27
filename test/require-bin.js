/**
 * Fail-loud binary prerequisite for e2e scenarios and benchmarks.
 *
 * A missing harness binary is a hard error (exit 1), never a silent skip:
 * a skipped e2e run reports success while verifying nothing — that is the
 * "cheat" this guard exists to prevent. The harness distributions are plain
 * npm installs (ADR-0012; see README "Prerequisites") — there is no
 * environment in which running the real thing is not possible.
 */
import { execFile } from 'node:child_process'

/**
 * @param {string} binary the binary name resolved from PATH
 * @param {string[]} [args] arguments that make the binary exit fast (e.g. ['--version'])
 * @param {string} installHint the npm install command line for the failure message
 * @returns {Promise<void>} resolves when the binary runs; exits the process loudly otherwise
 */
export async function requireBinary(binary, args = [], installHint = '') {
  const ok = await new Promise((resolve) => {
    execFile(binary, args, (error) => { resolve(error?.code !== 'ENOENT') })
  })
  if (!ok) {
    console.error(`FATAL: '${binary}' not found on PATH — this scenario requires the real harness binary.`)
    if (installHint) console.error(`       ${installHint}`)
    process.exit(1)
  }
}
