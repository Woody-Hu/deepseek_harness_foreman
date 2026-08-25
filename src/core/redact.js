/**
 * Secret redaction utilities — the three defensive layers of the cloud runner.
 *
 *  1. Path exclusion (workspace packaging): `.env*`, credential files, key
 *     material, `.git` and other non-essential directories never enter an
 *     uploaded archive;
 *  2. Content masking (workspace packaging): known secret values (env-injected
 *     keys that are never written to disk) plus generic key-shaped patterns;
 *  3. Event-stream redaction (gateway forwarding): deep JSON traversal that
 *     applies the same masking to every string value.
 */

/** Basenames that must be excluded from packaged archives. */
export const SECRET_FILE_NAMES = new Set([
  '.env',
  '.credentials.yaml',
  '.credentials.yml',
  '.npmrc',
  '.netrc',
  'id_rsa',
])
export const SECRET_FILE_PREFIXES = ['.env.']
export const SECRET_FILE_SUFFIXES = ['.pem', '.key', '.p12', '.pfx', '.keystore']
export const EXCLUDED_DIR_NAMES = new Set(['.git', 'node_modules', '.sessions'])

/**
 * Whether a relative path must be excluded from packaging (segment-wise check).
 * @param {string} relativePath
 * @returns {boolean}
 */
export function isSecretOrExcludedPath(relativePath) {
  const segments = relativePath.split('/')
  for (const segment of segments) {
    if (EXCLUDED_DIR_NAMES.has(segment)) return true
    if (SECRET_FILE_NAMES.has(segment)) return true
    if (SECRET_FILE_PREFIXES.some((prefix) => segment.startsWith(prefix))) return true
    if (SECRET_FILE_SUFFIXES.some((suffix) => segment.endsWith(suffix))) return true
  }
  return false
}

/** Generic key-shaped patterns (demo-grade subset; production should plug in a dedicated secret scanner). */
const SECRET_PATTERNS = [
  /sk-[A-Za-z0-9_-]{8,}/g,        // LLM API key shape
  /AKIA[0-9A-Z]{16}/g,            // AWS access key
  /ghp_[A-Za-z0-9]{20,}/g,        // GitHub PAT
  /eyJ[A-Za-z0-9_-]{20,}\./g,     // JWT prefix
]

const MASK = '[REDACTED]'

/**
 * Mask text: known secret exact values first, then generic patterns.
 * @param {string} text
 * @param {string[]} secretValues Known secret values (e.g. env-injected keys)
 * @returns {string}
 */
export function redactText(text, secretValues = []) {
  let output = text
  for (const value of secretValues) {
    if (value.length === 0) continue
    while (output.includes(value)) output = output.replaceAll(value, MASK)
  }
  for (const pattern of SECRET_PATTERNS) output = output.replaceAll(pattern, MASK)
  return output
}

/** Whether a buffer should be treated as text (demo-grade: UTF-8 decodable and not oversized). */
function looksTextual(buffer) {
  if (buffer.length > 2 * 1024 * 1024) return false
  const text = buffer.toString('utf8')
  // Simple heuristic: a high ratio of U+FFFD replacement characters means binary.
  const replacementCount = (text.match(/\uFFFD/g) ?? []).length
  return replacementCount / Math.max(text.length, 1) < 0.01
}

/**
 * Mask file content (text is masked, binary passes through unchanged).
 * @returns {{ buffer: Buffer, masked: boolean }}
 */
export function redactFileBuffer(buffer, secretValues) {
  if (!looksTextual(buffer)) return { buffer, masked: false }
  const text = buffer.toString('utf8')
  const output = redactText(text, secretValues)
  if (output === text) return { buffer, masked: false }
  return { buffer: Buffer.from(output, 'utf8'), masked: true }
}

/**
 * Deep-traverse a JSON value and mask every string (used before trace forwarding).
 * @param {unknown} value
 * @returns {unknown}
 */
export function redactJson(value, secretValues) {
  if (typeof value === 'string') return redactText(value, secretValues)
  if (Array.isArray(value)) return value.map((item) => redactJson(item, secretValues))
  if (value !== null && typeof value === 'object') {
    const out = {}
    for (const [key, item] of Object.entries(value)) out[key] = redactJson(item, secretValues)
    return out
  }
  return value
}
