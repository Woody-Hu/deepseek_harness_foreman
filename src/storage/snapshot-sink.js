/**
 * SnapshotSink — storage abstraction for workspace snapshots.
 *
 * Purpose: snapshot retrieval and storage depend on the cloud system; access
 * keys / URLs are injected dynamically. The interface converges to two
 * methods (put/get); the implementation is chosen by configuration and
 * credentials are resolved from env **on every call** (the cloud can inject
 * or rotate them per task; foreman captures nothing at construction time and
 * writes nothing to disk).
 *
 *   kind: 'local'        local directory (development/debug: artifacts land
 *                        at dir/<bucket>/<key>)
 *   kind: 'object-store' HTTP object storage (S3-compatible PUT/GET;
 *                        endpoint/bucket/prefix from configuration, token from env)
 *
 * Production integration: the object-store implementation demonstrates the
 * credential and URL wiring contract (Bearer header + endpoint concatenation
 * + per-call env resolution); a real cloud can replace it with an AWS SDK
 * signed version behind the same interface. Missing credentials throw on
 * first use (fail loud: dynamic injection means the value may appear later
 * than construction).
 */
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

/**
 * @param {object} config
 * @param {'local'|'object-store'} config.kind
 * @param {string} [config.dir]      local: storage directory
 * @param {string} [config.endpoint] object-store: e.g. https://obs.internal
 * @param {string} [config.bucket]   object-store: bucket (agentId recommended)
 * @param {string} [config.prefix]   object-store: key prefix (sessionId/ recommended)
 * @param {string} [config.tokenEnv] object-store: env name of the Bearer token
 *                                   (default FOREMAN_SNAPSHOT_TOKEN; resolved per call)
 * @returns {{kind, put(key: string, buffer: Buffer): Promise<{url: string}>, get(key: string): Promise<Buffer>}}
 */
export function createSnapshotSink(config) {
  if (config.kind === 'local') {
    const { dir, bucket = 'default' } = config
    if (typeof dir !== 'string') throw new Error('snapshot-sink(local): dir is required')
    return {
      kind: 'local',
      async put(key, buffer) {
        const file = join(dir, bucket, key)
        await mkdir(dirname(file), { recursive: true })
        await writeFile(file, buffer)
        return { url: file }
      },
      async get(key) {
        return await readFile(join(dir, bucket, key))
      },
    }
  }
  if (config.kind === 'object-store') {
    const { endpoint, bucket, prefix = '', tokenEnv = 'FOREMAN_SNAPSHOT_TOKEN' } = config
    if (typeof endpoint !== 'string' || typeof bucket !== 'string') {
      throw new Error('snapshot-sink(object-store): endpoint and bucket are required')
    }
    const objectUrl = (key) => `${endpoint.replace(/\/$/, '')}/${bucket}/${prefix}${key}`
    return {
      kind: 'object-store',
      async put(key, buffer) {
        const response = await fetch(objectUrl(key), {
          method: 'PUT',
          headers: { 'content-type': 'application/octet-stream', authorization: `Bearer ${resolveToken(tokenEnv)}` },
          body: buffer,
        })
        if (!response.ok) throw new Error(`snapshot put failed: ${key} -> ${response.status}`)
        return { url: objectUrl(key) }
      },
      async get(key) {
        const response = await fetch(objectUrl(key), {
          headers: { authorization: `Bearer ${resolveToken(tokenEnv)}` },
        })
        if (!response.ok) throw new Error(`snapshot get failed: ${key} -> ${response.status}`)
        return Buffer.from(await response.arrayBuffer())
      },
    }
  }
  throw new Error(`snapshot-sink: unknown kind ${String(config.kind)} (local | object-store)`)
}

/** Resolve the credential from env per call; throw when missing (under the dynamic-injection contract, appearing later than construction is legal). */
function resolveToken(tokenEnv) {
  const token = process.env[tokenEnv]
  if (token === undefined || token.length === 0) {
    throw new Error(`snapshot-sink: env ${tokenEnv} is not set (dynamic credential missing)`)
  }
  return token
}
