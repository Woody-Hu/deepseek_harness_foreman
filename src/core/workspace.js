/**
 * Workspace utilities: manifest baseline/diff (authoritative change set),
 * fs-tool content diff extraction, packaging (exclusion + masking) and
 * archive extraction/creation.
 */
import { createHash } from 'node:crypto'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { createReadStream } from 'node:fs'
import { mkdir, readdir, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { isSecretOrExcludedPath, redactFileBuffer } from './redact.js'

const execFileAsync = promisify(execFile)

/** Recursively collect relative file paths under a directory. */
async function walkFiles(dir, prefix = '') {
  const out = []
  const entries = await readdir(dir, { withFileTypes: true })
  for (const entry of entries) {
    const rel = prefix === '' ? entry.name : `${prefix}/${entry.name}`
    if (entry.isDirectory()) out.push(...await walkFiles(join(dir, entry.name), rel))
    else if (entry.isFile()) out.push(rel)
  }
  return out
}

/** Hash a single file (streaming sha256). */
async function hashFile(path) {
  return await new Promise((resolve, reject) => {
    const hash = createHash('sha256')
    createReadStream(path)
      .on('error', reject)
      .on('data', (chunk) => { hash.update(chunk) })
      .on('end', () => { resolve(hash.digest('hex')) })
  })
}

/**
 * Build a full manifest: relPath -> { size, mtimeMs, sha256 }.
 * Both the baseline and the final state of the authoritative change set come
 * from this function, independent of the exact paths an agent touched.
 * @param {string} dir
 * @returns {Promise<Map<string, {size: number, mtimeMs: number, sha256: string}>>}
 */
export async function fileManifest(dir) {
  const manifest = new Map()
  for (const rel of await walkFiles(dir)) {
    const info = await stat(join(dir, rel))
    manifest.set(rel, {
      size: info.size,
      mtimeMs: Math.floor(info.mtimeMs),
      sha256: await hashFile(join(dir, rel)),
    })
  }
  return manifest
}

/**
 * Diff two manifests: added / removed / modified (sha256 decides).
 * @returns {{ added: string[], modified: string[], removed: string[] }}
 */
export function diffManifests(before, after) {
  const added = []
  const modified = []
  const removed = []
  for (const [rel, info] of after) {
    const prev = before.get(rel)
    if (prev === undefined) added.push(rel)
    else if (prev.sha256 !== info.sha256) modified.push(rel)
  }
  for (const rel of before.keys()) {
    if (!after.has(rel)) removed.push(rel)
  }
  added.sort(); modified.sort(); removed.sort()
  return { added, modified, removed }
}

/**
 * Extract content-level diffs of fs tools from the session event stream
 * (tool/result.meta.diffs).
 * @param {Array<{type: string, data: any}>} events the event list from session.event notifications
 * @returns {{ diffs: Array<{path: string, oldText: string|null, newText: string}>, toolCalls: Array<{name: string, arguments: any}> }}
 */
export function changesFromSessionEvents(events) {
  const diffs = []
  const toolCalls = []
  for (const event of events) {
    if (event.type === 'tool/call') {
      toolCalls.push({ name: event.data.name, arguments: event.data.arguments })
    }
    if (event.type === 'tool/result') {
      const meta = event.data?.meta
      if (meta !== null && typeof meta === 'object' && Array.isArray(meta.diffs)) {
        for (const diff of meta.diffs) {
          diffs.push({
            path: diff.path,
            oldText: diff.oldText ?? null,
            newText: diff.newText ?? '',
          })
        }
      }
    }
  }
  return { diffs, toolCalls }
}

/**
 * Package a workspace: exclude secret/irrelevant paths, mask textual content,
 * then tar.gz the staging directory.
 * @param {string} workspaceDir
 * @param {string} outPath
 * @param {{ secretValues?: string[] }} [options]
 * @returns {Promise<{ path: string, fileCount: number, excluded: string[], masked: string[] }>}
 */
export async function packageWorkspace(workspaceDir, outPath, { secretValues = [] } = {}) {
  const staging = `${outPath}.staging`
  await rm(staging, { recursive: true, force: true })
  await mkdir(staging, { recursive: true })
  const excluded = []
  const masked = []
  let fileCount = 0
  for (const rel of await walkFiles(workspaceDir)) {
    if (isSecretOrExcludedPath(rel)) { excluded.push(rel); continue }
    const source = join(workspaceDir, rel)
    const target = join(staging, rel)
    await mkdir(dirname(target), { recursive: true })
    const raw = await readFile(source)
    const { buffer, masked: didMask } = redactFileBuffer(raw, secretValues)
    if (didMask) masked.push(rel)
    await writeFile(target, buffer)
    fileCount += 1
  }
  await execFileAsync('tar', ['-czf', outPath, '-C', staging, '.'])
  await rm(staging, { recursive: true, force: true })
  return { path: outPath, fileCount, excluded, masked }
}

/** Extract a tar.gz archive into a destination directory (object-store restore path). */
export async function extractArchive(archivePath, destDir) {
  await mkdir(destDir, { recursive: true })
  await execFileAsync('tar', ['-xzf', archivePath, '-C', destDir])
}

/** Archive a directory into tar.gz (no redaction; for trusted data such as seeds and session logs). */
export async function archiveDirectory(dir, outPath) {
  await execFileAsync('tar', ['-czf', outPath, '-C', dir, '.'])
  return outPath
}
