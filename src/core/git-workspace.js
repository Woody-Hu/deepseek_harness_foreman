/**
 * GitWorkspace — local git tracking of the sandbox workspace.
 *
 * Rationale: dsh itself carries no workspace trace, so foreman introduces a
 * purely local git repository inside the workspace — commits capture each
 * turn's outputs and file changes, and a pre-commit scan intercepts secrets
 * (never uploaded, never recorded in git history).
 *
 * Lifecycle (wired by foreman):
 *   after prepare()  ensureRepo() + baseline commit (restored workspace = "before" state)
 *   at collect()     commitTurn() (this turn's changes = "after" state; secrets intercepted)
 *   change set       changedSinceBaseline() (git diff is authoritative; manifest diff is the fallback)
 *
 * Secret interception semantics: staged content is scanned before committing —
 * known secret values (exact substrings of env-injected secretValues) and
 * secret shapes (secretPatterns regexes, e.g. key-shaped strings). Matching
 * files are unstaged (git reset): excluded from this commit, from git history
 * and from any archive that ships `.git`. Violations (file + rules) are
 * returned to the runner for reporting. Interception never fails the whole
 * turn — remaining clean files commit as usual.
 *
 * Dependency: the git binary must exist in the sandbox image (deployment requirement).
 */
import { execFile } from 'node:child_process'
import { open } from 'node:fs/promises'
import { join } from 'node:path'

/** Default secret shapes: OpenAI/DeepSeek-style keys, AWS-style access keys. */
export const DEFAULT_SECRET_PATTERNS = [
  { name: 'openai-key', pattern: /sk-[A-Za-z0-9_-]{12,}/ },
  { name: 'aws-access-key', pattern: /AKIA[0-9A-Z]{16}/ },
]

/** Chunk carry-over (chars) for the streaming secret scan — exceeds every pattern's match length. */
const SCAN_OVERLAP = 4096

/**
 * @param {object} options
 * @param {string} options.cwd workspace directory (git repository root)
 * @param {string[]} [options.secretValues] known secret values (exact substring match; same source as env injection)
 * @param {Array<{name: string, pattern: RegExp}>} [options.secretPatterns] secret shapes
 * @param {number} [options.maxScanBytes] streaming secret-scan chunk size (files of any size are scanned in overlapping chunks)
 * @param {string} [options.branch] initial branch name (default 'main')
 * @param {{name: string, email: string}} [options.identity] local committer identity (default 'Foreman <foreman@localhost>')
 */
export class GitWorkspace {
  constructor(options) {
    // undefined values must not override the defaults (callers pass sparse option objects)
    const provided = Object.fromEntries(Object.entries(options).filter(([, value]) => value !== undefined))
    this.options = {
      maxScanBytes: 2_000_000,
      branch: 'main',
      identity: { name: 'foreman', email: 'foreman@localhost' },
      ...provided,
      secretValues: options.secretValues ?? [],
      secretPatterns: options.secretPatterns ?? DEFAULT_SECRET_PATTERNS,
    }
    if (typeof options.cwd !== 'string') throw new Error('git-workspace: cwd is required')
    this.baselineOid = undefined
    this.violations = []
    this.commits = []
  }

  git(args) {
    return new Promise((resolve, reject) => {
      execFile('git', args, { cwd: this.options.cwd, maxBuffer: 20 * 1024 * 1024 }, (error, stdout, stderr) => {
        if (error !== null) reject(new Error(`git ${args.join(' ')}: ${stderr || error.message}`))
        else resolve(stdout)
      })
    })
  }

  /** Same as git(), but stdout is returned as a Buffer (for binary file content, avoiding utf8 corruption). */
  gitRaw(args) {
    return new Promise((resolve, reject) => {
      execFile('git', args, { cwd: this.options.cwd, maxBuffer: 20 * 1024 * 1024, encoding: 'buffer' }, (error, stdout, stderr) => {
        if (error !== null) reject(new Error(`git ${args.join(' ')}: ${stderr || error.message}`))
        else resolve(stdout)
      })
    })
  }

  /** List every file path under the given commit tree (change source for full checkpoint packs). */
  async lsTree(ref) {
    const out = await this.git(['ls-tree', '-r', '--name-only', ref])
    return out.split('\n').filter((line) => line.length > 0)
  }

  /**
   * File-level changes between two commits (--no-renames: a rename splits into D+A for exact change-set semantics).
   * @returns {Promise<Array<{status: 'A'|'M'|'D', path: string}>>}
   */
  async diffFiles(fromRef, toRef) {
    const out = await this.git(['diff', '--name-status', '--no-renames', '--diff-filter=ADMR', fromRef, toRef])
    return out.split('\n').filter((line) => line.length > 0).map((line) => {
      const [status, ...paths] = line.split('\t')
      return { status: status[0], path: paths[0] }
    })
  }

  /** Read a file's content (Buffer) from a commit; null when the file is absent from that commit. */
  async readFileAt(ref, path) {
    try {
      return await this.gitRaw(['show', `${ref}:${path}`])
    } catch {
      return null
    }
  }

  /** Initialize the repository (idempotent): git init -b <branch> + local identity (commits work without global git config). */
  async ensureRepo() {
    await this.git(['init', '-q', '-b', this.options.branch])
    await this.git(['config', 'user.email', this.options.identity.email])
    await this.git(['config', 'user.name', this.options.identity.name])
    await this.git(['config', 'commit.gpgsign', 'false'])
  }

  /** Current HEAD commit oid ('' when no commit exists yet). */
  async headOid() {
    return (await this.git(['rev-parse', 'HEAD']).catch(() => '')).trim()
  }

  /**
   * Commit all current changes (after secret interception). Returns committed:false when there is nothing to commit.
   * @returns {Promise<{committed: boolean, oid?: string, files: string[], violations: Array<{file: string, rules: string[]}>}>}
   */
  async commitAll(message) {
    await this.git(['add', '-A'])
    const violations = await this.scanStaged()
    for (const violation of violations) {
      await this.git(['reset', '-q', '--', violation.file]) // unstage: never enters history, never uploaded
    }
    this.violations.push(...violations)
    const staged = await this.stagedFiles()
    if (staged.length === 0) return { committed: false, files: [], violations }
    await this.git(['commit', '-q', '-m', message, '--no-verify'])
    const oid = (await this.git(['rev-parse', 'HEAD'])).trim() // -q suppresses stdout; oid comes from rev-parse
    this.commits.push({ oid, message, files: staged })
    return { committed: true, oid, files: staged, violations }
  }

  /** Baseline commit (the restored workspace's "before" state). */
  async commitBaseline() {
    const result = await this.commitAll('foreman: baseline (restored workspace)')
    this.baselineOid = result.oid
    return result
  }

  /** Turn commit (at collect time). */
  async commitTurn(label) {
    return await this.commitAll(`foreman: turn ${label}`)
  }

  /** Staged file list (relative paths). */
  async stagedFiles() {
    const out = await this.git(['diff', '--cached', '--name-only', '--diff-filter=ACMR'])
    return out.split('\n').filter((line) => line.length > 0)
  }

  /**
   * Scan staged content: known exact values + shape regexes; hits are
   * aggregated per file (one violation entry per file). Files larger than
   * maxScanBytes are scanned in overlapping chunks — a secret spanning a
   * chunk boundary is still caught, and large files are never silently
   * dropped from history (an unstaged file would vanish from every
   * checkpoint pack and from restores).
   */
  async scanStaged() {
    const violations = []
    for (const file of await this.stagedFiles()) {
      const rules = await this.#scanFile(file)
      if (rules !== null && rules.length > 0) violations.push({ file, rules })
    }
    return violations
  }

  /**
   * Scan one staged file in maxScanBytes chunks with SCAN_OVERLAP bytes of
   * carry-over (patterns are ASCII-shaped, so a partial multi-byte character
   * at a chunk edge cannot split a match). Returns null when the file is
   * unreadable (deleted paths are skipped); the rule list is deduplicated.
   */
  async #scanFile(file) {
    const handle = await open(join(this.options.cwd, file)).catch(() => null)
    if (handle === null) return null
    const rules = new Set()
    const buffer = Buffer.allocUnsafe(this.options.maxScanBytes)
    let carry = ''
    try {
      for (;;) {
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, null)
        if (bytesRead === 0) break
        const text = carry + buffer.subarray(0, bytesRead).toString('utf8')
        for (const value of this.options.secretValues) {
          if (value.length > 0 && text.includes(value)) rules.add('known-secret')
        }
        for (const { name, pattern } of this.options.secretPatterns) {
          if (pattern.test(text)) rules.add(`pattern:${name}`)
        }
        carry = text.slice(Math.max(0, text.length - SCAN_OVERLAP))
      }
    } finally {
      await handle.close()
    }
    return [...rules]
  }

  /** Changes since the baseline (authoritative change set): [{path, status}] (A/M/D/R). */
  async changedSinceBaseline() {
    if (this.baselineOid === undefined) return []
    const out = await this.git(['diff', '--name-status', this.baselineOid, 'HEAD'])
    return out.split('\n').filter((line) => line.length > 0).map((line) => {
      const [status, ...paths] = line.split('\t')
      return { status, path: paths.at(-1) }
    })
  }

  /** Uncommitted residue (e.g. intercepted secret files). */
  async uncommitted() {
    const out = await this.git(['status', '--porcelain'])
    return out.split('\n').filter((line) => line.length > 0)
  }
}
