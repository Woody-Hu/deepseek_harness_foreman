/**
 * Harness entry resolution (ADR-0011): channels launch harnesses from their
 * published distribution packages, resolved out of the runner's own
 * dependency closure — never from a source checkout.
 *
 * Resolution base: this module's location (createRequire is symlink-aware),
 * so both npm-flat and pnpm-isolated layouts work (the parent-walk from a
 * package's real location reaches the hoisted store). Spawn is always
 * `process.execPath <entry.js> …` — no shebang/PATH/exec-bit dependency.
 *
 * A `binary` override (config file or constructor) bypasses resolution for
 * deployments that install harnesses outside the runner's closure.
 */
import { createRequire } from 'node:module'
import { isAbsolute, join } from 'node:path'

const require = createRequire(import.meta.url)

/**
 * Resolve a file inside an installed harness package from the runner's
 * dependency closure.
 * @param {string} packageId e.g. '@deepseek-ai/dsh-sdk-jsonrpc-demo'
 * @param {string} entryPath package-relative entry — an export subpath when the
 *   package restricts exports (e.g. 'packaged-bin'), else a file path (e.g. 'lib/bin.js')
 * @param {object} [override] optional explicit path (config `binary`-style):
 *   absolute used as-is, relative resolved against cwd
 * @returns {string} absolute entry path (spawnable via process.execPath)
 * @throws {Error} package/entry unresolvable (dependency missing — fail loud,
 *   naming the package and the install expectation)
 */
export function resolveHarnessEntry(packageId, entryPath, override) {
  if (override !== undefined && override !== '') {
    return isAbsolute(override) ? override : join(process.cwd(), override)
  }
  const target = `${packageId}/${entryPath.replace(/^\.?\//, '')}`
  try {
    return require.resolve(target)
  } catch {
    throw new Error(
      `harness-resolution: cannot resolve '${target}' from the runner install — `
      + `the harness package is missing. Install it as a runner dependency `
      + `(e.g. pnpm add ${packageId}) or override the channel's binary option `
      + `with an explicit path.`,
    )
  }
}

/**
 * Resolve an installed harness package's root directory (for package-layout
 * entries not exposed through package.json exports).
 * @param {string} packageId e.g. '@deepseek-ai/dsh'
 * @param {object} [override] explicit package root override
 * @returns {string} absolute package directory
 */
export function resolvePackageDir(packageId, override) {
  if (override !== undefined && override !== '') {
    return isAbsolute(override) ? override : join(process.cwd(), override)
  }
  try {
    return require.resolve(`${packageId}/package.json`).replace(/[/\\]package\.json$/, '')
  } catch {
    throw new Error(
      `harness-resolution: cannot resolve package '${packageId}' from the runner install — `
      + `the harness package is missing. Install it as a runner dependency `
      + `(e.g. pnpm add ${packageId}) or override the channel's binary option.`,
    )
  }
}
