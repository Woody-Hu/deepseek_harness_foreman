# ADR-0011: Distribution-package harness launch and config-only channel switching

**Status:** Accepted
**Date:** 2026-08-26

## Context

The three integration channels do not launch their harnesses the same way:

| Channel | Launch mechanism today | Source dependence |
|---|---|---|
| `dsh-sdk` | `node --import tsx <repoRoot>/packages/examples/jsonrpc-demo/src/bin.ts <cordis.yml>` | **dsh source checkout** (repoRoot + tsx + workspace packages) |
| `dsh-web` | `node --import tsx <repoRoot>/apps/cli/src/bin.ts web --patch …` | **dsh source checkout** (repoRoot + tsx + workspace packages) |
| `codex` | `codex app-server --stdio` (binary resolved from PATH/config) | none — installed package |

A source checkout is a build artifact of the harness project, not a deployment
unit: it requires the repo at a pinned revision, its toolchain (tsx), and a
workspace install. The acceptance criterion for the generalization mechanism is
the opposite: **both harnesses start by changing only the runner config**, from
their published distribution packages — the same units a deployment installs.
(Separately: the e2e tests resolved `repoRoot` four levels above the test file,
which already broke when the project moved out of the source tree — a
portability bug of the same root cause.)

Verified distribution facts (registry, live-booted in this sandbox):

1. `@deepseek-ai/dsh` (0.1.1-rc.2 line, dist-tag `next`) ships the `dsh` CLI.
   `dsh web --patch <file> --no-open` boots the web profile — a directory
   auto-initialized under `$DSH_HOME/profiles/web` from the shipped template
   (`dsh-base` + `dsh-web-app` bundles) — and prints the readiness line
   `dsh web: http://127.0.0.1:<port>` on stdout. The full HTTP/WS apiproxy
   (unary methods, mux downlink, HITL, persisted sessions) ships in the
   distribution.
2. `@deepseek-ai/dsh-sdk-jsonrpc-demo` (same line) ships
   `lib/packaged-bin.js` — a closed-runtime JSON-RPC agent bin:
   **bare plugin names resolve from the installed runtime closure** while
   relative names (`./plugins/*`) stay config-relative. Booted with foreman's
   own `cordis.yml` + adapter plugins, the `initialize` handshake completes
   (`serverInfo: deepseek-harness-sdk-runtime`). The composition's bare
   plugin packages (`dsh-sdk-jsonrpc-server`, `dsh-llm-deepseek`,
   `dsh-agent-spine-demo`, …) are all published on the same line; their
   resolution base is whatever node_modules the demo package is installed in.
3. `@openai/codex` (0.149.1) ships the `codex` binary — the channel already
   drives it; it becomes an explicit dependency instead of a PATH assumption.

## Options considered

1. **Keep the source launch, add a "dist mode" flag.** Two launch paths per
   channel, both must be tested; the flag would outlive the migration. Rejected.
2. **Point the channels at a harness install root (`harnessRoot` option),
   spawning `.bin` shims.** PATH/shim dependent (exec bits, `env node`),
   fails loud only at spawn time, and pnpm's isolated `.bin` shims hide the
   resolution base for bare plugins. Rejected as the primary mechanism.
3. **Resolve entry modules from the runner's own dependency closure
   (chosen).** Foreman declares the harness packages as dependencies
   (the deployment installs runner + harnesses together), and each channel
   resolves its entry module via `createRequire(foremanModule)` — the standard
   Node resolution, symlink-aware, layout-agnostic (npm flat or pnpm isolated
   both work: parent-walk reaches the hoisted store). Spawn is always
   `process.execPath <entry.js> …` — no shebang/PATH dependency. An explicit
   `binary`/entry override stays available per channel for deployments that
   install harnesses elsewhere.

## Decision

### Launch resolution

- New module `src/harness-resolution.js` (foreman-owned):
  `resolveHarnessEntry(packageId, entryExportPath)` resolves an entry script
  inside an installed harness package from foreman's own module location via
  `createRequire(import.meta.url).resolve('<pkg>/<export>')`, and
  `resolvePackageDir(packageId)` (via `resolve('<pkg>/package.json')`) for
  package-root-relative entries. Failures are loud and actionable (name the
  package, the declaring config key, and the install expectation).
- `dsh-sdk` channel spawns `process.execPath [resolveHarnessEntry(
  '@deepseek-ai/dsh-sdk-jsonrpc-demo', 'lib/packaged-bin.js'), configPath]`
  with `cwd` = the config project directory. Bare composition plugins resolve
  from the installed closure (packaged-bin semantics); `./plugins/*` adapter
  plugins resolve relative to the config — exactly the old source-launch
  layout, so `cordis.yml` and the foreman adapter plugins are unchanged.
- `dsh-web` channel spawns `process.execPath [resolvePackageDir(
  '@deepseek-ai/dsh') + '/lib/bin.js', 'web', '--patch', patchPath,
  '--no-open']` (the CLI's own bin entry; `web` is the profile alias). The
  profile auto-initializes under `$DSH_HOME/profiles/web` as before; the
  `repoRoot` option disappears from both channels.
- `codex` channel unchanged (binary first, config/constructor override); the
  `codex` binary becomes a declared dependency of the runner so the default
  PATH resolution is satisfiable from the same install.

### Config surface (ADR-0002 extension)

`foreman.config.json` → `harness` gains two channel sections, mirroring the
existing `harness.codex`:

```jsonc
{
  "harness": {
    "channel": "dsh-sdk" | "dsh-web" | "codex",
    "dshSdk": { "binary": "<path>", "provider": "<id>", "model": "<name>" },
    "dshWeb": { "binary": "<path>" },
    "codex":  { "binary": "<path>", "args": [], "model": "<name>",
                "provider": { "name", "baseUrl", "envKey" },
                "approvalPolicy": "<policy>", "sandbox": "<mode>", "timeoutMs": 60000 }
  }
}
```

- `binary` overrides the resolved distribution entry (absolute or relative
  path; relative resolves against the runner's cwd) — deployments with a
  separately installed harness keep working without code changes.
- Precedence (consistent with ADR-0002): constructor option > config file >
  resolved default. Validation is unknown-key-fail-loud like the rest of the
  schema. Secrets stay out of the config file by design (env injection only).
- `foreman.js` selects and constructs the channel through a single factory
  (`src/channels/factory.js`, ADR-0013 extracts it): channel id → class +
  merged options. Adding a channel no longer means editing `start()`'s inline
  branches.

### Acceptance (the user-visible criterion)

A new e2e scenario (`test/e2e/config-switch.e2e.js`): two `Foreman` instances
built from **identical constructor options except `configPath`**; the two
config files differ only inside `harness` (`channel` + that channel's section).
Both must complete a full run (prepare → start → prompt → collect → publish)
against their real harness: `dsh-sdk` from the installed distribution with the
mock DeepSeek endpoint, `codex` from the installed binary with the scripted
Responses endpoint (model endpoints are env/constructor-injected secrets —
never config). No source checkout of either harness exists anywhere in the
test environment.

## Consequences

- The `repoRoot` option disappears from `SdkChannel`/`WebChannel` and from all
  e2e tests; the runner's harness surface is exactly its declared dependencies
  (package.json), resolved at runtime. Adaptation depends on published
  packages only — the harness source may be consulted for analysis but is
  never a runtime input.
- Version pinning moves to package.json (deployments choose the harness
  version by the installed dependency range; the 0.1.1-rc.x `next` line is the
  first complete published set).
- The pnpm-isolated layout works because parent-walk from a package's real
  location reaches the hoisted store; npm flat layouts work trivially. A
  custom install root overrides per channel via `binary`.
- One more moving part at spawn time (entry resolution) with a clear failure
  mode: unresolvable entry = dependency missing = fail loud at `start()` with
  the declaring config key named.
- The codex channel's PATH lookup remains as a fallback for existing
  deployments (ADR-0005 unchanged); the declared dependency only guarantees
  availability.
