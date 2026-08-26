# ADR-0012: dsh channels launch from the npm distribution

**Status:** Accepted
**Date:** 2026-08-27

## Context

ADR-0001/0002 established the channel abstraction and ADR-0005 added the codex
channel as a distribution-launched harness (the `codex` binary on PATH). The
two dsh channels, however, still launch the harness **from a source checkout**:

- `dsh-sdk`: `node --import tsx <dshRepo>/packages/examples/jsonrpc-demo/src/bin.ts <cordis.yml>`
- `dsh-web`: `node --import tsx <dshRepo>/apps/cli/src/bin.ts web --patch ... --no-open`

This carries a hard runtime dependency on the dsh repository: the `repoRoot`
constructor option, a repo build (`pnpm run build`) before any e2e can run, and
e2e files that assume foreman lives *inside* the dsh checkout
(`<repoRoot>/foreman/solution/...`). In a sandbox without the checkout every
dsh scenario is unrunnable — while the same harnesses are officially
distributed as npm packages.

Distribution research (verified empirically, 2026-08-27):

- `@deepseek-ai/dsh` — the CLI (`dsh` bin): `dsh web --patch <yml> --no-open`
  prints the same readiness line (`dsh web: http://127.0.0.1:<port>`) and
  serves the same HTTP/WS API surface (`POST /api/<method>` ClientRequest
  envelope, `/api/events.mux` downlink) the web channel already speaks.
- `@deepseek-ai/dsh-sdk-jsonrpc-demo` — `dsh-jsonrpc-agent <cordis.yml>`: the
  published form of the jsonrpc-demo bin; the stdio NDJSON JSON-RPC protocol
  (`initialize` / `session/prompt` / `session.event` notifications) is
  identical to what `sdk-channel.js` implements.
- The bare plugin packages referenced by our `cordis.yml`
  (`@deepseek-ai/dsh-llm-deepseek`, `dsh-bash-local`, …) resolve **from the
  loader's own install location** (`cordis-plugin-loader` uses the Node
  internal module loader with the loader file as base URL), NOT from the
  config directory. Node's upward `node_modules` walk from a *globally
  installed* loader reaches `$(npm root -g)/@deepseek-ai/<pkg>` — so installing
  the plugin packages at the global root makes them resolvable. (A
  `workdir/node_modules` symlink farm does NOT work — verified.)

Version alignment matters in the rc phase: the `latest` dist-tag of several
plugin packages (0.0.1-rc.1) is older than the CLI (0.1.1-rc.2) and its
transitive deps can 404; the plugin set must be pinned to the CLI's version.

## Decision

1. **sdk-channel** launches `dsh-jsonrpc-agent <configPath>` (default command
   name resolved from PATH). The child's cwd is the config directory so that
   `./plugins/*.mjs` relative plugin references and the config resolve
   naturally.
2. **web-channel** launches `dsh web --patch <patchPath> --no-open` (default
   command name `dsh`). `DSH_HOME`, model env, telemetry env and the
   pluginsDir materialization (`<DSH_HOME>/profiles/web/plugins`) are
   unchanged — verified compatible with the distribution.
3. **Commands are configuration** (the ADR-0002 pattern): constructor
   `options.dsh` > config file `harness.dsh` > defaults
   (`{ command: 'dsh', jsonrpcCommand: 'dsh-jsonrpc-agent' }`). Switching or
   overriding the harness binary stays config-only.
4. **`options.repoRoot` is removed.** No code path references the dsh source
   tree anymore. Breaking change to the constructor surface, recorded here.
5. **Runtime installation is a documented prerequisite, not runner logic.**
   foreman never installs packages; the environment provides the distribution
   (see README "Prerequisites"). The pinned, version-aligned install set is
   recorded there (`@deepseek-ai/dsh@0.1.1-rc.2` + `dsh-sdk-jsonrpc-demo` +
   `dsh-sdk-jsonrpc-server` + the bare plugin packages, all at the matching
   version). e2e scenarios skip with a clear message when the binaries are
   absent — same convention as the codex e2e.
6. e2e scenario files stop assuming the nested-repo layout: `cordis.yml`,
   `web-patch.yml` and `plugins/` are read from this repository's root (they
   are deployment-owned files that live here).

## Consequences

- The dsh e2e scenarios (basic / web / checkpoint / cloud) run in any
  environment with the npm distribution installed — no source checkout, no
  `pnpm run build`, no nested-repo layout. Verified in this sandbox.
- The runner no longer depends on tsx or on dsh source layout; the channel
  surface (`start/prompt/shutdown/kill`, sessionRoot) is unchanged.
- Version skew between the CLI and the plugin packages becomes an
  environment concern: the pinned install set in the README is the contract.
  A mismatch fails loudly at plugin-tree load time (fail-loud loader).
- Upgrading the harness = upgrading the npm packages; foreman code is
  untouched (the generalization goal of ADR-0009 fully realized: both
  harnesses now switch by configuration alone).
- ADR-0001's source-launch mechanism for dsh is superseded; ADR-0001 remains
  accepted for the adapter-layer design itself.
