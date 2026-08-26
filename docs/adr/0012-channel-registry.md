# ADR-0012: Channel registry and config-only composition

**Status:** Accepted
**Date:** 2026-08-26

## Context

Foreman's generalization promise (ADR-0002, ADR-0005, ADR-0009) is that selecting a
harness channel is a *configuration* decision: `foreman.config.json` →
`harness.channel` → the right channel starts, with no code changes. Three channels
ship today (`dsh-sdk`, `dsh-web`, `codex`), but two things undercut the promise:

1. **Selection is inline if/else in the orchestrator.** `Foreman.start()` branches on
   `this.channelId` to construct each channel with channel-specific option plumbing,
   and `Foreman.prepare()`/`sessionRoot` branch on it again. Adding a fourth channel
   means editing the orchestrator in three places — the outbound protocol side
   already solved this with a registry (ADR-0001); the inbound channel side never
   got the same treatment.
2. **No acceptance test proves the config-only path.** The e2e scenarios select the
   channel via the constructor option; nothing fails if config-file selection
   silently diverges from constructor selection.

Along the way, review surfaced incidental hygiene issues in the touched code:
`Foreman`'s git fallback (`rev-parse HEAD` with catch) is inlined three times; the
workspace git identity/branch (`foreman@localhost`, `main`) is hardcoded inside
`GitWorkspace`; and `SseGateway` (~110 lines of event-egress machinery) lives inside
`foreman.js`, blurring orchestrator vs. gateway concerns.

## Options considered

1. **Keep the if/else, add a config-selection e2e test only.** Rejected: the test
   would pin behavior the structure makes fragile; every new channel still edits the
   orchestrator.
2. **Full dynamic channel registry with external registration** (mirror of the
   protocol registry: `registerChannel({ id, aliases, create })`). Rejected as
   over-engineering for now: channels need orchestrator-owned context (workdir,
   sessionRoot, telemetry wiring, config), there is no external-adapter requirement
   for channels today, and a dynamic surface would validate ids the orchestrator
   cannot wire.
3. **Static channel factory table (chosen).** A module-level registry
   (`src/channels/registry.js`) maps each canonical channel id to a factory
   `{ id, aliases, create(ctx) }` plus channel-declared capabilities (e.g. HITL).
   `config.js` validates `harness.channel` against the registry's id list (unknown
   ids still fail loud with the accepted list); `Foreman` looks up the factory and
   calls it with a uniform context object. Adding a channel = adding one registry
   entry (+ tests); the orchestrator stops naming channels.

## Decision

1. **`src/channels/registry.js`** — the single source of truth for channel ids:
   `CHANNELS` (canonical ids, re-exported for config validation), legacy aliases,
   `resolveChannelId(id)` (moved from `config.js`; throws loud on unknown ids), and
   `createChannel(id, ctx)` returning a channel instance. `ctx` carries the
   orchestrator-owned inputs: `{ options, config, workspaceDir, sessionRoot,
   configPath, telemetry, modelEnv, onEvent, onStatus, onApproval,
   onApprovalResolved }`. Each factory maps ctx onto its channel's constructor.
2. **Capability flags on the registry entry** (initially `hitl: true` for `dsh-web`):
   the orchestrator wires approval handlers and `POST /hitl` generically when the
   selected channel declares the capability, instead of branch-checking `isWeb`.
   Codex HITL remains a roadmap item; when it lands it flips a flag, not a branch.
3. **Acceptance: config-only composition.** A dedicated e2e scenario
   (`test/e2e/composition.e2e.js`) drives the *same* codex run twice — once with the
   constructor `channel` option, once with **only** `foreman.config.json`
   (`harness.channel: "codex"`, model wiring under `harness.codex`) — and asserts
   both paths complete the full lifecycle (prepare/start/prompt/collect/publish)
   with equivalent results. Unit tests additionally assert every canonical channel
   id resolves through the registry to the right class and that unknown ids fail
   loud. No mocks in the measured path (ADR-0004/0008): real codex binary, real
   scripted Responses fixture, real control plane; the scenario skips when the
   binary is absent, exactly like the codex e2e.
4. **Incidental cleanups in the touched surface** (no behavior change):
   - `SseGateway` moves to `src/events/gateway.js` (re-exported from
     `src/index.js`; `foreman.js` re-exports it for compatibility).
   - `GitWorkspace` gains `headOid()` (the previously inlined rev-parse fallback)
     and configurable `branch` / `identity` options (defaults preserve current
     behavior: `main`, `foreman <foreman@localhost>`); `Foreman` uses `headOid()`
     at its three former inline sites.
   - Orchestrator doc comments trimmed where they described removed machinery.

## Consequences

- Adding a channel becomes: write the channel class, add one registry entry with a
  factory + capabilities, extend config validation only if the channel adds config
  keys. The orchestrator and `config.js` no longer enumerate channels inline
  (`config.js` imports the id list from the registry — inverted dependency, no
  cycle: channels never import `config.js`).
- The acceptance e2e pins the generalization contract end-to-end: config-only and
  constructor-only selection must both work and stay equivalent. Divergence fails
  the build instead of surfacing in production.
- `dsh-sdk`/`dsh-web` config-only acceptance is exercised at unit level (registry →
  class) plus the existing constructor-driven e2e; a full dsh config-only e2e waits
  until the harness repo is available in CI (the dsh e2e scenarios already skip
  gracefully when it is absent).
- Registry entries are static (no runtime registration). If external channel
  adapters become a requirement, this ADR is extended — not bypassed.
