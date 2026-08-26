# ADR-0009: Harness-scoped channel naming

**Status:** Accepted
**Date:** 2026-08-26

## Context

Foreman grew from a DeepSeek-Harness-only runner, and its naming still shows it: the
two dsh channels are named after their *transport* (`stdio`, `web`), the outbound
frame protocol is called `native`, and "the SDK" colloquially means "the dsh SDK".
With a second harness (Codex, ADR-0005) in the tree, these names no longer identify
what they point at:

- `channel: 'stdio'` / `channel: 'web'` say nothing about *which harness* is driven —
  the Codex app-server also speaks stdio.
- "SDK channel" is ambiguous between dsh's SDK and any other harness SDK.
- The config surface (`foreman.config.json` → `harness.channel`) is user-facing;
  harness-neutral names there are actively misleading when choosing a backend.

The outbound protocol id `native` (ADR-0001) is a *different* concept — the
foreman-native frame format — and is not affected by this decision.

## Options considered

1. **Keep transport-based names, add `harness` as a separate dimension** —
   `channel: 'stdio'` + `harness: 'dsh'|'codex'`. Rejected: the transport choice is
   already harness-specific (dsh web apiproxy vs codex app-server); two orthogonal knobs
   where one is derivable adds configuration surface without adding power.
2. **Rename channels to harness-scoped ids** (chosen) — each channel id names the
   harness *and* the integration face: `dsh-sdk`, `dsh-web`, `codex`. Transports remain
   documented properties of each channel, not identities.
3. **Free-form strings mapping to registered channel classes** — a channel registry
   mirroring the protocol adapter registry. Deferred: with three channels the
   constructor switch is still trivial; a registry can be introduced when channels
   grow beyond the built-ins without changing the public ids.

## Decision

1. Canonical channel ids, accepted by the `channel` constructor option and by
   `foreman.config.json` → `harness.channel`:
   - **`dsh-sdk`** — DeepSeek Harness over the SDK JSON-RPC stdio face.
   - **`dsh-web`** — DeepSeek Harness over the web apiproxy (HTTP + WebSocket).
   - **`codex`** — Codex Harness app-server over stdio JSON-RPC.
2. Legacy ids `stdio` and `web` remain **accepted aliases** (resolved to the canonical
   ids at construction; unknown ids fail loud with the list of accepted values). The
   legacy ids are documented as deprecated and may be removed in a future major.
3. The default channel is `dsh-sdk` (unchanged behavior, new name).
4. Emitted metadata (e.g. the `foreman.phase` frame's `channel` field) reports the
   canonical id.
5. The outbound protocol id `native` keeps its name: it denotes foreman's own frame
   format (ADR-0001), orthogonal to the harness. Where docs need to disambiguate, they
   say "the `native` *protocol*" vs "the `dsh-sdk` *channel*".

## Consequences

- Channel selection is self-describing in configs, logs, and the roadmap table; each
  harness is identifiable at a glance.
- Existing callers using `stdio`/`web` keep working via aliases — no breaking change
  for in-tree tests and deployments, while new code and docs use canonical ids.
- Validation becomes explicit: the accepted set is enumerated in one place
  (`src/config.js` channel resolution), typos fail loud (consistent with ADR-0002's
  philosophy for protocols).
- Documentation and README are updated to canonical names; the transport remains a
  documented column, not an identity.
