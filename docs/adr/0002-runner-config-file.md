# ADR-0002: Runner configuration file and protocol selection

**Status:** Accepted
**Date:** 2026-08-25

## Context

Foreman is configured programmatically through `new Foreman(options)`. The
outbound event protocol (`events.format`) is one of those options today. The
operational requirement is that protocol switching — and eventually other
runner-level wiring — is a **configuration-file** concern: a deployment should
be able to switch a sandbox fleet from `openai-chat` to `openai-responses`
without code changes, exactly like the dsh composition files (`cordis.yml`,
`web-patch.yml`) already make the dsh side deployment-owned.

Constraints:

- The repository has **zero runtime dependencies** (Node >= 22 built-ins only);
  introducing a YAML parser for one config file would break that property.
- The dsh composition files are cloud-owned artifacts delivered through object
  storage; foreman-owned settings must not silently mix into them.
- The programmatic API is public and used by tests; it must stay authoritative
  when it speaks.

## Options considered

1. **Extend the dsh composition YAML** with a `foreman:` section — rejected:
  ownership mixing. The composition file is consumed by dsh's loader; foreman
  would need a YAML parser and would be parsing a file it does not own.
2. **Environment variables only** — rejected: structured settings
  (`events.bus` etc.) do not fit flat env vars; no schema.
3. **A JSON runner config file** (chosen): `foreman.config.json`, opt-in via a
  `configPath` option or the `FOREMAN_CONFIG` environment variable. JSON is
  parsed by the platform, needs no dependency, and validates cheaply.

## Decision

1. **File format & location** — `foreman.config.json`, an explicit path. It is
   opt-in: with no `configPath` option and no `FOREMAN_CONFIG` env var, foreman
   behaves exactly as before (all defaults / programmatic options).
2. **Schema** (validated; unknown keys fail loud, see below):

   ```json
   {
     "events": {
       "protocol": "openai-responses",
       "delivery": "sse",
       "model": "deepseek-v4-pro",
       "bus": { "kind": "http", "url": "..." }
     }
   }
   ```

   `events.protocol` selects the outbound protocol (registry id or alias,
   ADR-0001). `delivery`, `model`, and `bus` have the same meaning as the
   existing constructor options.
3. **Precedence** — per key: constructor option > config file > built-in
   default. The constructor stays authoritative; the file fills the gaps. The
   legacy `events.format` option is honored as an alias with lower precedence
   than `events.protocol`.
4. **Failure mode** — a missing file at an explicit path, invalid JSON, or an
   unknown key/protocol fails construction with a descriptive error. Silent
   fallback would make a typo'd protocol selection look like a working native
   stream — the worst possible failure mode for a wire-format switch.

## Consequences

- Protocol switching is a one-line config change; fleets can A/B protocols per
  deployment.
- The config surface starts small (`events`) but the loader/merge mechanism is
  generic: future sections (e.g. `gateway`, `bench`) follow the same
  precedence and validation rules without new mechanism.
- Settings that stay programmatic-only for now (secrets, `modelEnv`,
  checkpoints, git) are deliberately absent from the file; the boundary is
  documented in the design doc and the ROADMAP.
