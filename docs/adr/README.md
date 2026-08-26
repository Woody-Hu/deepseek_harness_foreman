# Architecture Decision Records

ADRs capture the significant design decisions of Foreman: context, options
considered, the decision, and its consequences. They are written **before**
implementation ("docs-first") and updated only by superseding ADRs — never
edited in place after acceptance.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-generic-sse-protocol-adapter-layer.md) | Generic outbound SSE protocol adapter layer | Accepted |
| [0002](0002-runner-config-file.md) | Runner configuration file and protocol selection | Accepted |
| [0003](0003-openai-responses-protocol.md) | `openai-responses` protocol dialect (Codex) | Accepted |
| [0004](0004-hermetic-tests-and-benchmarks.md) | Hermetic testing and benchmarking strategy | Accepted |
| [0005](0005-codex-app-server-channel.md) | Codex Harness app-server integration channel | Accepted |
| [0006](0006-anthropic-messages-protocol.md) | Anthropic Messages streaming protocol adapter (Claude Code) | Proposed |
| [0007](0007-inbound-protocol-adaptation.md) | Inbound protocol adaptation (generalized parse direction) | Proposed |
| [0008](0008-harness-protocol-testing-and-benchmarks.md) | Independent testing and benchmark strategy for harness protocols | Proposed |
| [0009](0009-channel-naming.md) | Harness-scoped channel naming (`dsh-sdk` / `dsh-web` / `codex`) | Accepted |
| [0010](0010-overlap-scheduling.md) | Overlap scheduling for run-lifecycle I/O | Accepted (3a superseded by ADR-0011) |
| [0011](0011-simplified-overlap-scheduling.md) | Simplified overlap scheduling | Accepted |
| [0012](0012-channel-registry.md) | Channel registry and config-only composition | Accepted |

## Process

1. New decisions start as a new numbered ADR in `Proposed` status.
2. During review the record may change; once implementation starts the
   decision is `Accepted` and the record is frozen.
3. Reversal or replacement happens through a new ADR that supersedes the old
   one (`Status: Superseded by ADR-XXXX`).

Supporting design documents (interface, data model, mechanism detail) live in
[docs/design/](../design/); ADRs reference them instead of duplicating them.