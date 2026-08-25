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

## Process

1. New decisions start as a new numbered ADR in `Proposed` status.
2. During review the record may change; once implementation starts the
   decision is `Accepted` and the record is frozen.
3. Reversal or replacement happens through a new ADR that supersedes the old
   one (`Status: Superseded by ADR-XXXX`).

Supporting design documents (interface, data model, mechanism detail) live in
[docs/design/](../design/); ADRs reference them instead of duplicating them.
