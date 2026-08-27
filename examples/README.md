# Real-API examples

Two runnable examples drive the full foreman lifecycle (`prepare → start →
prompt → collect → publish → shutdown`) against the **real DeepSeek API** —
no scripted model, no mocks on the model path. The only thing that stays local
is the control plane (object storage + message bus on 127.0.0.1): every
artifact stays on this machine, the only network egress is the model API call.

| Example | Channel | Harness binary | Model endpoint |
| --- | --- | --- | --- |
| `dsh-real.js` | `dsh-sdk` | `dsh-jsonrpc-agent` (npm distribution, ADR-0012) | `https://api.deepseek.com` (chat-completions wire, `deepseek-v4-pro`) |
| `codex-real.js` | `codex` | `codex` (npm distribution, codex-cli 0.150.x) | `https://api.deepseek.com/v1` (Responses wire — codex 0.150 removed `wire_api = "chat"`) |

## Prerequisites

- Node.js >= 22.19
- The harness distributions on `PATH` (install one at a time — installing both
  concurrently has proven flaky; use a China mirror if an install stalls):

  ```sh
  npm install -g @deepseek-ai/dsh-sdk-jsonrpc-demo   # provides dsh-jsonrpc-agent
  npm install -g @openai/codex                        # provides codex
  ```

- An API key in the environment (`DEEPSEEK_API_KEY` or `deepseek_key`).

## Run

```sh
DEEPSEEK_API_KEY=sk-... node examples/dsh-real.js
DEEPSEEK_API_KEY=sk-... node examples/codex-real.js
```

Both examples `--keep` the run directory when passed the flag (inspect
`workspace/`, `artifacts/`, and the seeded object storage afterwards).

## What each example shows

1. **Seed** — composition config (dsh) and a workspace tarball are uploaded
   to object storage, as the cloud would before a run.
2. **A real agent turn** — the harness performs real tool calls in the
   restored workspace (dsh: `bash`/`read`/`write`; codex: `exec_command`)
   driven by the real model, then answers.
3. **Workspace control** — local git commits the turn; a checkpoint pack is
   built and uploaded (skip-list retention, ADR-0010).
4. **Publish** — workspace/session/trace/result artifacts are packaged,
   redacted and uploaded; the reclaim event fires on the bus.
5. **Throughput view** — the profile (ADR-0013) prints the time
   decomposition (`prepare/boot/execution/commit/collect/publish`), the
   useful-work ratio and the turn throughput, exactly as the scheduler
   would consume them from `profile.json`.
6. **Secret hygiene** — the API key is env-injected only and asserted absent
   from every uploaded artifact.

Expected output (abbreviated):

```
===== run result =====
final answer: I inspected the workspace and created notes.md ...
tool calls: bash, read, bash, read, write          # dsh
tool calls: exec_command, exec_command, ...        # codex
===== profile (ADR-0013 throughput view) =====
wall=10737ms  prepare=97ms  boot=967ms  execution=9405ms  commit=78ms  collect=37ms  publish=132ms
usefulWorkRatio=87.6%  turnThroughput=0.0931/s  turns=1
secret hygiene: API key never persisted in artifacts — clean
```
