# PR 0 connection contract

- Implementation language: Go, as explicitly requested. No generated TypeScript runtime.
- Pinned executable: codex-cli 0.163.0; JSON schemas and full schema hash manifest are in `protocol/codex`.
- Transport: local child process, newline-delimited JSON RPC; initialize followed by initialized.
- Observed repository: xpadev-net/codex-appserver-harness, numeric ID 1426858504.
- Repository default branch: resolved through GitHub metadata, not inferred from a name.
- Readiness observations: `evidence/pr0-doctor.json`; observations are evidence of that invocation only.
- Thread resume, Cloud request identity, clean/dirty/re-push Cloud review results,
  formal APPROVED SHA correspondence, rate limits, and CI SHA mapping remain unknown
  until observed with the actual deployment identity and configured repository.
- Unknown or unsupported capabilities prevent automatic merge. Development can proceed.
- `codex exec review` used for this repository's development is not evidence that
  GitHub Cloud review supports formal approval. No local review is substituted for it.

Source: https://learn.chatgpt.com/docs/app-server (accessed 2026-10-21).
The generated schema from the pinned executable is the adapter contract.

Current-environment observations (2026-10-21): see [live connection evidence](issue20-live-connections.md). Actual CI execution and isolated app-server initialization were observed; checkout-SHA semantics, native Cloud approval, dedicated model turns/resume and non-bypass rules remain unverified. The capability report keeps automatic merge disabled.

The live Cloud request received an account/GitHub-connection prerequisite from the provider, not an accepted review or approval. Requester ID, provider candidate ID, marker and request head are retained in [the Cloud observation](../evidence/issue20/cloud-observation.json). No trusted-reviewer policy was inferred from the display name.

## S-stage dynamic tool spike

`internal/contract/session_live_test.go` is a standalone opt-in probe for AT-050.
The [observation](../evidence/background-wave/spike/session-observation.json) records the pinned experimental schema hashes, model, pseudonymous authenticated subject, launcher hash, thread/turn/call/RPC identifiers and completion checks. One non-skipped test performs two model turns. A controller-generated random acknowledgement, absent from the prompt, must return through the tool and model response. Normal turn completion alone does not pass.

The operator explicitly authorized the current session's ChatGPT authentication for this probe. Only its authentication file and minimal model configuration were copied into a separate private Codex home. The controller's home, DB sentinel and GitHub credential paths remain hidden. Anonymous GitHub API probes returned 401 before model execution. These bounded observations do not attest every possible deployment/network credential path or grant Cloud approval capabilities.

The real launcher initially hid the distribution's sibling `codex-code-mode-host`, so model turns ended without a tool request. The launcher now mounts that executable read-only when present; it does not mount its surrounding directory. The successful probe used `gpt-6.1-sol`, experimental API opt-in, `thread/start.dynamicTools` and `item/tool/call`. The first process was stopped before a new process resumed the same thread, verified the first completed turn in history, and called the persisted tool again. The logical task remains incomplete between the two turns; no completed Task/session is reused.

```sh
HARNESS_LIVE_SESSION=1 \
HARNESS_LIVE_CODEX_BINARY=/absolute/native/codex \
HARNESS_LIVE_WORKER_CODEX_HOME=/absolute/private/dedicated-codex-home \
HARNESS_LIVE_MODEL=gpt-6.1-sol \
HARNESS_LIVE_EVIDENCE=/absolute/private/session-observation.json \
go test ./internal/contract -run '^TestLiveDynamicToolResume$' -count=1 -v -timeout 7m
```

The selected home must contain ChatGPT authentication and only trusted configuration, with unrelated MCP/plugins disabled. Keep it outside the repository. Credential values, email, full model output and private paths are not recorded. Normal tests skip the live probe. Missing authentication, uncompleted requests, failed/interrupted turns or an unknown start cannot produce `verified`. No outputSchema request-array fallback is enabled. K runtime implementation may now proceed; K/P/M and #20 remain incomplete.

The [official App Server documentation](https://learn.chatgpt.com/docs/app-server) describes experimental opt-in, persisted dynamic tools on resume and the server-request response flow. The generated schema from the adopted 0.163.0 executable remains the wire contract.

## Kernel session and tool contracts

`SessionRPC` opts into the experimental API and owns one transport reader. RPC replies go directly to their callers; notifications remain queued while dynamic tool handlers run separately. Unsupported approval/input requests stop dispatch with the existing `RequestError` contract. Queue, RPC and handler limits fail explicitly rather than silently dropping input. Closing this wrapper does not prove process quiescence: the process owner must close/wait the underlying client before releasing its reservation.

`SessionDriver` is common to main and Task sessions. It reads controller bindings through `SessionPersistence`, reserves thread creation before `thread/start`, and reserves a snapshot/start intent before a model turn. An unknown thread/turn start is reconciled, never automatically resent. A terminal Task cannot prepare or execute another turn; read-only reconciliation of sent work remains possible. Main rebinding requires the persistence implementation to verify stopped processes, reconciled runs and a saved snapshot before writing its audit. Task thread loss requires a new Task/session.

Kernel tool and result schemas are in `schemas/handoffs/{kernel-tools,task-result,wave-plan}-v1.json`; regenerate them with `go run ./internal/contract/cmd/session-schemas`. Experimental wire schemas and hashes are under `protocol/codex/experimental`, generated with `codex app-server generate-json-schema --experimental --out DIR` using 0.163.0. The existing stable schemas and PR workflow remain separate during staged adoption.

Task sessions do not see `wave_create` or `wave_plan`. The common gateway also rejects direct/aliased/namespaced Wave calls from Tasks, stale session/thread/turn/scope/plan/policy bindings, invalid UTF-8, duplicate JSON properties and oversized inline content. Parent/Task tools share a backend authorization port; the backend must verify resource ownership (including direct parent/Task instruction and message recipients), replay identity and budgets in the same transaction as each effect. A message body never grants authority. Accepted operations require a saved operation/subscription; unknown operations require a reconciliation ID.

This is the Task_1 foundation, tested with port fixtures. Production new-DB bindings and atomic effects belong to Task_2/3, scheduler/process ownership to Task_4/5, and main planning to Task_6. It does not activate the new kernel CLI, migrate legacy sessions or satisfy K acceptance by itself.
