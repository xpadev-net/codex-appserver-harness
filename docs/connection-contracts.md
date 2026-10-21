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

## New kernel database foundation

`storage.OpenKernel` explicitly opens schema 2 in a private root. A genuinely empty DB is initialized atomically; v1, unknown versions, unversioned nonempty databases and incomplete kernel schemas are refused before kernel DDL. Reopening a valid kernel DB retains its records. The current legacy `Open` also checks the version before its DDL, so it cannot silently initialize a kernel/unknown DB as a legacy runtime. No legacy migration is run by opening either store.

Task_2 is split into reviewable PRs. The first adds the Wave/Phase/Task domain, policy/coverage checks, new schema and explicit migration registry. Session DTOs live in `internal/contract`; existing worker type names are aliases with identical JSON fields. `MigrationRegistry` registers consecutive, unique version/key steps for Task_12 to execute only after offline reconciliation and backup. Registration alone is not a migrated deployment or M acceptance.

The next foundation persists direct requests without WorkItems, versioned plan adoption, unique Task/session bindings, immutable snapshots and run start intents. Adoption keys replay the original receipt after restart; changed input under the same key fails. Plans, cumulative structure budgets, canceled superseded Tasks, events and durable notification intents commit together. Explicitly carried completed results require unchanged scope, policy, specification and input/dependency digests. Active or unknown Task processes must be reconciled and stopped before replanning. New plans allocate fresh Task/session identities rather than reusing unfinished sessions.

`KernelStore` implements the common session persistence port. Preparation is reserved before start/resume RPCs and holds the process even before turn/start. Each preparation receives a unique identity, so delayed stopping proofs cannot release a newer process. Unknown creation cannot silently allocate another thread; after stopping proof it may be retired as a failed Task and replaced through a fresh plan identity. Run reservation checks bound dependencies and attempt/deadline limits. A physical completion remains separate from logical Task completion, and unknown starts cannot be archived. Trusted process ownership must confirm stopping before disposition. Active history reconciliation retains the recovered turn association even while the protocol reports `inProgress`. Main rebinding also requires applied old runs and a saved snapshot. A stopped, unbound main with a lost initial creation response has a separate audited recovery path: it saves a bounded snapshot and a new generation before fresh creation; the old unknown creation intent remains in history. Artifact import/read/retention and Task-result application are described below; bounded template expansion is described below; scheduler leases, actual process proof, event fanout and the running CLI belong to later Tasks. These APIs alone do not satisfy K acceptance.

## Immutable kernel artifacts

Artifact IDs are controller-issued and carry actual SHA-256/byte count, media type, source Task/session, scope/plan/policy, provenance and input artifact IDs. Trusted main imports may describe repository/external sources; a Task may only publish its own Task provenance. These are generic bytes and do not replace product Candidate/SHA gates.

Import commits a capacity reservation before writing. The controller fsyncs private object bytes and the directory, then commits the manifest and notification intent together. Staged and deleting objects remain charged against Wave/controller limits. The same import key and content resume the same ID after restart; changed inputs conflict. Concurrent replay serializes file writes, and committed objects are never overwritten. Recovery reports unresolved partial/missing staged imports separately from committed imports; it never substitutes empty content.

File import confines normalized relative paths to the stored workspace using `os.Root`, checks readability/regular files and actual byte bounds, and rejects escapes, special files and hard links. Object access rejects symlinks, non-private permissions and hard links. Every read rechecks actual size and digest. Tools receive IDs/manifests/excerpts rather than object paths. Main owns its Waves; Tasks can read their own outputs, declared inputs and artifacts in bound completed dependency results. Cross-Wave/stale scope or policy and undeclared sibling access fail. Plan adoption and run reservation also recheck declared artifact bytes.

Retention starts from the producer Task's terminal time, or the Wave's terminal time for main imports. Unfinished Tasks protect their declared inputs, dependency results/evidence, own outputs and the transitive input provenance. Missing terminal timestamps retain data conservatively. GC saves a deletion intent before removing bytes and only releases capacity after saving expiration; a crash between removal and expiration is recoverable. Unknown staged imports stay charged for reconciliation. TaskResult application is described below; bounded bounded template expansion is described below; running kernel tools and independent K acceptance remain incomplete.


## Logical Task results

`ApplyTaskResult` applies a recorded normal completed turn only after its process is stopped. It verifies the run/thread/session, strict result schema and identity, assigned criterion coverage, immutable artifacts/evidence, review input digest and owned durable subscriptions. A receipt, result, Task state, run disposition and notification intent commit together; a repeated application after restart returns the saved receipt without a new turn or event.

Output validation uses the same strict Task output contract as the worker; the 64 KiB session-input limit is not imposed on otherwise valid output. A contract violation or permanently undeclared/expired/never-issued artifact reference makes the Task failed while retaining the physical run/output. Unavailable references block application and keep the completed run for reconciliation; committing the original staged artifact can unblock application without consuming another attempt. Valid waiting/blocked outcomes remain unfinished. Pause does not discard an already-running turn's valid result, and still prevents dispatch. Canceled/terminal or superseded results are archived without adopting them as current success. Completed Task results and all referenced evidence are revalidated before dependency dispatch or explicit carry.

This is storage/application groundwork, not Wave acceptance: a completed review with findings is still a completed Task report. Independent quality gates and bounded template branching belong to Task_7, subscription production/consumption to Task_3 and scheduling to Task_4/5. Product Candidate results fail closed until the P validator is connected. No generic artifact or local review is converted into product merge approval.


## Bounded K template transactions

`ExpandReviewTemplates` reads the current adopted plan and completed review results; it never accepts a model-generated expansion plan or calls main. It groups one phase's findings for the same input digest into a batch and requires exactly one saved template owner per classification. New fix Tasks run in deterministic owner order, followed by fresh review Tasks that depend on the last fix. Unknown/ambiguous classifications, exhausted repeat limits, progressed later phases and unsupported product transitions require parent planning instead of arbitrary fanout.

Plan revision, fresh phases/Tasks/sessions, explicit completed-result carry, replacement of unstarted future Tasks, cumulative structure/fix budgets, expansion receipts and notification intents commit together. Existing future instructions/criteria are preserved with fresh identities and rewritten dependencies. An immutable batch key includes template definitions, iteration, scope/policy, input digest and the finding batch. Replay after restart returns the prior admission, including review-event aliases for the same batch. Normal template steps consume no main turn or parent replan budget. Initial run dispatch resolves dependency input digests into the plan binding; successful result application adds its immutable result binding.

The tests complete actual storage run/result lifecycles with controlled fixtures, exercise three fresh-session rounds and saved-owner batching, and inject transaction failures. They are not live model/controller K acceptance. Task_7 still connects automatic branching and quality gates; P expansion remains fail-closed until its independent verification/Candidate adapter is connected. M migration remains Task_12.
