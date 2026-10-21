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
