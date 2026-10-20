# Codex Issue / PR harness

Go implementation of [requirements](requirements.md) and [implementation plan](implementation-plan.md).
The user's Go requirement supersedes the documents' TypeScript proposal.

Requires Go 1.26, Git, authenticated `gh`, and Codex CLI 0.163.0.

```sh
go test ./...
go run ./cmd/harness doctor --repo OWNER/REPOSITORY
```

`doctor` reads GitHub repository metadata and initializes app-server, without starting
an inference turn or writing to GitHub. Unknown capabilities keep automatic merge disabled.
The installed binary must match `protocol/codex/manifest.json`. The selected protocol
schemas are generated from that binary; the manifest records hashes for the full bundle.

## Development sequence

PR 0: connection contracts; PR 1: persistent workflow; PR 2: implementation worker;
PR 3: external runner; PR 4: independent verifier; PR 5: GitHub review and merge;
PR 6: splitting and partial PRs; PR 7: operations and recovery.

Each PR is reviewed with `codex exec review --base <repository-default-branch>` and
merged only after its findings are resolved. This development review is distinct
from the harness's Cloud review integration, which requires a trusted formal
`APPROVED` review on the current candidate SHA.
