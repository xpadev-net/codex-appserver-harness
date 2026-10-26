# Task_11: Product Wave pipeline integration evidence

This records the local Go implementation boundary for Task_11 (#42). It is a
component and fixture acceptance record, not a claim that the external product
pipeline or P as a whole has passed.

## Acceptance evidence

| Task_11 condition | Go implementation and evidence |
| --- | --- |
| Direct product requests and Issue pool admissions enter the product Wave model | Both paths call the trusted `CreateProductWave` admission port. Direct CLI coverage is in `internal/kernelcli/product_create_test.go`; Issue admission, repository identity, dependency and scope checks are in `internal/product/issue_admission_test.go` (`TestIssueAdmissionUsesNumericRepositoryAndCommonProductPort`, `TestIssueAdmissionRequiresClosedDependenciesAndCompletedClaimedWave`). The daemon Issue pool is configured in `internal/kernelcli/issue_pool.go`. |
| Product Tasks receive a fixed workspace and are held until runtime/readiness is valid | `internal/product/execution_worker.go`, `internal/product/executor.go`, and the product workspace ports own preparation and stopped-result application. `internal/kernelcli/product_model_runtime_test.go` verifies that model readiness is bound only while the configured executor worker owns the same store and is revoked when it stops. |
| A newly merged predecessor can advance the base; conflict repair uses a fresh fix Task/session | `internal/product/base_planner.go` verifies repository/ref identity, stable HEAD bookends and predecessor ancestry. `TestExecutionWorkerSelectsActualBaseRefreshesThenNotifiesChangedHead` and `TestConflictFixUsesFreshCheckoutThenHostSealsOrRetainsFailure` verify refresh and new conflict-repair lineage. The old failed Task/workspace remains historical. |
| Candidate publication/review/merge keep canonical gate and one-use dispatcher checks | `internal/service/product_gates_test.go`, `internal/product/publisher_test.go`, `internal/product/dispatcher_test.go`, and `internal/kernelcli/product_executor.go` cover canonical evidence, source/target revalidation, unknown-send reconciliation, guarded review requests, and daemon placement. These fixtures do not manufacture formal approval. |
| Partial work, split lineage, budgets and versioned continuation preserve their proof | `internal/storage/product_continuations_test.go` verifies merged history and budget retention; `internal/storage/product_merge_dependencies_test.go` checks authenticated predecessor merge; split-source and bounded-tree coverage is recorded in PR #281 and `docs/product-splitting.md`. |
| Existing review findings and full-merge requirement edits remain recoverable | `internal/service/assessment_test.go` (`TestNoActionCannotResolveUnassessedFeedback`) and `internal/service/adoption_test.go` (`TestAdoptChangedRequirementsAfterFullMerge`, `TestAdoptFullMergeSerializesWithFinalAcceptance`) cover #44/#45. Shared repository exclusion across Waves is covered by `internal/storage/operation_reservations_test.go` (`TestGitCapacityUsesCommonRepositoryAcrossWavesAndLinkedWorktrees`). |
| K completion is distinct from product completion; unknown Cloud capability cannot pass a product gate | Product tool and publication gates require the product policy and current candidate evidence. Missing/unknown external capability stays held. `docs/acceptance.md`, `docs/task8-github-acceptance.md`, and issue #20 retain actual Cloud approval, CI/rules, and authority as unverified. |

## Verification on the merged implementation

The following commands completed successfully on the Task_11 review branch:

```sh
go test -race ./internal/service ./internal/workspace
go test ./internal/product ./internal/storage ./internal/kernelcli ./internal/service \
  -run '^(TestIssueAdmissionUsesNumericRepositoryAndCommonProductPort|TestIssueAdmissionRejectsUnavailableHeldChangedAndUnsupportedLineage|TestExecutionWorkerSelectsActualBaseRefreshesThenNotifiesChangedHead|TestProductBasePlanKeepsSemanticHistoryAndRequiresFreshCurrentSource|TestProductSuccessorWaitsForAuthenticatedMergeBeforePreparation|TestConflictFixUsesFreshCheckoutThenHostSealsOrRetainsFailure|TestProductExecutionWorkerPreparesAndResumesOnlyUnclaimedGitIntent|TestProductExecutionWorkerAppliesSavedStoppedRunWithoutModelDispatch|TestProductModelRuntimeRequiresCompletePrivateCurrentConfiguration|TestProductExecutorBindsReadinessOnlyWhileSameStoreWorkerRuns|TestProductCanonicalPublicationGateUsesActualValidatedJournalWithoutSideEffects|TestProductCanonicalReviewGateReloadsRequestProofWithoutInventingApproval|TestPreparedPublicationRetryKeepsExactPayloadAndDoesNotSpendAnotherSeat|TestLostPublicationResponseReconcilesOnlyItsOwnMarkerWithoutResend|TestPRDispatcherPreparedRestartRetriesThroughSameOneUseSendFence|TestPRDispatcherLostResponseNeverRequeuesUnknownAcrossRestart)$' \
  -count=1 -timeout=15m
```

The selected product pipeline tests passed in `internal/product` (61.374s),
`internal/storage` (8.265s), `internal/kernelcli` (2.238s), and
`internal/service` (2.159s). The race run passed in `internal/service`
(134.545s) and `internal/workspace` (185.438s).

The #44/#45, shared repository, source hold/recovery, split freshness,
base-refresh and conflict-repair regression selection also passed:
`internal/service` (9.957s), `internal/storage` (34.067s), and
`internal/product` (48.767s).

## Remaining P evidence

These fixtures do not prove a real product repository's end-to-end PR creation,
Cloud review, formal APPROVED, required CI/rules, non-bypass authority, or a
product-specific final acceptance. Those remain P unknown/blocked evidence in
issue #20 and the Task_13 acceptance issue #43. The K result and this local
Task_11 evidence do not change those statuses.
