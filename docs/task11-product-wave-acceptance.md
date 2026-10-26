# Task_11: Product Wave pipeline integration evidence

This records existing local Go components and fixture coverage relevant to
Task_11 (#42). It is **not Task_11 acceptance**: the components have not yet
been shown working together through one product Wave from direct/Issue
admission to a published, subscribed, reviewed, and merged candidate.

## Acceptance evidence

| Task_11 area | Existing component evidence; missing integrated acceptance |
| --- | --- |
| Direct product and Issue pool admission | Both paths call `CreateProductWave`; direct CLI coverage is in `internal/kernelcli/product_create_test.go`, while Issue admission/repository/dependency checks are in `internal/product/issue_admission_test.go`. New `TestProductWaveAdmissionsReachTheSameExecutorReadiness` proves both source kinds reach the same running executor and Task readiness. Missing: continuing that integrated flow through publication/subscription/merge. |
| Product Task workspace, readiness, and stopped candidate | `TestProductWaveAdmissionsReachTheSameExecutorReadiness` joins both direct and Issue pool admission to the configured executor's real Git workspace preparation, model readiness/revocation, and subsequent stopped-result candidate seal/WorkItem update. `TestProductVerificationSealerChecksRealDiffAndTrackedPlanBeforeReceipt/publication_direct` joins direct admission through candidate sealing, formal local verification, review-result adoption, and publication readiness. Existing `publication` subtests exercise the same downstream path with Issue admission. `TestIssueAdmittedProductWavePreparesAndSealsStoppedCandidate` independently covers Issue-source executor application. Missing: one flow joining live model readiness through publication readiness. All stopped model/review result fixtures are synthetic; no real model dispatch is claimed. |
| Base refresh and conflict repair | `internal/product/base_planner.go`, `TestExecutionWorkerSelectsActualBaseRefreshesThenNotifiesChangedHead`, and `TestConflictFixUsesFreshCheckoutThenHostSealsOrRetainsFailure` cover those components. Missing: integrated same-Wave tests for predecessor merge followed by successor candidate, and concurrent same-repository Waves where one candidate changes the other's base. |
| Publication, review, subscription, and merge | `internal/service/product_gates_test.go`, `internal/product/publisher_test.go`, and `internal/product/dispatcher_test.go` verify separate gate/dispatch boundaries and unknown-send reconciliation. Missing: one Wave-level flow joining publication, automatic subscription, review update, canonical merge gate, and resulting WorkItem/Wave state. |
| Partial work, split lineage, budgets, and versioned continuation | `internal/storage/product_continuations_test.go`, `internal/storage/product_merge_dependencies_test.go`, PR #281, and `docs/product-splitting.md` cover component contracts. Missing: integrated multi-WorkItem/multi-Wave acceptance proving invalidation and cumulative budgets remain connected through publication and continuation. |
| #44/#45 and shared-repository regressions | `internal/service/assessment_test.go` (`TestNoActionCannotResolveUnassessedFeedback`), `internal/service/adoption_test.go` (`TestAdoptChangedRequirementsAfterFullMerge`, `TestAdoptFullMergeSerializesWithFinalAcceptance`), and `internal/storage/operation_reservations_test.go` (`TestGitCapacityUsesCommonRepositoryAcrossWavesAndLinkedWorktrees`) cover these independently. |
| K/P separation and unknown Cloud capability | Product tool/publication gates keep unknown external capability held. This is a required safety boundary, not evidence that formal approval, CI/rules, non-bypass authority, or P acceptance has succeeded. |

## Verification on the merged implementation

The following commands completed successfully on the Task_11 review branch:

```sh
go test -race ./internal/service ./internal/workspace
go test ./internal/kernelcli -run '^TestProductWaveAdmissionsReachTheSameExecutorReadiness$' -count=1 -v -timeout=2m
go test -race ./internal/kernelcli -run '^TestProductWaveAdmissionsReachTheSameExecutorReadiness$' -count=1 -timeout=5m
go test ./internal/product -run '^TestIssueAdmittedProductWavePreparesAndSealsStoppedCandidate$' -count=1 -v -timeout=2m
go test ./internal/product -run '^TestProductVerificationSealerChecksRealDiffAndTrackedPlanBeforeReceipt/publication_direct$' -count=1 -v -timeout=2m
go test ./internal/product ./internal/storage ./internal/kernelcli ./internal/service \
  -run '^(TestIssueAdmissionUsesNumericRepositoryAndCommonProductPort|TestIssueAdmissionRejectsUnavailableHeldChangedAndUnsupportedLineage|TestExecutionWorkerSelectsActualBaseRefreshesThenNotifiesChangedHead|TestProductBasePlanKeepsSemanticHistoryAndRequiresFreshCurrentSource|TestProductSuccessorWaitsForAuthenticatedMergeBeforePreparation|TestConflictFixUsesFreshCheckoutThenHostSealsOrRetainsFailure|TestProductExecutionWorkerPreparesAndResumesOnlyUnclaimedGitIntent|TestProductExecutionWorkerAppliesSavedStoppedRunWithoutModelDispatch|TestProductModelRuntimeRequiresCompletePrivateCurrentConfiguration|TestProductExecutorBindsReadinessOnlyWhileSameStoreWorkerRuns|TestProductCanonicalPublicationGateUsesActualValidatedJournalWithoutSideEffects|TestProductCanonicalReviewGateReloadsRequestProofWithoutInventingApproval|TestPreparedPublicationRetryKeepsExactPayloadAndDoesNotSpendAnotherSeat|TestLostPublicationResponseReconcilesOnlyItsOwnMarkerWithoutResend|TestPRDispatcherPreparedRestartRetriesThroughSameOneUseSendFence|TestPRDispatcherLostResponseNeverRequeuesUnknownAcrossRestart)$' \
  -count=1 -timeout=15m
```

The selected product pipeline tests passed in `internal/product` (61.374s),
`internal/storage` (8.265s), `internal/kernelcli` (2.238s), and
`internal/service` (2.159s). The race run passed in `internal/service`
(134.545s) and `internal/workspace` (185.438s).
The source-to-candidate integration test passed for both `direct` and
`issue_pool` sources (3.652s normally; 6.538s with the race detector); its stopped
model result is synthetic and no real model dispatch is claimed.
The Issue-source-to-candidate executor test passed in `internal/product`
(1.566s); its stop receipt/result fixture is synthetic while Git checkout,
sealing, and SQLite adoption use the real local implementations.
The new direct-admission-to-publication-readiness subtest passed in
`internal/product` (6.415s normally; 16.542s with the race detector). Its model,
verification, and review run receipts are synthetic; Git sealing, verifier
execution, review adoption, and publication readiness use local implementations.

The #44/#45, shared repository, source hold/recovery, split freshness,
base-refresh and conflict-repair regression selection also passed with this
exact command:

```sh
go test ./internal/service ./internal/storage ./internal/product \
  -run '^(TestNoActionCannotResolveUnassessedFeedback|TestAdoptChangedRequirementsAfterFullMerge|TestAdoptFullMergeSerializesWithFinalAcceptance|TestGitCapacityUsesCommonRepositoryAcrossWavesAndLinkedWorktrees|TestProductContinuationRegistrationPreservesOldMergeAndWaveBudget|TestProductContinuationRequiresNewVersionOriginalParentAndAtomicSave|TestIssueSourceObserverPersistsHoldAndRecoveryWithoutModelTurn|TestProductSplitChildSourcePinsIdentityPolicyAndBoundedFreshnessWithoutWaveHold|TestProductBasePlanKeepsSemanticHistoryAndRequiresFreshCurrentSource|TestConflictFixUsesFreshCheckoutThenHostSealsOrRetainsFailure)$' \
  -count=1 -timeout=15m
```

Results:
`internal/service` (9.957s), `internal/storage` (34.067s), and
`internal/product` (48.767s).

## Remaining P evidence

The following remain unimplemented or unverified for Task_11: an integrated
product-Wave acceptance flow across the listed component boundaries, especially
base changes between concurrent Waves and the durable link from automatic
subscription updates back to product Task/WorkItem state. Do not close #42
based on this component evidence.

Real product repository PR creation, Cloud review, formal APPROVED, required
CI/rules, non-bypass authority, and product-specific final acceptance also
remain P unknown/blocked evidence in issue #20 and Task_13/#43. The K result
does not change those statuses.
