# Background Wave v0.4 の段階別受入

2026-10-23、Go実装 `589f4bc` をKの判定基点とする。Kは**合格**、PとMは**未完了**。この判定は新DBの汎用Waveに限り、製品PRの自動mergeや全体完成を表さない。要件原本は [requirements.md §11](../requirements.md#11-段階別の完了条件)。

| 段階 | 判定 | 根拠と適用境界 |
| --- | --- | --- |
| S | 合格 | [native dynamic toolと同一未完了thread再開](../evidence/background-wave/spike/session-observation.json)。後続Kより先に実施したAT-050 |
| K | 合格 | 下表のK適用AT、実モデルによる独立Task並行・findings→新fix/review→report→検証済み完了、CLI操作、障害注入、通知参照負荷 |
| P | 未完了 | [Task_8 component](task8-github-acceptance.md)はfixture合格。AT-009の新fix Task/session経路はKで実装・検証済み。Task_11の各ローカルcomponent fixtureは存在するが、同一Waveの製品工程としての統合受入は未完了（[不足と証拠](task11-product-wave-acceptance.md)）。実repositoryのPR・Cloud/CI/rules・正式承認と製品最終受入もTask_13および [#20](https://github.com/xpadev-net/codex-appserver-harness/issues/20) に残し、unknownを合格へ読み替えない |
| M | 未完了 | Task_12のv1 DB/config fixture移行が必要。依頼者確認で運用DBは存在せず、導入先の実DB切替だけがnot_applicable |

## 実行した経路

[実モデルの観測](../evidence/background-wave/kernel/native-fix-review.json)は新DBのCLI init/create/runから、Issue/WorkItem/PRなしで、元データの調査、独立review、新fix、新review、報告、成果参照の検証済み受入を完了した。native0.163.0、gpt-6.1-sol、現セッション認証、未skip 1 test、8 completed model run。初期2Taskの実行区間が重なり、各Task/sessionは一意。mainは初回/最終判断の2回で、定型fix/review中の起動は0だった。最終成果の実bytes、SHA256、元データと計算値、seed不在を照合し、独立clean reviewerと必須criterionを持つWaveCompletion proofを確認した。全managed process停止済み。

実接続testとその再現envは [受入記録](acceptance.md#native-k-の完了経路と結果契約) にある。レビューで追加した厳密な数値照合と実行区間検査は、同じ保存済み正常DBに対する読み取りだけの再確認でも成功した。後から予算やrun結果を編集していない。試験の新規rootには開始前に2M token上限を設定した。観測下限908212は最終請求量ではない。既定1Mでの [予算停止](../evidence/background-wave/kernel/native-token-budget-stop.json)、producer未検証の [受入保留](../evidence/background-wave/kernel/native-criterion-hold.json)、初回provider schema不整合と監査付きmain復旧の [履歴](../evidence/background-wave/kernel/strict-output-schema.json) は別試行として保持する。

[別プロセスのCLI操作](../evidence/background-wave/kernel/cli-observation.json)はビルドしたGoバイナリと新しいprivate DBを使用した。依頼2件、一覧2ページ、pause/resume、steerのqueued保存、cancel、終端後resume拒否、7件の状態eventをcursorで重複なく回収、session/runs閲覧を確認した。daemon/model/GitHubを起動せず、DB上のmodel runは0だった。streamの切断・再接続は後述のIPC fixture試験で確認している。

[本番KernelSourceの参照負荷](../evidence/background-wave/kernel/notification-latency.json)はprocess/model/外部I/Oをstubにし、実際のKernelSource/Loop/SQLite/IPCと独立Task2件で100通知を100再開intentへ予約・適用した。同じ未完了threadを継続し、main起動0、各組の物理停止と枠解放を検査した。保存呼出し前から有界source選択終了までの保守的な上限で、p95 64.032ms、max79.840ms。NFR-012のSHOULD目標1秒以内を満たす。実nativeの [2 process上限・第三予約拒否・停止/RSS](../evidence/background-wave/runtime/parallel-native.json) と [起動/停止](../evidence/background-wave/runtime/isolated-launcher.json) を別記し、stubの時間をnative性能と扱わない。

[全体回帰記録](../evidence/background-wave/kernel/regression.json): `go test ./...`、`go vet ./...`、主要9packageのrace、Go CLI build成功。通常suiteではopt-in native試験をskipし、上記未skip実接続と区別した。PR #114/115はnative `codex exec review --base origin/master`の指摘を解消してからGitHub CI通過を確認しマージした。reviewのsandboxでUnix socketが使えない制約は実行失敗として記録し、rootでの回帰/race成功と混同しない。CodeRabbitのrate limit、Devinのskip、Cloud unknownを承認として数えない。

## K適用ATの対応

「合格」はK適用部分だけの判定である。表内の名前は実行済みGo test、リンクはその試験の実ファイルを指す。SQLiteの失敗注入・別接続・再open、fake model/process、実temp Gitで順序とcrash境界を検証し、実モデル試験と併用する。

| AT | K判定 | 実装・試験の根拠 / Pへ残す部分 |
| --- | --- | --- |
| 031 | 合格 | [waves_test.go](../internal/storage/waves_test.go) `TestDirectWavePlanAdmissionIsAtomicAndReplayable`、[contract plan](../internal/contract/session_tools_test.go) `TestPhaseLanguageRejectsUnsupportedPlans` による所属/coverage/依存・循環拒否 |
| 032 | 合格 | [scheduler_test.go](../internal/storage/scheduler_test.go) `TestReadyPhaseSelectsIndependentTasksAndAdvancesOnlyOnAllSuccess`、[loop_test.go](../internal/control/loop_test.go) `TestLoopStartsIndependentTaskWhileAnotherTurnIsWaiting`、実model初期2Taskの重なり |
| 033 | 合格 | 同scheduler、[kernel_source_test.go](../internal/control/kernel_source_test.go) `TestKernelSourceInitialTaskAndNotificationResumeUseSavedInputs`。Task待機中の独立進行、次Phase保留、確認用turnなし。外部review自体はP |
| 034 | 合格 | [notifications_test.go](../internal/storage/notifications_test.go) `TestSubscribeBeforeAfterAndConcurrentCompletion`、[wakes_test.go](../internal/storage/wakes_test.go) `TestWakeBindsOnlyItsRunAndAppliesWithTheLogicalResult` |
| 035 | 合格 | 同notifications `TestNotificationFailureRollsBackTaskResult` / `TestNotificationDeliveryAckIsSeparateAndSurvivesReboot`、同wakes `TestWakeReservationIsReplayableAndInputDoesNotDependOnDeliveryAck`、source `TestKernelSourceRestartAppliesCompletedRunWithoutAnotherModel` |
| 036 | 合格 | [template_expansion_test.go](../internal/storage/template_expansion_test.go) `TestTemplateExpansionCreatesFreshSessionsEveryRoundAndStopsAtLimit`、[template_progression_test.go](../internal/control/template_progression_test.go)、実modelの新fix/review。candidate SHAと製品routingはP |
| 037 | 合格 | [operator_controls_test.go](../internal/storage/operator_controls_test.go) `TestOperatorCancelArchivesLateResultAndKeepsRunUntilStopped`、[directions_test.go](../internal/storage/directions_test.go)、同wakes `TestWakePauseAndSteeringCannotLaunchReservedOldInput` |
| 038 | 合格 | [main_results_test.go](../internal/storage/main_results_test.go) `TestMainResultAppliesOrRequeuesSharedWake`、同wakes `TestSharedMainWakeReleasesOnlyStillCurrentWaveAfterSteering`、[session_dispatch_test.go](../internal/storage/session_dispatch_test.go) |
| 040 | 合格 | [reservations_test.go](../internal/storage/reservations_test.go) 別接続でのcapacity/lease、[operation_budgets_test.go](../internal/storage/operation_budgets_test.go)、[parent_fix_budget_test.go](../internal/storage/parent_fix_budget_test.go)、実nativeの第三予約拒否 |
| 041 | 合格 | [watch_test.go](../internal/kernelcli/watch_test.go) `TestKernelCLIWatchReconnectsWithoutWakeOrAcknowledgement` / `TestKernelCLIWatchOutputFailureRetainsCursorForReconnect`、[observations_test.go](../internal/kernelcli/observations_test.go)、[runs_test.go](../internal/kernelcli/runs_test.go)、別process CLI |
| 045 | 合格 | 同scheduler/template、[task_result_handoff_test.go](../internal/control/task_result_handoff_test.go)。実modelでも新sessionへfix/reviewを引き渡し、終了Taskを再開しない |
| 046 | 合格 | [session_tools_test.go](../internal/contract/session_tools_test.go) `TestKernelPublicToolsAndControllerAuthorization`、[kernel_tools_test.go](../internal/storage/kernel_tools_test.go) `TestKernelToolWaitOwnsItsSubscriptionsAndTaskCannotManageWave`。公開toolとcontroller認可の両方で拒否 |
| 047 | 合格 | [messages_test.go](../internal/storage/messages_test.go) `TestParentChildMailboxesAreAutomaticAndBidirectional` / `TestMessageConversationCannotCrossSiblingWaveOrParent`、source `TestKernelToolMessageWakesChildWhileMainTurnContinues` |
| 048 | 合格 | 同messages `TestMessagesRetainPauseAndTerminalRecipientWithoutChangingAuthority`、同wakes / operator controls、[interruptions_test.go](../internal/control/interruptions_test.go)。待機中は同thread、禁止状態は保持だけ |
| 051 | 合格 | 実modelの汎用成果完了、[artifact_acceptance_test.go](../internal/storage/artifact_acceptance_test.go)、[artifacts_test.go](../internal/storage/artifacts_test.go)、[input_cache_test.go](../internal/storage/input_cache_test.go)。実bytes/digest、容量、正確なgrant/失効、GC後の根拠保持 |
| 052 | 合格 | 同scheduler `TestReadyDrainAndBlockHoldsNewTasksButAllowsExplicitRecovery` / `TestReadyUnknownRunHoldsSiblingAcrossRestartAndStoppingProof`、[task_results_test.go](../internal/storage/task_results_test.go)、[task_recovery_dispatch_test.go](../internal/control/task_recovery_dispatch_test.go) |
| 053 | 合格 | 同control template `TestKernelSourceAutomaticallyRunsSavedFixReviewWithoutIntermediateMain`、[parent_attention_test.go](../internal/storage/parent_attention_test.go)、実modelの定型反復中main0 |
| 054 | 合格 | 同waves `TestPlanAdmissionRollsBackStructureAndBudget`、同template `TestTemplateExpansionIsAtomicReplayableAndKeepsCumulativeBudgets`、[wave_tools_test.go](../internal/storage/wave_tools_test.go)。監査/通知を含めた失敗rollbackと再実行 |
| 055 | 合格 | 同waves/template、[deadlines_test.go](../internal/storage/deadlines_test.go)、[run_budgets_test.go](../internal/storage/run_budgets_test.go)、同operation budgets。実model既定token超過は次dispatchを保留 |
| 056 | K部分合格 | [operation_reservations_test.go](../internal/storage/operation_reservations_test.go) `TestKPolicyAdmitsLocalGitAndExcludesSharedRepositoryAcrossWaves` / `TestGitCapacityUsesCommonRepositoryAcrossWavesAndLinkedWorktrees`。temp Gitの共有repo排他。実PR競合/失効/期待SHA mergeはP |
| 057 | K部分合格 | 同kernel toolsのruntime identity / 旧版 / source run検査、同messagesの親子scope、同contract tool認可、同main resultsのpolicy緩和拒否。外部comment取得と出所付き中継は[Task_8 component受入](task8-github-acceptance.md)を参照し、P全体の工程は別判定 |
| 058 | 合格 | [session_input_test.go](../internal/storage/session_input_test.go)、[context_test.go](../internal/storage/context_test.go)、[main_rotation_test.go](../internal/storage/main_rotation_test.go)、[main_run_rejection_test.go](../internal/storage/main_run_rejection_test.go)、[history_observations_test.go](../internal/control/history_observations_test.go)。64KiB入力・8turn更新・停止/unknown照合・通知再queue・論理session/予算保持 |
| 059 | 合格 | [kernel_source_latency_test.go](../internal/control/kernel_source_latency_test.go)、[loop_test.go](../internal/control/loop_test.go)、同operator controls/directions/interruptions、native上限/停止/RSS、実model完了後の全停止 |

AT-039/049はM、042〜044はP、050は先行Sへ割り当てる。AT-001〜030の製品gateを旧suiteの成功だけで新版P合格へ読み替えない。AT-009の新fix TaskはKで確認済みだが、製品経路への接続は後続である。

## K共通FR/NFRの追跡

| 要件 | Kで確認したもの |
| --- | --- |
| FR-001/004/005 | 固定native版・設定model・session/profile・共通隔離runtime・thread同一性。S/native実接続、[auth/profile/factory tests](../internal/control/isolated_factory_test.go)、managed processの予約と停止証拠 |
| FR-003/006/026 | scope/plan/policy版、strict schema、元要求criterion、64KiB snapshotとmanifest、roleごとの結果契約。上表031/037/046/051/054/057/058 |
| FR-023〜025 | 版付き操作、優先順位、累積turn/token/fix/Attempt/deadline/容量、run/使用量/未知量/根拠の追跡。上表037/040/041/055/059 |
| FR-027〜029 | direct Wave/main、独立Artifact review、clean/findings/inconclusive、必須成果/criterionと完了proof。上表031/036/038/051〜054 |
| FR-030〜033 | 永続購読、通知/ACK/論理適用の分離、IPC hint+再照合、原子的capacity/lease、cursor CLI。上表032〜035/040/041/047/048 |
| FR-036〜040 | Task一対一session、親子message、有界template、上限とprocess停止、main文脈/復旧。上表045〜048/052〜059 |
| NFR-001/007/011 | 通常待機・閲覧・定型反復でmain追加起動0、未完了thread継続、manifest/verified cacheのmissでも同じ根拠。source/template/watch/input-cache testsと実model |
| NFR-002/009 | 原子的保存、通知/ACK crash、unknown起動、停止証拠、再open/再配送のidempotency。上表034/035/037/038/054/058 |
| NFR-003 | private root/socket/認証の分離とLinux launcher probe、controller資格情報へworkerが到達しないこと。隔離factory/Auth testsとnative launcher観測。製品GitHub主体の権限確認はP |
| NFR-004/005/008 | 固定0.163.0生成schemaとadapter、構造化DB/CLI、秘匿値を除いた証拠、clock/process/API注入可能なmodule境界。contract/control/storage/kernelcli tests |
| NFR-006/010 | process/runner/review/Gitの別枠、同thread/worktree排他、独立Task2件並行、待機時の枠解放。reservation/operation tests、native並行観測、race |
| NFR-012/013 | 上記100sampleのp95、実nativeの2 process/第三拒否/RSS/起動停止、待機中の停止と操作競合。環境・設定・stub境界を各JSONへ保存 |

## 再確認と残作業

Go全suite/vet、主要packageのrace、CLI buildは [regression.json](../evidence/background-wave/kernel/regression.json) のcommandで再現できる。新しい実接続は新規private rootと明示envで実施し、既存DBの予算や結果を変更しない。CLIの直接依頼/操作JSONと各commandは [README](../README.md) にあり、cursor/reconnectの再現は `go test ./internal/kernelcli -run 'TestKernelCLIWatch|TestKernelCLIVersionedControls|TestKernelCLISteer' -count=1`、crash/message/main復旧は対応表のGo testで再実行できる。

Task_8のgateway/購読componentを受け入れ、次はTask_11（#42）、Task_12（#35）、Task_13（#43）の順に進める。#40の製品Candidate部分、AT-056の実PR経路、FR-035の製品runtimeへの配置を引き継ぐ。Cloud/CI/rulesがunknownの間は製品Wave/P/全体完成を保留する。運用DB不存在でもM fixtureを省略しない。
