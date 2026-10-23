# Task_8: GitHub gateway・購読 component の受入

requirements.md / implementation-plan.md v0.4、実装基点 `a22b663`（PR #117〜136）。判定は **Task_8 の Go component / fixture 合格**。製品 Wave 全体の P 合格ではない。

## 判定範囲

共通 session gateway、外部 PR adapter、永続購読/通知、外部観測の読取を受け入れる。外部送信は、caller が canonical source gate と transport を明示して controller owner 下で `product.Dispatcher` / `Publisher` / `ReviewRequester` を使う契約である。保存した operation の受付や review 依頼受付を、公開・正式承認・merge・Wave 完了へ読み替えない。

新しい P source の作成/同期、製品 Task の実行準備/候補結果、daemon の書込 dispatcher 配置、製品 gate/merge は Task_11（#42、製品候補は #40）へ引き継ぐ。`wave run` の P tool 公開だけで製品 PR パイプラインが一通り動くとは主張しない。K は既存の独立合格を維持し、P の外部能力と M fixture は未合格のまま扱う。

## Task_8 の条件と証拠

| 条件 | component 判定 | 実装・検証 |
| --- | --- | --- |
| session/turn/call の検証、送信前の operation/購読保存、直前の候補/scope/権限/pause/cancel 照合 | 合格 | [受付](../internal/storage/product_pr_intents_test.go)、[claim/容量/一度だけの送信 fence](../internal/storage/product_pr_dispatch_test.go)、[production Runtime の native RPC fixture](../internal/control/product_tools_test.go)。[Publisher](../internal/product/publisher_test.go) と [ReviewRequester](../internal/product/review_request_test.go) は unknown の保存後にだけ HTTP mutation を許す。[canonical gate](../internal/service/product_gates_test.go) は実 git worktree と既存 journal の検証証拠を読む |
| 共通 gateway と親子 message 認可を再利用し、外部本文から権限/承認を作らない | 合格 | [外部出所の message 継承](../internal/storage/message_sources_test.go)、[共通 message 境界](../internal/storage/messages_test.go)、[native tool の role と K 拒否](../internal/control/product_tools_test.go)。Task に Wave tool を公開しない。中継 source の run label を改変しても本文読取 grant にしない |
| PR 成功/不明応答の marker 照合と資源 binding、登録境界の raw/最新状態の回収 | 合格 | `TestLostPublicationResponseReconcilesOnlyItsOwnMarkerWithoutResend`、[binding の原子性/停止証拠/rollback](../internal/storage/product_pr_resources_test.go)、`TestPRWebhookCatchupRecoversRegistrationBoundaryWithoutBodyAuthorityOrDuplicateInput`（[catchup](../internal/storage/product_pr_webhook_catchup_test.go)）。不明送信の不在や別 marker は再送許可にしない |
| comment/inline/reaction/review/取消/CI/close の識別と重複排除、取得不能の明示 | 合格 | [bounded API collector](../internal/product/resource_collector_test.go)、[pagination/byte 上限](../internal/github/resource_reads_test.go)、[canonical diff](../internal/storage/product_pr_updates_test.go)、[署名と repository 固定の raw ingress](../internal/storage/product_pr_inbox_test.go)、[HTTP 配置](../internal/kernelcli/pr_webhook_test.go)。unavailable な section では以前の事実を保持し、削除や完全取得を捏造しない |
| 関連 Task への永続通知、同じ call の非重複、不要 main wake 抑止 | 合格 | [割当 Task の read 購読](../internal/storage/product_task_subscriptions_test.go)、[operation 完了通知](../internal/storage/product_pr_notifications_test.go)、[API 差分通知](../internal/storage/product_pr_update_notifications_test.go)、catchup の backlog/cursor/再open。明示 wait がなければ deferred、旧 scope/cancel は archived。Task dispatch の製品 readiness 解放は Task_11 |

[独立 observer](../internal/product/observer_test.go) / [書込 pool](../internal/product/dispatcher_test.go) は、遅い資源と別資源/DB 読取の並行、失敗の非 starvation、cancel join、prepared 再open、unknown 非再送を検証した。[daemon lifecycle](../internal/kernelcli/pr_observer_test.go) は全 worker/handler の join 前に store/controller lock を解放しない。

[外部 context](../internal/storage/external_context_test.go) は、現在 run の直接通知に限る raw bytes / 不変 snapshot の UTF-8 page、digest、後続 scan と再open後の同一版、peer/ID 推測/relay-only/破損 source の拒否を確認した。schema は product_v2 の明示 opt-in で公開し、kernel_v1 / product_v1 と既存 thread を変更した扱いにしない（[profile schema](../internal/contract/product_tools_test.go)、[profile 保存](../internal/storage/session_tools_test.go)）。

## AT の境界

- AT-020/021/042/043: 署名/raw delivery・差分・登録前後・応答欠落・再送・再open・通知 binding の Task_8 部分を fixture 合格。実サービスを用いる P 全体は Task_13 で別判定する。
- AT-015/017/018/019/044: 本文や reaction から承認を作らず、source SHA/取消/取得不能を保持する部分を fixture 合格。現在候補への正式 approval / CI mapping / gate 失効の製品工程接続は Task_11/13 に残す。
- AT-046/047/057: 共通 sender/宛先/返信/現 run/版の検証、出所保持、Task の Wave tool 拒否を fixture 合格。K の既存合格は [K受入表](background-wave-acceptance.md) を参照する。
- AT-014/016/022/056: 期待 SHA merge、製品 findings から新 fix Task への工程、merge 応答不明、実 PR/base 競合は、この component 受入で全体合格にしない。Task_11/13 で検証する。

## 検査の記録と再現

各実装 PR は GitHub の default branch `master` を base とし、`codex exec review --base origin/master` の actionable findings がなくなるまで追補 commit と再 review を行ってから merge した。PR #135 は CodeRabbit の所有者検査を修正し、native review の UID mapping fixture 指摘も修正後、3 回目で clean。root の別所有者実 file fixture は skip なしで成功した。PR #136 は native 1 回目で clean。review sandbox の socket 制限/ディスク不足は test 成功には数えず、root と CI の結果を区別して PR に記録している。CodeRabbit の rate limit / Devin の skip は承認ではない。

PR #136 の検証は最新 source の `go test ./...` / vet / build と GitHub Go CI が成功。5 package の full race baseline（contract/worker/control/storage/kernelcli）に続き、直接通知へ絞る変更後の storage message/context 対象 race も成功した。full baseline と最後の対象検査を同一 head の full race と表現しない。Go は 1.26.5、通常 suite の opt-in native skip と既存 K の実接続証拠も分ける。

```sh
go test ./...
go test -race ./internal/contract ./internal/product ./internal/github ./internal/storage ./internal/control ./internal/worker ./internal/kernelcli ./internal/service
go vet ./...
go build ./...
```

[追加の共通 Source 参照回帰](../evidence/background-wave/product/task8-source-reference.json) は実 KernelSource/Loop/SQLite/IPC と K の mailbox fixture、独立 Task 2 件で 100 通知 / 100 予約 / 100 適用、main 起動 0。p95 63.155 ms、最大 75.877 ms で、SHOULD 目標 1 秒以内を満たした。追加した P reconciliation 入口を含む共通経路の回帰であり、登録済み PR の大量配送、HTTP、native model、Cloud の性能証拠にはしない。

```sh
mkdir -p evidence/background-wave/product
HARNESS_KERNEL_LATENCY_EVIDENCE="$PWD/evidence/background-wave/product/task8-source-reference.json" \
  go test ./internal/control -run '^TestKernelSourceNotificationReservationReferenceLoad$' -count=1 -v
```

## 次の工程

Task_11 では新 P source/WorkItem/Candidate と common kernel を接続し、caller 側の source owner/容量/lease を一貫させる。既存 WorkItem loop を kernel と無確認に同時実行しない。Task の製品入力/worktree/readiness/停止後の候補結果、canonical gate の配置、daemon 書込 dispatch、Issue/direct、部分 PR/分割、base 更新と競合、新 fix Task、正式承認/CI/rules と期待 SHA merge、親最終受入を完成させる。

[#20](https://github.com/xpadev-net/codex-appserver-harness/issues/20) の実 Cloud / CI / non-bypass 能力は unknown を維持する。開発用 PR の手動 merge や component fixture を正式承認の実成立へ読み替えない。Task_12（#35）の offline migration fixture、Task_13（#43）の P/M 受入へ続ける。運用 DB は依頼者確認で存在しないが、M fixture は省略しない。
