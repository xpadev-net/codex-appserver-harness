# Background Waveへの要件差分

- 更新日: 2026-10-22（Asia/Tokyo）
- 調査基点: `fd9f745`
- 役割: 既存要件と実装の差分、採用・変更・保留の判断根拠。
- 要件の原本: [要件定義書](../requirements.md)。設計: [実装計画書](../implementation-plan.md)。実装順序: [実行計画](coding-agent/plans/active/background-wave-plan.md)。

大きな依頼をWaveとして保持し、担当セッションが工程を進め、待機後はイベントで再開する体験を中心へ移す。
現行実装にはthread再利用と個別turnの完了通知があるため、その基盤を活かす。
Wave全体を管理するLLMセッション、永続購読、tool callに伴う自動購読、並行schedulerは追加する。

## 要求の根拠と設計判断

2026-10-22の依頼者の説明から、次を要求として取り込む。

- 大きな依頼からWaveオブジェクトとTask群を作る。
- メインセッションがOrchestratorとなり、担当セッションが実装・レビュー・修正を進める。
- 待機条件を購読してturnを終了し、通知で必要な作業を再開する。
- PR作成等のtool callに連動して購読し、コメント・リアクション・レビュー等を通知する。

添付画面から他製品の内部設計やAPIは断定しない。
キャッシュ切れがなくなったという依頼者の観測は、同じ担当threadを再利用する効率上の前提として扱う。
期限の保証や特定の料金・cache hitは、要件にも完了条件にも加えない。

単一ホスト、Go、SQLite、既存WorkItemの継続、ローカルCLIを最初の操作面とすることは設計選択である。
全Taskの実装完了までレビューを止めるbarrier、全通知でのmain起動、分散broker、Web UIの新規実装は初期範囲に加えない。

## 中心となる差分

| 領域 | 既存要件・実装 | 改訂案 | 対応 |
| --- | --- | --- | --- |
| 依頼の入口 | GitHub Issue pool | 直接依頼とIssue poolからWaveを作る。Issueの作成を必須にしない | FR-027、Task_2/6 |
| 管理単位 | 一つの主PRを持つWorkItem | Wave→Task→WorkItem。既存PR系譜は保持 | FR-027/034、Task_2 |
| メイン担当 | Goのcontrollerが順次進行 | LLMのOrchestratorが分解・方向修正・例外判断、controllerが実行を管理 | FR-028、Task_6 |
| 担当の工程 | 実装・独立検証threadとCloud review | 読み取り専用のローカルReviewerを追加し、fixを同じ実装threadへ返す | FR-029、Task_7 |
| 待機 | 個別turnは通知受信、controller全体はtimerでTick | 購読・通知・再開理由を永続化し、イベント起点でdispatch | FR-030/031、Task_3/5 |
| GitHub操作 | controllerの内部操作とoutbox | モデルが呼ぶtool operationを公開し、同じcontrollerの権限・outboxを使う | FR-035、Task_1/8 |
| PRの通知 | Webhook保存と状態照合、一般の購読者なし | tool call時に購読intentを保存し、作成資源へ自動関連付け。コメント・リアクション等も配送 | FR-035、Task_8 |
| 並行性 | active turn/PR/job各1、busy時は他Readyを開始しない | 独立Task・外部待機中の別Taskを上限内で進行 | FR-032、Task_4/5 |
| 観測 | WorkItem単位のCLI・metrics | Wave→Task→工程→担当→試行と通知・待機理由を一覧・購読 | FR-033、Task_9 |

## 実装上の根拠

リンクは調査基点での行を指す。行番号が変わった場合は記載した識別子を検索する。

| 根拠 | 観測した事実 | 計画への影響 |
| --- | --- | --- |
| [WorkItem/Event/Operation](../internal/domain/workflow.go#L25) | Wave・subscription型はない | WorkItemの単なる改名では表現できない |
| [Controller.Run/Tick](../internal/service/service.go#L129) | 固定間隔Tick、同期handle一件、busyによる開始抑止 | 非同期実行とready集合の再評価が必要 |
| [LiveAgents.Run](../internal/service/agents.go#L75) | WorkItem×roleのthreadを記録し再利用 | 継続threadと一回のrunを分ける既存契約を使う |
| [Worker.Prepare/Execute](../internal/worker/worker.go#L63) | implementation/verification限定、turn/start、通知から完了を判定 | 共通session driverとOrchestrator/Reviewer roleを追加 |
| [Client](../internal/codex/client.go#L31) | stdio reader/channel、単一consumer、server requestは入力要求扱い | 全sessionの一接続多重化は不要。許可済みtool request専用の応答経路を追加 |
| [Store.Open](../internal/storage/store.go#L33) | WAL、単一connection、schema version 1、JSON payload/records | versioned migration runnerと移行fixtureが必要 |
| [Store.Apply](../internal/storage/store.go#L138) | inbox重複排除、状態とoutboxをtransaction保存 | 通知intent・再開intentも同じ原則で保存 |
| [CLI](../internal/service/cli.go#L124) | webhookは別process、operatorも直接DBへ書く | in-memory channelのみでは即時起動できない |
| [FileJobs](../internal/service/service.go#L550) | runnerは独立process、結果fileを後で読む | runner完了のIPCと保存済みfileからの回復が必要 |
| [Config.Load](../internal/service/config.go#L69) | strict JSON、version 1、repository外の設定・DB | 設定更新・旧設定の明示移行を計画に含める |
| [受入検証](acceptance.md) | 実モデル往復・Cloud正式承認等は未確認 | 新Wave機能の証拠と既存外部能力の成立を混同しない |

### tool callの成立範囲

2026-10-22にローカルの`codex-cli 0.163.0`で`codex app-server generate-json-schema --experimental --out DIR`を一時ディレクトリへ実行した。
`ThreadStartParams.dynamicTools`、`DynamicToolCallParams`の`threadId / turnId / callId / tool / arguments`、応答の`contentItems / success`が生成されることを確認した。
コミット済みの`protocol/codex/ThreadStartParams.json`には実験的fieldがなく、既存clientもtool requestを実行しない。
この観測はschemaの存在の証拠であり、実モデルのtool選択・成功・再開の証拠ではない。

[公式App Server資料](https://learn.chatgpt.com/docs/app-server)のDynamic tool calls節も参照した（2026-10-22）。
実験的APIのopt-inと`item/tool/call`への応答を要するため、Task_1で採用版の生成schema・契約テストを固定する。
購読やWave再開はハーネス独自の契約であり、Codex標準のPR購読機能があるとは仮定しない。

## 引き継ぐ要求と変更する前提

### 引き継ぐもの

- FR-001〜026の固定候補、独立検証、明示承認、未確認のmerge拒否、権限分離、予算、残作業と親最終受入。
- AT-001〜030の回帰条件。ローカルレビュー成功をGitHubのAPPROVEDへ代入しない。
- 単一controller所有、送信済み・応答不明の外部操作を無条件に再送しないこと。

### 変更するもの

- 直列実行は初期固定制約から設定値へ変更し、並行実行を初期完成条件へ昇格する。
- Issue取得を唯一の入口にせず、直接依頼にも要求・coverage・WorkItemを持たせる。
- 全体進行をtimerに依存させず、イベントとローカルIPCを通常経路にする。
- PR作成をtool operationとして呼べるようにし、手動のsubscribe忘れを防ぐ。

### 増やさないもの

- 新しいCloud承認方式、自動承認の緩和、merge queue/stacked PR、複数ホスト。
- キャッシュ常駐を守るための無意味なkeepalive turn。
- コメントやリアクションごとの無条件LLM再実行、外部テキストへの操作権限付与。

## 完成を確認する差分

AT-031〜044を追加し、Task DAG、並行性、購読と完了の競合、crash後の通知、同一threadの二重起動抑止、方向修正、移行、自動購読を確認する。
最小の実演は「直接依頼→二つの独立Task→各Taskのレビュー・修正→PR作成tool→通知待機→コメント等から担当再開」とする。
Cloudの正式承認等がunknownなら実演はそこで保留し、Wave全体や自動mergeの完成とは報告しない。

このPRは文書改訂であり、AT-031〜044の合格や新runtimeの完成を主張しない。
