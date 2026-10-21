# Background Waveへの要件差分

- 更新日: 2026-10-22（Asia/Tokyo）
- 調査基点: `7dfa964`（v0.3文書反映後。runtimeは`fd9f745`と同一）
- 役割: 既存要件と実装の差分、採用・変更・保留の判断根拠。
- 要件の原本: [要件定義書](../requirements.md)。設計: [実装計画書](../implementation-plan.md)。実装順序: [実行計画](coding-agent/plans/active/background-wave-plan.md)。

大きな依頼をWave→Phase→Taskとして保持し、Taskごとの使い捨てsessionが仕事を進め、実行中の待機後はイベントで再開する体験を中心へ移す。
現行実装にはthread再利用と個別turnの完了通知があるため、その基盤を活かす。
Wave全体を管理するLLMセッション、永続購読、tool callに伴う自動購読、並行schedulerは追加する。

## v0.4で反映するレビューと判断

v0.3の要件・設計・計画に対する文書レビューと、`7dfa964`の実装レビューを照合した。以下は改訂案であり、実装完了や実接続の成功ではない。

| 指摘 | 採る変更 | 追跡 |
| --- | --- | --- |
| dynamic toolの実往復が未検証のまま後段へ進む | S / Task_0で先行確認。未成立は依存runtimeを保留し、要求配列方式へ自動fallbackしない | #46、AT-050 |
| 初期完成がCloud/merge・移行と一体 | S/K/P/Mへ分離。KはPRなしの新DBで独立受入、P/Mと全体完成の要件は保持 | #48、要件§2.2/11 |
| Phase条件・失敗・反復が曖昧 | all_success、drain_and_block、明示Task終端、有界fix_review templateを定義。例外だけ親の再計画 | #47、FR-038、AT-052〜054 |
| 汎用Taskと大きな成果を表現できない | WorkItem参照0件以上、controller管理Artifact、digest/容量/出所/権限/保持、参照渡し | #48、FR-006、AT-051 |
| 親の構造生成・費用・時間が無制限 | Phase/Task・親turn/再計画・Attempt/反復・期限/容量に既定値と原子的計数 | #49、FR-039、AT-055 |
| main文脈の増大・履歴欠落 | 上限付きsnapshotとWave境界、DB/成果から再構成、旧run照合後の監査付きmain復旧 | #50、FR-040、AT-058 |
| 並行PRの競合・外部messageの中継 | 既存base更新処理を新fix Task方式へ接続し、共有Git排他と出所/認可を試験 | #40/#41/#42、AT-056/057 |
| process常駐・優先操作・性能の未定義 | 待機process停止、pause/cancel/steer/interruptの表、通知p95とRSS/cold startを観測 | #37/#38、NFR-012/013、AT-059 |

migrationはFR-034の保持要求を維持し、schema基盤（Task_2）とlegacy作業移行（Task_12）を分ける。実DBの有無は導入前に確認し、移行対象がない場合はfixture合格と実移行not_applicableを別に記録する。
Phaseの順次barrierは初期方式として維持する。Task成果依存を別fieldにし、将来pipelineを追加する場合はschema版で扱う。quorum/任意fan-out/任意JSは今回追加しない。
現行initにはログ30日・容量1 GiB・照合60秒等があるため、「すべての既定値がない」とは扱わず、新Wave設定へ引き継ぐ値と新たな提案値を区別する。

起票済みの未実装Taskは#34〜#43。現行コードで再現した部分no_actionによる未評価指摘の解決（#44）と、全範囲merge後の要求変更の復旧不能（#45）はコード修正が必要であり、この文書改訂で閉じない。

## 要求の根拠と設計判断

2026-10-22の依頼者の説明から、次を要求として取り込む。

- Waveの下にPhase、その下にTaskを置く。各Taskに使い捨てsessionを割り当て、完了後は再利用しない。
- メインセッションがOrchestratorとなり、PhaseとTaskを管理する。Taskで禁止する追加能力はWave呼び出しとする。
- 親とTaskが双方向messageで質問・回答・指示・blockerを共有し、通知で再開する。
- 待機条件を購読してturnを終了し、通知で必要な作業を再開する。
- PR作成等のtool callに連動して購読し、コメント・リアクション・レビュー等を通知する。

添付画面から他製品の内部設計やAPIは断定しない。
キャッシュ切れがなくなったという依頼者の観測は、実行中のTask継続や共通入力の効率上の前提として扱う。Task完了後のsession再利用の根拠にはしない。
期限の保証や特定の料金・cache hitは、要件にも完了条件にも加えない。

単一ホスト、Go、SQLite、既存WorkItemの継続、ローカルCLIを最初の操作面とすることは設計選択である。
Phaseの順次進行とPhase内Taskの並行実行を初期設計に採用する。全通知でのmain起動、分散broker、Web UIの新規実装は初期範囲に加えない。

## 公式Dynamic Workflows資料との比較

参照日: 2026-10-22。[紹介記事（英語版）](https://claude.com/resources/articles/introducing-dynamic-workflows-in-claude-code)と、記事からリンクされた[公式workflow資料](https://code.claude.com/docs/en/workflows)を確認した。指定された日本語URLは取得できなかったため、同じ記事の英語版を参照した。
紹介記事は、並行処理・独立検証・反復・進行の保存という方向を裏付ける。
公式資料は、生成したJavaScriptを会話と別のruntimeが実行する方式を説明している。
`phase()`は進捗表示のグループ、`parallel()`等は実行と待ち合わせを担うため、表示階層だけからPhase直列barrierを断定しない。

本ハーネスでは次の区別を保つ。

| 論点 | この計画での扱い |
| --- | --- |
| 進行の保持 | 型付きの構造化プラン・結果をSQLiteへ保存し、定義済み遷移をGoのcontrollerが進める |
| Phaseの順序 | 初期は順次進行という設計案。外部製品の`phase()`の意味をそのまま採用したものではない |
| スクリプト実行 | 任意JavaScriptの生成・実行・replayエンジンは初期範囲に追加しない。採る場合は実行境界・検証・移行を含む別の設計判断が必要 |
| 参考資料の範囲 | Taskの使い捨て・Wave呼び出し禁止・親子message・PR自動購読は依頼者の要求として扱い、この記事が内部契約まで保証したとは記載しない |

## 中心となる差分

| 領域 | 既存要件・実装 | 改訂案 | 対応 |
| --- | --- | --- | --- |
| 依頼の入口 | GitHub Issue pool | 直接依頼とIssue poolからWaveを作る。Issueの作成を必須にしない | FR-027、Task_2/6 |
| 管理単位 | 一つの主PRを持つWorkItem | Wave→Phase→Task 1対1の使い捨てsession。WorkItemは成果参照として複数Taskから関連付ける | FR-027/034/036、Task_2 |
| メイン担当 | Goのcontrollerが順次進行 | LLMのOrchestratorが分解・方向修正・例外判断、controllerが実行を管理 | FR-028、Task_6 |
| 担当の工程 | WorkItem×roleの実装・検証thread | 親が独立検証・レビュー・fixをPhase内の新Taskへ指示する。完了後のsessionは再利用しない | FR-029/036、Task_7 |
| 親子の連絡 | 専用のmessage/mailboxなし | 双方向message・返信・受信購読、実行中の保留と待機中の再開 | FR-037、Task_3/6/8 |
| Taskの能力 | 固定role、native機能無効化を伴うworker起動 | 親子共通runtime。TaskからのWave呼び出しはtool非公開とcontroller認可で拒否 | FR-036、Task_1/8 |
| 待機 | 個別turnは通知受信、controller全体はtimerでTick | 購読・通知・再開理由を永続化し、イベント起点でdispatch | FR-030/031、Task_3/5 |
| GitHub操作 | controllerの内部操作とoutbox | モデルが呼ぶtool operationを公開し、同じcontrollerの権限・outboxを使う | FR-035、Task_1/8 |
| PRの通知 | Webhook保存と状態照合、一般の購読者なし | tool call時に購読intentを保存し、作成資源へ自動関連付け。コメント・リアクション等も配送 | FR-035、Task_8 |
| 並行性 | active turn/PR/job各1、busy時は他Readyを開始しない | 独立Task・外部待機中の別Taskを上限内で進行 | FR-032、Task_4/5 |
| 観測 | WorkItem単位のCLI・metrics | Wave→Phase→Task→session/runと通知・待機理由を一覧・購読 | FR-033、Task_9 |

## 実装上の根拠

リンクは調査基点での行を指す。行番号が変わった場合は記載した識別子を検索する。

| 根拠 | 観測した事実 | 計画への影響 |
| --- | --- | --- |
| [WorkItem/Event/Operation](../internal/domain/workflow.go#L25) | Wave・subscription型はない | WorkItemの単なる改名では表現できない |
| [Controller.Run/Tick](../internal/service/service.go#L129) | 固定間隔Tick、同期handle一件、busyによる開始抑止 | 非同期実行とready集合の再評価が必要 |
| [LiveAgents.Run](../internal/service/agents.go#L75) | WorkItem×roleのthreadを記録し再利用 | 未完了Taskの復旧に既存thread/run分離を使い、完了後の再利用は取りやめる |
| [Worker.Prepare/Execute](../internal/worker/worker.go#L63) | implementation/verification限定、turn/start、通知から完了を判定 | 共通session driverで親とTaskを扱い、目的はTaskへの指示として表す |
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

- FR-001〜026の固定候補、独立検証、明示承認、未確認のmerge拒否、権限分離、予算、残作業と親最終受入。ただしFR-004/013の完了後thread再利用は下記の通り変更する。
- AT-001〜030の品質・外部操作の回帰条件。AT-009の修正手段は同じthreadの再開から新fix Taskへの引き渡しへ変更する。ローカルレビュー成功をGitHubのAPPROVEDへ代入しない。
- 単一controller所有、送信済み・応答不明の外部操作を無条件に再送しないこと。

### 変更するもの

- Wave→Phase→Taskを明示し、Phase内Taskを並行実行する。Phase間は保存した進行条件を評価する。
- Task/sessionを1対1の使い捨て単位とし、実行中の待機・復旧だけで同じthreadを使う。完了後のfix・再レビューは新Task/sessionへ渡す（FR-004/013/036、AT-009/036/045）。
- 既存role threadは履歴を保存し、実行中のものだけを移行Taskへ結び付ける。終了済みthreadは後続へ再利用しない。
- Taskへ親と同じruntimeを用い、Wave呼び出しを禁止する。再分割や独立レビューは親へmessageで依頼する。
- 直列実行は初期固定制約から設定値へ変更し、並行実行を初期完成条件へ昇格する。
- Issue取得を唯一の入口にせず、直接依頼にも要求・coverage・汎用成果を持たせる。WorkItemは製品/PR成果がある場合に関連付ける。
- 全体進行をtimerに依存させず、イベントとローカルIPCを通常経路にする。
- PR作成をtool operationとして呼べるようにし、手動のsubscribe忘れを防ぐ。

### 増やさないもの

- 新しいCloud承認方式、自動承認の緩和、merge queue/stacked PR、複数ホスト。
- キャッシュ常駐を守るための無意味なkeepalive turn。
- コメントやリアクションごとの無条件LLM再実行、外部テキストへの操作権限付与。

## 完成を確認する差分

AT-031〜059をK/P/Mの適用と別々の証拠で確認し、Task DAG、並行性、購読と完了の競合、crash後の通知、同一threadの二重起動抑止、方向修正、移行、自動購読、Wave/Phase/Task所属、使い捨てsession、Wave呼び出し禁止、親子messageを確認する。
Kの最小実演は「直接依頼→汎用成果を作る独立Task 2件→レビュー→templateの新fix/review Task→報告、親子messageと通知再開」とする。PではPR作成tool→自動購読→外部通知→正式承認・mergeを追加する。
Cloudの正式承認等がunknownならPと製品Waveは保留する。Kの合格は独立して記録できるが、自動mergeや全体完成とは報告しない。

この改訂は文書のみであり、AT-031〜059の合格や新runtimeの完成を主張しない。
