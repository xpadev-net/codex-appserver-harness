# Codex Background Wave ハーネス 実装計画

| 項目 | 内容 |
| --- | --- |
| 文書版 | 0.3 |
| 更新日 | 2026-10-22（Asia/Tokyo） |
| 状態 | 設計案。実装着手の承認前 |
| 要件 | [要件定義書](requirements.md) |
| 差分の根拠 | [現行実装との差分](docs/background-wave-gap-analysis.md)（基点`fd9f745`） |
| 実装順序・担当範囲・検証 | [Background Wave実行計画](docs/coding-agent/plans/active/background-wave-plan.md) |

Waveを永続的な作業単位とし、メインOrchestratorが分解・方向修正を、通常コードのcontrollerがdispatch・購読・通知・外部操作を担う。
階層はWave→Phase→Taskとし、各Taskに独立した使い捨てsession/threadを割り当てる。未完了Taskの待機中はturnを終了し、Task完了後はsessionを再利用しない。Taskは親と同じruntimeを使い、Wave呼び出しだけを禁止する。
親とTaskは永続受信箱を通じて双方向にメッセージを送り、通知から必要なturnを再開する。
PR作成等のtool callはcontrollerを経由し、関連資源の購読を自動登録する。

本書は改訂後の構成と契約を定める。
実装済みの範囲と導入先の未確認能力は[受入検証記録](docs/acceptance.md)を原本とし、本書にある構成を実装済みとはみなさない。

## 1. 構成と責務

```mermaid
flowchart TD
  Input[直接依頼 / Issue pool] --> Main[Orchestrator thread]
  Main -->|tool call / 分解案| Runtime[Controller]
  Runtime --> DB[(Wave / Task / WorkItem / 購読 / 通知 / outbox)]
  Runtime --> Wave[Wave]
  Wave --> Phase[Phase: 実装 / レビュー / fix]
  Phase --> Task[Task / 使い捨てsession]
  Runtime --> Scheduler[Phase条件と実行枠のscheduler]
  Scheduler --> Task
  Scheduler --> Impl[実装Task / 1 session]
  Scheduler --> Verify[検証Task / 1 session]
  Scheduler --> Review[レビューTask / 1 session]
  Main <-->|永続メッセージ / 通知| Impl
  Main <-->|永続メッセージ / 通知| Verify
  Main <-->|永続メッセージ / 通知| Review
  Runtime --> Runner[外部runner]
  Runtime --> GitHub[GitHub tool operation]
  Impl --> Events[永続イベント]
  Verify --> Events
  Review --> Events
  Runner --> Events
  GitHub -->|Webhook / 再照合| Events
  Events --> DB
  DB -->|必要な通知と再開intent| Runtime
  Runtime -->|判断が必要な時に次turn| Main
```

| 領域 | 主な配置 | 責務 |
| --- | --- | --- |
| Wave/Phase/Task | `internal/domain/wave.go`、`internal/storage/waves.go`（新設） | 元要求・版、coverage、Phaseの順序・進行条件、所属Taskと依存、既存WorkItemの関連 |
| Runtime | `internal/service/` | 状態適用、外部操作、イベント取り込み、公開・merge前の再照合 |
| Scheduler | `internal/service/scheduler.go`（新設） | ready集合、実行枠、thread/worktree/共有Git操作の予約と解放 |
| Orchestrator | `internal/service/orchestrator.go`（新設） | 元要求・通知batchをmain threadへ渡し、構造化提案を検証・採用 |
| Subscription | `internal/storage/notifications.go`、`internal/service/notifications.go`（新設） | cursor、通知、配送確認、再開理由とrunの対応 |
| Mailbox | `internal/storage/messages.go`、`internal/service/messages.go`（新設） | 親子間の宛先検証、双方向メッセージ・返信、受信箱と通知 |
| Tool gateway | `internal/service/tools.go`（新設）、`internal/codex/` | 許可済みtoolの検証、operation・購読intent、server request応答 |
| Local IPC | `internal/control/`（新設） | CLI/webhook/runnerからdaemonへ通知、状態streamの再接続 |
| 既存基盤 | `internal/worker/`、`internal/runner/`、`internal/github/`、`internal/review/`、`internal/gate/` | 継続thread、runner、GitHub連携、既存の承認・検証契約 |

新設先は所有範囲を明確にするための配置案であり、各Task内の内部構造まで固定しない。
Go、SQLite、ローカルrunner、単一ホスト・単一controllerを維持する。
実行プランは型付きの構造化出力として保存し、定義済みのPhase条件・Task依存・tool操作をcontrollerが実行する。定型遷移のたびにmain turnを起動せず、未定義の分岐や方向修正だけを親へ返す。
Claudeの生成JavaScript方式との比較は[公式資料の比較](docs/background-wave-gap-analysis.md#公式dynamic-workflows資料との比較)を参照する。本計画は任意スクリプトの実行エンジンを導入しない。
分散brokerや全threadを一つのapp-server接続に多重化する構成は導入しない。

## 2. 管理単位と状態の原本

`OrchestratorSession 1 → N Wave → N Phase → N Task`と`Task 1 → 1 Session/Thread`を基本関係とする。
Taskは一つのPhaseに所属する。親はPhaseを構成し、各Phaseの必要Taskと条件を満たした時に次Phaseを開始する。
初期のPhaseは順次進行し、Phase内では依存・実行枠を満たすTaskを並行実行する。
Phase順序とTask/成果依存を合わせて検証し、先行PhaseのTaskが後続Phaseの成果を待つ分解は拒否する。先行変更のmergeが必要な製品後続Taskは、その成果が成立した後のPhaseへ配置する。
WorkItemはWave内の成果物・候補の管理単位としてTaskと関連付け、同じWorkItemを実装・検証・レビューの複数Taskが参照できる。
WorkItemは一つの主PRに対応し、同じ候補の修正では増やさない。
部分PRの後続・分割・最終受入修正には既存のWorkItem系譜を用い、Taskから全系譜を追跡する。
直接依頼にはIssueを捏造せず、元要求のsourceを`direct`として保存する。
Issue経由はrepository ID・Issue番号・scope revisionと既存claimを維持する。

| 保存対象 | 状態の原本と主な関連 |
| --- | --- |
| Wave | 元要求、要求の版、完了条件、Phase集合、budget ledgerへの参照 |
| Phase | Wave ID、順序、所属Task、開始/完了条件、結果、親による追加・方向修正の版 |
| Task | Phase ID、一つの使い捨てsession ID、親session ID、指示・criterion coverage、入力成果・前Task、依存先、WorkItem関連、pause/cancel、待機理由、終了結果 |
| WorkItem | 既存のPRライフサイクル、candidate、scope/policy、親最終受入・後続系譜 |
| Session / Run | 親またはTask固有のthread、一回の起動intent、turn、結果、使用量。実装/検証/レビューはTaskの指示・成果種別 |
| Message | ID、発行元session・宛先、親子binding、Wave/Task・要求の版、本文・返信先、送信・配送・処理状態 |
| Event | source ID、種別、resource、Wave/Task/WorkItem、要求の版、candidate、payload参照 |
| Subscription / Notification | 対象・種別・再開条件、購読者、cursor、通知ID、配送・処理状態 |
| Operation | 外部操作intent、tool callとの対応、資源marker、送信状態、結果不明と照合情報 |

WorkItemの品質・公開工程は`implement → verify → validate → local_review → publish/review/merge`を維持する。
親が実装・検証・レビュー・fixのPhaseを構成し、その中の別Taskへの指示で担当を分ける。
指摘は親が次のfix Phaseの新Taskへ引き渡す。完了済みTaskを再開せず、更新候補の再レビューも新Taskとする。
プロダクトのPhaseはTaskを束ねる進行単位であり、既存WorkItemの`phase`とは別の型・識別子として扱う。
Phaseの状態は開始/完了条件と所属Taskから評価し、WorkItemの状態を二重転記しない。Attempt/Runは未完了Taskのturn・復旧に用い、完了Taskへの追加作業を表さない。
Waveの進捗はTask/WorkItemと最終受入から集約する。
依存は必要な成果を明示する。検証・レビューTaskは先行Taskの固定候補SHAが利用可能になれば開始でき、先行PRのmergeを待たない。
製品実装の後続で先行変更を取り込む場合はmerge・最終受入を待ち、未merge branchのstackを暗黙に作らない。
レビューの差し戻しは過去Taskへ循環edgeを加える処理にせず、候補SHA・round・指摘を入力とする新しいfix Phase/Taskにする。
再レビューは更新候補を入力とする新Phase/Taskを作り、旧SHAの判定は失効させる。
Task Aのレビュー中でも、独立Task Bは実行できる。
次Phaseの開始は保存したPhase完了条件に従う。失敗・cancel・終了したTask数だけでPhaseを成功にしない。

Waveの`completed`には、対象criterionの充足、必要なWorkItemのmergeと最終受入、残作業の不存在が必要である。
ローカル実装・レビューが終わってもCloud承認が未成立なら、外部待機・保留として理由を表示する。

## 3. セッションとtool call

### 3.1 担当threadの継続

メインは複数Waveを扱えるOrchestratorSessionのthreadを持つ。
各Taskを新しい独立session/threadで開始し、そのTaskが完了するまでの待機・復旧だけに同じthreadを使う。
Task完了後はsessionを再利用せず、独立検証・レビュー・fix・再レビューは親が対応Phaseに新Taskを作り、共通session driverで新sessionを開始する。
使い捨ては論理的な再利用禁止を意味し、Codexの`ephemeral: true`による非永続threadを要求しない。実行中の復旧と監査に必要な履歴は保持し、完了後にpolicyに従ってGCする。
レビューTaskには元要求・固定候補差分・検証証拠と読み取り専用の作業指示を渡す。これはTaskのscope/policyであり、レビュー専用の縮小session runtimeを作る意味ではない。

既存の`Session/Run`とunknown-start照合を共通session driverへ整理する。
threadの同時turnは禁止する。通知は実行中turnを勝手に中断せず、次の安全な開始境界へまとめる。
運用者のpause/cancelは実行中断と区別し、新規入力と通知の優先順位を記録する。
新Taskの入力には元要求・scope/policy版・対象候補・前Taskの構造化成果・指摘・実行証拠・残作業を渡す。過去thread全体の再投入やTask間のcache hitを必須にしない。
cacheの維持を目的とするkeepaliveやプロセス常駐は要求しない。

### 3.2 tool operationの境界

親とTaskは共通session runtime・tool gatewayを使う。初期toolはWave呼び出し、Taskへの指示、待機条件登録、状態取得、親子メッセージ、PR作成・更新、レビュー依頼とする。
Waveの作成・実行・再分割等、入れ子Waveを生む呼び出しは親だけに公開し、Taskには公開しない。controllerも実際のsession/親子bindingから認可し、非公開tool名・別名・中継要求を拒否する。
Taskは必要な再分割・独立レビューを親へメッセージで依頼できる。Wave/Phase/Taskの構造変更は親の管理操作とし、メッセージ本文だけでは適用しない。
名称・入力・応答はTask_1で版付きschemaとして固定する。
直接GitHubを書ける資格情報はモデルの実行環境へ渡さない。
モデルはtoolを選び、controllerがそのsessionに許されたWave・Task・repository・candidate・scope・予算を検証して実行する。
親子に共通のscope・sandbox・予算・検証・承認policyを適用する。Taskであることだけを理由にPR作成や親子メッセージ等の共通toolを剥奪せず、個々の要求がscope/policyを満たすかを検証する。

Codex側は採用版の実験的dynamic tool契約を明示的にopt-inし、`item/tool/call`を許可済みgatewayへ配送する。
その他の承認・入力要求をtool実行と誤認しない。
接続内のRPC応答とserver requestは一つのreaderで振り分け、tool処理が同じreader待ちでdeadlockしない構成とする。

長い外部操作のtool応答は保存済み`operation_id`と`subscription_id`を返せる契約にし、受付を完了と表示しない。
完了は後続イベントで配送する。
分解・方向修正の構造化提案も、coverage・依存循環・権限・要求の版を検証してtransactionで採用する。
自然文の「完了」やtoolの自己申告でgateを成立させない。

## 4. 自動購読と通知再開

### 4.1 PR操作に伴う自動購読

1. tool callのsession/thread/turn/call IDと、Wave/Task/WorkItem・候補・要求の版を確認する。
2. 外部操作intent、再照合marker、未解決資源を対象とする購読intentを同じtransactionへ保存する。
3. controllerだけが既存adapter経由で外部操作を送信する。
4. 応答またはmarker照合からPR ID等を確定し、操作結果・資源binding・購読を同じtransactionで確定する。
5. 操作前に記録したイベント境界以降のraw deliveryと資源の最新状態を照合し、購読確定までの更新を回収する。
6. tool応答にoperation・資源・購読IDと受付/完了/不明の区別を返す。再送は既存の結果へ関連付ける。

購読はPRのissue comment、inline review comment、review、review dismissal、reaction、checks/status、close/merge等を対象にする。
GitHubが対象イベントを配信しない資源・更新は通常コードの差分照合で補う。
Webhook署名、delivery重複排除、pagination、編集/削除・取消、権限不足、rate limitを扱う。
全更新の完全な履歴が取得できない場合は差分の不明範囲を記録し、観測していないコメント等を生成しない。

既存のcontrollerによるPR操作も同じgatewayのoperation作成処理を使い、モデルtoolと二重の公開経路を作らない。
自動購読先は発行元Taskとその待機sessionとし、mainにはpolicyが必要とする集約通知を渡す。
コメント・リアクションはデータであり、肯定的な反応をAPPROVEDへ変換しない。

### 4.2 通知からの再開

イベント適用と通知intentを同じtransactionに保存する。
購読を作る際は保存済みterminal stateとイベント境界も確認し、subscribe-before/after-completionの両方で同じ結果を取得できるようにする。

通知は少なくとも`pending → bound_to_run → applied`を区別する。
clientの配送ackだけでは判断済みにせず、再開intentに通知ID集合・要求の版・再開理由を固定し、対応runの結果を適用してから処理済みにする。
クラッシュ時はそのrunへ再関連付けし、新しいturnを無条件に開始しない。
同一threadの通知群は集約し、実行枠・入力待ち・pause/cancelを再評価してから起動する。

待機条件はDBへ保存してからturnを正常終了する。
その間に届いた通知は保持し、終了を確認した後でのみ次turnを開始する。
新候補・方向修正後の旧通知は履歴に残すが、新版のdispatch理由に自動変換しない。

### 4.3 親子間メッセージと受信箱

親は`send_message(task_id, body, reply_to)`、Taskは`send_message(parent, body, reply_to)`を共通gatewayから呼ぶ。
引数に指定された発行元を信用せず、呼び出したsessionのbindingからsenderを確定する。
宛先は同じ親子関係に限定し、別Wave・兄弟Task・無関係なsessionを指定した転送を拒否する。
Task作成時に親子双方の受信購読を登録し、保存済みメッセージとの競合も通知基盤で回収する。

message、受信箱entry、通知intentを同じtransactionで保存してから、message IDと受付状態を返す。
message IDとtool callの対応で再送を照合し、`reply_to`は同じ親子間の既存messageを参照する。
受信側が実行中なら受信箱へ保持し、turn終了確認後に未処理messageをbatchで入力する。待機中なら同じthreadを次turnで再開する。
一方のthreadが動き続けている間ももう一方の送信toolを待たせず、質問の回答待ちは受信購読を保存してturnを終了する。

メッセージの配送ack、runへの関連付け、判断の適用・返信を分ける。
pauseは自動再開を抑止して未処理messageを保持し、cancel/終端は保存・通知しても自動再開しない。
完了後の追加指示は親が新Taskへ引き渡し、元Taskと成果を関連付ける。完了済みTaskを新attemptとして再開しない。
旧版のmessageは履歴に残し、自動で方向修正や完了を適用しない。

## 5. イベント配送と並行scheduler

単一controllerが管理するローカルUnix domain socketをIPCの通常経路とする。
socketはcontrollerのprivate root内に置き、workerから見えない権限を維持する。
CLIとwebhookはDBへの永続記録後にdaemonへwake hintを送る。
runnerは結果fileを原子的に保存した後、最小のjob IDを通知する。controllerは保存したintentとfileを照合し、runnerからの自己申告だけで成功にしない。
agent completionは同一processのイベント経路へ渡す。

IPCはwake hintと読み取りstreamであり、状態の原本にしない。
通知失敗・daemon不在でも保存済み記録は残し、起動時および低頻度reconciliationで回収する。
worker隔離からprivate socketへ到達させず、信頼された`_runner`ラッパーが結果保存後にjob IDだけの完了hintを送る。
検証コマンドの子processは既存launcherで隔離し、socket・通知設定・controllerの一般操作権限を渡さない。
`_runner`のhintは再照合の契機に限定し、operationやgateの更新命令として扱わない。

schedulerはイベントでready集合を再評価し、LLM・runner・進行中PRの枠をtransactionで予約する。
同一thread/worktreeと共有repositoryのbranch作成・commit・fetch/push等、競合するGit操作を排他する。
待機はLLM枠を解放するが、停止確認のないprocessや不明な起動の枠は再照合まで解放しない。
公平なready順序、pause/cancel、期限・予算超過、終了結果を保存して別Taskの進行を止めない。

tokenはWave/Taskの累積ledgerへrun ID単位で計上し、既存WorkItemの使用量を二重加算しない。
分割・部分PR・最終受入修正も同じWave予算を引き継ぐ。
観測tokenの遅延による上限超過可能性は明示し、次turnの起動を停止する。

## 6. 永続化と移行

新しいschema versionを導入し、Wave/Phase/Task/session binding、message/mailbox、subscription、notification、wake intent、resource binding、予算予約・計上を保存する。
既存`records`をすべて専用tableへ移す整理は今回の必須条件にしない。
既存のID、role_thread key、candidate、outbox、不明操作、scope/policyと親最終受入は保持する。

移行はcontroller停止・未知process照合後、SQLiteの整合したbackupを作って実行する。
既存の一つの元要求のsplit/continuation/repair系譜を一つのWaveへまとめ、保存済み状態からPhaseとTaskを関連付ける。
既存thread ID・履歴・role_thread keyは監査用に保持し、実行中のthreadだけを対応する移行Taskのsessionへ結び付ける。
終了済みのrole threadを後続Taskへ再利用せず、未開始の仕事・fixは新しいPhase/Task/sessionへ引き渡す。
移行bindingには再実行用の安定keyを保存し、未知のturnは再照合完了まで再開しない。
移行が要件上のcoverageを復元できなければ、その作業だけを理由付き保留にし、範囲を推測しない。
version 1のtrusted configは明示的な更新コマンドで新versionへ変換し、既存policy snapshotを上書きしない。
新しい並行上限は旧設定の直列値を引き継ぎ、導入者が2以上に設定して並行性を有効にする。
旧CLIによるWorkItem参照・操作は維持し、Task/Waveのpause等との優先関係を検証する。

新バイナリは未対応schema versionを拒否する。移行後のDBを旧バイナリへ渡さないことは起動・切戻し手順の制約とし、旧バイナリ自身が新versionを拒否できるとは仮定しない。
失敗時は新dispatchを停止し、送信済み外部操作を照合した上でbackupを復元する。
移行後の書き込みがあるDBを単に旧版で開くrollbackはしない。

## 7. 操作面と実装順序

最初の操作面はローカルCLIとtool契約とする。
`wave create`（request fileとOrchestrator session）、`wave list/inspect`、`wave pause/resume/cancel`、`wave steer`（版付き方向修正）、cursor付き`wave watch`を追加する。
これらは実装予定のコマンドであり、現行バイナリでは使用できない。
一覧はTaskの工程・担当・試行・待機理由・成果リンク・使用量へ辿れる構造化出力を提供する。
外部チャット製品への接続や画面再現は別のhost adapterとし、初期完成の条件にしない。

[実行計画](docs/coding-agent/plans/active/background-wave-plan.md)は次の順序で進める。

1. tool/session契約とWave/Phase/Taskデータ・migration。
2. 永続購読・再開とscheduler。
3. process間イベント配送、親子messageとメインOrchestrator。
4. 独立ローカルレビューとGitHub tool・自動購読。
5. Wave操作・観測、回帰・障害注入・限定実接続。

各Taskは小さなPRとしてレビューできる単位とし、依存が満たされ所有範囲が分かれるTaskだけを並行実装する。
AT-001〜030の品質・外部操作を回帰検証する。AT-009は同じthreadのfixから新fix Taskへの引き渡しへ意図的に改訂し、AT-031〜049を各実装PRで検証する。
DB・clock・process・外部APIはfakeで順序とクラッシュを決定的に再現し、並行境界には`go test -race`を加える。
実サービスはdynamic tool往復、thread再開、PR資源購読の代表経路に絞り、未成立のCloud承認をfakeの成功で補わない。

## 8. 外部契約と未確認事項

Codexは`0.163.0`を維持し、必要な実験的tool schemaを同じバイナリから生成して固定する。
[公式App Server資料](https://learn.chatgpt.com/docs/app-server)（2026-10-22参照）のtool呼び出し・thread継続契約を使う。
Waveの購読・通知・再開は独自runtimeの責務とする。
実際のmodel・認証・隔離環境でのdynamic tool往復と同一thread再開はTask_10の限定接続検証まで未確認である。

GitHubの正式承認・CIとSHA・rules/bypassの未確認事項は[既存の接続証拠](docs/issue20-live-connections.md)を引き継ぐ。
これらは自動mergeの可否を制約するが、Waveの実装・レビュー・通知機構の開発を止めない。
