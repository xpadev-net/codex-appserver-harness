# Plan: Background Waveの実装

- status: in_progress
- document_version: 0.4
- generated: 2026-10-22
- last_updated: 2026-10-22
- work_type: mixed

## Goal

大きな依頼をWave→Phase→Taskへ分解し、各Taskに使い捨てsessionを割り当てる。Taskは親と共通runtimeを使ってWave呼び出しだけを禁止し、実行中は親子メッセージと通知で再開する。完了後のfix・再レビューは新Task/sessionへ引き渡す。
本計画を含むPRの範囲は要件・差分・設計・計画の改訂までとする。
Task_0/1/2は完了（PR #54〜60）。Task_3はPR #61で永続購読・終端通知を実装し、PR #62で親子受信箱を実装し、PR #63でwake/run入力bindingを実装し、PR #64でkernel tool認可・操作履歴を実装し、Artifact公開を接続中。runtimeの自動分岐・品質gate接続はTask_7で行う。Task_4〜13はpending。依頼者の実装・逐次PR/review/mergeの指示に基づいて進める。Task_1〜10の既存番号は保持し、役割分割は下記の追跡表へ記録する。

## Definition of Done

- S: Task_0で無害なdynamic toolの実往復と同一未完了threadの再開を確認し、未成立なら依存runtimeへ進まない。
- K: Task_1〜7/9〜10でIssue/WorkItem/PRを持たないWaveを新DBで実行し、汎用成果・独立レビュー・有界fix反復・Task 2件の並行性・通知/親子message・CLI・復旧を確認する。
- P: Task_8/11/13で既存PR工程を接続し、正式承認・merge・親最終受入・自動購読・base競合を確認する。Cloud未成立はPと該当製品Waveをblockedとし、Kの合格と分ける。
- M: Task_12で既存ID/thread/予算/unknown操作/旧CLIを保持する移行を確認する。Kは別rootの空DBで検証し、M前に旧作業を新runtimeへ切り替えない。
- 全体完成は[要件§11](../../../../requirements.md#11-段階別の完了条件)のK/P/M全部、FR-001〜040、NFR-001〜013、AT-001〜059の適用と証拠が揃った時だけとする。
- 未確認能力・未実行・既知不具合を受入表へ記録し、文書の更新やfake成功をruntime完成の証拠にしない。

## Planner-added requirements

- ローカルUnix domain socketによるwake hintとcursor付き状態streamを設ける。理由: CLI・Webhook・runnerが別processであり、in-memory通知だけでは要求されたイベント再開を接続できないため。分散brokerは加えない。
- schema基盤とlegacy移行を分ける。Task_2でversion検査/新DBを、Task_12で旧作業・停止/backup/復旧を扱う。既存DBの不存在は仮定せず、Kをlegacy移行完了へ依存させない。
- 同じバイナリの実験的dynamic tool schemaを固定し、許可済みtool requestを処理する。理由: モデルからの実tool callを実現し、GitHub資格情報をworkerへ渡さず既存controllerで実行するため。
- 同一thread/worktreeと共有Git操作の排他、通知とrunの関連付けを原子的に保存する。理由: 並行Taskと再配送が通常動作になり、二重起動・競合書き込みを防ぐ必要があるため。
- 最初の操作面をローカルCLIと構造化tool契約に限定する。理由: 本リポジトリにhost UIがなく、外部チャットへの自動入力機能を仮定せずWaveの実行・観測を完成させるため。
- 初期のPhaseを順次進行とし、Phase内のTaskを並行実行する。理由: 観測されたWave→Phase→Task階層を進行可能な契約に具体化するため。複雑なPhase間DAGや重なり実行は加えず、次Phaseの開始条件を明示する。これは他製品の内部実装の断定ではなく設計案である。

## Scope / Non-goals

- Scope: Wave→Phase→Task 1対1の使い捨てsession、main thread、検証・レビュー・fixの新Task、Phase進行条件、親子メッセージと受信箱、Wave呼び出し禁止、購読・通知・再開、tool gateway、自動購読、並行scheduler、既存DB/設定移行、CLIと証拠。
- Non-goals: Web UIや添付画面の再現、外部チャットhost adapter、複数ホスト、分散broker、stacked PR/merge queue、Cloud承認方式の変更、任意JavaScriptの生成/実行/replayエンジン、cache寿命の保証。
- 実装の契約原本は[実装設計](../../../../implementation-plan.md)。各Taskはそれを満たし、内部構造の細部は担当Workerが決める。

## Design

- Chosen: 継続threadと通常コードcontrollerの二層構成、SQLiteを原本とするWave/Phase/Task・購読・outbox、単一daemonとローカルIPCを採用する。Structure: 状態のownerをDB/controllerへ寄せ、親とTaskは同じsession driverとtool gatewayを使い、TaskはWave呼び出しを禁止する。各Taskのsessionは完了後再利用せず、次のPhase/Taskへ構造化成果を渡す。WorkItem状態をTaskへ二重管理しない。メッセージは既存の通知基盤に載せる。Evolution: [要件定義書](../../../../requirements.md)のWave中心の方向へ既存PR基盤を接続し、brokerやhost UIを独立拡張にする。Verification: clock・process・APIを注入し、順序/再配送/crashをローカルで再現する。Operation: 待機はモデル枠を消費せず、IPC障害時は永続記録と低頻度再照合へ縮退する。Human: Wave→Phase→Task→session/runと再開理由を一貫したIDで追跡する。Safety: 外部操作・購読・親子宛先・Wave呼び出し権限はcontrollerが検証し、資格情報とprivate socketをworkerから隠す。
- Alternative: 現行のWorkItem/Tickにgoroutineと一時的channelだけを追加する。Structure: 新概念は少ないが購読・再開・Wave状態を会話やprocessに分散する。Evolution: 変更量は小さいが、再起動や複数Wave対応時に通知基盤を作り直す。Verification: 正常系は安価、別process・購読競合・再起動を一貫して再生しにくい。Operation: timer一巡待ちと通知喪失が残る。Human: 複数PRは進むがWaveと待機理由を辿れない。Safety: shared worktreeや不明turnを個別の場当たり的な排他で守ることになる。
- Script alternative: 生成JavaScriptを隔離runtimeで実行する方式。Structure: 分岐/反復をscriptへ移すが言語runtimeと永続replayの境界が増える。Evolution: 表現力は高いが現在のGo/SQLite/controllerに別実行基盤を足す。Verification: scriptの決定性と副作用replay試験が必要になる。Operation: mainの介入は抑えられるがscript障害の復旧手段が増える。Human: 生成codeを読んで再実行できる一方、調査対象が二つのruntimeになる。Safety: scriptとtoolの権限境界を別途定義する。初期は既存controller上の型付きプランで要求された体験を実現し、任意scriptは採用しない。
- Why chosen: 単なる並列化では要求の中心であるWave・購読・通知再開・tool連動を満たせない。既存SQLite/outbox/session基盤を使い、必要な永続状態とIPCだけを追加する。Task内に実装/検証/Reviewer専用sessionを自動追加する別案は、Task 1対1の使い捨てsessionという依頼に反し、再開・メッセージ宛先と履歴のownerを増やすため採用しない。独立検証・レビュー・fixはPhase内の新Taskで表す。

## Compatibility stance

- surface: WorkItem/records/outboxの永続形式、version 1 trusted config、既存CLI、Codex wire schema。
- stance: staged migration（Kは新DB、Mで既存作業移行）
- justification: 永続consumerは`internal/storage/store.go`、`internal/service/cli.go`、`internal/worker/worker.go`とREADMEにあるCLI契約。DBはrepository外で保持されるため、探索で利用中DBを否定できない。既存ID・系譜・証拠・unknown操作は保持し、既存role threadは履歴として保持し、実行中のものだけを移行Phase/Taskへ関連付ける。終了済みthreadは新Taskで再利用しない。旧CLIのWorkItem操作は維持する。追加tool schemaは同一Codex版の実験的生成物へ明示移行し、旧schemaを静かに再解釈しない。
- 外部チャットhostとの接続契約は未定義であり、本計画は外部製品がこのmain threadを自動表示・再開できると約束しない。

## Context (workspace)

- 調査基点: `fd9f745`。詳細とsourceリンクは[差分分析](../../../background-wave-gap-analysis.md)。
- `internal/service/service.go`は単一owner、timer駆動Tick、同期handle、busy時のReady抑止を持つ。
- `internal/worker/worker.go`と`internal/codex/client.go`はthread継続・個別turn通知・開始不明時の履歴照合を持つ。roleは実装/検証に限定される。
- `internal/storage/store.go`にはversion 1、inbox/outbox、汎用recordsとWorkItem単位のleaseがある。移行runnerや永続subscriberはない。
- `internal/service/cli.go`、`internal/service/validation.go`、`internal/runner/runner.go`のCLI・Webhook・runnerはprocess境界を跨ぐ。
- 生成済み`protocol/codex/ThreadStartParams.json`にはdynamicToolsがない。同じローカル0.163.0で実験的schemaを一時生成し、dynamicToolsとtool request/responseが存在することを確認済み。
- [受入検証記録](../../../acceptance.md)と[実接続記録](../../../issue20-live-connections.md)は、実モデル往復・Cloud正式承認等の未確認を保持する。
- repository rule suiteとlessonsは調査時に存在しない。bootstrapは今回の対象外。Researcherによるread-only調査を実施済み。
- 公式Dynamic Workflowsの紹介記事と実行資料を参照し、スクリプト実行方式と本計画の型付きプラン方式を[比較](../../../background-wave-gap-analysis.md#公式dynamic-workflows資料との比較)した。Phaseを順次とするのは初期設計案であり、公式`phase()`の実行保証ではない。
- Design record consulted: 既存要件・実装計画を改訂した本PRが判断案を保持する。別ADRは提案しない。

## Open Questions (max 3)

- Q1: 実model/認証/隔離下のdynamic tool往復と同一未完了thread再開は未確認。Task_0で先行確認し、未成立なら依存runtimeをblockedとする。outputSchema要求配列への自動fallbackは採らず、契約版とATを再計画する。
- Q2: 導入先GitHub主体でreaction等の購読・差分取得がどこまで可能か。Task_8でadapter契約と不足範囲を実装し、Task_13で権限・代表経路を観測する。通知非対応は通常コードの差分照合で補い、取得不能は明示する。
- Q3: Cloud正式APPROVED・候補SHA対応・非bypass rulesという既存接続条件が成立するか。既存capability reportを維持し、未成立ならP・製品Wave・全体完成を保留する。Sが成立すればKの実装・独立受入は進められる。

## Assumptions

- A1: Go、SQLite、Linuxの隔離launcher、ローカルrunner、Codex 0.163.0を維持する。source: `README.md`、`go.mod`、`internal/service/config.go`、`protocol/codex/manifest.json`。
- A2: 既存DB/CLIは移行対象のconsumerである。source: `internal/storage/store.go`、`internal/service/cli.go`、READMEの状態保存先と操作契約。repository外の実DBの有無は未確認であり、移行試験はfixtureを使う。
- A3: 同一threadの継続は採用版のschemaとworker実装で表現できる。source: `protocol/codex/ThreadResumeParams.json`、`internal/worker/worker.go`。実モデル・cache効果の成立はQ1の検証まで未確認。
- A4: cache期限の改善は依頼者の運用観測である。source: 2026-10-22の会話。公式な無期限保証として実装しない。
- A5: 階層はWave→Phase→Taskで、Taskは使い捨てsession、Wave呼び出し禁止と親子メッセージを持つ。source: 2026-10-22の追加依頼。「waveの下にphaseがあってその下にtask」「taskはそれぞれ使い捨てのsession」。使い捨てはTask完了後の再利用禁止とし、実行中の永続化・待機復旧は維持する。

## Tasks

Task_0〜2はcompleted、Task_3はin_progress、Task_4〜13はpending。指定ownsを実装単位とし、異なるTaskの同一file所有は依存順で引き継ぐ。未実装Taskの検証コマンドを実行済みとは扱わない。
下記の`rtk`はこのrepositoryでのコマンド実行prefixである。

### Task_0: 先行成立確認（S）

- status: completed
- evidence: [AT-050実接続](../../../../evidence/background-wave/spike/session-observation.json)。実model・認証・隔離launcherで2回のtool往復/正常終了、別processで同一未完了thread再開を確認。code-mode hostの限定mountをlauncherへ追加。K/P/Mは未完了。
- type: test
- owns:
  - internal/contract/session_live_test.go
  - scripts/isolate-linux（実tool往復に必要な補助executableの限定mount）
  - docs/connection-contracts.md
  - evidence/background-wave/spike/**
- depends_on: []
- description: 本体runtimeから分離した小さなopt-in probeで実model・専用認証・隔離launcherのdynamic tool一往復と同一未完了threadの再開を確認する。GitHub書き込みは使わない。
- acceptance:
  - 採用バイナリ/生成schemaのopt-in、無害なtoolのcall/thread/turn、応答、正常終了、新processでのthread再開を観測する。cache hitは成立条件にしない。
  - 未対応、認証不足、unknown-start、未完了requestを成功にしない。未成立ならTask_1以降の依存runtimeを保留し、必要操作を記録する。
  - outputSchema要求配列は意味の異なる代案と記録し、自動fallbackとして有効にしない。
- validation:
  - kind: manual
    required: true
    owner: worker
    detail: "AT-050をopt-in実接続で確認し、model/主体/launcher/版、request-responseの識別子、未skip件数を秘匿値なしで保存する。schema存在だけを実往復成功としない。"

### Task_1: Sessionとtoolの契約・driver

- status: completed
- merged: PR #55。Codex review指摘2件を修正し、再レビュー指摘なし・全体/race/vet/build/CIを確認。
- implemented: 共通SessionRPC/SessionDriver、kernel tool gateway認可port、汎用成果・Phase条件schema、未知thread/turnの起動intent、main再bindingの監査port。Task_2/3のDB実装とTask_4以降のcontroller接続は後続。
- type: impl
- owns:
  - protocol/codex/**
  - schemas/handoffs/**
  - internal/codex/**
  - internal/worker/**
  - internal/contract/json.go
  - internal/contract/session_tools.go
  - internal/contract/session_tools_test.go
- depends_on: [Task_0]
- description: 採用版のexperimental tool schemaを固定し、親とTaskに共通のsession driverを用意する。Task 1対1の使い捨てthreadとWave呼び出し禁止、分解案・通知入力・親子メッセージ・tool操作・レビュー出力の版付き契約を定める。
- acceptance:
  - 実験的APIの明示opt-inと許可済みserver requestのhandlerを持ち、未対応承認/入力要求は既存どおり保留する。
  - RPC応答・通知・tool requestを取りこぼさず、tool handlerの応答待ちがreaderをdeadlockさせない。
  - Task 1対1 sessionを新規作成し、未完了Taskの待機・復旧でだけ同じthreadを継続する。完了後の再開を拒否し、次Taskには新threadと構造化引き渡しを使う。unknown turn照合とstrictな構造化契約を維持する。
  - K用のWave/状態/待機/成果参照/messageのgateway契約・公開allowlist・認可portを定義し、TaskのWave呼び出しを拒否する。永続bindingを読む認可とhandler登録はTask_3で実装し、Pの外部handlerはTask_8で追加する。Task_2の型へ依存しないDTO/portにする。
  - FR-038のTask終端/Attemptと§2.2の汎用成果schemaを固定し、WorkItem参照は0件以上にする。動的fan-out/未対応Phase条件を拒否する。
  - 主入力64 KiB・inline16 KiBの上限、同一thread継続とmainの監査付き再bindingをdriver境界へ反映する。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/codex ./internal/worker ./internal/contract; tool request中の通知・RPC、再接続、unknown startのfixtureを含む。AT-003/004/021/028の回帰、AT-045/046のsession/tool公開契約。"
  - kind: review
    required: true
    owner: reviewer
    detail: "生成manifestとschema、toolのallowlist、既存の入力要求・権限境界、実観測とfake証拠の区別を確認する。"

### Task_2: Wave/Phase/Task・成果と新DBのschema基盤

- status: completed
- split: PR #56でversion/schema・domain・migration registryを追加。PR #57でplan/sessionのtransactionを追加。PR #58で不変Artifactを追加。PR #59で成果適用を追加。PR #60でKの有界展開transactionを実装し、最終Codex review指摘なし・全体/race/vet/build/CI後にmerge。自動分岐・品質gateとPの検証adapterはTask_7/8で接続する。Task_12の既存作業移行はまだ行わない。
- type: impl
- owns:
  - internal/domain/wave.go
  - internal/domain/wave_test.go
  - internal/storage/waves.go
  - internal/storage/waves_test.go
  - internal/storage/sessions.go
  - internal/storage/sessions_test.go
  - internal/storage/task_results.go
  - internal/storage/task_results_test.go
  - internal/storage/template_expansion.go
  - internal/storage/template_expansion_test.go
  - internal/storage/template_subjects.go
  - internal/storage/migrations.go
  - internal/storage/migrations_test.go
  - internal/storage/store.go
  - internal/storage/artifacts.go
  - internal/storage/artifacts_test.go
  - internal/storage/artifact_retention.go
  - internal/domain/artifact.go
  - internal/storage/kernel.go
  - internal/contract/session.go（共有DTO。SQLite実装と既存worker testsのimport cycleを避けるため）
  - internal/worker/session.go、internal/worker/worker.go（公開型名とJSON契約を保つalias）
- depends_on: [Task_0]
- description: K用の新DBへWave/Phase/Task、scope/plan版、coverage、任意WorkItem binding、汎用Artifactを保存する。version検査とmigration登録基盤を作り、v1作業の変換本体はTask_12へ分ける。
- acceptance:
  - WorkItemなしの直接依頼と不変Artifactを保存し、digest/サイズ/出所/権限/保持を検証する。製品Candidateの契約を汎用成果で置換しない。
  - all_successの必須集合、Task終端/復旧、drain_and_block、有界fix_review templateを型で表し、後続Phase待ち/循環/coverage欠落/任意fan-outを拒否する。
  - plan採用/展開key、版・Task・coverage・予算・通知のtransaction境界を提供する。crash後の再適用で二重生成しない。
  - 空の新DBだけを初期化し、v1/未知versionの既存DBを暗黙変換しない。Task_12が使うmigration登録関数を提供し、version/key重複を拒否する。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/domain ./internal/storage ./internal/service; AT-031/045/051/052/054/055、成果の参照・容量・権限・失効と新DB/version拒否を検証する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "K用DBが既存状態を上書きしないこと、Task_12へ渡す版・登録契約と不変成果/coverageを確認する。"

### Task_3: 永続購読・親子受信箱・再開intent

- status: completed
- split: PR #61でKのTask/Wave終端購読・原子的通知fanout・登録時snapshot・再照合・配送ackを実装。PR #62で親子受信箱・双方向自動購読・返信とmessage通知を追加。PR #63でwake/run binding・Task判断適用・未起動intent復旧を追加。PR #64でkernel tool認可・操作履歴・RPC handlerを接続。PR #65でArtifact公開transaction・完了通知・復旧を追加した。main判断はTask_6、processへの実接続はTask_5で行い、今回の保存APIをK受入とはしない。
- type: impl
- owns:
  - internal/storage/notifications.go
  - internal/storage/notifications_test.go
  - internal/storage/wakes.go
  - internal/storage/wakes_test.go
  - internal/storage/sessions.go、internal/storage/task_results.go（run予約と成果適用の同一transactionへの接続）
  - internal/service/notifications.go
  - internal/service/notifications_test.go
  - internal/storage/messages.go
  - internal/storage/messages_test.go
  - internal/service/messages.go
  - internal/service/messages_test.go
  - internal/storage/kernel_tools.go、internal/storage/kernel_tools_test.go（SQLite内の認可・操作transaction）
  - internal/storage/kernel_publications.go、internal/storage/kernel_publications_test.go（公開予約・完了通知・ファイル復旧）
  - internal/storage/artifacts.go、internal/domain/artifact.go（共有予約transactionとsession出所）
  - internal/worker/kernel_tools.go、internal/worker/kernel_tools_test.go（信頼されたprocess識別子でのRPC登録port）
  - internal/contract/session_tools.go（内部run識別子。公開tool引数には追加しない）
  - internal/service/kernel_tools.go
  - internal/service/kernel_tools_test.go
- depends_on: [Task_1, Task_2]
- description: 保存済みイベントとterminal stateから購読を成立させ、通知ID集合・要求の版・再開理由をrunへ結び付ける。親子メッセージの保存・受信箱・返信参照・通知を同じ基盤へ載せる。schema適用は所有範囲の`notifications.go`/`messages.go`からTask_2の共通登録関数へ登録する。Task_4の`reservations.go`と後続Task_8の`resource_bindings.go`も同じ登録契約を使い、migration version/keyの重複は起動時に拒否する。
- acceptance:
  - subscribe前/同時/後の完了から通知を失わず、重複・逆順を同じ論理イベントへ関連付ける。
  - 通知intentを状態更新と同じtransactionで保存し、配送ack・run関連付け・結果適用を分ける。
  - クラッシュ後は未完了Taskの既存wake/runへ関連付け、完了/pause/cancel/方向修正後の旧通知で新turnを起こさない。
  - 親子双方の受信購読、message ID・sender/recipient・返信先を保存する。実行中は受信箱へ保持し、待機中は同じthreadを再開する。配送・run関連付け・処理/返信を分け、pause/cancel/終端や再配送で禁止された再開をしない。
  - Task_1のportへ永続bindingによるkernel tool認可・登録を実装する。Wave操作の適用はTask_6、枠/予算はTask_4のportへ接続でき、KがTask_8のPR handlerへ依存しない。
  - Artifact参照と出所、scope/plan bindingを通知入力へ付ける。親への中継で権限を増やさず、別Wave/兄弟宛先を拒否する。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/storage ./internal/service; AT-034/035/037/038/047/048/049の親子受信箱、返信、transaction境界・再配送・複数Wave同一subscriberを決定的に再生する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "未処理通知が消えないこと、通知と再開がexactly-onceの外部保証へ依存しないことを確認する。"

### Task_4: 実行枠・排他・予算scheduler

- status: completed
- split: PR #66で保存済みplan/Phase/Task成果に基づくready集合・turn予約時の再照合・Phase証拠の保持を追加。PR #67で復旧許可のrun/input固定・waitingのwake必須・運用エラーの分類を補強した。PR #68でcontroller設定・同時Wave/app-server枠・worktree/thread世代排他と停止証拠を接続した。PR #69でrunner/PR/Cloud枠・共通repository Git排他・終端証拠による原子的な解放と復旧ページを追加した。PR #70でrun単位のtoken配分・unknown保持・親turn/thread上限・Wave共有予算を同じ予約transactionへ接続した。PR #71でWave固定のvalidation job/retry/同一候補review上限を実行枠と同時予約した。本PRでdeadlineの永続blocked化・停止/外部追跡intent・監査付き期限変更と、RSS/起動/停止時間のgeneration別観測portを追加し、Task_4のstorage/scheduler境界を完成させる。実timer・signal・計測の取得とdispatch接続はTask_5、WorkItem/Cloud接続はTask_8が担当する。製品Candidate/merge判定はTask_8の認可adapterまでfail closedとする。processを起動するTask_5より先に予約portを完成させる。
- type: impl
- owns:
  - internal/storage/scheduler.go
  - internal/storage/scheduler_test.go
  - internal/storage/sessions.go（turn予約時のready再照合）
  - internal/storage/artifact_retention.go（現在のPhase完了証拠の保持）
  - internal/storage/task_recovery.go、internal/storage/task_recovery_test.go（復旧許可とrun/inputへの原子的消費）
  - internal/storage/task_evidence.go（保存証拠のJSON破損と運用エラーの分類）
  - internal/service/scheduler.go
  - internal/service/scheduler_test.go
  - internal/service/config.go
  - internal/storage/reservations.go
  - internal/storage/reservations_test.go
  - internal/storage/operation_reservations.go
  - internal/storage/operation_reservations_test.go
  - internal/storage/run_budgets.go
  - internal/storage/run_budgets_test.go
  - internal/storage/operation_budgets.go
  - internal/storage/operation_budgets_test.go
  - internal/storage/deadlines.go、internal/storage/deadlines_test.go
  - internal/storage/process_metrics.go、internal/storage/process_metrics_test.go
  - internal/domain/execution.go（controller全体の枠と新規/旧設定既定値）
  - internal/service/execution_config_test.go
  - internal/service/cli.go（新規initの枠設定）
  - internal/workspace/git.go
  - internal/workspace/git_test.go
- depends_on: [Task_1, Task_2]
- description: ready工程の選択と枠/排他/予算予約を実装し、Task_5が非同期dispatchへ接続できる境界を提供する。
- acceptance:
  - 実行中Phase内の独立A/Bを同時選択し、次Phaseは保存した完了条件まで開始しない。失敗/cancelを成功にしない。Task依存・候補・要求版も照合し、候補が必要なレビューPhaseとmergeが必要な製品後続を区別する。
  - turn/runner/PR上限、同一thread/worktree/共有Git排他を原子的に予約し、unknown実行の枠を停止確認前に再利用しない。
  - Wave/Taskの累積使用量をrun単位で一度だけ計上し、分割・修正・再起動でも上限を保つ。旧設定は直列値へ変換できる。
  - 実装計画§5.1のPhase/Task総数・親turn/再計画・Attempt・fix反復・壁時計期限を原子的に計数する。token観測が遅延/不明でも構造・時間上限を適用する。
  - 同一repoの複数Waveの共有Git操作を排他し、process停止確認まで予約を解放しない。RSS/起動/停止所要時間を観測可能にする。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test -race ./internal/service ./internal/storage ./internal/workspace; AT-032/040/055/056/059とAT-026/028の枠・予算・排他をfake clock/processで検証する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "予約のowner、解放条件、共有Gitの範囲、Taskと既存WorkItem予算の二重加算防止を確認する。"

### Task_5: イベントloopとprocess間wake

- status: in_progress
- split: PR #73でLinux process identity/group照合の共通化、Codex transportの終了一回化、起動ごとの停止証拠、停止未確認時の再起動抑止を追加した。PR #74でKernelのpreparation/owner/run固定・起動admission・read専用復旧予約・世代付き停止の原子的portを追加した。PR #75で共通SessionDriverの予約後にだけ起動するlazy process ownerと、停止証拠による枠返却・read専用照合・固定CWD/明示環境のfactoryを接続した。PR #76で非同期session loop、完了時の即時再評価、定期再照合、停止確認を待つshutdownとdeadlineの世代付き停止を追加した。PR #77でprivate Unix socketのhint/read専用状態streamと永続event cursorの読み取り境界を追加した。PR #78で既存daemonのhint再照合、CLI/webhookの保存後wake、子group停止後のrunner結果公開とtrustedラッパーだけのjob ID通知を配線した。本PRで元process identityと子孫treeに基づく500ms周期のRSS取得をmanaged ownerへ接続し、終了時に観測済みpeak・方式・間隔を同じpreparationへ保存する。共有pageはprocessごとに加算し、走査中の生成/終了/親変更、daemon crash直前の未保存値は完全には捕捉しない。取得不能を0へ変換せず、RSSを停止証拠やmemory上限保証に使わない。ready/input/結果適用のWorkSourceはTask_6が接続し、Kernel daemon/production launcher設定、優先操作と実測負荷試験は後続PRで接続する。
- type: impl
- owns:
  - internal/control/**
  - internal/storage/process_metrics.go、internal/storage/process_metrics_test.go（RSS方式・間隔と世代付き観測）
  - internal/ipc/**（private hint/read-only stream transport）
  - internal/contract/state_stream.go（dispatch Targetと区別したevent DTO）
  - internal/storage/state_events.go、internal/storage/state_events_test.go
  - internal/storage/managed_processes.go、internal/storage/managed_processes_test.go
  - internal/storage/sessions.go（共通予約helper・managed run認可・世代付き停止）
  - internal/process/**
  - internal/codex/client.go、internal/codex/client_test.go
  - internal/runner/lifecycle.go（process identityの共通化）
  - internal/service/cli.go、internal/service/process_stop_test.go（停止証拠とretry gate）
  - internal/service/service.go
  - internal/service/agents.go
  - internal/service/validation.go
  - internal/service/runtime.go
  - internal/service/event_loop_test.go
  - internal/runner/completion.go
  - internal/runner/completion_test.go
  - cmd/harness/main.go、cmd/harness/main_test.go
- depends_on: [Task_3, Task_4]
- description: 同期Tickを非同期dispatch・結果取り込みへ接続し、controller所有のprivate IPC、runnerの制限付き完了hintを実装する。既存の永続DB/fileと低頻度再照合を回復経路として残す。
- acceptance:
  - agent/runner完了でready集合を即座に再評価し、一つの長いturnや外部待機が独立Taskを止めない。
  - DB/file保存後のhint消失、daemon停止、再接続でも記録から回復する。モデル起動を伴わない状態stream境界を提供する。
  - 信頼された`_runner`ラッパーが結果保存後にjob IDだけのhintを送り、controllerが保存intent・結果で検証する。検証コマンドの子process/workerへsocket・通知設定・GitHub資格情報を公開しない。
  - 既存のunknown operation照合、送信直前gate、runner停止確認を維持する。親子を共通runtime設定で起動し、Taskであることだけを理由に共通toolやnative機能を一律無効化しない。
  - 待機turnの正常終了後にapp-server/子processを停止し、再開時に永続threadへ接続する。process数は同時turn上限以下とし、unknown起動は照合まで枠を保持する。
  - 通知100件以上の参照負荷試験で通知保存→予約のp95を測定し、cold start/model待ちを分ける。pause/cancel/方向修正/明示interruptの優先表を実装する。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test -race ./internal/control ./internal/service ./internal/runner; AT-008/020/021/022/028/029/032/033/035/040、IPC前後のcrashとなりすましhintを検証する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "別processからのwakeが通り、通知障害時の回復と資格情報分離が両立することを確認する。"

### Task_6: メインOrchestratorと直接依頼

- type: impl
- owns:
  - internal/service/orchestrator.go
  - internal/service/orchestrator_test.go
  - internal/service/orchestrator_input.go
- depends_on: [Task_5]
- description: direct/Issue sourceをmain threadへ渡し、Phase構成・Task分解・方向修正・例外判断・待機の提案を検証して適用する。既存のIssue取得経路から呼べる入口も提供する。
- acceptance:
  - 元要求・Phase群・Task coverage・依存・要求の版を保存し、承認済みpolicyを緩める提案を拒否する。結果に応じてfix・再レビューPhaseと新Taskを追加し、過去Task/sessionを再利用しない。
  - メインthreadを保持し、意味のある通知batch/新規入力だけでrunを開始する。同一mainの複数Waveを直列化する。Taskからの質問・blocker・再分割依頼を受け、親の指示・回答を同じ親子間へ返す。
  - 待機条件を保存してturnを終了し、方向修正の旧通知・成果を失効させ、結果と残作業を説明できる。
  - 保存済みfix_review templateの定型展開ではmainを起動せず、分類不能・仕様変更だけ理由付き再計画へ戻す。親turn/再計画数の上限を共有ledgerで計上する。
  - 64 KiB以内のWave境界付き入力snapshotをDB/成果から再構成する。main一thread 8 turnの保守更新と履歴利用不能時の復旧では、旧run/通知/停止を照合し、論理session/予算を保ってbindingを監査付き更新する。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/service; AT-031/034/037/038/047/048/053/054/057/058と、定型成功・閲覧でmain起動ゼロをfake driverで確認する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "元要求にない範囲追加、main自己申告での完了、全通知への無条件再開がないことを確認する。"

### Task_7: 検証・レビュー・fix PhaseとTask引き渡し

- type: impl
- owns:
  - internal/localreview/**
  - internal/service/local_review.go
  - internal/service/local_review_test.go
  - internal/domain/workflow.go
  - internal/domain/workflow_test.go
- depends_on: [Task_5]
- description: 親のplan/templateから独立した検証・レビュー・fixのPhase/Taskを作り、汎用Artifactと固定候補の出力を検証する。各Taskは共通driverで新sessionを開始する。指摘の引き渡しは元要求・前Taskの構造化成果・候補・証拠を用い、完了済みsessionを再利用しない。
- acceptance:
  - 元要求・契約・Artifact digestまたはcandidate SHA・証拠に結び付いた構造化指摘を別Taskの成果として保存し、読み取り専用scopeに反する変更を拒否する。レビューTaskは候補準備後、merge前に開始できる。
  - 製品不具合は新しい実装fix Task、テスト不具合は新しい検証fix Taskへ渡す。更新候補の必須gateを再評価し、新しいレビューTaskへ引き渡す。前Taskへの循環依存や完了済みthreadの再開を作らない。
  - Kでは汎用成果の独立評価とtemplate内のfix/reviewを、Pでは製品検証/修正を扱う。clean/findings/inconclusiveをschemaで分ける。
  - ローカルreview成功をGitHub承認へ変換せず、Phaseの実行完了と品質gateを分ける。部分的no_actionで未評価指摘を解決しない（#44）。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/localreview ./internal/domain ./internal/service; AT-005/009/010/013/015/016/017/036/045を候補更新と読み取り専用違反込みで確認する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "実装と独立した期待値、fix担当・予算の維持、Cloud gateとの分離を確認する。"

### Task_8: GitHub tool gatewayと自動購読

- type: impl
- owns:
  - internal/service/tools.go
  - internal/service/tools_test.go
  - internal/service/reviews.go
  - internal/service/external_events.go
  - internal/service/external_events_test.go
  - internal/github/**
  - internal/review/**
  - internal/storage/resource_bindings.go
  - internal/storage/resource_bindings_test.go
- depends_on: [Task_10]
- description: 既存の公開・review操作を共通gatewayへ接続し、tool operationと購読intent、外部資源binding、コメント/reaction/review等のイベント取り込みを実装する。
- acceptance:
  - session/turn/callの識別子を検証し、外部送信前にoperationと購読intentを保存する。候補・scope・権限・pause/cancelを送信直前にも照合する。
  - Task_1/3で実装したkernel gatewayのsender/宛先/返信・Wave呼び出し認可を再利用し、外部comment由来の親messageでもscope/policy緩和を拒否する。Pで別の権限経路を作らない。
  - PR作成の成功/応答不明をmarkerで照合し、自動購読へ関連付け、登録境界のraw deliveryと最新資源状態から差分を回収する。
  - コメント/inline/reaction/review/取消/CI/close等を識別・重複排除し、非通知の更新は差分照合で補う。取得不能な履歴と権限不足は明示する。
  - 関連Taskへ通知を集約し、同じtool再送から重複PR・購読・turnを作らず、外部テキストやreactionから権限・承認を生成しない。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/github ./internal/review ./internal/storage ./internal/service; AT-014〜022/042〜044/046/047のWave認可・親子宛先、Webhook・pagination・削除/取消・応答欠落・登録前更新をfixtureで検証する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "副作用と購読のtransaction境界、同一call/再接続の照合、署名・権限、正式承認維持、mainへの不要wake抑制を確認する。"

### Task_9: Wave kernelの操作・観測とruntime統合（K）

- type: impl
- owns:
  - internal/service/cli.go
  - internal/service/wave_cli.go
  - internal/service/wave_cli_test.go
  - internal/service/service.go
  - internal/service/agents.go
  - internal/service/config.go
  - internal/service/wave_integration_test.go
  - README.md
- depends_on: [Task_6, Task_7]
- description: kernel境界を新DBのruntime/CLIへ結線する。Issue pool/PR接続はTask_11、既存DB/config移行はTask_12へ分ける。
- acceptance:
  - Issue/WorkItem/PRを要求しない直接依頼から、main tools、Artifact、独立Reviewer、template、schedulerが実際のdispatchに接続される。
  - wave create/list/inspect/pause/resume/cancel/steer/watchを提供し、cursor再接続でWave→Phase→Task→session/run、Phase条件、待機理由・成果・既知使用量・親子メッセージと返信を辿れる。
  - Wave/Taskのpause/cancel/方向修正/明示interruptを優先表どおりに扱い、出所・予算・成果失効・main復旧を表示する。
  - READMEがKで実装済みのコマンド・設定version・上限とP/Mの未接続範囲を説明し、閲覧でモデルを起動しない。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test -race ./internal/service; K適用ATとAT-051〜055/057〜059、direct→並行汎用Task→review/fix→通知のfake統合を確認する。"
  - kind: e2e
    required: true
    owner: reviewer
    detail: "ローカルCLIを実行して直接依頼、一覧、watch切断/再接続、方向修正、pause/cancelを確認する。fake外部adapter、実SQLite/temp Gitを使用し、command/outputを証拠とする。Web UIは対象外。"

### Task_10: Wave kernelの受入と実演（K）

- type: test
- owns:
  - internal/service/background_wave_acceptance_test.go
  - internal/service/connection_live_test.go
  - docs/acceptance.md
  - docs/connection-contracts.md
  - docs/background-wave-acceptance.md
  - evidence/background-wave/**
- depends_on: [Task_9]
- description: Task_0の成立証拠とKのATを照合し、新DBで汎用Waveを実演してKを独立判定する。PR/Cloud接続とMの総合証拠はTask_13へ分ける。
- acceptance:
  - 要件§11.1のK適用AT、任意WorkItem、Artifact参照/GC、Task 2件並行、template反復、親子message、process停止、main復旧を確認する。
  - 実modelで直接依頼→汎用Task→独立review→新fix/review Task→報告を観測する。通常待機・定型反復・閲覧のmain起動は0である。
  - Task_0のtool往復を初めてここで確認する順序に戻さず、未確認能力・未実行とK/P/Mの受入状態を別に記録する。
  - CloudがunknownでもKの汎用Waveを完了でき、PRを必要とする製品Waveのgateは省略できない。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./...; rtk go vet ./...; rtk go test -race ./internal/control ./internal/storage ./internal/service ./internal/codex ./internal/worker ./internal/localreview ./internal/runner ./internal/workspace; buildはrtk go build -o /tmp/harness-wave ./cmd/harness。"
  - kind: manual
    required: true
    owner: worker
    detail: "opt-in実接続を実行し、対象model・主体・repository・candidateと未skip実行数を記録する。資格情報/権限不足ならblockedとして必要操作を明示する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "Kの証拠とAT適用表を確認し、unknown能力、公開範囲、未解決blockerを確認する。"

### Task_11: PRパイプラインとWaveの統合（P）

- type: impl
- owns:
  - internal/service/cli.go
  - internal/service/service.go
  - internal/service/agents.go
  - internal/service/reviews.go
  - internal/service/validation.go
  - internal/service/final.go
  - internal/service/splits.go
  - internal/service/wave_integration_test.go
  - README.md
- depends_on: [Task_8, Task_9]
- description: kernelと既存Issue/WorkItem/Candidate/PR工程を接続する。Task_9と同じfileは依存順で引き継ぐ。
- acceptance:
  - Issue poolとdirect製品依頼が同じWaveへ入り、必要な検証/正式承認を保って公開・購読・mergeへ進む。
  - 先行mergeによるbase更新を通常mergeし、競合は新fix Task/sessionへ渡す。複数Wave・同じファイルのPR・部分PR・分割でも失効と予算を維持する。
  - 全範囲merge後の要求/policy変更も、古い履歴を保持する版付き後続作業として復旧できる（#45）。
  - K/Pの完了とWorkItem操作優先を区別し、Cloud unknownを製品Waveの合格にしない。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test -race ./internal/service ./internal/workspace; AT-001〜030/042〜044/056、#44/#45の回帰、同一repo複数Waveとbase競合を確認する。"

### Task_12: 既存DB/configとCLIの移行（M）

- type: impl
- owns:
  - internal/storage/migrations.go
  - internal/storage/migrations_test.go
  - internal/storage/store.go
  - internal/storage/testdata/**
  - internal/service/config_migration.go
  - internal/service/config_migration_test.go
  - docs/connection-contracts.md
  - README.md
- depends_on: [Task_2, Task_11]
- description: v1作業を新Waveへ変換し、未知process照合・整合backup・旧CLIを含む切替を提供する。実DBの不存在は仮定しない。
- acceptance:
  - split/continuation/repair、ID、thread履歴、unknown outbox、予算、親最終受入を保持する。実行中threadだけを移行Taskへbindingし、終了済みthreadは再利用しない。
  - 途中失敗・再実行でも二重登録/再送せず、未知version拒否・backup復旧・trusted config変換でpolicy snapshotを保持する。
  - 移行前に対象DBの有無/所有と未知処理を確認し、新規導入の実移行not_applicableとfixture合格を分ける。旧DBの削除や無断fresh-startを行わない。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/storage ./internal/service; AT-039/049、v1 fixture・中途失敗・再実行・旧CLI/configと未知操作を検証する。"

### Task_13: PR/移行の総合受入と限定実接続（P/M）

- type: test
- owns:
  - internal/service/background_wave_acceptance_test.go
  - internal/service/connection_live_test.go
  - docs/acceptance.md
  - docs/connection-contracts.md
  - docs/background-wave-acceptance.md
  - evidence/background-wave/**
- depends_on: [Task_10, Task_11, Task_12]
- description: P/Mを含むFR/NFR/ATの追跡を完成させ、PR自動購読と既存接続を限定観測する。Kの合格とPのunknownを混ぜない。
- acceptance:
  - FR-001〜040、NFR-001〜013、AT-001〜059の段階/実装/試験/証拠/未達を記録する。
  - 検証用PRのtool作成→comment/reaction/review→再接続・重複通知を実観測し、正式承認・SHA・CI・非bypass rulesを#20の契約で確認する。
  - Cloud未成立時はP/製品Wave/全体完成をblockedとし、fakeやローカルreviewで代用しない。秘密値/private pathを公開証拠へ含めない。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./...; rtk go vet ./...; rtk go test -race ./internal/control ./internal/storage ./internal/service ./internal/codex ./internal/worker ./internal/localreview ./internal/runner ./internal/workspace; rtk go build -o /tmp/harness-wave ./cmd/harness。"
  - kind: manual
    required: true
    owner: worker
    detail: "opt-in実接続の対象model・主体・repository・候補・未skip件数を記録し、資格情報や権限不足は必要操作付きblockedとする。"

## Task Waves (explicit parallel dispatch sets)

ここでのTask Wavesは本計画を実装する担当のdispatch集合を指す。プロダクトのWave→Phase→Taskとは別の開発手順である。
同じ集合のTaskは依存が満たされownsが分かれるため並行dispatchし、集合間は順次進める。

- S: [Task_0]
- K / Wave 1 (parallel): [Task_1, Task_2]
- K / Wave 2 (parallel): [Task_3, Task_4]
- K / Wave 3: [Task_5]
- K / Wave 4 (parallel): [Task_6, Task_7]
- K / Wave 5: [Task_9]
- K / Wave 6: [Task_10] — Kの独立受入
- P / Wave 7: [Task_8]
- P / Wave 8: [Task_11]
- M / Wave 9: [Task_12]
- P/M / Wave 10: [Task_13] — 全体受入

### 起票済みTaskと分割後の対応

| Issue | v0.4の実装単位 |
| --- | --- |
| #34 | Task_1。先行成立確認は#46 / Task_0 |
| #35 | Task_2のschema基盤とTask_12の既存作業移行 |
| #36 / #37 / #38 / #39 / #40 | Task_3 / Task_4 / Task_5 / Task_6 / Task_7 |
| #41 | Task_8のP接続。kernelの契約/認可/message gatewayはTask_1/3へ前倒し |
| #42 | Task_9のK操作とTask_11のP統合 |
| #43 | Task_10のK受入とTask_13のP/M総合受入 |
| #44 / #45 | Task_7/11で修正しTask_13で回帰。文書変更では解消しない |
| #46〜#50 | Task_0、FR-038、汎用Artifact、上限、main復旧の設計追跡 |

## 検証の追跡

| 受入条件 | 主担当 | 重点 |
| --- | --- | --- |
| AT-001〜030 | Task_1〜8/11の該当領域、Task_13で統合 | 既存固定候補・正式承認・復旧・親最終受入を回帰させない |
| AT-031 | Task_2/6 | 直接依頼、Phase/Task所属、coverage、依存 |
| AT-032/033 | Task_4/5/7 | 独立Taskの並行性とレビュー待機 |
| AT-034/035/038 | Task_3/5/6 | 購読競合、crash、通知と入力 |
| AT-036 | Task_1/7 | 新しいfix・再レビューTask/sessionへの引き渡し |
| AT-037/040 | Task_3/4/9 | 版・操作・予算・排他 |
| AT-039 | Task_12 | DB/config/既存CLI移行 |
| AT-041 | Task_5/9 | 状態streamと再接続 |
| AT-042〜044 | Task_1/3/8、Task_13実接続 | tool自動購読、資源binding、外部更新 |
| AT-045/046 | Task_1/2/4/6/7/8/9 | Phase進行、Task 1対1の使い捨てsession、Wave呼び出し禁止、新Taskへの引き渡し |
| AT-047/048/049 | Task_1/3/6/9/12、Task_10/13実接続 | 親子message、sender/宛先認可、受信箱・返信・再開・移行 |
| AT-050 | Task_0 | dynamic toolの先行成立・未対応時の保留 |
| AT-051/052/053/054 | Task_1/2/6/7/10 | 汎用成果、Phase/Task意味論、有界反復、原子的展開 |
| AT-055/056/057 | Task_3/4/6/8/11/13 | 構造・時間上限、複数WaveのPR競合、出所/権限 |
| AT-058/059 | Task_1/5/6/9/10 | main文脈・復旧、process上限と優先操作、通知p95 |

## Rollback / Safety

- 本文書PRはruntime・DB・設定・生成schemaを変更しない。取り消しは文書commitのrevertでよい。
- 実装時は単一controller所有を維持し、移行・切戻し前に新規dispatchを止める。送信済み・unknown operationとrunnerを照合し、整合backupを使う。
- 新binaryは未知のschema versionを拒否する。旧binary自身に新version拒否があるとは仮定せず、新DBを旧binaryへ渡さない運用手順を設ける。移行後に外部操作がある場合は単純backup復元をせず、ID・履歴を照合して復旧する。
- 自動mergeは既存capability reportと正式承認gateの成立後だけ有効にする。

## Progress Log

- 2026-10-22（v0.4）: 共有文書レビューと#44〜#50を照合し、S/K/P/M、汎用成果、Phase条件/Task終端/有界template、累積上限、process停止、main復旧、優先操作を具体化。Task_0/11/12/13を追加し、KとPR接続/移行の依存を分離した。以下のAPPROVED/validator記録はv0.3以前の履歴であり、今回の改訂の承認・試験結果へ流用しない。

- 2026-10-22: 現行要件・実装・接続証拠を調査し、差分分析・要件改訂・設計・Task分解を作成。runtimeのTask_1〜10はpending。計画書PR作成の依頼に基づく作業であり、runtime着手の承認は未取得。
- 2026-10-22: 独立Reviewerが文書・計画をAPPROVED。balanced plan validator、ローカル参照34件、FR/NFR/AT連番、privacy sweep、diff whitespaceの検査がpass。10 Taskの分解を独立確認し、validatorのTask数警告に未解決指摘なし。migration登録の所有境界を共通契約で明確化した。Go/runtime/実接続の試験は本PRでは未実行。
- 2026-10-22: 上記APPROVEDはcommit `a5bd066`の初回案に対するbaselineであり、親子message・使い捨てsession・Phase階層の追加補正より前の証拠。追加補正の要件はFR-036/037、AT-045〜049として反映し、別途再レビューする。
- 2026-10-22: 追加補正後のPhase階層・使い捨てsession・親子受信箱と、公式資料比較を独立Reviewerが再確認しAPPROVED。Phase/成果依存の不可能な待機も検証対象へ明記。balanced plan validator、ローカル参照/anchor36件、FR-001〜037・NFR-001〜011・AT-001〜049の連番/表、privacy sweep、diff whitespaceがpass。runtime・実接続試験は未実行。

## Decision Log

- 2026-10-22（v0.4）: レビューを踏まえた文書改訂・PR作成の依頼に基づく。初期Phaseはall_success/drain_and_block、定型反復は親の有界templateをcontrollerが展開、任意fan-out/pipelineは後続とする。Sで前提確認後にKを独立受入し、P/Mと全体完成は保持する。実運用DB不存在は仮定せず、Mの実行をKの先行条件から外す。新たな毎回の手動承認は設けない。

- 2026-10-22: Wave/TaskをWorkItemの上位へ追加し、既存PR契約を保持する案を採用。根拠は直接依頼・main session・通知再開という依頼。実装着手の承認: no。ADR: none。
- 2026-10-22: PR作成等のtool callに連動する自動購読を要件へ追加。根拠は依頼者の追加説明。操作intentと購読intentを先に保存し、外部資源確定前後の更新を回収する。実装着手の承認: no。
- 2026-10-22: 追加依頼に基づきTask 1対1の独立session、TaskでのWave呼び出し禁止、親子messageを採用。工程別の隠れたsession生成は取りやめ、独立検証・レビューは別Taskへ変更。Task_1/2/3/4/6/7/8/9/10の契約・移行・認可・検証へ反映。検証/レビューの候補依存と製品後続のmerge依存を分ける。実装着手の承認: no。
- 2026-10-22: 続く補正「Waveの下にPhase、その下にTask」「Taskは使い捨てsession」に基づき、上の継続Task案を置き換える。Task完了後のfix・再レビューは新Phase/Task/sessionへ渡し、同じthreadの継続は未完了Taskの待機・復旧に限定。既存FR-004/013・AT-009も意図的に改訂。初期Phaseは順次、Phase内Taskは並行とする設計案。実装着手の承認: no。
- 2026-10-22: 依頼者の公式記事リンクと、その先のworkflow資料を確認。会話外runtimeによる生成script実行、Phase表示と待ち合わせの区別を比較へ記録。初期の型付きプラン/Go controller方式を維持し、任意JavaScript実行は範囲に追加しない。親子message・PR自動購読の内部仕様を公式記事から推測しない。実装着手の承認: no。

## Notes

- リスク: 同じmainへの複数通知、tool requestとRPC応答の競合、移行時のunknown operation、並行PRのbase更新が主な重点。
- cache hitは性能観測とし、DB・成果物・履歴復旧やTask間の引き渡しをcacheへ依存させない。
- 文書PRではリンク/ID/参照、Task依存DAG・所有範囲、段階別ATの整合、JSON例とgit diff --checkを確認する。過去の独立レビュー/validator合格を今回の結果として扱わず、runtimeのATや実接続と区別する。
