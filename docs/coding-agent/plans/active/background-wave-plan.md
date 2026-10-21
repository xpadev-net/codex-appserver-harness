# Plan: Background Waveの実装

- status: draft
- generated: 2026-10-22
- last_updated: 2026-10-22
- work_type: mixed

## Goal

大きな依頼をWave→Phase→Taskへ分解し、各Taskに使い捨てsessionを割り当てる。Taskは親と共通runtimeを使ってWave呼び出しだけを禁止し、実行中は親子メッセージと通知で再開する。完了後のfix・再レビューは新Task/sessionへ引き渡す。
本計画を含むPRの範囲は要件・差分・設計・計画の改訂までとする。
Task_1〜10のruntime実装は未着手であり、計画のレビュー・PR公開を実装着手の承認とはみなさない。

## Definition of Done

- [要件定義書](../../../../requirements.md)のFR-001〜037、NFR-001〜011に実装・証拠の対応がある。
- AT-001〜030の品質・外部操作を回帰検証する。AT-009は新fix Taskへの引き渡しへ改訂し、AT-031〜049でPhase/Task階層、使い捨てsession、直接依頼、並行性、通知再開、自動購読、移行を確認する。
- 直接依頼からWaveとPhase群を作り、Phase内の独立Task二つを並行実行して、レビューPhase・fix Phaseの新Task・PR tool・外部通知からの再開を実演できる。
- Task 1対1の使い捨てsessionと実行中だけのthread継続、Wave呼び出し禁止、親子の質問・回答・指示、実行中/待機中/crash後の受信を確認する。完了Taskを再利用せず、通常待機・閲覧・通知配送だけのLLM呼び出しがゼロである。
- 未確認の外部能力はunknownのまま表示し、正式承認・mergeを成立したことにしない。
- 全Taskの必須検証と独立レビューが揃い、残る運用上の制約をREADMEと受入検証記録に反映する。

## Planner-added requirements

- ローカルUnix domain socketによるwake hintとcursor付き状態streamを設ける。理由: CLI・Webhook・runnerが別processであり、in-memory通知だけでは要求されたイベント再開を接続できないため。分散brokerは加えない。
- versioned migrationと停止・backup・復旧手順を設ける。理由: 既存WorkItemと会話・予算・送信済み操作を保持してWaveへ移行するため。利用中DBの不存在は確認できない。
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
- stance: migrate
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

- Q1: 採用model・認証・隔離環境でdynamic tool往復と同じthread再開が成立するか。schemaの存在は確認済み、実推論は未確認。Task_10で限定検証し、不成立なら該当能力をblockedとして契約を再計画する。
- Q2: 導入先GitHub主体でreaction等の購読・差分取得がどこまで可能か。Task_8でadapter契約と不足範囲を実装し、Task_10で権限・代表経路を観測する。通知非対応は通常コードの差分照合で補い、取得不能は明示する。
- Q3: Cloud正式APPROVED・候補SHA対応・非bypass rulesという既存接続条件が成立するか。既存capability reportを維持し、未成立なら自動mergeとWave全体の完了を保留する。Wave基盤の実装は進められる。

## Assumptions

- A1: Go、SQLite、Linuxの隔離launcher、ローカルrunner、Codex 0.163.0を維持する。source: `README.md`、`go.mod`、`internal/service/config.go`、`protocol/codex/manifest.json`。
- A2: 既存DB/CLIは移行対象のconsumerである。source: `internal/storage/store.go`、`internal/service/cli.go`、READMEの状態保存先と操作契約。repository外の実DBの有無は未確認であり、移行試験はfixtureを使う。
- A3: 同一threadの継続は採用版のschemaとworker実装で表現できる。source: `protocol/codex/ThreadResumeParams.json`、`internal/worker/worker.go`。実モデル・cache効果の成立はQ1の検証まで未確認。
- A4: cache期限の改善は依頼者の運用観測である。source: 2026-10-22の会話。公式な無期限保証として実装しない。
- A5: 階層はWave→Phase→Taskで、Taskは使い捨てsession、Wave呼び出し禁止と親子メッセージを持つ。source: 2026-10-22の追加依頼。「waveの下にphaseがあってその下にtask」「taskはそれぞれ使い捨てのsession」。使い捨てはTask完了後の再利用禁止とし、実行中の永続化・待機復旧は維持する。

## Tasks

全Taskのstatusはpending。各Taskを担当するWorkerは指定owns内で作業し、共有Git変更はOrchestratorが行う。
下記の`rtk`はこのrepositoryでのコマンド実行prefixである。

### Task_1: Sessionとtoolの契約・driver

- type: impl
- owns:
  - protocol/codex/**
  - schemas/handoffs/**
  - internal/codex/**
  - internal/worker/**
  - internal/contract/json.go
- depends_on: []
- description: 採用版のexperimental tool schemaを固定し、親とTaskに共通のsession driverを用意する。Task 1対1の使い捨てthreadとWave呼び出し禁止、分解案・通知入力・親子メッセージ・tool操作・レビュー出力の版付き契約を定める。
- acceptance:
  - 実験的APIの明示opt-inと許可済みserver requestのhandlerを持ち、未対応承認/入力要求は既存どおり保留する。
  - RPC応答・通知・tool requestを取りこぼさず、tool handlerの応答待ちがreaderをdeadlockさせない。
  - Task 1対1 sessionを新規作成し、未完了Taskの待機・復旧でだけ同じthreadを継続する。完了後の再開を拒否し、次Taskには新threadと構造化引き渡しを使う。unknown turn照合とstrictな構造化契約を維持する。
  - Taskのtool公開からWave呼び出しを除き、親と同じruntime・共通toolを使えることをdriver契約として確認する。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/codex ./internal/worker ./internal/contract; tool request中の通知・RPC、再接続、unknown startのfixtureを含む。AT-003/004/021/028の回帰、AT-045/046のsession/tool公開契約。"
  - kind: review
    required: true
    owner: reviewer
    detail: "生成manifestとschema、toolのallowlist、既存の入力要求・権限境界、実観測とfake証拠の区別を確認する。"

### Task_2: Wave/Phase/Taskと既存DB・設定の移行

- type: impl
- owns:
  - internal/domain/wave.go
  - internal/domain/wave_test.go
  - internal/storage/waves.go
  - internal/storage/waves_test.go
  - internal/storage/migrations.go
  - internal/storage/migrations_test.go
  - internal/storage/store.go
  - internal/storage/testdata/**
  - internal/service/config_migration.go
  - internal/service/config_migration_test.go
- depends_on: []
- description: 直接依頼、Wave→Phase→Task所属、Phase順序・進行条件、Task依存、coverageと既存WorkItemのbindingを保存する。versioned migration、version 1設定の変換、既存系譜とIDを保持する移行を実装する。
- acceptance:
  - Issueなしの元要求、Wave→Phase→Task、Task 1対1 session、WorkItemとの成果参照を保持する。Taskの一意なPhase所属、Phase進行条件、coverage欠落・旧版bindingを検証する。Phase順序とTask/成果依存を合わせて循環・後続Phaseへの不可能な待機を拒否する。レビュー用の候補依存と、製品後続のmerge・最終受入依存を区別する。
  - v1 fixtureのsplit/continuation/repair、thread履歴、outbox unknown、予算、親最終受入を保持する。実行中threadだけを移行Phase/Taskへbindingし、終了済みthreadは後続へ再利用しない。再実行が二重登録を生まない。
  - migration前の整合backup、途中失敗の復旧、未対応versionの拒否、config変換とpolicy snapshot保持を提供する。後続schemaは各所有fileから登録できる共通migration登録関数を提供し、集約fileへの追記を不要にする。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/domain ./internal/storage ./internal/service; AT-031/039/045/049、移行再実行・中途失敗・旧設定fixtureを検証する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "実DBがないとの仮定を置かず、既存ID/系譜・unknown操作・証拠・予算とbackupの対応を確認する。"

### Task_3: 永続購読・親子受信箱・再開intent

- type: impl
- owns:
  - internal/storage/notifications.go
  - internal/storage/notifications_test.go
  - internal/service/notifications.go
  - internal/service/notifications_test.go
  - internal/storage/messages.go
  - internal/storage/messages_test.go
  - internal/service/messages.go
  - internal/service/messages_test.go
- depends_on: [Task_1, Task_2]
- description: 保存済みイベントとterminal stateから購読を成立させ、通知ID集合・要求の版・再開理由をrunへ結び付ける。親子メッセージの保存・受信箱・返信参照・通知を同じ基盤へ載せる。schema適用は所有範囲の`notifications.go`/`messages.go`からTask_2の共通登録関数へ登録する。Task_4の`reservations.go`とTask_8の`resource_bindings.go`も同じ登録契約を使い、migration version/keyの重複は起動時に拒否する。
- acceptance:
  - subscribe前/同時/後の完了から通知を失わず、重複・逆順を同じ論理イベントへ関連付ける。
  - 通知intentを状態更新と同じtransactionで保存し、配送ack・run関連付け・結果適用を分ける。
  - クラッシュ後は未完了Taskの既存wake/runへ関連付け、完了/pause/cancel/方向修正後の旧通知で新turnを起こさない。
  - 親子双方の受信購読、message ID・sender/recipient・返信先を保存する。実行中は受信箱へ保持し、待機中は同じthreadを再開する。配送・run関連付け・処理/返信を分け、pause/cancel/終端や再配送で禁止された再開をしない。
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

- type: impl
- owns:
  - internal/service/scheduler.go
  - internal/service/scheduler_test.go
  - internal/service/config.go
  - internal/storage/reservations.go
  - internal/storage/reservations_test.go
  - internal/workspace/git.go
  - internal/workspace/git_test.go
- depends_on: [Task_1, Task_2]
- description: ready工程の選択と枠/排他/予算予約を実装し、Task_5が非同期dispatchへ接続できる境界を提供する。
- acceptance:
  - 実行中Phase内の独立A/Bを同時選択し、次Phaseは保存した完了条件まで開始しない。失敗/cancelを成功にしない。Task依存・候補・要求版も照合し、候補が必要なレビューPhaseとmergeが必要な製品後続を区別する。
  - turn/runner/PR上限、同一thread/worktree/共有Git排他を原子的に予約し、unknown実行の枠を停止確認前に再利用しない。
  - Wave/Taskの累積使用量をrun単位で一度だけ計上し、分割・修正・再起動でも上限を保つ。旧設定は直列値へ変換できる。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test -race ./internal/service ./internal/storage ./internal/workspace; AT-032/040とAT-026/028の枠・予算・排他をfake clock/processで検証する。"
  - kind: review
    required: true
    owner: reviewer
    detail: "予約のowner、解放条件、共有Gitの範囲、Taskと既存WorkItem予算の二重加算防止を確認する。"

### Task_5: イベントloopとprocess間wake

- type: impl
- owns:
  - internal/control/**
  - internal/service/service.go
  - internal/service/agents.go
  - internal/service/validation.go
  - internal/service/runtime.go
  - internal/service/event_loop_test.go
  - internal/runner/completion.go
  - internal/runner/completion_test.go
  - cmd/harness/main.go
- depends_on: [Task_3, Task_4]
- description: 同期Tickを非同期dispatch・結果取り込みへ接続し、controller所有のprivate IPC、runnerの制限付き完了hintを実装する。既存の永続DB/fileと低頻度再照合を回復経路として残す。
- acceptance:
  - agent/runner完了でready集合を即座に再評価し、一つの長いturnや外部待機が独立Taskを止めない。
  - DB/file保存後のhint消失、daemon停止、再接続でも記録から回復する。モデル起動を伴わない状態stream境界を提供する。
  - 信頼された`_runner`ラッパーが結果保存後にjob IDだけのhintを送り、controllerが保存intent・結果で検証する。検証コマンドの子process/workerへsocket・通知設定・GitHub資格情報を公開しない。
  - 既存のunknown operation照合、送信直前gate、runner停止確認を維持する。親子を共通runtime設定で起動し、Taskであることだけを理由に共通toolやnative機能を一律無効化しない。
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
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test ./internal/service; AT-031/034/037/038/047/048と、定型成功・閲覧でmain起動ゼロをfake driverで確認する。"
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
- description: 親が独立した検証・レビュー・fixのPhase/Taskを作る工程を定義し、固定候補の出力を検証する。各Taskは共通driverで新sessionを開始する。指摘の引き渡しは元要求・前Taskの構造化成果・候補・証拠を用い、完了済みsessionを再利用しない。
- acceptance:
  - 元要求・契約・candidate SHA・証拠に結び付いた構造化指摘を別Taskの成果として保存し、読み取り専用scopeに反する変更を拒否する。レビューTaskは候補準備後、merge前に開始できる。
  - 製品不具合は新しい実装fix Task、テスト不具合は新しい検証fix Taskへ渡す。更新候補の必須gateを再評価し、新しいレビューTaskへ引き渡す。前Taskへの循環依存や完了済みthreadの再開を作らない。
  - ローカルreview成功をGitHub承認へ変換せず、Phaseの進行条件と個別Taskの結果を分ける。
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
- depends_on: [Task_5]
- description: 既存の公開・review操作を共通gatewayへ接続し、tool operationと購読intent、外部資源binding、コメント/reaction/review等のイベント取り込みを実装する。
- acceptance:
  - session/turn/callの識別子を検証し、外部送信前にoperationと購読intentを保存する。候補・scope・権限・pause/cancelを送信直前にも照合する。
  - 親子message toolのsender・宛先・reply_toをbindingから認可する。TaskからのWave呼び出しを非公開tool名・別名・中継でも拒否し、scope内の共通PR/message toolは使える。
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

### Task_9: Wave操作・観測とruntime統合

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
- depends_on: [Task_6, Task_7, Task_8]
- description: 各境界をruntimeとCLIへ結線し、Wave作成・方向修正・操作・watch、既存Issue pool/WorkItem CLIとの対応を提供する。
- acceptance:
  - direct依頼とIssue poolが同じWave実行へ入り、main tools、Reviewer、GitHub gatewayが実際のdispatchに接続される。
  - wave create/list/inspect/pause/resume/cancel/steer/watchを提供し、cursor再接続でWave→Phase→Task→session/run、Phase条件、待機理由・成果・既知使用量・親子メッセージと返信を辿れる。
  - Wave/Task操作と既存WorkItem操作の優先関係を守り、複数Task/PRのbase更新・失効・部分PR・親最終受入を維持する。
  - READMEが実装済みコマンド・設定version・上限と移行方法を説明し、閲覧でモデルを起動しない。
- validation:
  - kind: command
    required: true
    owner: worker
    detail: "rtk go test -race ./internal/service; AT-023〜030/037/039/040/041/045〜049とdirect→並行Task→local review→tool→通知のfake統合を確認する。"
  - kind: e2e
    required: true
    owner: reviewer
    detail: "ローカルCLIを実行して直接依頼、一覧、watch切断/再接続、方向修正、pause/cancelを確認する。fake外部adapter、実SQLite/temp Gitを使用し、command/outputを証拠とする。Web UIは対象外。"

### Task_10: 総合受入・限定実接続と証拠

- type: test
- owns:
  - internal/service/background_wave_acceptance_test.go
  - internal/service/connection_live_test.go
  - docs/acceptance.md
  - docs/connection-contracts.md
  - docs/background-wave-acceptance.md
  - evidence/background-wave/**
- depends_on: [Task_9]
- description: AT対応を完結させ、実Codexのtool往復・thread再開と、検証用PRの自動購読を限定観測する。runtime不具合は担当Taskへ返し、試験側で回避しない。
- acceptance:
  - AT-001〜049の実装/試験/証拠対応を作り、障害注入・migration・並行性の必須チェックを実行する。
  - 採用model/認証/隔離下でmain tool呼出し、Wave→Phase→Task、Task 1対1の使い捨てthread、レビューPhase→新fix Task→新レビューTaskを観測する。未完了Taskのturn終了→同一thread再開、親子の質問・回答、TaskからのWave呼び出し拒否も確認する。
  - 検証用repositoryのPR作成toolから購読し、コメント/reaction/review等の代表更新、再接続、重複通知を観測する。送信先・操作主体・検証の許可範囲を証拠に残す。
  - Cloud正式承認等が未成立なら該当ATとWave完了をblockedとして記録し、fakeやローカルreviewで代替しない。秘密値・private pathを公開証拠へ含めない。
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
    detail: "総合受入の証拠とAT追跡表を独立確認し、unknown能力、公開範囲、未解決blockerを確認する。"

## Task Waves (explicit parallel dispatch sets)

ここでのTask Wavesは本計画を実装する担当のdispatch集合を指す。プロダクトのWave→Phase→Taskとは別の開発手順である。
同じ集合のTaskは依存が満たされownsが分かれるため並行dispatchし、集合間は順次進める。

- Wave 1 (parallel): [Task_1, Task_2]
- Wave 2 (parallel): [Task_3, Task_4]
- Wave 3 (parallel): [Task_5]
- Wave 4 (parallel): [Task_6, Task_7, Task_8]
- Wave 5 (parallel): [Task_9]
- Wave 6 (parallel): [Task_10]

## 検証の追跡

| 受入条件 | 主担当 | 重点 |
| --- | --- | --- |
| AT-001〜030 | Task_1〜9の該当領域、Task_10で統合 | 既存固定候補・正式承認・復旧・親最終受入を回帰させない |
| AT-031 | Task_2/6 | 直接依頼、Phase/Task所属、coverage、依存 |
| AT-032/033 | Task_4/5/7 | 独立Taskの並行性とレビュー待機 |
| AT-034/035/038 | Task_3/5/6 | 購読競合、crash、通知と入力 |
| AT-036 | Task_1/7 | 新しいfix・再レビューTask/sessionへの引き渡し |
| AT-037/040 | Task_3/4/9 | 版・操作・予算・排他 |
| AT-039 | Task_2/9 | DB/config/既存CLI移行 |
| AT-041 | Task_5/9 | 状態streamと再接続 |
| AT-042〜044 | Task_1/3/8、Task_10実接続 | tool自動購読、資源binding、外部更新 |
| AT-045/046 | Task_1/2/4/6/7/8/9 | Phase進行、Task 1対1の使い捨てsession、Wave呼び出し禁止、新Taskへの引き渡し |
| AT-047/048/049 | Task_2/3/6/8/9、Task_10実接続 | 親子message、sender/宛先認可、受信箱・返信・再開・移行 |

## Rollback / Safety

- 本文書PRはruntime・DB・設定・生成schemaを変更しない。取り消しは文書commitのrevertでよい。
- 実装時は単一controller所有を維持し、移行・切戻し前に新規dispatchを止める。送信済み・unknown operationとrunnerを照合し、整合backupを使う。
- 新binaryは未知のschema versionを拒否する。旧binary自身に新version拒否があるとは仮定せず、新DBを旧binaryへ渡さない運用手順を設ける。移行後に外部操作がある場合は単純backup復元をせず、ID・履歴を照合して復旧する。
- 自動mergeは既存capability reportと正式承認gateの成立後だけ有効にする。

## Progress Log

- 2026-10-22: 現行要件・実装・接続証拠を調査し、差分分析・要件改訂・設計・Task分解を作成。runtimeのTask_1〜10はpending。計画書PR作成の依頼に基づく作業であり、runtime着手の承認は未取得。
- 2026-10-22: 独立Reviewerが文書・計画をAPPROVED。balanced plan validator、ローカル参照34件、FR/NFR/AT連番、privacy sweep、diff whitespaceの検査がpass。10 Taskの分解を独立確認し、validatorのTask数警告に未解決指摘なし。migration登録の所有境界を共通契約で明確化した。Go/runtime/実接続の試験は本PRでは未実行。
- 2026-10-22: 上記APPROVEDはcommit `a5bd066`の初回案に対するbaselineであり、親子message・使い捨てsession・Phase階層の追加補正より前の証拠。追加補正の要件はFR-036/037、AT-045〜049として反映し、別途再レビューする。
- 2026-10-22: 追加補正後のPhase階層・使い捨てsession・親子受信箱と、公式資料比較を独立Reviewerが再確認しAPPROVED。Phase/成果依存の不可能な待機も検証対象へ明記。balanced plan validator、ローカル参照/anchor36件、FR-001〜037・NFR-001〜011・AT-001〜049の連番/表、privacy sweep、diff whitespaceがpass。runtime・実接続試験は未実行。

## Decision Log

- 2026-10-22: Wave/TaskをWorkItemの上位へ追加し、既存PR契約を保持する案を採用。根拠は直接依頼・main session・通知再開という依頼。実装着手の承認: no。ADR: none。
- 2026-10-22: PR作成等のtool callに連動する自動購読を要件へ追加。根拠は依頼者の追加説明。操作intentと購読intentを先に保存し、外部資源確定前後の更新を回収する。実装着手の承認: no。
- 2026-10-22: 追加依頼に基づきTask 1対1の独立session、TaskでのWave呼び出し禁止、親子messageを採用。工程別の隠れたsession生成は取りやめ、独立検証・レビューは別Taskへ変更。Task_1/2/3/4/6/7/8/9/10の契約・移行・認可・検証へ反映。検証/レビューの候補依存と製品後続のmerge依存を分ける。実装着手の承認: no。
- 2026-10-22: 続く補正「Waveの下にPhase、その下にTask」「Taskは使い捨てsession」に基づき、上の継続Task案を置き換える。Task完了後のfix・再レビューは新Phase/Task/sessionへ渡し、同じthreadの継続は未完了Taskの待機・復旧に限定。既存FR-004/013・AT-009も意図的に改訂。初期Phaseは順次、Phase内Taskは並行とする設計案。実装着手の承認: no。
- 2026-10-22: 依頼者の公式記事リンクと、その先のworkflow資料を確認。会話外runtimeによる生成script実行、Phase表示と待ち合わせの区別を比較へ記録。初期の型付きプラン/Go controller方式を維持し、任意JavaScript実行は範囲に追加しない。親子message・PR自動購読の内部仕様を公式記事から推測しない。実装着手の承認: no。

## Notes

- リスク: 同じmainへの複数通知、tool requestとRPC応答の競合、移行時のunknown operation、並行PRのbase更新が主な重点。
- cache hitは性能観測とし、DB・成果物・履歴復旧やTask間の引き渡しをcacheへ依存させない。
- 文書PRの検証はplan validator、リンク/ID/参照の整合、git diff --check、独立レビュー。runtimeのATやGoテストの実行結果とは区別する。
