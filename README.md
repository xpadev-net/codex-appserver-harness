# Codex Issue / PR harness

Go製のIssue実装ハーネスです。SQLiteで進行状態と外部操作を保存します。

設計の改訂案（v0.4）は、大きな依頼をWave→Phase→Taskへ分解し、各Taskに独立した使い捨てsessionを割り当て、実行中は親子メッセージとイベント通知で再開する構成です。Taskは親と同じruntimeを使い、Wave呼び出しを禁止します。[改訂要件](requirements.md)、[現行との差分](docs/background-wave-gap-analysis.md)、[実装設計](implementation-plan.md)、[実行計画](docs/coding-agent/plans/active/background-wave-plan.md)を参照してください。
実接続の先行確認（S）、PRなしのWave基盤（K）、GitHub接続（P）、旧作業の移行（M）を分けて受け入れる計画です。Phase条件・失敗/反復・汎用成果・上限・親thread復旧は実装設計へ記録しています。
Waveの新DB、共通runtime、main/Task tools、並行scheduler、独立review/fixと検証済み完了のcomponent実装を追加しています。K用CLIの初期操作は以下を参照してください。実modelを含むK単独受入は[受入表](docs/background-wave-acceptance.md)に記録しています。Pへの接続、Mのfixture移行は追跡中です。

必要環境: Linux、Go 1.26とCコンパイラ、Git、認証済み`gh`、Codex CLI **0.163.0**、Bubblewrap (`bwrap`)。

```sh
go build -o harness ./cmd/harness
go test ./...
go vet ./...
./harness doctor --repo OWNER/REPOSITORY
```

初期構成はcontroller 1、実行中turn 1、進行中PR 1、runner job 1です。実装・独立検証のthreadを再利用し、テスト・CI・review・mergeの待機は通常コードで処理します。workerのgoalsとmulti-agent機能は無効にします。

## Wave kernel のローカル操作（K）

GitHub設定なしで、新しいprivate rootに直接依頼を保存できます。

```sh
./harness wave init --root /absolute/private/kernel --model YOUR_AVAILABLE_MODEL
cat > request.json <<'JSON'
{"schema_version":1,"request_id":"request-001","request":"調査結果を根拠付きで説明する","criteria":[{"id":"AC","requirement":"根拠を辿れるレポートがある"}]}
JSON
./harness wave create --root /absolute/private/kernel --file request.json
./harness wave list --root /absolute/private/kernel --limit 64
./harness wave inspect --root /absolute/private/kernel --id WAVE_ID --limit 64
```

`kernel.json`は設定version 2、`state.db`はkernel schema 2です。同じinit/request IDは同じ内容で再実行でき、設定や依頼を上書きしません。同じrootへの初期化はprocess間で排他し、初期化済みDBの欠損時は再作成しません。既存の未設定DB・v1 DBは明示移行/復旧が必要です。既存のPR用rootと混ぜずに使用してください。

これらの保存・閲覧コマンドはmodel・GitHubを起動しません。`create`は保存後にdaemonへbest-effort hintを送り、daemonが動いていなくても依頼を保持します。一覧は元要求本文をコピーせず、byte数と版・状態・予算の概要を返します。`next_after`で一覧、`next_task_after`/`next_phase_after`で詳細の続きを読み取れます。次ページにはそれぞれ`--after`/`--after-phase`を渡します。run本文はID・byte数・使用量への参照で表示します。

request JSONは12 MiBまで、元要求は8 MiBまで、criterionは256件までです。モデル用入力64 KiB/参照manifest、既定の累積Phase 16/Task 64/main turn 16/replan 8/fix round 3/Task Attempt 2などは保存したpolicyに従います。閲覧は通知ACKや判断適用を行いません。mainの監査付き復旧は下記コマンドで操作できます。K単独受入の証拠は[受入表](docs/background-wave-acceptance.md)を参照してください。

### Cursor付きwatch

```sh
./harness wave watch --root /absolute/private/kernel --once --cursor 0 --limit 32
./harness wave watch --root /absolute/private/kernel --cursor LAST_CURSOR --limit 32
```

JSON Linesでroot全体の保存済みイベントと`cursor`を返します。`--once`はdaemonなしで一ページを読みます。継続watchはprivate IPC daemonを利用し、切断後は最後に正常表示された`cursor`から再接続できます。空のheartbeatは重複表示せず、大きなpayloadはID・byte数の参照になります。watchはwakeや通知ACKを送らず、modelを起動しません。実行daemonは次の `wave run` で起動します。

### Kernel daemon

privateなruntime設定fileを用意し、foregroundで起動します。kernelのmodelは`wave init`で保存したものを使います。

```json
{"schema_version":1,"codex_binary":"/absolute/path/to/native/codex","worker_codex_home":"/absolute/private/empty-worker-template","session_auth_file":"/absolute/path/to/session/auth.json","launcher":["/absolute/path/to/scripts/isolate-linux"],"launcher_contract":"argv_passthrough_v1","isolation_verified":true,"git_directories":[]}
```

```sh
chmod 600 runtime.json
./harness wave run --root /absolute/private/kernel --runtime runtime.json
```

`codex_binary`はnative 0.163.0、launcherは子孫を含めた隔離を確認済みのものを指定します。`isolation_verified`はoperatorの確認を記録する値で、起動ごとの隔離/version probeも必須です。`worker_codex_home`はcontrollerのCODEX_HOMEとは別のprivate templateで、現在のsession認証を使う場合は空のdirectoryを用意し、`session_auth_file`で元sessionの認証fileを参照します。tokenをJSONへコピーせず、更新は元sessionが所有します。runtime設定・実行file・認証templateはkernel rootとworker workspaceの外に置きます。

daemonはroot単位で排他し、起動時に保存済み依頼/未適用結果を再照合します。保存後hintと定期再照合から共通scheduler/runtimeへ接続し、Task用のprivate固定workspaceを起動予約後に作成します。SIGINT/SIGTERMでは停止確認と保存を待ち、停止未確認の枠を解放しません。KではGitHub設定やPRを要求しません。native K workflow全体の受入は[受入表](docs/background-wave-acceptance.md)に記録しています。

### Wave / Task の優先操作

`wave inspect`の現在の版を確認し、operator JSONにtarget・一意key・理由を指定します。

```json
{"key":"pause-001","target":{"wave_id":"WAVE_ID","scope_revision":1,"plan_revision":1,"policy_revision":1},"action":"pause","reason":"作業を確認する"}
```

```sh
./harness wave pause --root /absolute/private/kernel --file operator.json
./harness wave resume --root /absolute/private/kernel --file operator.json
./harness wave cancel --root /absolute/private/kernel --file operator.json
```

各fileの`action`をコマンドと一致させ、新しい操作には新しいkeyを使います。同じkey/同じ内容の再送は元のreceiptを返し、操作を再適用しません。`task_id`を追加すると、そのTaskだけを操作します。cancelは終端で、resumeで戻しません。

`wave steer --file direction.json`は`{key,target,instruction,criteria?}`を保存し、旧runの停止・結果照合後にcontrollerが新scopeへ適用します。任意の新criterionを指定する場合もoperatorの明示入力として記録します。保存だけで実行中turnへsteer/interruptを送りません。

`wave interrupt --file interrupt.json`は`{key,target,session_id,run_id,preparation_id,reason}`で、inspectに表示された正しい実行世代を指定します。CLIは停止intentを保存し、共通runtimeがprocess停止を確認するまで枠を保持します。結果不明のrunを成功/失敗へ書き換えず、自動再実行も許可しません。

### 履歴の確認と監査付き main 復旧

```sh
./harness wave session --root /absolute/private/kernel --session main --limit 64
./harness wave history --root /absolute/private/kernel --id OBSERVATION_ID --limit 64
./harness wave history --root /absolute/private/kernel --id OBSERVATION_ID --after LAST_TURN_ID --limit 64
```

`session`は現在のbinding/停止状態/最後のrunと予算、および保存済みread-only履歴観測のheaderを返します。観測は`next_after`→`--after`でページングします。`history`は一つの観測のturn headerとcandidate/baselineを同じ方法でページングします。モデル本文は表示せず、観測のresponse/error digestを残します。これらの閲覧はmodel・wake・ACKを起動しません。

既知の停止済みmain runをoperatorが棄却する場合は、`wave reject-main --file rejection.json`を使います。fileは`{key,expected,run_id,snapshot:{id,digest},reason}`です。`expected`には直前の`session.binding`全体を渡し、snapshotは保存済みの入力参照を指定します。現在のtargetから入力を準備するには、`{"targets":[CURRENT_TARGET]}`をfileへ保存し、次を実行します。

```sh
./harness wave recovery-input --root /absolute/private/kernel --session main --file targets.json
./harness wave reject-main --root /absolute/private/kernel --file rejection.json
```

`recovery-input`は現在のscopeに基づく入力を保存してID/digestを返し、実行を開始しません。棄却はrunの物理的結果・使用量を保持し、理由付きで旧入力を処理して次のmain threadへのrotationを記録します。unknown・未停止・未照合の外部操作がある場合は拒否します。

曖昧なunknown開始について、保存された観測からrunに属するturnをoperatorが特定できた場合は、`wave associate-turn --file association.json`を使います。fileは`{key,expected,run_id,observation_id,turn_id,reason}`です。観測したrecovery ownerも停止済みでなければ拒否します。対応付けは選択したturn IDのみを保存し、unknownを成功へ変換しません。結果はdaemonが通常のread-only照合で確定します。同じkey/内容の再送は元receiptを返し、変更した再送は拒否します。未確定thread作成は次の監査操作で扱います。

### blocked Task の明示再試行

`wave retry-task --file retry.json`は`{expected,run_id,snapshot:{id,digest},reason}`を受け取ります。`expected`は直前のinspectの`task`全体、`run_id`は新しい一意IDです。`recovery-input --session TASK_SESSION_ID --file targets.json`で準備した保存入力を指定します。認可時はTask・停止状態・scope・予算を照合し、schedulerが同じrun ID/入力で共通runtimeへ渡します。認可・dispatch選択ではAttemptを消費せず、run予約と同じtransactionで一度だけ許可を消費します。

再試行も元のTask/threadを保持し、pause/cancel/方向修正・期限・Attempt上限を迂回しません。unknownや前run未照合では許可しません。起動準備がrun IDを取得した後でturn予約前に失敗した場合は、自動反復せず、`session.dispatch_hold`に理由を残します。設定を直した後は別のrun IDで新しく明示認可してください。同じID/内容の再送は過去の認可を再適用しません。

### 応答を失った thread 作成の処理

`session`で`creating=true`かつthread IDが未確定の場合、同じthread/startを自動反復しません。元processと同sessionのmanaged ownerがすべて停止済みであることを確認してから、次を使います。

```sh
./harness wave recover-thread --root /absolute/private/kernel --file recover.json
./harness wave retire-thread --root /absolute/private/kernel --file retire.json
```

main用の`recover.json`は`{key,action:"recover_main",expected,snapshot:{id,digest},reason}`です。`expected`は現在の`session.binding`全体、snapshotは`recovery-input --session main`で準備した保存参照です。元の作成intent・予算・Waveを保持し、新しいgenerationを許可する処理をreceiptと同じtransactionで記録します。pause等は解除しません。

Task用の`retire.json`は`{key,action:"retire_task",expected,reason}`です。旧Task/sessionを終端にし、置き換えが必要な場合は新しいTask/session IDを計画します。canceled Taskの作成を処理してもcancel状態は保持します。どちらも未停止・古いbindingやscopeを拒否し、既にthreadがあるunknown runを棄却する操作には使えません。同じkeyの再送は元receiptを返し、後で生じた別の未確定作成を処理しません。

### 成果物と親子メッセージの閲覧

```sh
./harness wave artifact --root /absolute/private/kernel --id ARTIFACT_ID
./harness wave artifact --root /absolute/private/kernel --id ARTIFACT_ID --content --offset 0 --bytes 16384
./harness wave mailbox --root /absolute/private/kernel --session TASK_SESSION_ID --cursor 0 --limit 64
./harness wave message --root /absolute/private/kernel --session TASK_SESSION_ID --id MESSAGE_ID
```

`artifact`は保存manifestを表示し、`--content`は内容全体のdigestを検証して最大16 KiBの抜粋を返します。`next_offset`を次の`--offset`へ渡し、`done`まで読めます。抜粋はUTF-8なら`encoding: utf8`、binaryや文字境界で分かれたbyte列は`encoding: base64`です。読み取りは有界bufferで検証し、破損した抜粋を返しません。

private rootを持つoperatorは過去scopeの保存成果も確認できます。これはworker toolのscope/grantとは別の閲覧portで、モデルの参照権限や現在の成果判定を変更しません。期限切れで削除された内容は復元しません。

`mailbox`は`cursor`/`more`で続きを辿り、送達状態・ACK状態・返信の有無を表示します。`message`の`request.reply_to`から元の質問を参照できます。sessionを省略すると保存したOrchestrator IDを使い、明示したsession以外の会話は読みません。閲覧はACK・返信送信・wake・model実行を行いません。

### run履歴と保存出力

```sh
./harness wave runs --root /absolute/private/kernel --session TASK_SESSION_ID --cursor 0 --limit 64
./harness wave output --root /absolute/private/kernel --id RUN_ID --offset 0 --bytes 16384
```

`runs`はsessionごとのrunを保存順にページングし、元のthread/turn、snapshot・target・使用量の台帳、active/停止/処理状態を返します。観測のないtoken数を確定使用量へ変換せず、欠けたbindingやbudgetは明示します。Task結果の処理記録には`result_id`があり、Wave inspectの成果参照と照合できます。一覧はモデル出力本文をコピーしません。

`output`は保存済み本文の最大16 KiBのbyte抜粋を返し、artifactと同じ`next_offset`/`done`/`encoding`で続きを読めます。unknownやarchivedを成功に変更しません。`disposed`はrunの処理記録があることを表し、独立reviewやWaveの成果受入を表しません。処理理由は有界previewと元のbyte数・省略の有無を示します。これらの閲覧は保存内容・通知・budget・実行状態を変更しません。

## direct 製品依頼の受付（P）

同じkernel rootへ、WorkItemを伴う製品依頼を明示的に保存できます。operatorがrepository名・数値ID・criterion割当を確認したprivate fileを用意します。例のrepositoryとIDは実際の対象へ置き換えてください。

```sh
umask 077
cat > product-request.json <<'JSON'
{"schema_version":1,"request_id":"product-001","request":"製品の動作を変更する","criteria":[{"id":"AC","requirement":"固定候補の検証が通る"}],"work_items":[{"id":"product-work-001","repository":"OWNER/REPOSITORY","repository_id":12345,"criteria":["AC"]}]}
JSON
chmod 600 product-request.json
./harness wave create-product --root /absolute/private/kernel --file product-request.json
```

入力は同じuser所有・単一hardlinkの通常fileに限定し、公開permissionやsymlinkを拒否します。Waveと全WorkItemのsource所属・初回通知を一transactionで保存し、同時再送は同じ受付へ収束します。既存の`wave create`はK用です。同じrequest IDをKとPの依頼で共用せず、内容の変更には明示的な方向修正を使います。

Pのstage/product flagと初期policy版はコマンドが指定し、実行上限・main sessionは保存した`kernel.json`から決めます。入力からpolicy、session、candidateを設定できません。JSONは12 MiB、元要求は8 MiB、request IDは4096 bytes、WorkItemは1〜128件/各定義64 KiBまでです。コマンド自体はmodelやGitHubを呼び出さず、commit後にdaemonへbest-effort hintを送ります。

これはPのsource受付です。既存Issueのhold/close/編集同期、製品worktree/readiness・停止後候補・daemonの書込worker配置は接続途中であり、製品Taskは`product_readiness_unavailable`で保留されます。公開gateや正式なCloud承認、P Waveの完了を省略しません。[保存契約](docs/product-operations.md)を参照してください。

### GitHub Issueの明示的な受付

ready/hold条件を満たす一つのIssueを、同じkernelのP Waveへ読み込めます。controller側の認証済み`gh`を使用し、IssueやPRへの書込は行いません。

```json
{"schema_version":1,"issue_number":42,"source_policy":{"repository":"OWNER/REPOSITORY","repository_id":12345,"ready_label":"harness:ready","hold_label":"harness:hold"}}
```

```sh
chmod 600 issue-import.json
./harness wave import-issue --root /absolute/private/kernel --file issue-import.json
```

privateなoperator fileでrepository名・数値IDと選択labelを指定します。Issue本文には下記Issue poolと同じ`harness-scope` JSON fenceが必要です。repository ID・open/ready/hold・IssueとPRの区別・依存Issueを確認し、取得前後のrepositoryとIssue本文/label/更新時刻を照合してから原子的に保存します。同じIssueは`rREPOSITORY_ID-iISSUE_NUMBER`をsource/WorkItem IDとし、編集内容での再送はscopeを上書きしません。

初回scopeはrevision 1・全criterionをincludedとするものに限定します。改訂済み・部分/既mergeのscopeは履歴を取り込む後続工程まで保留します。`harness-depends-on: 41,40`は32件までで、依存はclosed、対応するWorkItemを管理するkernel Waveがある場合は、direct/GitHubの入口にかかわらず製品Waveの完了も要求します。読取は最大30秒（CLI全体のretry枠は10秒）、各gh応答4 MiB/合計16 MiBの上限で、不明を成功にしません。HTTP取得とDB保存を跨ぐ外部の原子性はなく、公開前の再確認は引き続き必要です。

このコマンドは一件の明示受付です。初回以降のscope同期、製品Task/候補/正式承認/mergeの統合は継続中です。実GitHub接続やP全体の受入証拠は別途必要です。

### kernel daemonのIssue pool入口

一つのrepositoryの初回Issue受付を、ready走査と独立した読取workerで継続できます。privateな`issue-pool.json`を用意します。

```json
{"schema_version":1,"source_policy":{"repository":"OWNER/REPOSITORY","repository_id":12345,"ready_label":"harness:ready","hold_label":"harness:hold"},"priority_labels":["priority:high","priority:normal"],"attempts":8,"every_seconds":60}
```

```sh
chmod 600 issue-pool.json
./harness wave run --root /absolute/private/kernel --runtime runtime.json --issue-pool issue-pool.json
```

受付済みIssueのhold/close/要求編集/取得不能も定期観測する場合は、同じsource policyのprivate設定を追加します。

```json
{"schema_version":1,"source_policy":{"repository":"OWNER/REPOSITORY","repository_id":12345,"ready_label":"harness:ready","hold_label":"harness:hold"},"attempts":8,"every_seconds":60}
```

```bash
chmod 600 issue-source-observer.json
./harness wave run --root /absolute/private/kernel --runtime runtime.json --issue-pool issue-pool.json --issue-source-observer issue-source-observer.json
```

観測はsource保留だけを更新し、手動pauseを解除しません。Issueの編集は版付き同期まで保留します。[観測の制限と検証](docs/product-operations.md#受付済みissueの定期観測)も参照してください。

`attempts`は1走査1〜32件、間隔は1〜3600秒です。priority labelのgroup順に作成順のmetadataページを読み、各ページをIssue番号順で処理します。ページcursorとページ内の処理位置を保存して続きを読みます。各groupを読み終えると次へ進み、全groupの完了後は先頭から再走査します。保留された先頭Issueが独立した後続を塞ぎ続けず、再起動でも進行を保持します。走査のbyte/call予算が尽きたら停止し、未取得Issueのcursorは進めずに次回へ残します。走査のtimeoutは取得開始済み候補の進行を保存し、daemonの停止要求とは区別します。各候補は`import-issue`と同じfresh read/依存/原子的な受付を通り、controller全体のWave上限も維持します。

一覧は固定の読取専用GraphQL queryで番号・状態・labelだけを32件ずつ取得し、選んだIssueの本文だけをfresh GETで読みます。GraphQLのPOST transportはmutationを公開せず、任意queryも受け付けません。label一覧が部分的でも推測で選ばず、受付時の完全なlabelを再確認します。

1走査は最大2048 metadata entities・API data 16 MiB・128読取呼出し・45秒で、各gh応答は4 MiBまでです。失敗した過大応答も共通のtransport予算へ計上し、子の取得で走査予算をresetしません。取得不能・部分応答・不正なページ境界を完全な空集合にせず、保留/障害をdaemon診断へ出します。初回通知をcommitしてからwake hintを送ります。`IssuePool`は受付とhintを行い、model起動は共通scheduler/runtimeが担当します。daemonはcancel時に読取workerもjoinするまでcontroller lock/storeを保持します。

これは初回受付のworkerです。既存Waveを再取得/再openせず、既存Issueのhold/close/編集を禁止へ反映するには`--issue-source-observer`を併用します。版付き要求の採用は後続です。製品Taskのreadiness保留・正式承認・公開gateは維持します。PR observer/Webhookとは別workerで、必要なら同時にopt-inできます。

### 製品Taskのworktree準備

host設定でlocal repositoryと数値repository IDを対応付け、未開始のP work/fix Taskを一件ずつ準備できます。privateな要求ファイルを渡します。

```json
{"schema_version":1,"request":{"task_id":"TASK_ID","repository_id":12345,"repository":"/absolute/local/repository","base_sha":"0123456789abcdef0123456789abcdef01234567"}}
```

```sh
chmod 600 product-workspace.json
./harness wave prepare-product-task --root /absolute/private/kernel --file product-workspace.json
./harness wave product-workspace --root /absolute/private/kernel --id TASK_ID
```

固定SHAのprivate checkoutと停止証拠を保存します。既存の作業ディレクトリを上書きせず、開始途中の要求を自動で二重実行しません。modelを起動するコマンドではなく、製品候補・runner・正式gateの接続まではTaskの実行保留を維持します。[準備の契約と制限](docs/product-operations.md#製品taskの固定worktree準備)を参照してください。

## 設定と起動

```sh
./harness init \
  --config /absolute/private/harness/config.json \
  --repo OWNER/REPOSITORY \
  --repository-path /absolute/clone \
  --model YOUR_AVAILABLE_MODEL \
  --codex-binary /absolute/native/codex \
  --launcher /absolute/path/to/scripts/isolate-linux
```

既定ブランチとrepository IDはGitHub metadataから解決します。設定・状態ディレクトリは候補リポジトリの外に置きます。`init`は既存設定を上書きしません。

生成した設定でprofileコマンド、検証担当に許可するファイルパス、優先label、予算を確定してください。既定の`go test -v ./...`はGoプロジェクト向けの初期設定です。各profileは実行・成功の証拠となる`success_pattern`を必須とし、既定では実際のテストの`--- PASS:`行を要求します。テスト0件・全skip・空出力を合格にしません。既存設定にもprofileに適したpatternを追加し、policy revisionを更新してください。`verifier_paths`は正確なファイル名または`tests/`のようなディレクトリprefixです。`./scripts/ci`等の相対entrypointには、信頼する版の`entrypoint_sha256`も設定します。

`worker_codex_home`にはGitHub資格情報を含まない専用のCodex設定・認証を用意してください。ネイティブCodex実行ファイルを指定します。Nodeランチャーへのパスではありません。launcherは`launcher_contract: argv_passthrough_v1`として、後続のprogram/argv、cwd、環境、終了コードを保ち、終了時に子プロセスを残さないものを指定します。runnerは引数・cwd・終了コードのcanaryも確認します。実行環境の資格情報とネットワーク権限を検証してから`isolation_verified`を有効にします。workerとrunnerは実行時にもcontroller DB・既知のGitHub資格情報パスが見えないことを検査します。共有proxyが認証を注入する環境では、workerにGitHub書き込み権限を渡さない別のネットワーク設定も必要です。

```sh
./harness serve --config /absolute/private/harness/config.json
./harness status --config /absolute/private/harness/config.json
./harness inspect --config /absolute/private/harness/config.json --id WORK_ITEM_ID
./harness metrics --config /absolute/private/harness/config.json --id WORK_ITEM_ID
```

## Issue pool

`ready_label`が付き、`hold_label`がないopen Issueを取得します。優先labelの設定順、Issue番号順で選択します。Issue本文には明示された受入条件を置きます。

````text
Implement the requested behavior.

```harness-scope
{
  "revision": 1,
  "original_request": "",
  "criteria": [{"id": "AC-1", "requirement": "Reject duplicate IDs"}],
  "included": ["AC-1"],
  "remaining": [],
  "previously_merged": []
}
```
````

`original_request`は取り込み時に実際のIssue本文で置き換えます。分割・途中検証は実装threadから要求できます。部分PRでは`remaining`を残作業としてmerge前に保存します。先行PRのmerge後、最新baseから次のWorkItemを作成します。

依存するIssueは本文の独立した行に`harness-depends-on: 12, 34`と記載できます。管理対象の依存Issueは最終受入検証まで完了している必要があります。分割で作った子作業はDBの依存DAGでもdispatchを制御します。

Issue本文・title・policyが変わると古い判定を保留します。新しいscope JSON（revisionを1増やす）と理由を渡して明示的に取り込みます。部分PRのmerge後、後続登録前に編集された場合は`continuation_hold`を保存します。同じ`adopt`操作で新revisionの後続WorkItemを登録し、merge済みPRの履歴は保持します。この新scopeでは`previously_merged`を空にし、全条件を新revisionで検証してください。policyを変える場合は`verification.revision`も増やし、以前のpolicy snapshotを保持します。

```sh
./harness adopt --config CONFIG --id WORK_ITEM --file scope.json --reason 'updated acceptance criteria'
```

全範囲の PR を merge した後でも、親の最終受入前に要求・policy が変わった場合は同じ `adopt` で新しい後続 WorkItem を登録できます。残作業が空でも受理し、新 revision の全条件を実装・独立検証・review・最終受入で確認します。旧 PR・scope・policy と最終受入の履歴、Issue claim、累積予算は保持します。`inspect` が示す最終受入 owner を指定してください。

## Reviewとmerge

正式なGitHub `APPROVED`、信頼するreviewer ID、現在のcandidate SHA、scope/policy/roundの一致が必要です。COMMENTED・指摘なし・reaction・timeoutから承認を生成しません。必要なCIの名前・発行App ID・結論も固定できます。`ci_ref_mode: test_merge`ではtest merge commitのparentsと現在のhead/baseを照合します。

`capabilities_path`には導入先で採取した能力・証拠を指定します。Cloud依頼、正式承認、SHA対応、CI、保護ルールを迂回しない権限等が未確認なら自動mergeは無効です。`doctor`の初期結果だけでは有効になりません。ハーネス自身は承認reviewを発行しません。

開発時の`codex exec review`とGitHub Cloud reviewは別の経路です。開発PR 0〜7では前者を使用しています。

## 操作と復旧

```sh
./harness pause     --config CONFIG --id WORK_ITEM
./harness resume    --config CONFIG --id WORK_ITEM
./harness hold      --config CONFIG --id WORK_ITEM
./harness release   --config CONFIG --id WORK_ITEM
./harness cancel    --config CONFIG --id WORK_ITEM --reason 'canceled'
./harness interrupt --config CONFIG --id WORK_ITEM --operation RUN_ID --reason 'stop active turn'
./harness cancel-job --config CONFIG --id WORK_ITEM --operation JOB_ID --reason 'stop runner'
./harness rereview --config CONFIG --id WORK_ITEM --reason 'retry a completed or timed-out review'
./harness retry     --config CONFIG --id WORK_ITEM --reason 'blocker resolved'
./harness split     --config CONFIG --id WORK_ITEM --file proposal.json
```

`pause`は新規turn、`hold`はmerge送信を停止します。`cancel`後も送信済み操作の結果を追跡します。`interrupt`は実行中turnへの別の要求です。retryで使用量や修正回数は消えません。

未知のturn開始は保存した開始前のturn一覧と照合します。一意に関連付けられない場合は保留し、調査したIDを指定できます。

```sh
./harness associate --config CONFIG --id WORK_ITEM --operation RUN_ID --turn TURN_ID --reason 'verified thread history'
./harness reconcile --config CONFIG --id WORK_ITEM --operation RUN_ID --reason 'read existing turn result'
```

送信済み・応答不明の操作は自動再送しません。未実行を確認した証拠がある場合だけ、理由と証拠参照を指定します。

```sh
./harness retry --config CONFIG --id WORK_ITEM --operation OPERATION_ID \
  --reason 'external reconciliation proved no effect' --evidence EVIDENCE_REFERENCE
```

分割中の子Issue作成が不明な場合、既存markerを照合し、未作成を確認した子だけを指定します。`split_pending`と作成済みの子は維持し、対象の`creating`状態とoutboxの再試行を同じtransactionで更新します。

```sh
./harness retry --config CONFIG --id WORK_ITEM --operation SPLIT_OPERATION_ID \
  --child CHILD_KEY --evidence ABSENCE_AUDIT_REFERENCE --reason 'confirmed child creation was not executed'
```

runnerはjobごとのleaseとプロセス識別情報から`not_started` / `running` / `completed` / `stopped` / `unknown`を照合します。`Launched=false`だけでは再起動しません。未起動または全子プロセスの停止を確認した運用者は、証拠参照と確認状態を指定します。不明な古いjobには外部で確認した完全な停止の証拠が必要です。既知の稼働プロセスがある場合は拒否します。解決したjob IDは遅れて到着する起動からも保護し、成功結果にはしません。続行は予算を維持した`retry`で行います。

```sh
./harness reconcile-job --config CONFIG --id WORK_ITEM --operation JOB_ID \
  --job-state not_started --evidence AUDIT_REFERENCE --reason 'confirmed process was never started'
./harness retry --config CONFIG --id WORK_ITEM --reason 'runner launch repaired'
```

Webhookは任意です。未受信イベントは定期照合で回復します。署名secretは設定ファイルに入れず`HARNESS_WEBHOOK_SECRET`で渡します。

```sh
./harness webhook --config CONFIG --listen 127.0.0.1:8080
./harness gc --config CONFIG
```

ログはjobごとに8 MiBまで保存し、切り詰め時のパターン判定は判定不能にします。`max_artifact_bytes`で新規jobを停止し、`gc`は終端作業の保存期間を過ぎたログ・検証checkout・一時homeを削除し、ログの失効記録を残します。

詳細: [接続契約](docs/connection-contracts.md)、[受入検証と未確認事項](docs/acceptance.md)。

### 親の最終受入の復旧

PR の `MERGED` / 分割元の `SPLIT` と親全体の完了は別に保存します。`inspect` の `parent_final_state` は `waiting` / `running` / `blocked` / `repairing` / `completed`、失敗理由・分類・対象 SHA・失敗 job・必要操作・後続 WorkItem を示します。`blocked` の失敗結果を再読してモデルを起動し続けることはありません。終端 WorkItem の最終受入が保留された場合、取得済み Issue の claim を保ち、新しい Issue の取得を許可します。

環境障害・timeout・cancel・不明な実行は、原因と runner の停止を確認した上で明示的に再試行します。不明な job は先に `reconcile-job` で未起動 / 停止と証拠を記録します。`retry-final` は新しい job ID を発行し、`runner_retries` と検証 job 上限を引き継ぎます。失敗を成功に変更せず、古い証拠も保持します。

```sh
./harness retry-final --config CONFIG --id WORK_ITEM --evidence 'incident:runner-restored' --reason '実行環境を復旧し、旧 job の終了を確認'
```

テスト失敗は自動で後続修正 WorkItem を作り、失敗 SHA の checkout と元の要求・runner 証拠を独立検証担当へ渡します。分類後、実装不具合は実装担当、テスト不具合は検証担当、仕様不明や未分類は理由付き保留へ進みます。元の PR 履歴と scope は保持し、修正回数・既知の token 使用量・検証 job 数を引き継ぎます。修正 PR の独立検証・Cloud review・merge と親全体の最終受入が成立するまで親を閉じません。修正予算等で後続登録を保留した場合も `inspect` に必要操作が残ります。

後続修正をさらに分割した場合、その修正ツリー自体の全作業と最終受入が完了するまで親を閉じません。後続の scope を `adopt` すると旧失敗証拠を履歴へ移し、新しい scope で実装・独立検証を開始します。部分 PR の後続へ最終受入の担当を原子的に引き継ぎます。修正 WorkItem の中止は最終受入の保留として照合し、元の Issue claim と PR 履歴を保って別の Issue の取得を許可します。

実行前の hold / pause 等の保留を解除して再開する操作では、runner の再試行回数を消費しません。`runner_retries: 0` でも初回の最終受入を実行できます。修正の部分 PR とその後続も、累積の修正回数・検証 job 数・既知 token 使用量を共有します。

修正の分割・部分 PR でも既存の分割深さを保持します。子の修正 scope を更新した場合、親と依存先は現在の Issue claim と保存した修正・後続の系譜を確認して、その完了証拠を参照します。Issue 番号や版の大小だけで異なる scope を同一視しません。

### PR の検証証拠

初回公開と候補更新の PR 本文に、正式検証計画の profile と版、現在候補の最新結果、tested SHA、job ID、`harness-journal:work-items/WORK_ITEM/validation/JOB_ID` を掲載します。参照は controller の journal の識別子です。権限のある運用者は `harness inspect --config CONFIG --id WORK_ITEM` の `validation` で同じ job ID を探し、`log_path` の完全ログを controller 上で確認できます。`evidence_retention` が `expired` の場合、保持期間を過ぎたログは取得できません。PR 本文に private path、ログ本文、実行環境や秘密値は出力しません。探索的な profile や旧候補の結果を正式検証の要約へ混ぜません。
