# Codex Issue / PR harness

Go製のIssue実装ハーネスです。SQLiteで進行状態と外部操作を保存します。

設計の改訂案（v0.4）は、大きな依頼をWave→Phase→Taskへ分解し、各Taskに独立した使い捨てsessionを割り当て、実行中は親子メッセージとイベント通知で再開する構成です。Taskは親と同じruntimeを使い、Wave呼び出しを禁止します。[改訂要件](requirements.md)、[現行との差分](docs/background-wave-gap-analysis.md)、[実装設計](implementation-plan.md)、[実行計画](docs/coding-agent/plans/active/background-wave-plan.md)を参照してください。
実接続の先行確認（S）、PRなしのWave基盤（K）、GitHub接続（P）、旧作業の移行（M）を分けて受け入れる計画です。Phase条件・失敗/反復・汎用成果・上限・親thread復旧は実装設計へ記録しています。
Wave・メインOrchestrator・tool callによる自動購読・並行schedulerは未実装です。以下は現行バイナリの操作と制約です。

必要環境: Linux、Go 1.26とCコンパイラ、Git、認証済み`gh`、Codex CLI **0.163.0**、Bubblewrap (`bwrap`)。

```sh
go build -o harness ./cmd/harness
go test ./...
go vet ./...
./harness doctor --repo OWNER/REPOSITORY
```

初期構成はcontroller 1、実行中turn 1、進行中PR 1、runner job 1です。実装・独立検証のthreadを再利用し、テスト・CI・review・mergeの待機は通常コードで処理します。workerのgoalsとmulti-agent機能は無効にします。

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
