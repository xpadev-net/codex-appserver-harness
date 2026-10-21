# Codex Issue / PR harness

Go製のIssue実装ハーネスです。[要件](requirements.md)と[実装計画](implementation-plan.md)に基づき、SQLiteで進行状態と外部操作を保存します。

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
