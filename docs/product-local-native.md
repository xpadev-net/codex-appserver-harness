# P ローカルモデル工程の実接続

2026-10-24、native Codex 0.163.0 / `gpt-6-luna` / 現セッション認証で [opt-in Go test](../internal/kernelcli/product_live_test.go) を未skipで1件実行した。[証跡](../evidence/background-wave/product/native-local-quality.json) は実装→独立検証→隔離された正式runner→read-only独立reviewの完了を記録する。Task/session/threadは3件でそれぞれ一意、model runは3、main runは0、正式unit jobは1、全managed process停止を確認した。新しいprivate DBとremoteなしの一時Go repositoryを使用し、正負・ゼロを含む整数の積を実装・テストした。

これはhostが事前に採用した3 Phase計画のローカル工程検証である。modelによる初期計画、Issue取得、PR公開、正式Cloud APPROVED、CI/rules、merge、親最終受入は実行していない。P全体は未受入、Cloud能力は引き続きunknown。Kの受入証拠とは区別する。

実接続で、reviewの起動gateは正式runner成功を確認する一方、model入力にrunner証跡が含まれていない不足を検出した。`product_machine_verification`をreviewの保存manifestに追加した。対象Candidate、plan/policy、全profile、実際に停止したjobのstatus/tested SHA、completion/result/logのdigestとlog byte数のみを渡す。argv、private path、environment、raw reason/logは渡さない。subjectのCandidate/plan digestが一致しない場合や証拠欠落時はunavailableとする。過去の入力は不変で、この要約はPR公開・merge・最終受入の許可を与えない。

verification出力の`subject_digest`は`product_subject.digest`、reviewの`target_digest`は`product_subject.value.candidate_digest`である。両者の取り違えをガイダンスで明示した。旧試行では証拠なしのclaimed、異なるsubject digest、不正なplan field、review入力の不足が拒否された。保存済み結果や予算を編集して合格させず、修正ごとに新DBで実行した。既存runtimeの既定予算は変更せず、試験Waveに開始前に500,000 tokenの上限を設定した。これは請求token量の観測ではない。

再現はGoでproduction CLIをビルドしてから、明示envを指定する。通常suiteはこの試験をskipし、認証を読まずmodelを起動しない。

```sh
go build -o /absolute/path/to/harness ./cmd/harness
HARNESS_LIVE_PRODUCT_LOCAL=1 \
HARNESS_LIVE_HARNESS_BINARY=/absolute/path/to/harness \
HARNESS_LIVE_CODEX_BINARY=/absolute/path/to/native-0.163.0/codex \
HARNESS_LIVE_MODEL=gpt-6-luna \
HARNESS_LIVE_PRODUCT_ROOT=/absolute/path/to/NEW-private-root \
HARNESS_LIVE_PRODUCT_EVIDENCE=/absolute/path/to/NEW-evidence.json \
go test ./internal/kernelcli -run '^TestLiveProductLocalModelVerificationReview$' -count=1 -v -timeout 12m
```

必要環境はLinuxのbubblewrapと`/usr/local/go/bin/go`。`HARNESS_LIVE_SESSION_AUTH_FILE`を省略すると現`CODEX_HOME/auth.json`（未指定時は`~/.codex/auth.json`）を使い、専用worker homeへ認証を供給する。controllerのhomeやDBをmodel/runnerへ公開しない。root/evidenceは既存のものを指定すると拒否する。認証値、daemon raw log、private DBを公開証拠へコピーしない。

`TestReviewInputIncludesCurrentMachineEvidenceWithoutPrivateRunnerData`でpassed/failed/not_run/停止証拠欠落、exact SHA/profile、private情報非公開、過去snapshot不変、verification Taskへの非公開を検査する。fixtureの成功を上記native実行やP全体受入へ読み替えない。
