# P ローカルモデル工程の実接続

2026-10-24、native Codex 0.163.0 / `gpt-6.1-sol` / 現セッション認証で [opt-in Go test](../internal/kernelcli/product_live_test.go) を未skipで1件実行した。[証跡](../evidence/background-wave/product/native-local-quality.json) は実装→独立検証→隔離された正式runner→read-only独立reviewの完了を記録する。Task/session/threadは3件でそれぞれ一意、model runは3、main runは0、正式unit jobは1、全managed process停止を確認した。先行する[gpt-6-lunaの成功試行](../evidence/background-wave/product/native-local-quality-luna.json)も別に保持する。新しいprivate DBとremoteなしの一時Go repositoryを使用し、正負・ゼロを含む整数の積を実装・テストした。

これはhostが事前に採用した3 Phase計画のローカル工程検証である。modelによる初期計画、Issue取得、PR公開、正式Cloud APPROVED、CI/rules、merge、親最終受入は実行していない。P全体は未受入、Cloud能力は引き続きunknown。Kの受入証拠とは区別する。

実接続で、reviewの起動gateは正式runner成功を確認する一方、model入力にrunner証跡が含まれていない不足を検出した。`product_machine_verification`をreviewの保存manifestに追加した。対象Candidate、plan/policy、全profile、実際に停止したjobのstatus/tested SHA、completion/result/logのdigestとlog byte数のみを渡す。argv、private path、environment、raw reason/logは渡さない。subjectのCandidate/plan digestが一致しない場合や証拠欠落時はunavailableとする。過去の入力は不変で、この要約はPR公開・merge・最終受入の許可を与えない。

verification出力の`subject_digest`は`product_subject.digest`、reviewの`target_digest`は`product_subject.value.candidate_digest`である。両者の取り違えをガイダンスで明示した。旧試行では証拠なしのclaimed、異なるsubject digest、不正なplan field、review入力の不足が拒否された。保存済み結果や予算を編集して合格させず、修正ごとに新DBで実行した。最終再実行でもmodelが存在しないArtifact IDを返した試行は[参照拒否の証跡](../evidence/background-wave/product/native-local-reference-rejected.json)として別に保持する。IDを推定・補正して採用せず、その試行は未受入である。既存runtimeの既定予算は変更せず、最初の成功試験Waveには開始前に500,000 tokenの上限を設定した。別の試行は実装・検証の観測下限556,527 tokenで次のreview起動を保留したため、[予算停止の証跡](../evidence/background-wave/product/native-local-token-hold.json)を保持する。別のreviewは入力調査を完了できず、phase_idのない不正なblocked応答を返して拒否されたため、[未完レビューの証跡](../evidence/background-wave/product/native-local-incomplete-review.json)を保持する。最終再確認には新しいWaveと、以前のK実接続でも使ったgpt-6.1-solを選び、開始前に2,000,000 token上限を設定した。観測下限は請求token量の確定値ではない。

再現はGoでproduction CLIをビルドしてから、明示envを指定する。通常suiteはこの試験をskipし、認証を読まずmodelを起動しない。

```sh
go build -o /absolute/path/to/harness ./cmd/harness
HARNESS_LIVE_PRODUCT_LOCAL=1 \
HARNESS_LIVE_HARNESS_BINARY=/absolute/path/to/harness \
HARNESS_LIVE_CODEX_BINARY=/absolute/path/to/native-0.163.0/codex \
HARNESS_LIVE_MODEL=gpt-6.1-sol \
HARNESS_LIVE_PRODUCT_TOKENS=2000000 \
HARNESS_LIVE_PRODUCT_ROOT=/absolute/path/to/NEW-private-root \
HARNESS_LIVE_PRODUCT_EVIDENCE=/absolute/path/to/NEW-evidence.json \
go test ./internal/kernelcli -run '^TestLiveProductLocalModelVerificationReview$' -count=1 -v -timeout 12m
```

必要環境はLinuxのbubblewrapと`/usr/local/go/bin/go`。`HARNESS_LIVE_SESSION_AUTH_FILE`を省略すると現`CODEX_HOME/auth.json`（未指定時は`~/.codex/auth.json`）を使い、専用worker homeへ認証を供給する。controllerのhomeやDBをmodel/runnerへ公開しない。root/evidenceは既存のものを指定すると拒否する。認証値、daemon raw log、private DBを公開証拠へコピーしない。

`TestReviewInputIncludesCurrentMachineEvidenceWithoutPrivateRunnerData`でpassed/failed/not_run/prepared/launching/停止証拠欠落/破損証拠、exact SHA/profile、private情報非公開、過去snapshot不変、verification Taskへの非公開、SQL失敗の返却を検査する。fixtureの成功を上記native実行やP全体受入へ読み替えない。

## 登録済み分割子の実接続

2026-10-26、同じ native Codex 0.163.0 / `gpt-6.1-sol` / 現セッション認証を使い、`TestLiveProductScopedModelVerificationReview` を未skipで1件実行した。314.90秒（package全体314.918秒）で成功し、[証跡](../evidence/background-wave/product/native-scoped-local-quality.json)に3件の独立したTask/session/thread、model admission 3件、正式unit job 1件、固定candidateのtested SHAとlocal quality/scope digest、全managed process停止を記録した。main runは0、開始前の上限は2,000,000 token、観測下限は687,184 tokenだった。

元のMultiply要求を正・ゼロ部分と負数部分の2子に分割し、登録済みの正・ゼロ部分だけを実装→独立検証→正式runner→read-only reviewへ渡した。各Taskは元のACをunverifiedとして保持し、検証planとschema 4の`product_local_review`は完全なlocal criterionとscope digestに結び付く。Root品質は拒否され、coverageはmerge済み0件・未merge2件のままだった。local qualityを親受入へ流用しないことを検査している。

Issue APIはprivateなローカルadapterへ置換したが、productionの管理された作成subprocess、停止証明、list/GETによる作成照合、scope登録、周期的ソース監視と起動時GET、base planner、隔離、正式runnerを使用した。制御されたIssue POSTは2件で、実GitHub操作は0件。初期計画はhostが採用した3 Phase計画であり、modelによる初期分解・計画や実GitHub公開を証明しない。default 8/depth 3の全体実行、兄弟の完了、merge、正式Cloud APPROVED、親最終受入、P全体は引き続き未受入である。

実接続の先行試行で、checkout後のソース再確認が拒否されレビューを起動しなかった[未受入の証跡](../evidence/background-wave/product/native-scoped-source-revalidation-held.json)も保持する。保存済みDB・結果・予算を編集せず、新DBで再試行した。checkout前後の実GET、予約前product gate変更後の物理停止を条件とする再選択、新しい観測に追い越されたソースCASだけを対象とする一度の完全なGET再試行を追加した。観測時刻を再保存で更新せず、productionの鮮度期限も変更していない。競合の再現試験では一度の競合後の成功と、二度の競合後の保留を確認する。

再現には上記の共通envを使い、`HARNESS_LIVE_PRODUCT_LOCAL=1`を`HARNESS_LIVE_PRODUCT_SCOPED=1`へ、test名を`^TestLiveProductScopedModelVerificationReview$`へ置き換える。新しいroot/evidenceを必ず指定する。通常suiteではskipし、認証・model・GitHubへ接続しない。
