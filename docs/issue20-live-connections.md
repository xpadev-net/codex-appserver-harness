# #20 実接続の観測記録

観測日: 2026-10-21 UTC。利用者の指定に従い、現在の環境と `xpadev-net/codex-appserver-harness`（ID `1426858504`）で確認した。導入用の GitHub App、専用 worker 認証、model 設定は追加で指定されていない。この記録は完成した E2E の証明ではなく、確認できた範囲と残る接続条件の記録である。#20 は未完了として維持する。

| 対象 | 実際の観測 | 証拠 |
| --- | --- | --- |
| repository / 操作主体 | metadata が示す default branch は `master`。主体 `xpadev-net` / ID `96982836` は repository admin | [metadata](../evidence/issue20/repository-observation.json) |
| app-server | CLI `0.163.0` の initialize/initialized が成功 | [doctor](../evidence/issue20/doctor.json) |
| launcher | 実 `scripts/isolate-linux` 内で固定版 executable の起動と app-server 初期化が成功 | [launcher / isolation](../evidence/issue20/isolation-observation.json) |
| 既知の credential 到達性 | controller の資格情報パスが見えず、worker 環境へ GitHub token 変数を渡さない。無効な公開 sentinel で gh の client preflight だけを通した API read は 401。実認証済み worker の権限隔離全体を証明するものではない | [probe](../internal/service/connection_live_test.go) |
| 実 model / thread 継続 | 認証未設定の専用 probe home を使用。model turn / thread resume は未実行。controller の認証 profile を worker へ流用していない | [isolation observation](../evidence/issue20/isolation-observation.json) |
| CI 実行 | PR #31 の push / pull_request の両 job で Go test / vet が実行され成功 | [CI snapshot](../evidence/issue20/ci-observation.json) |
| CI 対象 SHA | 両 check の `head_sha` は head。一方 checkout は push が head、PR が test merge。両 SHA と test merge の parents を保存した | [CI snapshot](../evidence/issue20/ci-observation.json) |
| rules / bypass | branch protection は無効、effective rules は空。管理者主体であり、迂回権限のない操作主体の実証はできない。開発 PR の merge に `--admin` 等は使用していない | [metadata](../evidence/issue20/repository-observation.json) |
| Cloud | 実 provider の依頼受付、正式 APPROVED、head / round 対応は未確認。接続用 PR で provider 応答を観測し追記する | 本文下部 |

能力 report は [capabilities.json](../evidence/issue20/capabilities.json)。`auto_merge_ready` は false。CI の check 名や App ID と `head_sha` だけから、実 checkout SHA を捏造しない。未確認の model、thread 継続、Cloud 承認、SHA 対応、rules を verified にしていない。

## 再現

通常の Go tests は外部サービスへアクセスしない。信頼した読み取り probe だけを明示的に実行する場合:

```sh
HARNESS_LIVE_CONNECTION=1 \
HARNESS_LIVE_CODEX_BINARY=/absolute/path/to/native/codex \
HARNESS_LIVE_EVIDENCE=/private/path/isolation-observation.json \
go test ./internal/service -run '^TestLiveWorkerConnectionBoundary$' -count=1 -timeout 90s
```

この probe は production の worker environment と launcher を使い、実 model turn を起動しない。GitHub や Codex の credential 値、HTTP 応答本文、非公開ログを証拠へ保存しない。認証権限や controller path が見えた場合は失敗として記録する。読み取りの 401 と初期化だけでは、実 worker / model / 修正 turn の接続完了を意味しない。

CI 証拠は `gh api` で PR の head/base/test merge、check-run、workflow run を取得し、job log の checkout SHA と Go command を確認した。公開 fixture は識別情報と判定だけで、ログ本文を含まない。PR #31 の観測時点の test merge は `d66735841ee0cbc14644cb227c93b3f16c89362f`、parents は保存した head と base に一致する。

## 残る接続条件

- 専用 worker 認証と利用可能な model を設定した、実 launcher での実装・独立検証・修正と role thread 再開。
- 導入主体での Cloud 依頼受付、正式な GitHub review state `APPROVED`、immutable reviewer ID、review commit と round の対応。
- 実 provider での指摘あり/なし、re-push による旧結果の失効、利用制限の観測 fixture。
- 実 checkout SHA を識別できる CI 契約と、required checks / rules を迂回できない主体。
- 実認証済み worker の資格情報隔離。共有 proxy 等を使う導入先では、その経路による権限注入も確認する。

[公式 OpenAI documentation の GitHub 手順](https://learn.chatgpt.com/docs/third-party/github) は repository の接続と `@codex review` による起動を説明する。一般的なコメントや指摘なしを、本ハーネスの要求する正式 APPROVED の代替にはしない。[app-server の公式説明](https://learn.chatgpt.com/docs/app-server) にある接続初期化、thread の開始/再開、turn の開始/完了も、それぞれ区別して観測する。
