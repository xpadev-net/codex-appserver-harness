# 実装と受入検証

実装はGo。TypeScriptという計画上の候補はユーザー指定で置き換えた。

| 領域 / 受入条件 | 実装と証拠 |
| --- | --- |
| 接続契約・未確認の承認を拒否 (AT-001,015) | `internal/contract`, pinned schema manifest, doctor; Cloud能力はunknownのまま |
| 取得排他・重複/逆順・transaction復旧 (AT-002,020,021) | SQLite integration、inbox/outbox、Completeの原子的遷移、PR/依頼marker照合 |
| thread継続・引き渡し・中断・入力 (AT-003,004,012,028) | worker contract tests、開始前turn一覧、厳密なJSON、要求保存、運用CLI |
| 独立検証・部分範囲・テスト根拠 (AT-005,006,009,010,016) | verification plan/assessment tests、別role thread、ファイル範囲検査、修正routing |
| 固定SHA・runner・未実行/空結果の拒否 (AT-007,011,013,019) | real temporary Git + runner tests、profile集合、head/test-merge parents照合 |
| 待機でturnゼロ・成功後の不要な再承認なし (AT-008,029,030) | service integrationのrunner/review待機、phase付き起動記録、metrics、観測token/未知使用量を区別 |
| merge gate・失効・応答不明 (AT-014,017,018,022) | gate/review tests、serviceのbase更新、scope/policy失効、送信前照合、未知mergeの完了確認 |
| 分割・部分PR・親全体 (AT-023,024,025) | split/DAG tests、子作成応答欠落、依存dispatch、serviceの子PR/部分PRと最終受入runner |
| 修正・使用量・policy境界 (AT-026,027,028) | 全体カウンタ、token予算、外部policy digest、pause/cancel/hold、namespace launcherと秘匿パスprobe |

service統合テストはfake GitHub / model / job adapterと実際のローカルGit repositoryを使う。
この試験はCloudの実際のAPPROVED出力、アカウント権限、実モデルの品質や本番runner profileの十分性を証明しない。

## v0.4の適用と現在の未達

上表は旧PRハーネスの実装・試験への対応であり、v0.4の全ATが合格したという表ではない。
AT-009の新fix Task/sessionはKのcomponentで追加・検証した。Pの旧role thread経路への接続は未完で、旧試験を新版の合格へ読み替えない。
2026-10-22のコードレビューでは`7dfa964`でGo tests/vet/buildと主要packageのrace検査が成功した。一時的なoverlay試験で#44/#45を再現したため、テスト全体の成功を要件全充足と扱わない。

| 段階 | 現在の状態 | 次の証拠 |
| --- | --- | --- |
| S | 成立。実model/launcherでAT-050が成功（1 test、2正常turn） | [実接続証拠](../evidence/background-wave/spike/session-observation.json)、[再現手順](connection-contracts.md#s-stage-dynamic-tool-spike) |
| K | 合格。新DB・汎用WaveのK適用ATを確認 | [K受入表と実行証拠](background-wave-acceptance.md)。P/Cloud/移行の完了と分ける |
| P | [Task_8 component](task8-github-acceptance.md)はfixture合格。製品Wave工程の接続と外部能力の成立は未確認 | Task_11/13、既存#20。正式承認・CI/rulesと製品パイプラインの証拠 |
| M | fixture移行は未実装。依頼者確認で運用DBなし | Task_12 / AT-039/049のfixture、対象導入先の実移行またはnot_applicable |

新しいAT-050〜059は[要件の受入表](../requirements.md#10-受入テスト)を原本とする。K/P/Mで適用/未実行/blocked/合格を別々に記録し、実装前の目標値を実測値として掲載しない。
旧基盤の [#44](https://github.com/xpadev-net/codex-appserver-harness/issues/44) は [PR #52](https://github.com/xpadev-net/codex-appserver-harness/pull/52)、[#45](https://github.com/xpadev-net/codex-appserver-harness/issues/45) は [PR #53](https://github.com/xpadev-net/codex-appserver-harness/pull/53) で修正し、回帰・Codex review・CIを確認した。新しいTask/session工程の受入は引き続きKで検証する。

## 実環境で確認したもの

- GitHub repository ID/default branchのmetadata取得。
- Codex CLI 0.163.0のschema生成、app-server initialize/initialized。
- GitHub check-runsのpagination形式。
- `scripts/isolate-linux`を用いたmount namespace内から、既知のcontroller GitHub資格情報パスが見えないこと。
- 開発PRごとの`codex exec review --base <既定ブランチ>`とローカルGo tests/vet。

2026-10-21 の追加観測では GitHub Actions の push / pull_request job が実起動し、Go tests/vet が成功した。check の head SHA と実 checkout SHA の違いを保存した。現在の branch は保護されておらず、管理者主体での開発 merge は rules / bypass 契約の証明にはしない。

現在の接続証拠と未確認事項は [#20 実接続記録](issue20-live-connections.md) と [能力 report](../evidence/issue20/capabilities.json) を参照する。#20 の E2E 完成条件は未達。

## native構造化出力の接続確認（K component）

native 0.163.0と現在のsession認証、`gpt-6.1-sol`で新DBの`wave create/run`を実行した。初回main turnはproviderの`invalid_json_schema`（HTTP400、requiredからacceptanceが欠落）でfailedとなり、物理停止後に明示復旧待ちへ入り、自動反復しなかった。

provider向け出力schemaだけを全property requiredへ変換し、nullable pointer・既存のinput/Go decode契約を保持する修正を追加した。保存したfailed runを`reject-main`で監査付き処理し、累積予算を保持して新generationへ進めた。以降のmain/Task出力は受理され、初稿→findings→新fix→新review clean→reportと進んだ。定型fix/review間の追加main turnは0、全managed processは停止済みである。

[秘匿値とprivate pathを除いた観測](../evidence/background-wave/kernel/strict-output-schema.json)は8 run（7 completed、1 failed）を記録する。この最初のaccept提案は、以前の必須Task結果にremainingがあるため保留された。後述の別DBで結果契約を明確にした新しい実行を確認した。証拠gateは緩めていない。これはschema接続の修正・pipeline観測であり、K全体合格や製品P/Cloud承認の証拠には数えない。

## 導入先で残る接続検証

- 専用Codex認証、設定したmodel、実際のworker launcherでの実装・修正turn。
- 採用するGitHub App等の投稿主体によるCloud review起動。
- 指摘あり/なし、re-push、利用制限、正式APPROVEDのhead/round対応。
- 対象repositoryのrequired checks、test merge SHA、rules、bypass権限を持たない操作主体。
- 共有proxy等によるworkerへのGitHub書き込み資格情報の注入がないこと。
- プロダクト固有profile・最終受入検証の十分性。

これらをunknownからverifiedに変える根拠を能力reportへ保存する。一般的なコメントやローカルreviewを代替証拠にしない。

## native K の完了経路と結果契約

Taskの`remaining`/`blockers`は割り当てられた作業内の未完了を表し、後続Phaseの予定を含めない。`claimed`は成果に対する作業者の根拠付き主張であり、独立承認ではない。この区別をroleごとの保存入力へ追加した。未完了を隠す指示や受入gateの緩和は行っていない。

opt-in Go test `TestLiveKernelCLIFixReviewAcceptance`で、native0.163.0 / gpt-6.1-sol / 現session認証を使い、新DBのCLI init/create/runから実行した。[正常完了の観測](../evidence/background-wave/kernel/native-fix-review.json)は未skip 1 test、8 completed run、初期Task 2件の実行区間の重なり、findings→新fix→新review clean→report→検証済みWave completionを記録する。mainは初回と最終判断の2回で、定型反復中の起動は0。選択された修正版の実bytes・digest・計算値・marker不在をGo testで照合し、全managed process停止を確認した。

新しい試験rootの開始前にtoken上限2,000,000を明示設定した。製品既定値1,000,000は変更していない。観測下限908,212 tokenは最終請求量ではない。[既定予算での停止](../evidence/background-wave/kernel/native-token-budget-stop.json)と[producerのunverifiedによる受入保留](../evidence/background-wave/kernel/native-criterion-hold.json)も別の試行として保持する。実行済みDBの結果・予算を変更して通していない。content criterionと独立reviewのworkflow gateを区別した新規ケースで完了を確認した。

再現は新規のprivate rootと絶対パスを指定する。通常のGo testsではモデルを起動せずskipする。

```sh
HARNESS_LIVE_KERNEL=1 \
HARNESS_LIVE_CODEX_BINARY=/absolute/path/to/native/codex \
HARNESS_LIVE_MODEL=gpt-6.1-sol \
HARNESS_LIVE_SESSION_AUTH_FILE=/absolute/path/to/current-session/auth.json \
HARNESS_LIVE_KERNEL_ROOT=/absolute/private/NEW-kernel-root \
HARNESS_LIVE_KERNEL_TOKENS=2000000 \
HARNESS_LIVE_KERNEL_EVIDENCE=/absolute/path/to/sanitized-evidence.json \
go test ./internal/kernelcli -run '^TestLiveKernelCLIFixReviewAcceptance$' -count=1 -v
```

この経路の成功はKの実model pipeline証拠であり、K適用AT全体の判定は [K受入表](background-wave-acceptance.md) に記録する。P/Cloud正式承認・M移行の総合判定は別途行う。

## production KernelSource の通知遅延参照負荷

`TestKernelSourceNotificationReservationReferenceLoad` は本番 `KernelSource`/Loop/SQLite/IPCで独立Task 2件を初回起動して待機させ、親から子への通知を各50件、合計100件保存する。sourceの選択・保存入力の構築・受付・結果適用・wake確定は実装本体を通り、process/model/外部I/Oだけをstubにする。各通知を別の再開intentへ対応させ、全100件の適用、同じ未完了threadの継続、main起動0、各組の物理停止と枠解放を検査する。

[測定結果](../evidence/background-wave/kernel/notification-latency.json): 100 samples、p95 64.032 ms、max 79.840 ms、測定区間合計7.952秒で、NFR-012のSHOULD目標1秒以内を満たした。通知保存呼出し直前から、永続wakeを選択した有界Source.Readyの終了後までの保守的な上限を測る。モデル応答時間・実プロセスcold startは含めず、[native起動/停止/RSS](../evidence/background-wave/runtime/parallel-native.json)と[隔離launcher](../evidence/background-wave/runtime/isolated-launcher.json)へ分ける。hostとGo/OS/CPU、上限、全sampleはJSONに保存する。この参照負荷単独をK全体合格とはしない。

再現: `HARNESS_KERNEL_LATENCY_EVIDENCE=/absolute/path/to/evidence.json go test ./internal/control -run '^TestKernelSourceNotificationReservationReferenceLoad$' -count=1 -v`。runtime policyは試験rootだけTaskAttempts=51へ明示設定し、製品既定値を変更しない。従来の試験用WorkSourceによる[component測定](../evidence/background-wave/runtime/notification-latency-race.json)とは別の証拠である。

## P ローカルモデル工程（全体未受入）

実modelの実装・独立検証・正式runner・read-only reviewは[実接続記録](product-local-native.md)と[証跡](../evidence/background-wave/product/native-local-quality.json)を参照する。host採用済み計画のローカル品質確認であり、正式Cloud承認・PR/merge・親最終受入は未実行。P全体と#20は未達のままとする。

## P 最終 HEAD の取得 component

[固定 commit の取得](product-final-fetch.md)は共有 Git lease・一度限りの送信・実停止証拠を使い、取得した SHA を正式 runner へ渡す。GitHub 観測/承認は fixture として検査した component であり、[daemon の自動最終工程と親受入直前の確認](product-final-daemon.md)にも接続した。P 全体の native 受入とは区別する。
