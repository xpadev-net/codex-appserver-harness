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
特にAT-009の「新fix Task/session」は未実装で、旧role thread再利用の試験を新版の合格へ読み替えない。
2026-10-22のコードレビューでは`7dfa964`でGo tests/vet/buildと主要packageのrace検査が成功した。一時的なoverlay試験で#44/#45を再現したため、テスト全体の成功を要件全充足と扱わない。

| 段階 | 現在の状態 | 次の証拠 |
| --- | --- | --- |
| S | 成立。実model/launcherでAT-050が成功（1 test、2正常turn） | [実接続証拠](../evidence/background-wave/spike/session-observation.json)、[再現手順](connection-contracts.md#s-stage-dynamic-tool-spike) |
| K | 共通session/tool契約・driver基盤を追加。DB/controller未接続、K未成立 | Task_2〜7/9〜10。汎用成果、2 Task並行、独立review/fix、親子message、通知、CLI、復旧と上限 |
| P | 旧基盤のみ。Waveへの接続と外部能力の成立は未確認 | Task_8/11/13、既存#20。正式承認・CI/rulesとPR自動購読の証拠 |
| M | 未実装。実DB不存在は未確認 | Task_12 / AT-039/049のfixture、対象導入先の実移行またはnot_applicable |

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

## 導入先で残る接続検証

- 専用Codex認証、設定したmodel、実際のworker launcherでの実装・修正turn。
- 採用するGitHub App等の投稿主体によるCloud review起動。
- 指摘あり/なし、re-push、利用制限、正式APPROVEDのhead/round対応。
- 対象repositoryのrequired checks、test merge SHA、rules、bypass権限を持たない操作主体。
- 共有proxy等によるworkerへのGitHub書き込み資格情報の注入がないこと。
- プロダクト固有profile・最終受入検証の十分性。

これらをunknownからverifiedに変える根拠を能力reportへ保存する。一般的なコメントやローカルreviewを代替証拠にしない。
