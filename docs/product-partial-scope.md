# 部分 PR の公開範囲と追跡先

`PrepareProductPublicationScope` は、現在の P Wave と製品 WorkItem の登録から、今回の範囲と範囲外の条件・追跡先を Go/SQLite で保存する controller port である。候補・品質・正式承認・merge・親最終受入は変更しない。

元要求の digest、source、scope/plan/policy 版、全条件の ID と要求、今回の登録、範囲外の条件とその条件を持つ別の登録済み WorkItem を対応付ける。今回含めない全条件に永続した追跡先が必要であり、単なる残作業の文字列や model の完了主張では成立しない。追跡先は同じ Wave/source/scope/policy の head・不変 version・membership を照合する。登録済みの tracking WorkItem を使うので、新 Task や model/runner/Git/外部操作は起動しない。

scope は plan revision ごとに head と不変 history を同じ transaction で保存する。完全な再実行は同じ証拠を返し、欠落・改変を再構成しない。独立した membership と version から失われた登録 head も検出する。追跡先の追加・要求や版の変更は古い証拠を公開許可へ流用できない。新 plan の scope は新しい証拠とし、旧証拠と累積 Wave 予算を保持する。metadata は WorkItem 128件・登録履歴1,024件・scope 1 MiB で有界にし、超過は切り捨てず保留する。

範囲外の条件は「今回の PR では評価しない条件」であり、追跡先が既に merge 済みかどうかをこの静的証拠から推測しない。実際の merge と残作業・全寄与者の完了は別の authenticated coverage ledger で判定する。全範囲の scope は範囲外の追跡先を必要としない。

実 SQLite の検査は、取りこぼし拒否、原子的な保存・rollback、再実行・再起動、head/history/membership/version の喪失、改変、新寄与者・source/要求変更、pause/期限、新 plan での旧証拠保持と予算維持を確認する。公開・merge gate への接続は後述する。Issue 分割の外部作成・継続 Task の自動計画・native P 全体の受入は、この component の成立だけでは完了にしない。

## 品質準備・公開・merge gate への接続

部分範囲の WorkItem が独立検証・runner・review の品質条件を満たすと、daemon の品質準備は scope と readiness を同じ transaction で保存する。製品状態の更新や readiness の保存が失敗した場合は、scope/history も共に rollback する。readiness は optional な scope 全体を digest/history に含める。全範囲の既存 readiness は field を追加せず従来の JSON/checksum を保持する。

公開の品質読取は現在の追跡先と保存 scope を同じ transaction で再照合し、publication gate はこの証拠がある部分範囲を許可する。本文に評価した条件・Task result・検証/review 証拠と、今回の PR の範囲外の条件・登録済み tracking WorkItem・scope digest を表示する。範囲外の条件を今回の評価済み条件へ加えない。親 Issue を閉じるキーワードは従来の検査を通す。

merge の DB preflight と、送信前の外部 gate にも同じ照合を接続する。scope/head/history や追跡先の喪失・改変、変更された要求・版を古い品質証拠で通さない。Cloud 正式承認・CI/rules・source と固定候補の gate は引き続き必須とする。部分 PR の公開準備だけで親 Wave を完了にせず、実際の merge・残作業・全寄与者の coverage と親最終検証を別に確認する。

実 Git/SQLite/runner の統合検査は、最初から部分範囲で登録した Wave の work→検証→runner→独立 review→品質/scope 準備→publication gate、本文の範囲分離、追跡先喪失時の保留と元証拠の保持、途中失敗時の原子的 rollback を確認する。model の run/停止は fixture であり、GitHub への native 部分 PR/merge や P 全体の受入を示さない。
