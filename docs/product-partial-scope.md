# 部分 PR の公開範囲と追跡先

`PrepareProductPublicationScope` は、現在の P Wave と製品 WorkItem の登録から、今回の範囲と範囲外の条件・追跡先を Go/SQLite で保存する controller port である。候補・品質・正式承認・merge・親最終受入は変更しない。

元要求の digest、source、scope/plan/policy 版、全条件の ID と要求、今回の登録、範囲外の条件とその条件を持つ別の登録済み WorkItem を対応付ける。今回含めない全条件に永続した追跡先が必要であり、単なる残作業の文字列や model の完了主張では成立しない。追跡先は同じ Wave/source/scope/policy の head・不変 version・membership を照合する。登録済みの tracking WorkItem を使うので、新 Task や model/runner/Git/外部操作は起動しない。

scope は plan revision ごとに head と不変 history を同じ transaction で保存する。完全な再実行は同じ証拠を返し、欠落・改変を再構成しない。独立した membership と version から失われた登録 head も検出する。追跡先の追加・要求や版の変更は古い証拠を公開許可へ流用できない。新 plan の scope は新しい証拠とし、旧証拠と累積 Wave 予算を保持する。metadata は WorkItem 128件・登録履歴1,024件・scope 1 MiB で有界にし、超過は切り捨てず保留する。

範囲外の条件は「今回の PR では評価しない条件」であり、追跡先が既に merge 済みかどうかをこの静的証拠から推測しない。実際の merge と残作業・全寄与者の完了は別の authenticated coverage ledger で判定する。全範囲の scope は範囲外の追跡先を必要としない。

実 SQLite の検査は、取りこぼし拒否、原子的な保存・rollback、再実行・再起動、head/history/membership/version の喪失、改変、新寄与者・source/要求変更、pause/期限、新 plan での旧証拠保持と予算維持を確認する。この PR は保存・読取 component であり、公開 body と publication/merge gate への接続は後続で行う。それまでは部分 PR の公開は既存 gate が保留する。Issue 分割の外部作成・継続 Task の自動計画・native P 全体の受入は、この component の成立だけでは完了にしない。
