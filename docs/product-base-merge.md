# 固定候補への通常 base merge

`workspace.MergePinnedBase` は host が固定した候補と新 base SHA を受け、通常の `ort` merge を実行する Go の物理 Git port である。呼出し側は実 model/runner を停止し、共有 Git・worktree lease と永続した一度限りの mutation fence を持つ必要がある。この port 自体は DB、candidate 採用、Task、検証、PR を変更しない。

開始前と fence の直後に、canonical Git metadata、保存済み linked checkout・branch・HEAD、clean 状態、既存 Git operation の不在、固定 commit のローカル存在と旧 base からの ancestry を照合する。新 base は branch 名や URL ではなく固定 SHA であり、partial clone による暗黙取得を元 repository と worktree の設定の両方で拒否する。

新 base が候補へ既に入っていれば、fence を消費せず `already_current` を返す。それ以外は `--no-ff` の通常 merge を行い、正常時は元候補・新 base をその順に親へ持つ commit、clean 状態と保存 checkout を照合して `merged` を返す。既存の履歴や base branch は動かさない。hook、外部 filter/merge driver、署名、editor、rerere、自動 stash と branch 固有の merge strategy 指定を抑止する。

競合時は元候補の HEAD と新 base の `MERGE_HEAD` を照合し、有界の未解決 path 集合と `conflict` を返す。未解決 index と作業内容を保持する。エラーや取消し、停止未確認は完了とせず、呼出し側が保存済み intent/lease を保持して照合する。実行した command の停止が確認できた失敗では、取消しから独立した3秒上限の読取で HEAD・MERGE_HEAD・clean 状態・未解決 path を `Failure` に観測する。未解決 path がない非正常終了も `ErrBaseMergeUnsettled` と状態観測を返し、部分変更を成功や単なる未着手へ読み替えない。停止未確認では観測せず、読取失敗も元の失敗と共に返す。`Failure` は診断情報であり lease 解放や再試行を許可しない。自動 reset・abort・再実行は行わない。

実 temp Git の検査では、両変更と両親の保持、既に取り込み済みの read-only 結果、競合内容/HEAD の保持と二重実行拒否、branch strategy/独自 hooksPath の hook/署名/attribute driver の抑止、未知/別系統 SHA、汚れた checkout、既存 operation、fence 前後の変更、worktree 固有 promisor 設定、merge コマンド中の取消し、実際には index/worktree を変更したのに非正常終了して未解決 path がない場合の照合観測を確認する。

これは Task_11 の通常 merge の component 実装である。daemon の現在 HEAD 観測・取得・永続 claim/send/stop、候補の新版採用・検証の取り直し、および競合を新しい fix Task/session へ渡す接続は後続 PR に残る。P 全体の受入には数えない。
