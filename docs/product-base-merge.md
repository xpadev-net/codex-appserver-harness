# 固定候補への通常 base merge

`workspace.MergePinnedBase` は host が固定した候補と新 base SHA を受け、通常の `ort` merge を実行する Go の物理 Git port である。呼出し側は実 model/runner を停止し、共有 Git・worktree lease と永続した一度限りの mutation fence を持つ必要がある。この port 自体は DB、candidate 採用、Task、検証、PR を変更しない。

開始前と fence の直後に、canonical Git metadata、保存済み linked checkout・branch・HEAD、clean 状態、既存 Git operation の不在、固定 commit のローカル存在と旧 base からの ancestry を照合する。新 base は branch 名や URL ではなく固定 SHA であり、partial clone による暗黙取得を元 repository と worktree の設定の両方で拒否する。

新 base が候補へ既に入っていれば、fence を消費せず `already_current` を返す。それ以外は `--no-ff` の通常 merge を行い、正常時は元候補・新 base をその順に親へ持つ commit、clean 状態と保存 checkout を照合して `merged` を返す。既存の履歴や base branch は動かさない。hook、外部 filter/merge driver、署名、editor、rerere、自動 stash と branch 固有の merge strategy 指定を抑止する。

競合時は元候補の HEAD と新 base の `MERGE_HEAD` を照合し、有界の未解決 path 集合と `conflict` を返す。未解決 index と作業内容を保持する。エラーや取消し、停止未確認は完了とせず、呼出し側が保存済み intent/lease を保持して照合する。実行した command の停止が確認できた失敗では、取消しから独立した3秒上限の読取で HEAD・MERGE_HEAD・clean 状態・未解決 path を `Failure` に観測する。未解決 path がない非正常終了も `ErrBaseMergeUnsettled` と状態観測を返し、部分変更を成功や単なる未着手へ読み替えない。停止未確認では観測せず、読取失敗も元の失敗と共に返す。`Failure` は診断情報であり lease 解放や再試行を許可しない。自動 reset・abort・再実行は行わない。

実 temp Git の検査では、両変更と両親の保持、既に取り込み済みの read-only 結果、競合内容/HEAD の保持と二重実行拒否、branch strategy/独自 hooksPath の hook/署名/attribute driver の抑止、未知/別系統 SHA、汚れた checkout、既存 operation、fence 前後の変更、worktree 固有 promisor 設定、merge コマンド中の取消し、実際には index/worktree を変更したのに非正常終了して未解決 path がない場合の照合観測を確認する。

これは Task_11 の通常 merge の component 実装である。新規 WorkItem の daemon による現在 HEAD 観測・取得・永続 claim/send/stop は後述の工程へ接続した。旧候補への適用・新版採用・検証の取り直し、および競合を新しい fix Task/session へ渡す接続は後続 PR に残る。P 全体の受入には数えない。

## 別 WorkItem の merge を待つ準備

P の work/fix Task が `depends_on` で別 WorkItem の work/fix/verification/review Task を指定した場合、その成果を含む merge を待つ。`ReadProductTaskPreparation`、workspace の reserve/claim/finish、model の ready/turn 予約は共通の DB gate を使う。現在の scope/policy/source/registration/candidate projection に対応する authenticated coverage と merged receipt を読み、merge 時の品質パケットに依存 Task の保存済み結果 ID と入力 digest が含まれることを照合する。

先行 Task の正常完了や native merge 応答だけでは解除しない。receipt/history が失われた場合は準備、Git の claim、model の新起動を保留する。Git の開始後に証拠が失われた場合も、実停止の証拠は stale として保存し、準備成功や model 起動へ変換しない。P の同じ WorkItem 内の検証・レビュー・修正と、汎用 Wave は従来の成果依存を使う。

実 SQLite の検査は、完了済み Task と merge receipt の区別、同じ要求版での plan carry、receipt 復旧による準備再開、遅い proof 喪失による claim/ready の保留、親 turn・新 Task・Wave 完了の非生成を確認する。外部 merge の応答と品質証拠は fixture であり、native P の受入ではない。先行 commit を現在の作業に取り込む base 更新の接続は引き続き後続へ残る。

## Task ごとの固定 base 計画

`product.BasePlanner` は unused な P の work/fix Task について、現在の要求版・Task・WorkItem 登録・host verification policy と上記の先行 merge 証拠を `ProductBaseInput` にまとめる。host 設定から repository/default branch と初期 SHA を選び、source を確認し、GitHub の repository/ref を前後で読む。新 HEAD が既定の初期 base（既存候補がある場合はその comparison base）と、同じ repository の全先行 merge の子孫であることを比較 API で確かめる。metadata の clone URL も一致し、資格情報付き URL を拒否する。

計画は入力 digest・repository・branch・初期 SHA・HEAD・取得先・祖先集合で固定する。同じ HEAD の再観測は元計画の履歴を変えず、現在の選択と観測時刻を別に保存する。HEAD が変われば別計画を作る。effect 前の保存済み選択確認は30秒を超える観測、Task/source の変化、先行証拠の喪失、pause を保留する。plan/head/history/admission/current selection の欠落を、新しい HEAD の再観測で作り直さない。保存途中の失敗は計画と選択を共に rollback する。

この reader は read-only 外部観測と DB への host 計画保存を担い、単独では Git の取得・workspace への適用・候補の採用や model/runner 起動を許可しない。実 Git の取得と model readiness への接続は後述する。テストの GitHub HEAD/ancestry は fixture として扱い、native P の受入には含めない。

## 固定 base の取得と workspace 証拠

host が `ProductWorkspaceRequest` に base plan の ID/digest を渡すと、workspace の予約・claim・取得送信・停止証拠がその固定計画を参照する。現在の入力と30秒以内の選択を照合し、fresh な work/fix の作業開始だけを許可する。claim と送信は一度限りで、共通 Git directory・worktree の実 lease と generation を取得前にも確認する。plan/admission/claim/send/terminal の欠落は再生成や再送へ進めない。

`WorkspacePreparer.RefreshBase` に host reader を明示した場合、取得直前にも source/default HEAD を再観測し、固定 SHA の object を `FetchPinnedCommit` で取り込んでから private linked checkout を作る。元 repository の branch/checkout を動かさず、checkout の SHA・clean 状態・Git metadata と process group の停止を検査する。ローカルに object があれば送信 fence を消費しない。停止未確認では terminal 証拠と capacity 解放を保存しない。取得後の HEAD 変更や既知の失敗は、元の取得事実を保持して workspace 成功と model 起動を保留する。

準備時の元 Task 入力と、固定 workspace を追加した後の入力 digest の対応を保つ。同じ HEAD の観測更新で元計画を作り直さない。初回 turn は fresh な選択を必要とし、thread 作成後でも model run 前なら read-only の再観測を行える。開始済みの未完了 Task の通常再開は元の計画を参照し、scope/policy/入力/先行 merge と現在の制御を再確認する。run admission に base selection と実 snapshot/workspace の対応を保存する。

旧形式で計画参照がない workspace の JSON/checksum は変更しない。新しい証拠 field は使用時だけ保存する。

実 temp Git/SQLite の検査では、新 SHA の実取得・同じ SHA の checkout、既存 object の無送信、送信直前の source 保留、取得後の HEAD 変更、replay、停止未確認の claim/capacity 保持を確認する。DB の起動検査では初回 turn の時刻失効、thread 作成後の再観測、同じ未完了 Task の waiting→入力通知→再開、run/snapshot の監査対応、journal 喪失を検査する。HTTP/ancestry と model 実行は fixture で、native P の証拠には含めない。

## daemon の自動選択と準備例外

CLI の publication 設定がある場合、実 `KernelSourceGate` と GitHub reader を `Executor.BasePlanner` へ接続する。新しい WorkItem の unused な work/fix Task は現在の default HEAD を選び、上記の取得・workspace 証拠を経て model 起動へ進む。publication 設定を持たないローカル工程は host の初期 base を使う。

準備済みで未実行の Task は、観測時刻が失効したときだけ source/HEAD を読み直す。同じ HEAD なら保存 workspace を変更せず選択時刻を更新する。異なる HEAD なら既存 workspace を書き換えず初回 turn を保留する。照合は4 Task ごとに runner owner の確認へ戻り、残りは cursor を進めて扱う。未 claim の prepared profile は `not_started` の停止証拠を保存して Git capacity を解放する。開始済み・停止未確認の owner は再実行や解放を行わない。

既知の準備失敗、未解決 owner、証拠の欠落、準備後の base 変更は、Task・計画・SHA・停止結果の安全な要約を親通知へ保存する。private path、取得先、資格情報、command、raw log は親入力に渡さない。同一の原因と証拠は ACK や定期照合で追加通知せず、通知の利用時にも原因が現在有効か再照合する。親 turn、新しい Task/session、Git の再実行を定期照合だけで生成しない。

実 Git/SQLite の統合検査は、自動選択した新 SHA の取得、fresh な準備済み workspace の無操作、同じ HEAD の時刻更新、変更後の起動保留と一度の通知、未着手 profile の停止・capacity 解放、通知改変と古い原因の拒否を確認する。GitHub HEAD/ancestry と model は fixture であり、native P の受入ではない。旧候補への通常 merge と新 fix Task への競合引渡しは後続へ残る。
