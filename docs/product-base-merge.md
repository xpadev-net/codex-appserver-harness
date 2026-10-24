# 固定候補への通常 base merge

`workspace.MergePinnedBase` は host が固定した候補と新 base SHA を受け、通常の `ort` merge を実行する Go の物理 Git port である。呼出し側は実 model/runner を停止し、共有 Git・worktree lease と永続した一度限りの mutation fence を持つ必要がある。この port 自体は DB、candidate 採用、Task、検証、PR を変更しない。

開始前と fence の直後に、canonical Git metadata、保存済み linked checkout・branch・HEAD、clean 状態、既存 Git operation の不在、固定 commit のローカル存在と旧 base からの ancestry を照合する。新 base は branch 名や URL ではなく固定 SHA であり、partial clone による暗黙取得を元 repository と worktree の設定の両方で拒否する。

新 base が候補へ既に入っていれば、fence を消費せず `already_current` を返す。それ以外は `--no-ff` の通常 merge を行い、正常時は元候補・新 base をその順に親へ持つ commit、clean 状態と保存 checkout を照合して `merged` を返す。既存の履歴や base branch は動かさない。hook、外部 filter/merge driver、署名、editor、rerere、自動 stash と branch 固有の merge strategy 指定を抑止する。

競合時は元候補の HEAD と新 base の `MERGE_HEAD` を照合し、有界の未解決 path 集合と `conflict` を返す。未解決 index と作業内容を保持する。エラーや取消し、停止未確認は完了とせず、呼出し側が保存済み intent/lease を保持して照合する。実行した command の停止が確認できた失敗では、取消しから独立した3秒上限の読取で HEAD・MERGE_HEAD・clean 状態・未解決 path を `Failure` に観測する。未解決 path がない非正常終了も `ErrBaseMergeUnsettled` と状態観測を返し、部分変更を成功や単なる未着手へ読み替えない。停止未確認では観測せず、読取失敗も元の失敗と共に返す。`Failure` は診断情報であり lease 解放や再試行を許可しない。自動 reset・abort・再実行は行わない。

実 temp Git の検査では、両変更と両親の保持、既に取り込み済みの read-only 結果、競合内容/HEAD の保持と二重実行拒否、branch strategy/独自 hooksPath の hook/署名/attribute driver の抑止、未知/別系統 SHA、汚れた checkout、既存 operation、fence 前後の変更、worktree 固有 promisor 設定、merge コマンド中の取消し、実際には index/worktree を変更したのに非正常終了して未解決 path がない場合の照合観測を確認する。

これは Task_11 の通常 merge の component 実装である。新規 WorkItem の daemon による現在 HEAD 観測・取得・永続 claim/send/stop は後述の工程へ接続した。旧候補への適用と新版採用は後述の adapter へ接続した。競合を新しい fix Task/session へ渡す接続も後述する。P 全体の受入には数えない。

## 別 WorkItem の merge を待つ準備

P の work/fix Task が `depends_on` で別 WorkItem の work/fix/verification/review Task を指定した場合、その成果を含む merge を待つ。`ReadProductTaskPreparation`、workspace の reserve/claim/finish、model の ready/turn 予約は共通の DB gate を使う。現在の scope/policy/source/registration/candidate projection に対応する authenticated coverage と merged receipt を読み、merge 時の品質パケットに依存 Task の保存済み結果 ID と入力 digest が含まれることを照合する。

先行 Task の正常完了や native merge 応答だけでは解除しない。receipt/history が失われた場合は準備、Git の claim、model の新起動を保留する。Git の開始後に証拠が失われた場合も、実停止の証拠は stale として保存し、準備成功や model 起動へ変換しない。P の同じ WorkItem 内の検証・レビュー・修正と、汎用 Wave は従来の成果依存を使う。

実 SQLite の検査は、完了済み Task と merge receipt の区別、同じ要求版での plan carry、receipt 復旧による準備再開、遅い proof 喪失による claim/ready の保留、親 turn・新 Task・Wave 完了の非生成を確認する。外部 merge の応答と品質証拠は fixture であり、native P の受入ではない。新規 WorkItem の固定 base と既存候補への通常 merge は後述する。

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

実 Git/SQLite の統合検査は、自動選択した新 SHA の取得、fresh な準備済み workspace の無操作、同じ HEAD の時刻更新、変更後の起動保留と一度の通知、未着手 profile の停止・capacity 解放、通知改変と古い原因の拒否を確認する。GitHub HEAD/ancestry と model は fixture であり、native P の受入ではない。旧候補への通常 merge は後述する。新 fix Task への競合引渡しは後述する。

## 旧候補を取り込む workspace の保存契約

既存候補を参照する新しい work/fix Task では、固定 base 計画付き workspace の元 SHA を旧候補へ固定し、比較 base は観測した新 HEAD へ固定できる。通常 merge の実行前に、claim と別の `product_workspace_base_merge_send` を保存する。現在の source/入力/候補/計画、pause、Git/worktree lease の実 owner と generation を再照合し、一度だけ消費する。取得の送信記録と merge の送信記録は別であり、merge 後の取得送信は拒否する。

停止証拠は元 checkout・旧候補・旧比較 base・新 HEAD と `merged` / `already_current` / `conflict`、または既知の部分変更の観測を保持する。成功した merge commit の SHA と workspace の実 HEAD を一致させ、candidate の比較 base は新 HEAD とする。停止未確認の launching は capacity を保持し、terminal の保存失敗では解放も rollback する。競合は failed のまま保存し、model admission を許可しない。journal の一部喪失を再構成や再送の根拠にしない。

model admission は元候補の現在の projection が計画時と一致することも照合し、保存済みの merge 結果を別候補へ流用しない。任意の merge 結果だけで品質・検証・正式承認を継承しない。新 field は使用時だけ JSON に保存し、旧 workspace の checksum を保つ。

この節は実 SQLite の保存/admission component である。physical facts は trusted fixture として注入し、実 Git の merge は冒頭の port 検査に分ける。daemon の旧候補用 adapter と新版採用は後述する。競合から新 fix Task/session への引渡しは後述する。


## 旧候補への実 Git 接続

publication 設定がある daemon は、既存候補を扱う unused な work/fix Task でも実 source/default HEAD を固定する。取得する SHA は新 HEAD、作業用 checkout の起点は元候補 SHA とする。元候補の比較 base を ancestry の起点として `MergePinnedBase` を呼び、送信直前に reader と永続 fence を確認する。成功時は merge 後の実 HEAD・clean 状態・Git metadata を照合し、再度 source/HEAD を確認してから workspace を ready にする。

競合では元候補の HEAD、MERGE_HEAD、未解決 index と内容を保持し、failed の停止証拠と既存の一度限りの親例外通知を保存する。後から HEAD が進んだ場合も、完了済み merge/fetch の観測を消さず model 起動を保留する。停止未確認は launching と lease を保持する。settled/unknown の同じ workspace を定期照合で再実行しない。元 repository の checkout と履歴を変更しない。

model 入力には元候補 SHA と、準備済み HEAD・新比較 base の固定 SHA を区別して渡す。private path・取得先・raw proof は渡さない。writer 完了後の sealing は準備済み merge commit の ancestry を再確認し、worker が merge を取り消した成果を採用しない。採用候補の比較 base は新 HEAD とし、WorkItem は Verifying へ戻す。以前の検証・レビュー・承認の成功へ読み替えない。

実 temp Git/SQLite の統合検査は、固定 SHA の実取得と通常 merge の両変更/両親、already_current の無送信、競合内容保持、merge 前の source 保留、merge 後の HEAD 変更、停止未確認の lease 保持、replay、正常停止 writer の sealing/新版採用と新比較 base、準備済み merge を取り消した候補の拒否を確認する。GitHub 観測と model run は fixture であり native P の受入ではない。競合から新 fix Task/session への引渡しは後述する。

## 競合を新しい fix 入力へ渡す契約

親への `product_base_merge_conflict` 通知は、停止済み Task/workspace と workspace/proof digest の `conflict` 参照、元候補・旧比較 base・新 base・HEAD の固定 SHA を含む。path の要約は先頭4件かつ合計4,096 bytes を上限とし、全件数も渡す。完全な未解決 path と private Git metadata は停止証拠側へ保持する。同じ証拠の通知は ACK や定期照合で増やさない。

親は `TaskSpec.product_base_conflict` にこの参照を指定し、同じ WorkItem の新しい fix Task を計画できる。古い Task/session を再利用せず、通常の親再計画・fix・Task 予算を消費する。unused な元 Task の実停止、claim/send/terminal/history、登録版、元候補の現在の一致と capacity 解放を認証し、その候補だけを新 plan revision へ同じ transaction で引き継ぐ。通常の親再計画で全候補を暗黙に引き継がない。保存失敗では Task 作成・元 Task の取消し・計画・予算・候補を共に rollback する。

新入力の digest は参照先の停止証拠に結び付き、固定 base 計画は元の競合 base も ancestry の必須集合に加える。再起動後も証拠欠落・改変・未停止 owner・候補更新・scope/policy 変更は保留する。optional field を未使用の旧 plan/input/通知は従来の JSON/checksum を保持する。

この節は実 SQLite の親通知・新 Task/session・入力/admission component である。競合中の checkout を新 fix の model に開放する physical adapter と解消後の sealing は後述する。この参照単独では model 起動や Git の再実行を許可しない。native P の受入には含めない。

## 競合解消の物理 sealing port

`workspace.FreezeResolvedBase` は正常停止した fix のファイルを controller が stage/commit する Go port である。worker の Git metadata は読み取り専用のままとする。呼出し側は一度限りの永続 claim と Git/worktree lease を持ち、直前の permission check を明示する。この port 単体は Task/session 作成、model admission、候補の採用、品質・正式承認を行わない。

元候補の ancestry、canonical metadata、partial clone の不在、固定した MERGE_HEAD（既に commit 済みなら新 base の ancestry）、他の Git operation の不在を照合する。Git の leftover conflict marker 検査を行い、whitespace style は追加 gate にしない。permission check 後にも同じ HEAD/MERGE_HEAD と状態を照合する。host が stage して未解決 index がなくなったことを確かめ、tree が ours と同じ場合も merge commit を作る。両親・新 base の ancestry・clean 状態・operation 解消を保存する。hook・外部 diff/textconv・filter/merge driver・署名を実行しない。

既知の実停止後に失敗した場合は、変更開始の事実と HEAD/MERGE_HEAD・clean・未解決 path の有界な観測を返す。取消しでも3秒以内の照合だけを行い、停止未確認では成功や停止観測を返さない。これらの失敗は自動再試行や lease 解放を許可しない。

実 temp Git の検査は両変更/両親、ours と削除の解消、既に commit 済みの読取、未解決 marker、abort・別 operation・新 base/HEAD の変更、partial clone、permission 保留、取消し、hook/diff/署名抑止、stage 後の commit 失敗の保持を確認する。解消内容の意味や品質は独立検証・review に委ねる。この port と新 fix workspace/admission/停止証拠の接続は後述する。


## 新 fix workspace と解消候補の接続

親が競合通知を受けて明示的に採用した新 fix Task は、別 session/branch/private checkout で元候補と固定 base の通常 merge を実行する。元の failed workspace と取消し済み Task は履歴・内容・停止証拠を保持し、再実行しない。新 Task の `product_base_conflict` と計画・source が認証できた場合だけ、新しい未解決状態を `prepared` の証拠（clean=false）と共に ready にする。通常の work/fix の競合は failed のままであり、自己申告の clean=false で model permission を得ない。

model manifest は実際の新 checkout の固定 SHA・安全な conflict path 要約を含む。worker はファイルを解消し、Git metadata は読み取り専用のままにする。normal result と実停止の後、controller は保存 run/input/output、現在の元候補、全 lease generation と HEAD/MERGE_HEAD を再照合し、`FreezeResolvedBase` で stage/commit する。typed な resolution 証拠を workspace/run と対応させ、読取・finish・採用で検査する。未解消 marker、abort、古い許可や証拠欠落は採用しない。既知の部分失敗は観測を残し、停止未確認は launching/lease を保持する。

比較 base は固定した新 HEAD とし、採用後は Verifying へ戻す。sealing 後から採用 transaction までに別候補が選ばれても、その候補を古い結果で上書きしない。merge-only hold は現在の値を保持し、local fix や同じ HEAD の時刻更新だけを妨げない。pause/cancel、候補・source・scope/policy の変更は引き続き保留する。

実 Git/SQLite の統合検査は、通知→参照付き親計画→新 Task/session/checkout→解消ファイル→host sealing/新版採用、元の競合内容保持、marker/新規 marker/abort 拒否、現在の permission と lease、停止証拠の全 identity、採用直前の候補変更、merge hold を維持する model readiness と同じ HEAD の再観測、証拠喪失による起動保留を確認する。model run/停止と GitHub 応答は fixture であり、実 model の競合解消・P全体の native 受入を示さない。正式な独立検証・review・Cloud approval/CI/rules・merge と最終受入の gate は維持する。
