# Kernel Wave の動的分割

分割の深さ・子数は新規 Wave の WaveOperationLimits に固定する。既定値は split_depth=2 / split_children=8 で、trusted WaveRequest.OperationLimits で明示した値だけを新規受付時に採用する。範囲は深さ 1..128、子数 2..128。全体の WorkItem bound も128であり、上限値がその数の作成や実行を許可する意味ではない。

省略された旧保存記録の 0/0 は分割無効で、新しい既定値を補充しない。片側だけ 0、負数、子数1、上限超過は拒否する。policy/scope の方向修正、受付再送、再起動は lifetime snapshot を変えない。Task/Phase/main/token 等の既存の Wave 予算とも別に照合する必要がある。

この単位は kernel に不足していた上限 snapshot の接続である。分割 proposal の controller 採用、criterion と各 part の lossless mapping、子 WorkItem/Issue の作成・系譜・冪等 outbox、coverage の切替、親 gateway は後続で接続する。子の Wave を Task から作る許可や、旧 DB の自動 migration ではない。

SQLite 検査は、新規 snapshot、方向修正後の保持、旧形式の読取と JSON 省略、旧 source 再送と reopen 時の非補充、不正値の受付 transaction 全 rollback を確認する。外部 Issue/実 model の受入は含めない。

## 初期 root の分割 proposal

PrepareProductSplitProposal / ReadProductSplitProposal は trusted controller 専用の metadata port である。現在の P target、登録 head/version/membership、最初の ProductWave admission と元登録、元 request と現在 criterion、適用 direction / source 採用、固定分割上限、現在の親候補を照合する。

この段階は initial admission にある root WorkItem を深さ0として扱う。動的子・continuation からの分割深さ binding はまだ接続せず、後から登録した ID を root として受け入れない。親が終端・旧版・不明候補なら保留する。既存 splitting.Proposal.Validate を再利用し、criterion の全対応、part ID/総数/根拠と全 part の保持、子間依存 DAG、子数を検証する。proposal は1 MiB以下で、元要求の全文を重複保存せず scope digest を結び付ける。

head/history/admission と Wave 内 request-key を同じ transaction で保存する。同じ入力は同じ記録を返し、別親への key 流用・上書きは拒否する。head を失った場合は history/admission/request の独立証拠から既知であることだけを検出し、再構成しない。別 key でも喪失した intent を作り直さない。保存済み記録は現在の source・上限・候補を再照合し、Wave pause 中も監査読取だけを許可する。

この保存は子 WorkItem/Issue、Task/session、候補、予算消費、外部操作を作らず、親を retired/SPLIT/完了へ変えない。model tool / manifest にも公開しない。host 用記録に含む親候補の private 情報は、将来の model 入力では安全な要約へ変換する必要がある。

SQLite 検査は、初期 root の受付、完全 part 対応、元 Wave/予算の保持、exact retry/reopen、不正対応・循環・上限・旧登録・巨大入力の拒否、保存途中 rollback、各証拠と複数 index の喪失・復旧、別親 key の拒否、pause 中の読取と新 intent の拒否を確認する。legacy splitting の検査を外部 test package にして production validator を kernel から再利用し、legacy の子作成処理は新 port から呼ばない。

監査読取の pause 許可は Wave の pause を指す。WorkItem 自身の pause を含む親 projection の変更は、candidate/scope/policy と同じく現在 intent の照合を保留する。保存済み history を削除・書換えず、元の監査事実は保持する。

## 子 intent の一括準備

PrepareProductSplitChildPlan は、現在照合できる root proposal から子ごとの安定 ID / 将来の WorkItem ID / marker / Issue body / 依存先 ID を作る trusted controller port である。元の local criterion と parent criterion の part 対応・根拠をそのまま保存し、子 depth は親 proposal の depth + 1、repository は親の immutable 登録から得る。body は要求・対応 JSON・照合 marker を含み、独立した Issue-pool Wave を生成する harness-scope は付与しない。親候補の private workspace 情報を子 body に転記しない。

全子の head/history/admission と集合の head/history/admission を一つの transaction で保存する。同じ request は同じ集合を返す。各 admission に親 head 参照を残し、親 proposal や集合の index が失われても、残った子証拠を検出した場合は再構成を保留する。失われた証拠の代わりに新しい marker を作らない。

ReadProductSplitChildPlan と既存集合の exact replay は、保存された proposal・元登録・全子証拠を照合する静的な監査読取であり、pause や後続 scope によって消えない。新規集合の作成には現在の source/policy/上限/親候補と未 pause の target を必要とする。監査読取は送信許可を与えない。

この段階では WorkItem を登録せず、Wave 全体の子数・実行予算も予約しない。外部送信、未知応答の照合、physical stop、子 part scope の実行系への接続、親退役は後続段階で別途認証・予約・照合する必要がある。Task/session/model、HTTP、親状態、既存 lifetime 予算には変更を加えない。

SQLite 検査は、完全な part 対応と依存先、exact replay/reopen、遅い INSERT 失敗の全 rollback、各証拠の喪失と exact 復旧、子 admission だけ残る複合喪失、別 request・改変記録の拒否、pause/方向修正前後の新規作成と監査読取の違いを確認する。

## 子の lifetime 枠予約

ReserveProductSplitChildren は、外部作成の前提として集合内の全子 ID を同一 transaction で予約する。固定 WorkItem bound 128 に、現在の登録だけでなく immutable 登録 version、初期 root admission、既存の分割予約を合わせて照合する。通常の RegisterProductWorkItem も同じ予約を数え、別接続からの同時受付で上限を超えない。予約済みの子 ID は通常登録から流用できず、専用の認証済み child binding が必要になる。

集合の head/history/admission、各 WorkItem seat、split_children lifetime ledger を原子的に保存する。pause・方向修正・再起動・再送で枠を返却せず、別の Task/Phase/main/検証予算を増減しない。新しい予約には現在の未 pause の proposal/子集合を要求し、既存予約は immutable な監査事実として exact replay できる。

head/history/admission/seat/ledger の独立した証拠を列挙して全体を照合するため、一つを失っても空き枠とはみなさない。残った予約証拠は proposal・子集合の再生成も止める。登録 head の喪失も、immutable version や初期 admission に残る同じ ID の新規登録で修復しない。

この予約だけでは HTTP 送信・WorkItem 登録・Task 実行・親退役を許可しない。物理送信者と停止証拠、不明応答の再照合、子の part scope を接続する段階でも予約の証拠を再照合する必要がある。

## 子 Issue 作成の registered HTTP journal

PrepareProductSplitCreate は、予約済み集合の子一件について安定した作成 intent と mutation lease を保存する。元の子 ID・body・marker・repository・allocation を固定し、同じ intent を別 owner や別 request へ上書きしない。prepared → unknown → stopped → observed、または prepared → aborted の有界な経路だけを扱う。head と immutable な各 event を同時に書き、過去の全 event と元 value/admission を照合する。古い head への巻き戻し、記録の欠落、後続 lease の誤解放を拒否する。

ObserveProductSplitCreateGate は fresh source GET を行った trusted host 専用で、現在の source ID/digest と短い有効期限を固定する。direct は保存済み request を照合する。Github source は ready の head/history・policy pin・現在 scope/要求を照合する。同じ内容の GET は source snapshot の ObservedAt を更新しないため、この専用 gate を fresh GET の時点として用いる。model はこの記録を作れない。gate の head/history 喪失を新しい鮮度の証拠で自動修復しない。

ActivateProductSplitCreateHTTP は、実際に動く process group leader 自身だけが一度勝てる送信許可である。送信直前に現在の pause/方向修正/期限/親候補/source と未失効の gate、元 allocation/子集合を再照合し、process identity・spec digest・gate digest と unknown を同じ transaction で保存する。wrapper は Start=false なら送信できない。AbortUnactivatedProductSplitCreate は未起動 intent を原子的に封鎖し、遅れた wrapper も拒否する。

FinishProductSplitCreateHTTP は保存された process group の実停止を host が確認してから stopped を記録し、同じ lease generation だけを返却する。停止が確認できても結果は不明のままである。RecordProductSplitCreatedIssue は、その後の trusted GET で repository・非 PR・open・title/body/marker が一致した一件だけを関連付ける。重複・編集・不明応答・GET の一時的な不在を POST の再送許可へ変換しない。外部資源 binding の喪失も再生成を保留する。

outbox の head/history/admission/event/gate/資源 binding、および孤立した mutation lease は、上流 proposal・子集合・allocation と lifetime 枠の喪失時にも独立した禁止の証拠になる。

この単位は送信 journal と認可/停止/GET 結果の trusted storage port であり、HTTP adapter/worker はまだ呼び出さない。実プロセスの group 登録・一度だけの権限・遅延封鎖・停止前照合拒否・再起動・喪失/復旧・transaction rollback を Go/SQLite subprocess fixture で検査する。fixture は HTTP を送らず、source GET の証拠も host fixture と区別する。実 HTTP wrapper/検索・再照合、子の登録/part scope、depth 系譜、親退役と gateway は引き続き必要である。

## native HTTP wrapper と supervisor

internal product の SplitHTTPTransport と内部 _split-http を接続した。private な spec は registered creation ID/digest/owner と host の source policy だけを持ち、canonical な同一 user の private directory と O_EXCL/NOFOLLOW の単一 link file に保存する。model の command/URL/資格情報は受け取らず、split-http directory は製品 worker/runner の hidden path に加える。

wrapper は固定 repository の GET と、Github source なら既存 IssueSourceObserver の repository/Issue bookend GET・policy pin を照合する。同じ source 内容で元 snapshot が dedupe されても fresh GET 後に専用 gate を記録する。本文の JSON 符号化を終えてから一度だけ activation を取得し、その group に属する gh api POST で保存済み title/body/marker をそのまま送る。再送や自動 retry はしない。

supervisor は wrapper を group leader として起動し、結果ファイルを確認しても group 全体を停止するまで leader を reap しない。キャンセル時の cleanup は別の有界時間で join する。再起動後は未 activation を原子的に封鎖するか、保存した元 process group を停止する。欠落・不正・成功の各 response はいずれも GET 前の authoritative Issue binding にしない。停止済み replay は結果ファイルや PID を再利用しない。

Go subprocess と fixture gh による検査は、実際の argv/stdin・source GET 順序・本文同一性・単一 POST、unknown response、キャンセル時の native descendant 停止、controller reopen と結果喪失、遅延 wrapper の拒否、pause/resume、closed/policy mismatch、public/symlink/未知 field の spec 拒否を確認する。実 GitHub への POST や model/正式 Cloud 承認の受入ではない。

この段階は transport の接続であり、自動 marker 検索/GET 再照合 worker、controller/main の分割採用からの自動起動、子 WorkItem の part scope/depth/source binding、親退役/gateway は残る。未 activation の封鎖後に再開する場合の有界な新 transport attempt も別途照合する。unknown を GET の不在だけで再 POST する許可は追加しない。

## 停止後の GET 再照合 worker

publication を明示的に有効にした daemon pipeline に、GET 専用の SplitCreationReconciliationWorker を接続した。送信中の prepared/unknown intent を起動・停止せず、送信側 supervisor が保存した stopped と物理停止証拠がある一件だけを照合する。既知の observed/aborted は再 GET・再通知しない。controller 終了時は進行中の GET を cancel して join し、モデル turn を起動しない。

対象一覧は head/history/admission/event/gate/lease と外部資源 binding の opaque key を cursor 付きで読む。payload の不正や head の喪失を一件ずつ扱い、別の正常な intent を止めない。資源 binding の破損も unavailable として表示可能な対象へ残す。並行処理は既存 publication workers（1..8）内に固定し、同じ intent は flight/cooldown で重複処理しない。

照合は fixed repository の GET、state=all の Issue/PR 一覧（100件/頁、最大32頁、共有16MiB・30秒）の全頁、候補の point GET、repository bookend GET の順とする。全検索を終える前に単一と判断せず、marker の不在・重複・PR 混入、不正/null の一覧、ページ打切り/失敗、point GET での編集・別 ID・closed、repository の変化は保留する。native POST の成功応答をこの照合の代用にしない。

open の一件で固定済み title/body/marker と一致した場合だけ、trusted RecordProductSplitCreatedIssue へ確定し、commit 後に IPC hint を送る。元 Wave が cancel/旧 scope になった後も、送信済みの外部結果は元 epoch の履歴として追跡する。親の状態や予算を変えず、子 WorkItem/Task・quality・親退役は許可しない。検索の不在や失敗による POST 再送もない。

Go/SQLite と GET fixture の検査は、完全 pagination と bookend、重複・null・打切り・失敗・編集・PR・closed の拒否、cancel 後の追跡、壊れた対象による starvation 防止、commit 前の hint 不在、GET cancel/join、稼働中の実送信 group を停止しないことを確認する。main/controller の分割採用、POST dispatch の自動配置、有界な未送信 successor、子 part scope/depth/source 系譜、親 retirement/gateway は残る。

## 未 activation の封鎖済み attempt の有界 retry

新規 Wave の固定 WaveOperationLimits に split_create_retries を追加し、既定1・範囲0..32とする。Wave 全体で共有する lifetime の追加 transport attempt 数であり、子ごとの WorkItem 枠や runner/branch retry を流用しない。旧保存 JSON の省略値0は無効で、reopen・再送・方向修正から新しい既定値を補充しない。WaveOperationBudget は使用数と固定上限を表示する。

ReserveProductSplitCreateUnsentRetry は、前 attempt が原子的に aborted、HTTP owner なし、未 activation の封鎖証拠がある場合だけ、現在の proposal/control/source と残予算を照合して後継を保存する。unknown/activated/stopped/observed、あるいは GET の不在では retry できない。同じ child/WorkItem/marker/body/allocation/depth を保ち、別 transport ID・owner・lease generation を持つ。新しい source GET/gate と一度だけの activation は後継にも必要である。

後継は直前の aborted snapshot digest と系譜を結び付け、history/admission/event・独立 retry budget 記録・lifetime ledger を一つの transaction で保存する。全証拠の union から使用数を数え、head/ledger の喪失や再起動で予算を返却しない。retry budget には最初の creation ID も残すため、元の index をすべて失っても初回 POST の作り直しを拒否する。上流 proposal/子集合/子枠を失った場合も同じ禁止を維持する。

ReadLatestProductSplitCreate は最大32段の deterministic な後継を照合する。既知の後継が欠けた場合は古い aborted intent へ戻らない。元 attempt の監査記録は新しい lease generation の後も保持する。Save 時刻が封鎖より前なら保存せず、予算・lease を transaction 全体で rollback する。

検査は、同じ marker/子枠での実 wrapper の単一 POST、unknown の拒否、旧 JSON の非補充、方向修正/reopen 後の保持、子をまたぐ Wave 共通上限、別接続の同時予約、遅い ledger INSERT 失敗の全 rollback、各証拠喪失と exact 復旧、budget 証拠だけが残る場合の初回再生成拒否を含む。自動 POST dispatcher と main/controller 採用、子 part scope/depth/source 系譜と親 retirement/gateway は引き続き接続する。
