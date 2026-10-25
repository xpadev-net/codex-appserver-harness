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

## 明示 opt-in の自動 POST dispatcher

daemon の private product-executor 設定で publication.split を指定した場合だけ、準備済み creation intent の自動送信を有効にする。例は `"split": {"workers": 1, "repository_ids": [17]}`。workers は1..8、repository_ids は既存 execution repository の正の一意 ID を1..128件とする。省略された旧 publication 設定から新しい Issue 書込みを許可しない。GET 結果の観測は既存 publication worker として継続する。

SplitCreationDispatchWorker は全 journal 証拠から対象を列挙し、同じ子の全 transport attempt に一つの flight を割り当てる。新規送信には現在の allowlist、source policy、未 pause の proposal と全保存証拠を照合する。既存 private spec があれば二度目の launch をせず、未 activation の intent を封鎖するか、元の registered process group を停止する。aborted の後継は固定 Wave retry 予算内でだけ予約し、同じ marker/body/子枠を保つ。

unknown は repository が allowlist から外れた後も元の送信者の停止を回復する。停止と結果不明を区別し、GET の不在による再送を行わない。stopped/observed には POST を発行しない。owner cancel 時も全 flight と実送信 group の停止を join する。通知は保存値を再読取して commit 済みの停止・封鎖、または保存済み retry 予約だけに基づく。

Go/SQLite・実 subprocess と fixture gh の検査で、古い spec の封鎖から一度だけの POST・GET binding、pause/resume、allowlist 制限、allowlist 変更後の unknown 回復、commit 失敗時の通知抑制、cancel 時の物理停止と join を確認する。この dispatcher は既に採用された intent を処理するものであり、main/controller の分割判断、子 WorkItem の登録・part scope/depth/source 系譜、親 retirement/gateway は引き続き必要である。

## 全子の原子的な controller 準備

trusted host 用の PrepareProductSplitExecution は、現在の root proposal、全子 intent、Wave lifetime 枠、各子の初回 creation journal、完全な集合の receipt を同一 SQLite transaction で確定する。既存の個別 port も同じ transaction 内部関数を用い、再入可能でない DB 接続から public port を入れ子に呼ばない。子の owner は proposal/child から導く安定 ID とする。失敗・commit 失敗時は成功 receipt を返さない。

receipt は元 request の digest と proposal/plan/allocation、全 creation の ID/digest を固定し、head/history/admission および子別の独立 witness を保存する。receipt や既知 journal の欠落を新規 intent で補修せず、残った receipt だけでも上流の再作成・枠返却・初回 POST ID の再利用を拒否する。子別 witness を送信/照合 worker の対象列挙にも残す。exact replay/reopen は同じ集合を返し、Wave pause 中は既存集合の監査だけが可能である。

検査は二人目の子の journal 保存と最終 witness 保存の故障注入による全 rollback、保存後の各証拠喪失/exact 復旧、receipt だけ残る喪失、全初回 journal 喪失時の再生成拒否、複数 DB 接続の同時採用を確認する。外部送信や子 WorkItem 登録・親退役はこの port で行わない。model main からの版付き tool/実 session 認可は次段階でこの transaction へ接続する。

## main の明示採用 tool（product_v3）

common runtime の private 設定を `product_tools: true` / `product_tool_profile: product_v3` にした新規 parent thread だけに、product_split を追加する。v1/v2 の保存済み schema は変えず、profile の不一致は既存の明示 main rotation / 新 Task への引き渡しで解決する。Task の v3 tool set は v2 と同一で、分割の構造操作は公開しない。controller も実 session/run/thread/turn、現在の charged Wave scope、pinned profile と parent 種別を同じ transaction で認可する。

Task は split_proposal 種別の JSON Artifact と親へのメッセージで分割判断を依頼できる。メッセージ本文だけでは採用しない。main は product_split に schema_version=1、現在の target、proposal_artifact_id を渡して明示採用する。controller は同じ Wave/scope/policy の読取可能な committed Artifact（application/json、最大1MiB）を private object の byte 数・digest と照合し、型付き proposal の全 criterion/part 対応を検証する。repository、現在 registration digest、owner、全送信 journal は host が決める。

Artifact ID/digest、実 session/run/call と preparation receipt を結ぶ採用記録を、全子の準備と tool operation の確定と同じ transaction に保存する。末尾の tool receipt 保存に失敗すると早期 turn binding まで rollback する。exact call replay は同じ応答を返し、採用証拠の欠落は自動修復しない。別 call で保存済みの分割を置き換えない。採用記録だけが残った場合も、上流の新 key・枠返却・初回 journal の再生成を拒否する。

応答の completed は準備処理の完了を意味し、子 Issue の作成成功・子の実行許可・親退役・最終受入を表さない。private workspace を含む host projection は返さず、preparation ID/digest・子数・prepared stage だけを返す。実 POST には別途 publication.split の host allowlist と送信直前 gate が必要である。実 model の P 全体受入、子の登録・part scope/depth/source 系譜と親 gateway は引き続き必要である。

分割子の body に元の harness-scope が引用されても、harness-split-part fence または harness-split-child marker がある Issue は新しい root の受付対象にしない。明示 Issue 受付と Issue pool が同じ判定を使い、pool は保留した子を越えて次の独立 source を処理する。marker を除去された既知資源と子の実行 source の結付けは、子 registration 段階でも別途認証する。

全子の preparation が確定すると、その親 WorkItem の通常 model dispatch・workspace 準備・sealing・runner・review/quality を保留する。同じ scope/policy 内の plan 更新でも保留を引き継ぎ、別の登録 root には広げない。receipt と子別 witness に対象親と受付 target を保存し、親 source/proposal の監査と分割送信直前 gate はこの通常実行保留と区別する。旧形式で対象親/epoch が不明な証拠を、新しい既定値で補充しない。監査の exact replay は旧 JSON/digest を保ち、実行判定は不明として保留する。

同じ親・scope・policy の採用済み分割は、plan revision や call/key を変えても新しい子集合として採用しない。現在の intent と同じ ParentHeadID だけを分割用の監査/送信照合から除外して、別 head の採用済み証拠を保留にする。新しい main run からの再採用、直接 metadata port からの再作成、元 intent の direct/GitHub 単一 POST をそれぞれ検査する。

## 数値 identity による子 Issue の source 所有保護

CreateProductWave と通常の RegisterProductWorkItem は、canonical な r{repositoryID}-i{number} の source/WorkItem identity を新規 root として受け付ける前に、保存済みの split resource key と observed journal head/event を同じ transaction で調べる。既知の子 Issue は元の Wave の分割系譜で扱い、本文の marker が編集されても独立 Wave や通常の別名登録へ取り込まない。source identity の数値 repository を用い、別の repository を定義に指定することで回避できない。

この照合は既知の外部資源に対する禁止であり、child execution の許可ではない。resource payload が読めなくても同じ opaque key が残る場合は保留し、resource/head/event の独立証拠を失っても、残った証拠から同じ禁止を維持する。別 repository・別 Issue の新規 source は受け付け、Issue pool は保留した子を越えて次の独立 source を処理する。判定で元 Wave・履歴・予算を再生成/返却しない。

storage fixture は実 process の activation/停止を行った後に trusted GET 観測を注入し、resource/head/event ごとの喪失、canonical identity の別名・repository 差替え、通常登録の拒否と別 source の受付を確認する。product の GET/Issue pool fixture は marker のない有効な root scope でも既知の子を保留し、壊れた一件が次の source を妨げないことを確認する。child の local criterion/part・depth/source の実行登録と親 retirement は引き続き後続で接続する。

close 済み dependency が分割子の marker を持つ、または保存済み数値 identity で既知の分割子と分かる場合も、未管理 Issue の完了とは扱わない。IsKnownProductSplitChildIssue は禁止判定用の分類だけを返し、所有・品質・完了の許可を与えない。子の実行系譜と現在の完了 binding が接続されるまで依存を保留する。

## 作成済み全子の local scope binding

BindProductSplitChildScopes / ReadProductSplitChildScopes は、全子の最新 creation が trusted GET で observed になった後に、集合全体と各子の immutable binding・source claim を一つの transaction で保存/照合する。local criterion 全文、全 part ID/総数/根拠、依存先・元 WorkItem ID、depth、元の parent scope/source digest、allocation、Issue ID/number/body と creation/retry 系譜を保持する。local Scope の included は子の local ID であり、親 criterion 全体の達成へ変換しない。集合の保存は最大8MiBとする。

head/history/admission と子別 source claim を相互参照し、保存済み binding は元の proposal/plan/枠・最新 observed journal・停止証拠・全未送信 retry の予算証拠を再照合する。保存途中の故障や、二人目の子と既存 root の source 衝突は集合全体を rollback する。clock が trusted GET 観測より前なら作成しない。再起動・pause・後続 direction 後の exact replay は元 epoch の監査として保持し、現在 scope の実行権限へ昇格しない。

binding の独立証拠だけが残っても、元 proposal・allocation・POST journal の再生成、子枠/retry 予算の返却、新 root としての数値 Issue 再受付、元の親の実行再開を許可しない。retry の各 ID/digest/attempt/直前 ID を保存し、途中 attempt の喪失を最初の未送信 intent への fallback にしない。

この binding は Task/model/WorkItem の実行登録、fresh な子 source 観測、quality、親 retirement/完了を生成しない。実行登録と local verification/coverage の接続では、この binding と現行の scope/policy/source を別途認証する。Go/SQLite と実停止 process・注入 GET fixture で全子待機、local part/depth/source/retry の保持、故障注入・各証拠喪失と exact 復旧、単一 witness での再生成拒否、source 衝突、方向修正後の履歴、別 DB 接続の競合を検査する。

## 全子 WorkItem の原子的な登録

trusted host の RegisterProductSplitChildren は、全子の scope binding と元親の現在の採用根拠を認証し、予約済みの WorkItem ID を同じ Wave へ一括登録する。各子の registration/version/membership と全件の head/history/admission receipt を同じ transaction で保存する。二人目の所有権衝突や最後の receipt 保存の失敗でも全件 rollback し、予算・席を再課金しない。Task の作成や親 retirement は行わない。

registration の split_scope 参照は元 target・proposal・集合と子の binding ID/digest・子自身の numeric source・親 head・全 creation 履歴を保持する。Definition.Criteria は Wave の計画用に親の条件 ID を対応付けるもので、子の local criterion/part が親条件全体を満たしたという品質判定には使わない。子自身の最新 source と local quality の実行接続が揃うまでは、親と登録済みの子の実行を保留する。

新規登録は現在の稼働中 P scope でのみ許可する。保存済み集合は pause/reopen/方向修正後も元版の監査として exact replay できるが、現行版へ移さない。通常の RegisterProductWorkItem による子の再登録・方向修正時の lineage 消去は拒否する。旧 registration JSON は split_scope を付加せず、元の wire/digest を維持する。

全件 receipt、子 registration/version、membership のいずれかだけが残っても、上流 metadata・creation journal の新規再作成、子 issue の root 再取込み、親の再実行、split 枠・再試行予算の返却を許可しない。欠落した証拠の exact 復旧が必要であり、再試行で保存済み集合を補修しない。

## 子の local verification scope と入力

登録済みの子は、認証済みの binding にある local request・criterion ID・全 included criteria を検証計画の scope とする。計画読取、seal の候補固定、plan 再採用、runner の scope digest 照合で同じ transaction 内の認証関数を用いる。root の従来の scope は維持する。子の参照や上流証拠が失われた場合、親条件 ID に fallback しない。

session manifest の product_split_scopes は、元 Wave 要求と並べて子の local scope/digest・親条件の whole/part 対応・depth・source・依存を示す。main は登録済みの子、Task は自分に割り当てられた子だけを受け取り、private path・HTTP body・creation proof は含めない。Task input digest に binding ID/digest を固定し、欠落は unavailable として示す。元の binding は replan で書き換えず、方向修正後の旧 scope を現行の義務に転用しない。

メタデータ読取は pause/reopen 後も可能で、dispatch・source freshness・runner 成功・品質合格を与えない。子自身の最新 source、送信時 gate、親 retirement と part 集約 coverage、子の依存・多段 depth、main への非同期完了通知の接続は別途必要である。この段階でも子の実行保留を維持する。

## GET worker からの全子 binding・登録

split の照合 worker は stopped journal を既存の GET-only reconciler で observed にした後、全子の scope binding と登録を進める。全子が未観測なら保留し、一部の成功だけで登録しない。observed journal からも再開でき、保存済みの完全な集合は GET/POST を繰り返さず監査する。binding と登録はそれぞれ全件 transaction であり、途中の停止や登録失敗は次回の照合で再開する。Task/model の起動や親 retirement は行わない。

探索は既存 creation/resource に加え、scope・登録 receipt と registration/version/membership の独立 witness を含める。scope の opaque row cursor は payload や長いキーを解釈せずに有界のページ送りを行い、各行の不正な JSON/shape は unavailable として報告する。保存済みの witness から creation ID を見つけても送信権限は得ず、元の全証拠の認証を必要とする。

新規 scope/登録 history は本文と独立した所属列にも Wave ID を保存する。従来の proposal ID を所属とする history も本文を書き換えずに読める。旧 history の本文が壊れた場合は、digest/ID を照合した元 proposal の不変履歴で Wave を特定する。それも失われて所属を証明できなければ、元証拠の復旧まで P の処理を保留し、汎用 K には持ち込まない。

JSON probe は不正な payload が別 Wave の SQL を壊さないように扱う。一方、同じ Wave に所有者不明の scope/登録証拠が残れば、その Wave の枠・新規採用・親実行は保留する。壊れた証拠を無視して枠を返却したり、再作成で補修したりしない。メモリ上の OnMutation は保存後の再評価ヒントであり、main への durable な split 完了購読・通知は別途接続する。

実 subprocess と fixture gh で二つの POST/物理停止、trusted GET の bookend、全子登録、再起動 replay、末尾 commit 失敗と pause/resume、並行 GET/cancel join、別 Wave の不正証拠の隔離を検証する。これらは実 GitHub/model での製品受入ではない。

## 全子登録完了の durable 購読・通知

product_v3 の product_split は準備の完了を返し、その採用と同じ transaction で全子登録の operation subscription を保存する。応答の stage=prepared は registration/実行/品質の合格を示さず、購読はその時点では通知を発生させない。main は返された subscription ID を保存済みの wait に使える。

元の実 model operation・Artifact・preparation を照合し、全子 registration の commit と同じ transaction で completion receipt、product_split_registered event、notification intent、fanout と通知を保存する。通知保存に失敗すれば全子 registration も rollback し、observed journal から再開する。完了 receipt は private な全 scope/creation provenance を保持するが、main の通知は stage=registered、親/子 WorkItem ID、binding/scope digest、repository/issue 番号・depth だけを含む。品質合格、Task 起動、親 retirement ではない。

通知は通常の event fanout と ReconcileNotifications で扱い、独立した model polling turn を起こさない。ACK は配送確認であり入力の消費ではなく、停止後の wake reservation は同じ ID へ exact replay する。pause、期限、終了、scope/plan/policy 変更を同じ wake gate で扱い、Task や偽装した通知は拒否する。gate cache は通知ごとに source proof を照合し、一件の欠落が同じ target の正常通知の判定を省略させない。

保存済みの subscription、採用記録、registered group、completion、event/intent の証拠欠落を成功で補修しない。exact 復旧後に未消費の通知を再評価できる。従来の購読なし adoption は元応答を書き換えず、認証した元 operation に controller が登録完了時の購読を結び付ける。既知の購読が失われた場合の再作成は許可しない。completion だけが残った場合も、親再実行、子 source の root 再取込み、枠・消費済み retry の返却、元 journal の再作成を禁止する。

検査は実 Go test subprocess の停止と注入した trusted GET を使用し、通知/全件 registration の原子性、main wait、reopen/ACK/wake replay、証拠欠落/exact 復旧、偽装/Task 拒否、方向修正・pause、旧応答の互換と単独 completion witness を確認する。実 model/GitHub の子実行受入は引き続き別である。
