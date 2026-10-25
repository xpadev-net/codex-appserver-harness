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

## 子 issue 自身の source facts の保存

trusted host の ObserveProductSplitChildSource は、登録済みの local binding と current scope/policy を認証して、子ごとの issue identity、元 title/body の request digest、open/closed、hold、可用性、host policy digest を保存する。生成した子には ready label を要求せず、ready/取得不能/identity・本文変更/closed/hold/policy 変更を区別する。これらは元要求や policy を採用する操作ではなく、子の source ready も品質合格・実行・送信権限ではない。Wave.SourceHold は変更しない。

各子の host policy は初回に不変 pin と独立 history を同じ transaction で保存し、別 digest の観測で上書きしない。source head と version history も原子的に保存する。既知の head/history/pin 欠落を観測で補修せず、exact 復旧を必要とする。観測・policy の各レコードには compact な original binding/creation/source 参照を保持し、その記録だけ残る場合も再作成・親再実行・枠/消費済み retry の返却を禁止する。

CAS は semantic digest ではなく版を含む snapshot ID を使い、同じ facts に戻った後の遅延 callback による上書きも拒否する。facts が同じ GET 確認では ID/版/history を増やさず CheckedAt だけ更新する。意味のある変更は最大1024版まで元 history を保存し、上限を超えて reset/切捨てしない。鮮度チェックは caller が固定した policy と最大5分の有界期限、時計巻戻りを確認し、source-only の結果を返す。

SplitChildSourceObserver は private な repository/policy と登録済み local binding を確認してから、repository GET → 子 issue GET 2回 → repository GET を行う。issue ID・本文・状態・label・更新時刻が読み取り中に変化すれば取得不能として保存する。policy の pin が異なる場合や local proof が欠ける場合は GET を行わない。保存は GET 前の snapshot ID による CAS とし、親 context の cancel 後に detached write を行わない。

publication の source loop は repository ごとに root と子を確認する。子の一覧は opaque な row cursor で有界に走査し、同じ子の scope/history/admission を1 scan 内で重複確認しない。失敗行の後も独立した子へ進み、cursor を保存して再起動後に再開する。OnMutation は意味のある facts の commit 後だけ通知し、鮮度更新だけでは発生しない。

KernelSourceGate は親 Wave が direct でも子 issue 自身を公開直前に再取得し、ready と固定 policy・有界鮮度を確認する。GitHub 由来の親では引き続き親 source も確認する。source 確認だけで子の実行保留を解除せず、part 集約 coverage・親 retirement・依存・多段 depth の接続を後続で行う。

## 分割後の親の退役と part の coverage

ProductCoverage は current scope/policy/source の全子登録、scope/history/admission、creation/配賦の証拠と親 registration を照合し、全件が揃った分割だけを splits に示す。親は split ID/digest と全子 WorkItem ID を付けて retired に移し、子が未 merge の間は元条件を residual として維持する。親の registration・projection・要求・Wave 予算は書き換えない。登録証拠の欠落時は退役を成立させず、exact 復旧を必要とする。

各子の descriptor は local criterion と親条件の whole/part、part ID/count/basis、local scope/binding digest を保持する。子の merge fact では親条件の Criteria を空にし、独立して seal された verifier plan がその子の local scope 全体と一致することを監査し、停止済み runner の保存証拠から検証結果を再計算して quality の verifier digest・対象候補・合格状態との一致を確認した LocalCriteria / SplitScopeDigest を記録する。現行の実行 gate は従来どおり preflight を行い、履歴監査を新規実行の許可に使わない。同じ親条件に対応する各 part ID の集合・宣言個数と全 part の current merge fact が揃って初めて親条件の merge coverage を満たす。一つの part、親条件 ID だけの claim、別 scope/registration の fact、過去 epoch の分割を充足に転用しない。

これは merge coverage と受入候補の管理であり、親の最終検証・正式承認・外部 Issue 完了を代用しない。子の実行保留は維持しており、実 model/GitHub を通す分割 E2E は依存・多段 depth 等の接続後に確認する。

## 保存済みの子間依存と base 入力

実装/fix・検証・レビューの merge dependency gate は、Task DAG の指定に加えて、全登録証拠を認証した split intent の DependsOnWorkItemIDs を必須条件にする。モデルが Task の DependsOn を省略しても消せない。先行子の current scope/policy/source の merge coverage と実 receipt/readiness 証拠が必要であり、Task 完了や子 Issue の closed は代用しない。先行依存のない子に、別の子の依存を転用しない。

writer の ProductBaseInput も同じ保存済み依存を含める。依存が未充足なら入力を作らず、成立時は既存の RequiredAncestors が同じ repository の先行 merge SHA を要求する。通常の同一 WorkItem 内の実装→検証→レビューは、候補の利用に先立つ自分自身の PR merge を要求しない。

子の実行保留は引き続き維持する。この段階の子間依存の試験は全件登録・scope・source facts の fixture と gate の検査であり、既存の独立 WorkItem の merge→base→準備の回帰と区別する。子の実 merge 後の進行は分割 E2E で確認する。

## 再分割コンテキストの読み取り専用導出

InspectProductSplitProposalContext は、登録済みの子自身の local scope、binding、元の root criterion 対応と depth を検査する host 用の読み取り専用 port。子の元の title/body・identity・host policy が一致する fresh な own source を必要とする。Scope の Included は親 Wave 全体の ID に置き換えず、孫の coverage は immediate parent の local criterion ID を保持する。作業管理用 ParentCriteria だけを元 Wave の criterion ID へ投影し、深さは保存済みの子 depth + 1 とする。

入力は1MiBまでで、元登録・全件 group・source/history/pin の欠落、scope/policy/source の失効、hold/closed/本文変更、期限切れ・時計巻戻り、保存済み depth cap を保留として返す。作成時刻・journal/admission ID を持たず、記録・予算・Task を変更しない。通常 root の保存形式・digest は optional field の省略により維持する。

通常の PrepareProductSplitProposal / PrepareProductSplitExecution は nested context を保留する。再分割の永続採用と孫 Issue の作成は、後述の親 scope 証拠・祖先付き作成 journal・own source fence の下でのみ許可する。再帰 registration / part coverage と model/runner 実行は引き続き保留し、読み取り専用の検査結果を送信・品質の許可へ変換しない。

## 再分割のための親 scope 証拠

PrepareProductSplitParentScope は、登録済みの子自身の fresh な source と全件登録を認証し、元の全登録 group を head/history/admission の3記録へ同じ transaction で保存する host port。親となる一つの子だけでなく、全 sibling の numeric Issue identity、scope/登録 digest、creation chain と消費済み retry を保持する。子自身の private source policy digest も固定し、既知の source 記録を失った後に別 policy で初期化し直さない。

この証拠は既存の親/子実行保留、source 所有権、枠/予算、creation の欠落検出と recovery work list に参加する。3記録のどれか一つだけが残った場合も、子 Issue の root 再取込み、親の再実行、journal の再作成、消費済み枠/retry の返却を許可しない。証拠の欠落や内容不一致は exact 復旧を必要とする。reopen/pause の exact replay は保存済み値を保持し、方向修正後の旧 scope を現行の義務へ採用しない。

保存する proof 自体は新しい proposal・子 Issue・Task・実行/品質・親最終受入の許可ではない。再分割の永続採用と孫の送信は引き続き保留し、後続の接続でこの親証拠を固定する。

## 親証拠を固定した再分割 proposal の永続採用

PrepareProductNestedSplitProposal は、host が保存済み ParentScope の ID/digest を明示して再分割 proposal を採用する専用 port。local scope・保存済み depth・元条件への対応、fresh な own source と policy を再検査して固定する。head/history/admission/request はすべて元の全 sibling group を含む独立した envelope とし、親証拠の sidecar を失っても source 所有権・creation・予算の負の証拠を失わない。

root と再分割の operation key は Wave 内で共有し、同じ key の別用途への再利用、固定済み head の本文変更、欠落証拠の自動再作成を拒否する。既存の proposal reader は元 scope を検査して専用 envelope を返し、reopen/pause の exact replay と方向修正後の失効を区別する。root の保存形式は維持する。

通常の root 採用 port は再分割を暗黙に採用しない。再分割 proposal からの intent は後述の全祖先 envelope でのみ保存し、割当は後述の祖先証拠付き形式だけで許可し、作成 journal・Issue 送信は後述の祖先付き記録と own source fence の下でのみ行う。孫の各段階へ祖先の witness を引き継ぐ処理、own source の送信 fence、再帰登録/coverage を接続してから進める。

## 全祖先証拠を保持した再分割の child plan / intent

明示採用済みの再分割 proposal に対する PrepareProductSplitChildPlan は、proposal envelope と保存済み親証拠を再検査し、孫の集合・各 intent を全件 transaction で保存する。集合 head/history/admission と、各 intent の head/history/admission はいずれも元の全 sibling group と完全な plan を保持する。独立した一つの記録だけが残っても、祖先の source 所有権・creation chain・枠/retry の負の証拠を失わない。

local coverage、元 Wave の ParentCriteria、保存済み depth + 1、安定した intent/WorkItem ID を維持する。既知の集合・index・proposal・親証拠が欠けた場合は再生成せず、exact 復旧を必要とする。新規 intent は live な current target と fresh な own source を必要とし、reopen/pause の exact replay は新規保存や時刻/ID の変更をしない。旧 scope の plan は履歴として読めるが、新しい要求へ付け替えない。

この metadata 保存では実行枠や追加予算を消費せず、Task・作成 journal・Issue を生成しない。孫の割当は次の専用 envelope で行い、送信は次の作成 journal と own source fence に従う。通常 root の既存形式は変更しない。

## 全祖先証拠を保持した再分割の lifetime 枠予約

ReserveProductSplitChildren は再分割の全 plan・proposal・親証拠を認証し、全孫の枠を一つの transaction で予約する。専用の割当 head/history/admission、各 WorkItem seat、split_children 台帳はいずれも全祖先 sibling group と孫自身の完全な割当を保持する。通常登録・過去 version・祖先と孫の予約を合わせて lifetime 128 枠を照合し、一部だけの予約、再送・pause・方向修正・再起動による返却を許可しない。

台帳だけが残る場合も負の証拠として扱い、祖先の全 sibling Issue の root 取込み、元の creation/retry の再作成、親の再実行、孫の再予約を拒否する。台帳を含む独立 witness を recovery work list に載せるが、失われた正式記録を自動復元する権限は与えない。新規予約は live target と fresh own source が必要で、監査 replay は旧 scope の値を保持する。

SQLite の検査は枠上限・全件原子性・途中失敗 rollback・各証拠の欠落と exact 復旧・各 witness 単独残存・reopen/pause/方向修正を確認する。孫 Issue の作成は次の dedicated ancestry と source gate へ接続する。Task・model 実行、再帰登録・品質判定はまだ保留する。root の保存形式と既存の実行予算は維持する。

## 孫 Issue の作成 journal と親 Issue の送信直前検査

採用済みの再分割からの PrepareProductSplitCreate は、実在する完全な nested allocation と全祖先証拠を固定する。作成 head/event/history/admission、send gate/history、created resource、未送信 retry の記録と台帳はいずれも同じ ancestry を保持し、通常 root では optional field を省略して既存形式を保つ。正式読取では保存された ancestry を現在の実在する元証拠と照合し、sidecar の欠落を埋めない。

作成記録や retry 台帳の一つだけが残った場合も、祖先の全 sibling source の root 取り込み・creation の再構成・既知 retry の返却を許可しない。負の witness の共通 SQL と bounded recovery work list は ancestry を参照するが、実行許可の代用にはしない。

private HTTP spec は親となる子自身の保存済み source policy を必要とする。Wave が direct の場合も、実 HTTP helper はその子 Issue を repository/Issue の bookend GET で再取得する。send gate は5秒以内の ready snapshot の ID/digest を固定し、activation 時にも同じ snapshot と freshness を照合する。hold、closed、本文/identity の変更、古い source、変更後に本文だけ元へ戻す ABA は以前の gate を再利用できない。GitHub 起点の Wave では元の root source 検査も維持する。

one-use activation、物理 process group の停止、停止後の GET 照合、unknown 時の非再送と未 activation 時だけの有界 retry は既存契約を維持する。送信前に source が不成立なら activation/POST を行わず、安全な abort として戻る場合がある。試験は SQLite、実 subprocess/process-group と deterministic gh transport を使い、実 GitHub への孫 Issue 作成や model の split E2E とは区別する。孫の scope は次の祖先付き集合で保存する。孫の明示登録は後述の専用形式で行い、source 観測・再帰 part coverage・Task/model 実行は後続の接続まで保留する。

## 祖先と全 sibling を保持した孫 scope の一括保存

全孫の作成 journal が物理停止・GET 照合を終えた場合、BindProductSplitChildScopes は専用の nested envelope に完全な local scope 集合を保存する。集合 head/history/admission、各 binding の head/history/admission、各 numeric Issue source の記録は、元の全祖先 group、実在する nested allocation、今回の全孫 scope を共通の snapshot として保持する。2孫の場合は11記録を同一 transaction で保存し、16MiBを超える snapshot は採用しない。root の既存形式は変更しない。

いずれか一つの記録だけが残った場合も、祖先と今回の全孫の numeric Issue 所有権・creation chain・消費済み retry・予約枠を保持する。負の witness 照会と recovery work list は、外側の祖先 group と内側の孫集合を独立に参照する。失われた正式記録をそこから復元したり、通常 root として再取り込みしたりしない。

新規集合は live な current target と fresh な親 source を要求し、全子の停止・GET・local criterion/part/depth と元条件への対応を再検査する。pause/方向修正後も保存済み集合は静的な監査対象として保持するが、旧集合を新 scope へ付け替えない。各独立 index の欠落は exact 復旧を必要とし、途中保存失敗は全 rollback する。他の scope が保持する numeric source との衝突も保留する。

この保存は孫 WorkItem の登録、source ready、Task/model/runner、品質または親退役の許可ではない。孫の明示登録は次の専用形式に接続する。source 観測・local part coverage と実行 gate は引き続き保留する。欠落・原子性の検査では、全 subprocess を停止・GET 照合した fixture DB を private なディレクトリへ SQLite VACUUM INTO で複製し、外部操作を再送せず保存状態の喪失を検査する。

## 登録へ引き継ぐ祖先 witness の読み取り専用導出

InspectProductSplitAncestorWitnesses は実在する孫 scope の全証拠を認証してから、祖先と今回の sibling 集合の必要な負の証拠を導出する host port。要求本文・candidate・workspace・登録 snapshot を含めず、numeric Issue identity、WorkItem identity、scope 集合の参照、creation/retry chain と、実際に固定済みの親 source policy の識別情報を保持する。集合と子の順序を正規化して重複を除き、identity・policy pin・creation chain が矛盾する場合は保留する。

親も含む lifetime WorkItem 128枠と、全体32回までの retry witness の bound を検査する。任意の sibling を policy 初期化済みとは見なさず、固定済みの親 marker を区別する。導出結果は元の typed scope envelope の ID/digest を参照し、記録・予算・source ready・登録・Task/model を作らない。pause 中も同じ監査値を導出でき、元 scope の証拠を欠いた場合は再構成しない。

後述の明示登録がこの形式を使い、過去の登録証拠を再帰的に複製せず、全祖先の禁止情報を引き継ぐ。現段階の port は読み取り専用であり、persisted witness や登録許可として扱わない。

## compact ancestor set を固定した孫 WorkItem の全件登録

RegisterProductSplitChildren は、明示採用された再分割の全孫 scope と親登録・source/policy・保存済み枠を認証し、孫の登録 head/version/membership と専用 group head/history/admission を全件 transaction で保存する。2孫の場合は9記録。各 ScopeRef は実在する scope envelope と compact ancestor set に結び付き、Root の既存形式では追加 field を省略する。新規登録には live な current target と fresh な親 source が必要で、保存済み集合の監査 replay は pause 後も同じ値を返す。

Definition.Criteria は元 Wave の条件への対応を保持し、local scope は孫自身の criterion と part 対応を保持する。通常の登録 port で scoped identity を上書きしたり、部分集合だけを登録したり、枠や retry 予算を補充したりしない。登録・version・membership の一つだけが残っても、compact group witness が祖先と今回の全 sibling Issue、creation chain、予約枠、親の再実行禁止を保持する。復旧 cursor は各 compact group を識別し、同じ物理 row の複数 group を区別する。

この段階は WorkItem metadata の登録のみ。孫の source 観測/policy 初期化は後述の祖先 witness 付き source reference に接続する。さらに深い再分割には、後述の ancestor witness 付き parent scope proof と追加の admission 接続が必要。Task/model/runner 実行は既存の child gate で保留する。nested completion の main 購読・通知、再帰 coverage、実行許可は後続で接続する。compact witness や登録済みである事実を source ready・品質・親最終受入の代用にはしない。


## 孫 Issue の source 観測と祖先 witness

正式な孫登録から local binding を認証して source を観測する。source と policy の各 head/history は、登録が固定した同一の compact ancestor set を保持する。4種類の記録のいずれか一つしか残らない場合も、全祖先と今回の sibling の numeric Issue 所有権・creation chain・消費 retry・予約枠を忘れない。失われた正式 binding や source head を witness から再初期化しない。Root child の既存 JSON では追加 field を省略する。

source 観測の一覧は孫の typed scope head/history/admission も opaque cursor でたどる。実際の GET は登録と scope の認証後に行い、一覧にあるだけでは観測・実行の許可にしない。

host は最後の Issue GET を終えた時刻を ObserveProductSplitChildSourceAt に渡す。保存処理のための認証に時間がかかっても CheckedAt を保存完了時刻へ更新しない。通常 port も呼び出し開始時刻を固定する。future・時刻の逆行を拒否し、source gate の5秒の制約は維持する。保存の context は最大1分だが、親 context の取消・期限に従い、鮮度を延長しない。

source ready は source のみの状態。Task/model/runner 実行、再帰的な完了判定、さらに深い proposal/creation の admission、main への nested completion 通知はまだ許可しない。予算・登録・scope revision を変更しない。


## parent scope proof に固定する平坦な祖先 witness

PrepareProductSplitParentScope は登録済み孫の source/policy と完全な scope authority を確認し、選択した登録の compact ancestor set を AncestorWitnesses 配列へ正規化して固定する。現在の sibling group と、それ以前の祖先の全 sibling の identity・creation/retry・予約枠を、parent scope の head/history/admission の各記録が独立に保持する。既存の Root child proof では配列を省略し、JSON と digest を変更しない。

下位の typed proposal/plan/allocation/scope/registration の envelope は同じ parent scope value を埋め込むため、この配列を引き継げる。負の witness 照会は物理記録に加え、creation journal と retry/allocation ledger から取り出す祖先 envelope の配列も参照する。配列の各 group を bounded な virtual row として扱い、欠落・非配列・空・128超過・不正要素は所有 Wave の unavailable witness とする。他 Wave の照会を SQLite JSON エラーで止めない。

これは metadata proof の保存。設定上限を超える分割、canonical parent binding の接続が未完了の深い proposal admission、Task/model/runner、再帰 coverage や最終受入の許可にはしない。保存済み proof は実在する全登録・source policy・scope の各正式記録から再導出して照合し、compact witness 自体を正式証拠の代用にしない。


平坦な祖先の抽出では、配列が存在する記録と、配列が必須の深さの記録に限定した小さな summary を照会中に一度作る。compact witness は従来どおり物理記録から直接読む。正式 proof の認証結果を transaction 間で cache したり、元記録の欠落を summary で補ったりしない。

送信 gate の二つの内部経路は、同じ transaction 内の完全な creation ancestry 認証と preflight で検証済みの親 binding を使う。選択した親の現在の登録 head/version/membership と source/policy head/history は再度照合し、source の5秒の制約を適用する。新たな公開経路から未認証の snapshot を渡す用途には使わない。


## 読み取り検証中の重複した proof walk の抑制

split の正式な loader は、一つの同期的・読み取り専用 call tree の内部だけで、認証が完了した結果を再利用する。private な子 context に実際の sql.Tx identity と完全な入力を結び付け、上位の呼び出し元へ context を返さない。同じ transaction であっても loader が戻った後の次の呼び出しは、新しい検証を始める。別 transaction、書き込み後、方向修正後、scope の欠落後へ認証結果を持ち越さない。

再利用する値は毎回独立した JSON copy へ戻し、返した slice/map の変更が別の読み取りへ伝播しないようにする。失敗は再利用せず、従来の部分的な返却値と error を保持する。循環参照と過大な active walk は保留し、cache は最大2048件・64MiBまでとする。cache の容量に達した場合は追加の再利用を止め、正式な読み取り検証を続ける。

source head/policy の更新・鮮度判定、Task/model/runner の実行許可、予算はこの再利用の対象にしない。source の時刻と5秒の送信制約は変更しない。

## canonical binding を参照する深い分割提案

PrepareProductNestedSplitProposal とその正式 reader は、親 WorkItem の登録に結び付く canonical binding を読む。Root child の旧形式と、登録済み孫の typed scope を同じ認証経路で扱い、孫のために旧形式の scope head を作らない。親 proof・登録集合・すべての上流 scope の正式記録が必要で、compact ancestor witness から欠落を再構成しない。

trusted な初期設定で分割深さを3にした Wave では、登録済み孫の fresh な source と固定済み parent scope proof を使い、深さ3の子を計画する提案を保存できる。既定値2の Wave ではその提案を保留する。深さの設定は最初の Wave 作成時から各祖先の snapshot に固定し、途中で上限を付け替えない。

Coverage は直近の親の local criterion に対する part ID・part count・basis を保持する。元 Wave の条件への対応は ParentCriterionRoots と ParentCriteria に別途保持し、local criterion の名前が元条件と同じでも whole coverage の証拠にしない。提案は全4記録を同じ transaction で保存し、pause・古い source・途中失敗では部分保存しない。

この接続は提案と child plan の metadata に限る。Task/model/runner、再帰的な local coverage、main 通知、親最終受入の許可にはしない。枠・retry・operation budget を補充しない。


方向修正後の監査用 loader と plan 等の履歴読み取りは、保存時の parent proof が参照する正式な登録集合から binding を取得する。現在の Wave target を要求して過去の提案・plan・allocation・scope を読めなくしない。現在の提案状態を確認する port と新規提案は現在の登録 binding と target を照合し、過去の proof を新 scope の採用許可へ転用しない。

## 深さ3の作成から source 観測までの接続確認

初期設定で深さ3を採用した controlled Go fixture で、great-grandchild の作成 journal・物理 process group の停止・Issue 観測・scope 結合・全件登録・source/policy 観測を通す。最終登録と source policy history の各1記録しか残らなくても、Root child・孫・今回の子のすべての numeric Issue 所有権、消費済み retry、予約枠と recovery work list の各層を保持する。

最新の作成 journal の探索も、一つの読み取り専用 proof walk の中で再利用する。同じ呼び出し内の retry chain と末尾確認を繰り返さず、書き込み後や別呼び出しへ結果を持ち越さない。retry head が失われた後は、以前の読み取りで隠さず正式証拠の欠落として保留する。

retry は Wave 全体の予算を引き継ぐ。祖先の retry で上限に達した場合、深い分割でも追加 retry を拒否し、失敗した予約は予算を変更しない。この fixture は source と metadata の接続確認であり、Task/model/runner 実行、実 GitHub/native model の分割受入、正式承認や全体完了の証拠には数えない。

## 多段のローカル coverage と委譲した親の扱い

coverage ledger は各階層の正式な全件登録を読み、直近の親の canonical local scope・scope digest・登録 digest・深さと全子の mapping を照合する。正式に全件委譲したノードは、子孫の merge が未完了でも独自 PR の義務から退役し、実際に成果を作る各 leaf の義務を残す。どの階層でも正式証拠が欠ければ、当該委譲による退役と完了を認めない。

集約では、子孫の全ローカル criterion と宣言した全 part が満たされた場合にだけ、その子の `delegated_complete` を直近の親へ反映する。親への反映には元の登録・scope・local criterion 集合・深さの一致が必要で、委譲途中の親自身の PR で未完了の子孫を飛ばさない。`merged` はその WorkItem 自身の正式な merge fact だけを表し、委譲完了から仮想 merge receipt を作らない。`complete` と `covered_local_criteria` は各 group のローカル集約を示す。

Wave 全体の criterion へ射影するのは最上位の group だけとし、深い scope に同じ ID が現れても Wave 要件を直接満たさない。静的 verifier 監査も登録が指定する nested history namespace を使い、欠落した typed envelope を legacy history で代用しない。これは metadata と集約の接続であり、子の Task 実行・依存 base SHA・最終 source gate・継続作業の実行許可は後続の統合対象である。

## 祖先の固定依存と分割済み先行作業

子の依存は自身の intent だけでなく、canonical な親の登録を Root まで辿って各祖先の intent からも取得する。Task DAG が依存を省いていても、深い子が祖先の先行 sibling を追い越すことはできない。途中の登録・scope・全件証拠の欠落は依存なしへ変換せず保留する。

先行 WorkItem が全件委譲済みなら、同じ transaction で認証した coverage の全ローカル obligation と実 merge fact を再集約し、すべての leaf の receipt を要求する。親自身の PR、閉じた Issue、一部の leaf、矛盾した receipt は代用できない。通常の Task 依存が特定の親 result を要求する場合、その result を leaf の品質証拠へ推測して転用せず保留する。

base input は必要な全 leaf の実 receipt と repository を保存し、`split_dependencies` に委譲元 group・登録・scope の digest と leaf 集合を保持する。同じ repository のすべての merge SHA を固定 base の必要祖先に含め、別 repository の SHA を混ぜない。仮想 merge SHA は作らず、予算の追加許可や子の Task 実行許可は与えない。controlled reducer と静的接続のテストを、実サービスの分割 E2E の合格とは扱わない。

## ツリー全体の最終 source gate

最終受入では、実際に merge された leaf だけでなく、正式に登録されたすべての子と委譲した中間ノードを再取得する。最大4件の source 読み取りを並行し、エラー・キャンセル時も全 GET と保存処理を join してから戻る。各 GET の実観測時刻を保存し、全 canonical binding と source/policy history の照合後に、各 source が5秒以内であることを確認する。遅い読み取りは保留し、保存で観測時刻を更新しない。

publication は引き続き open な own Issue を要求する。最終目的の source 確認だけは、同じ登録・local scope・criterion に対応する正式な own merge receipt がある leaf の closed Issue を認める。closed でも明示 hold、内容・identity・policy の変更、証拠欠落は拒否する。閉じた delegated parent は、全 leaf の完了に加えて managed close binding が必要であり、その操作が未接続の間は保留する。

最終 plan の optional `split_sources_digest` は全 source の semantic ID・版・登録と scope を固定する。同一内容の GET による `CheckedAt` の更新では plan を変えず、編集後に元の内容へ戻った場合も新しい semantic version により古い plan を失効させる。plan 採用、final runner/fetch の起動前、最終検証 evidence と受入で、現在の全 source と pin を照合する。split のない旧 plan の wire/hash は維持し、pin がない旧 split plan へ実行権限を補わない。

canonical な source 確認と並列 GET の controlled Go テストを実施するが、これは子 Task の実行許可や実サービスでの全体完了を表さない。closed own merge の status reducer は認証済み入力境界の確認であり、実 merge の受入証拠には数えない。既定8子の実接続性能、managed delegated close、子の継続作業と実行許可は後続の受入対象である。

## 旧 continuation port の scope 制約

`RegisterProductContinuation` の既存の own-merge 起点形式は Root definition を登録するため、split child の local scope を保持できない。元の正式な登録 version に `split_scope` がある親は、登録・再送・live activation の共通チェックで保留する。古い leaf の merge を Root の作業へ暗黙拡大しない。Root の own-merge continuation は維持し、子の scope を保持する別形式と、own merge を持たない delegated root の tree-origin continuation は独立した受入対象とする。

Root Issue の定期観測では、`ObserveProductSourceAt` に読み取り前の snapshot ID と、外部取得を完了した時刻を渡す。取得中の状態変更と同じ内容への復帰も ID で検出する。`product_source_read` とその照合用コピーは現在の Target・source・snapshot ID/digest・policy・実取得時刻を結び付け、source の内容と同じ transaction で更新する。同じ内容の再取得は semantic snapshot/history、通知、model run を増やさず、保存の遅れを新しい GET 時刻として扱わない。従来の digest による観測や metadata の参照では実取得時刻を更新しない。

`ReadProductSourceRead` は照合済み metadata の参照であり、実行許可ではない。呼出側は現在の source 状態、Wave/WorkItem の優先操作と全証明の確認後の経過時間を別途検査する。失敗した取得の記録を ready と見なさず、欠落・不一致・古い snapshot から取得時刻を推定しない。この取得時刻の追加だけでは、子 WorkItem の一律保留、quality gate、Cloud 承認、親最終受入を解除しない。
