# P操作の永続受付

Task_8 / #41 の最初の分割は、外部I/O前の受付と原子的な記録を提供する。P全体の成立、GitHub送信、正式承認の証拠ではない。

`contract.ProductTools(kind)` は共通kernel toolへ `pr_publish` と `pr_review_request` を加える明示opt-inの契約である。現在のK runtimeは `KernelTools` を維持し、P toolを公開しない。モデルの引数はschema版・Wave target版・WorkItem/candidate ID・固定40桁SHAに限定する。repository、branch、PR番号、投稿本文、発行元sessionはcontrollerの保存事実から決める。TaskにWave管理toolを追加しない。

`KernelStore.SetProductTarget` はtrusted controller専用で、既存WorkItem・不変candidate・repository/default base・既存PR番号をcurrent P Waveへ関連付ける。モデルから呼べない。更新は前snapshot digestによるCASで、versionが変わらないpause/holdも含めて古い更新を拒否する。projection、監査snapshot、既存の`wave_work_item`所属を同時保存し、別WaveへのWorkItem所属の上書きを拒否する。過去を含むcandidate IDごとのSHA/base/版を不変記録と照合し、登録済みWorkItemのrepository IDの差し替えも拒否する。`ReadProductTarget`は同じdigestを復旧・照合用に取得する。

PR toolは既存のsession/run/thread/turn/call検査、元snapshot scope、pause/cancel/deadline、TaskのWorkItem割当を使う。登録済み候補とのID/SHA一致とWorkItemの工程・pause/holdを確認し、以下を一つのSQLite transactionで保存する。

- callの一意receiptと発行元/引数digest。
- `product_pr_intent`: 元Wave/Task/session/run、固定候補、product snapshot digest、marker用WorkItem ID、accepted状態。
- 共通operationの受信購読と`product_pr_subscription_intent`: 元session/Task/版、repository ID、既存または未確定PR番号、pending状態。

返す`accepted`は受付だけを表す。未完了operationの状態はaccepted/unknownに保ち、既存の再計画・復旧の保留判定を通す。同一callの再送は同じreceiptを返し、異なる引数・toolへの再利用を拒否する。購読保存に失敗した場合、操作・receipt・早期turn対応付けまでrollbackする。受信購読は共通`session_wait`で使える形式だが、外部成功/完了通知はこの分割では生成しない。

後続では送信直前にsource WorkItemの現状態・候補/検証gate・Wave/Taskの優先操作・capacityを照合し、送信試行とunknown結果を保存する。応答欠落はmarkerで再照合し、資源へのbindingと登録境界の更新回収を接続する。既存PR controllerへの接続（Task_11）までは製品Taskの`product_readiness_unavailable`を維持する。単なるprojection登録を送信許可、独立検証やCloud承認として扱わない。

検証は `go test ./internal/contract ./internal/storage -run 'TestProduct|TestPRIntent|TestPRTool|TestPRReview' -count=1`。再open/replay、失敗rollback、旧run/旧call・候補変更・未登録/未割当・Kの拒否、WorkItem pause/hold/cancel、既存PRのreview intent、CAS競合と監査保存失敗を確認する。全体回帰と対象packageのraceは各PRで実行する。

## 送信準備・未送信取消・unknownの保持

`PreparePRDispatch(operationID, ownerID)` はcontroller用の保存portで、HTTP送信を行わない。最新のWave/Task/WorkItem projection、scope/候補digest、pause/cancel/方向修正/期限を照合し、executorの一意claim、PRまたはCloudの枠、同じWorkItemのPR書き込み排他を一つのtransactionで取得する。PRの存続枠は複数plan版・更新操作で共有し、書き込み排他は操作ごとに取得する。新規PRを未送信取消した場合だけ、別の操作が新しい枠の世代を作り、旧予約と解放証拠を履歴に残す。外部close/mergeで終わった枠を再利用しない。Cloud開始には既存PRの枠が必要で、同一candidateのreview予算はrepository/PR番号/SHAに結び付ける。

実P adapterはcanonical WorkItemと検証/configのgateを確認し、その最後の`BeforeWrite`で`MarkPRMayHaveSent`を呼ぶ必要がある。このportが保存した最新projectionと優先操作・claim/leaseをもう一度照合し、preparedを一度だけunknownへ変更する。reviewではCloud枠に加え、準備時に対応付けたPRの存続枠が今も保持されていることを再確認する。変更後にHTTP送信へ進む。呼び出し直後・実HTTP前にcrashしても、再起動はunknownを保持し、読取専用の外部照合なしで再送しない。この変更自体は検証成功・Cloud承認の証拠ではない。

`CancelUnsentPRDispatch`は未取得またはpreparedの操作だけを取消できる。遅延した送信callbackをfenceし、新規PRの未送信枠・Cloud枠と書き込みleaseを原子的に解放する。既存のopen PRの存続枠は更新を取消しても保持する。先に外部close/mergeを確認して枠を解放した場合も、未送信claimのcleanupは可能である。unknownの取消・枠解放は拒否する。同一owner/reasonの取消再送は同じ監査結果となり、監査保存失敗は全解放をrollbackする。

この分割はstorage状態機械だけを追加する。実HTTP adapter、canonical sourceの直前guard、marker照合、resource binding/通知、native P tool公開は後続であり、現在のruntimeは外部送信を開始しない。SQLite BUSY/LOCKEDはdomain holdに変換せず返し、callerがrollback後の操作全体を再試行する。未知HTTP結果の再送と混同しない。

追加検証: `go test ./internal/storage -run 'TestPRDispatch|TestPRBeforeSend|TestPRClaim|TestPRPrepared|TestPRReviewClaim|TestPRMark|TestConcurrentPRClaim|TestPRUnsent' -count=1`。別接続でのclaim競合と再試行、送信前の優先操作、prepared/unknownの一度消費、再起動・cancel後のunknown保持、同じPRの排他と存続枠、Cloudの別枠/一度予算、既知未送信の取消、保存失敗rollbackを確認する。

## 観測したPR資源への確定

`BindObservedPRPublication`は、実際のPR応答または読取専用照合から得た正の観測を保存するcontroller portである。repository・head/base repository・PR番号・固定SHA・head/base branch・markerを元のpublicationへ照合する。preparedの`not_started`は送信前に既存資源を発見した場合だけに使う。unknownには元transportの正常応答完了または停止確認後の照合の証拠が必要で、callerが実transportを検証してからproofを渡す。PR一覧が空だったことだけでunknownを未送信へ変えない。

同じtransactionで、PRとWorkItemの一意な所有binding、購読intentの実PR番号/資源key、保存済み受信購読、操作結果、完了outbox、書き込みlease解放を確定する。open PRの存続枠を保持し、正のclose/merge観測だけで解放する。取消後やscope変更後の結果も元operationの履歴として照合できる。現在のprojectionにはPR identityだけを反映し、新しい候補・版・pause/holdを元snapshotへ戻さない。別WorkItemへのPR所有の差し替えを拒否する。

`resource_bound`は外部資源の確定を表す。検証/独立review/Cloud承認/merge gateの合格やWave完了を生成しない。完了outboxは永続化し、通知配送・待機条件との結合は後続sliceで接続する。実HTTP/marker取得adapterとP runtimeも引き続き未接続である。

追加検証: `go test ./internal/storage -run 'TestObservedPR|TestPRBinding|TestUnknownPRBinding|TestObservedTerminal|TestBoundPR|TestPreviouslyBound|TestCanceledUnsent' -count=1`。再送の同一receipt、購読/操作/候補/leaseの失敗rollback、誤った外部header/transport証拠拒否、再起動/cancel後の事実回収、現在の候補と禁止flagの保持、一意所有、terminal枠解放と既存PR更新の枠共有を確認する。

## 完了通知と明示的待機

`ReconcilePRCompletionNotifications`は完了outboxを有界に処理し、状態event・通知・cursor・queued状態を保存する。queuedは通知保存を表し、配送ACKやモデル判断の適用とは別である。各操作を独立transactionにし、失敗した行が他の完了を止めないよう永続scan cursorを進め、再起動/再試行では未処理を回収する。共通KernelSourceのready走査から呼び、モデルや外部APIを起動しない。

通常の完了はdeferredで保存し、`session_wait`または正常に適用したmain/Taskのwait結果が、本人の同じtarget/購読を明示して初めてpending入力へ進める。待機宣言と完了保存の前後関係を問わず同じ通知を使う。通常の成功や閲覧だけでmainを起動しない。cancel/終端session/旧scopeの通知はarchivedとして履歴に残す。未送信取消の通知はcanceledであり、資源作成や品質成功に読み替えない。ACK済みでも未判断入力を保持し、wake/runへの関連付けと論理適用は共通kernelの契約を使う。

追加検証: `go test ./internal/storage -run 'TestPRCompletionNotification|TestPRWaitBefore|TestPRNotice|TestFailedPRNotice|TestPRCompletionFromPrevious' -count=1`。前後/同一再送、ACK後のwait、main構造化wait、同じ再開intent、保存失敗と再open、bad rowの後続進行、cancel/旧版を確認する。実HTTPと継続的な外部資源更新・native P公開は後続である。

## Controller側のPR送信adapter

`internal/product.Publisher`はstorage claimと明示指定のAPI transportを接続する。callerが渡すcanonical gateは元WorkItem/configと必須検証証拠を読む必要があり、model主張やprojectionだけを検証成功の根拠にできない。git pushはこのadapterの前にcontrollerのGit工程で行い、adapterは送信直前のremote branchが固定candidate SHAかを読む。

送信前にcontroller由来のtitle/bodyと操作markerを不変packetへ保存する。`GitHubAPI`は認証済みghを使い、repository数値IDとAPIから取得したdefault branchを照合する。POST/PATCH直前のguardで対象PR、remote SHA、最新canonical gateと同じpublication内容、storageの優先操作/claim/枠を確認してからunknownを保存する。PUT/mergeなど想定外のmutationを拒否する。本文へWorkItem markerと操作固有markerを付け、応答のrepo/branch/SHA/title/bodyを照合してから資源へ確定する。

送信前の読取/DB/gateエラーはpreparedを保持し、dispatcherが有界再試行または明示的な未送信取消を判断する。送信開始後のエラーはunknownのまま保つ。`ReconcilePublication`は停止/応答完了を確認したcallerのproofで、保存packetと操作markerに一致する資源だけを読取専用で回収する。現policyの変更やpauseは過去の正の事実の回収を妨げない。一覧の不在や別操作のmarkerでunknownを解消せず、古い操作を再送しない。

この分割はlibrary adapterとfake/gh wire試験であり、canonical gateの既存PR工程への接続・daemon配置・native P公開はTask_11を含む後続である。Cloud正式承認やP全体の実接続成立の証拠にはしない。検証は`go test ./internal/product ./internal/github ./internal/storage`。書込直前のunknown、既定branch/remote SHA/候補/flag/gateの変化、packetの不変性、既知未送信の同一owner再試行、応答欠落と操作marker照合、再送なし、ghのroot metadata/branch取得を確認する。

## Review依頼adapterと受付結果

`internal/product.ReviewRequester`はcanonical candidate/検証/configと検証済みCloud依頼経路を確認する必須gateを受け取る。controller由来の依頼文・依頼者の数値ID・operation固有のround IDを送信前に不変packetへ保存する。モデルが依頼者や投稿内容を指定する経路は追加しない。`GitHubReviewAPI`は認証済みghの公開comment APIを使い、全pageを照合する。

POST直前にrepository/default branch、固定candidateのPR header/open/draft、最新gateと同じ依頼内容、優先操作と両capacity枠を再検査し、unknownを保存してから一度送る。応答のcomment IDを同じPRのcomment一覧へ照合し、元の依頼者ID・本文全体・round markerに一致した正の事実だけを保存する。既存の完全一致commentは読取だけで関連付ける。応答欠落や再起動後にunknownを再送しない。元transportの停止を確認したcallerだけが、保存packetに一致するcommentを読取専用で回収できる。欠落/編集/別依頼者/重複は未解決のまま保持する。

`BindObservedPRReviewRequest`は既存PR所有binding・自動購読・operation結果・完了outbox・書込lease解放を一transactionで確定する。`review_requested`は依頼受付であり、正式承認やreview終了ではない。PR存続枠とCloud実行枠を保持し、後続の正のreview終了観測までCloud枠を解放しない。通知はpublicationと同じ明示wait/旧scope archiveの処理を使う。pause/cancelやpolicy変更後も、既に送った依頼の正の事実を回収できる。

検証: `go test ./internal/product ./internal/storage -run 'TestReviewRequest|TestLostReview|TestObservedReview' -count=1`。送信直前のunknown・gate/remote/優先操作の変化、誤ったmutation、既存comment/必須guard、保存rollback、再open/cancel後の回収、Cloud枠保持、通常通知のdeferredを確認する。これはlibrary接続であり、既存canonical gate/Cloud結果終了の接続、継続資源更新、daemon/P tool公開は後続である。

## 継続資源のsnapshotと差分保存

`RecordPRResourceScan`は、binding済みPRに対するcontroller専用の読取観測portである。認証済みAPIの新しいsnapshotをcallerが取得し、前state digestをCASへ使う。競合時には旧snapshotへ新しいdigestを付け直さず、取得からやり直す。開始時刻が古いscan、同じscan IDの異なる再送、別repository/PRを拒否する。Webhookの順序で最新状態を上書きするportではない。

PR・issue comment・inline comment・reaction・review・check・status・historyの8種別について、全件取得の`complete`または理由付き`unavailable`を明示する。完全な種別だけ欠落entityを削除と判断する。取得不能なら既存entityを保持し、権限不足や取得不能履歴を空集合の成功にしない。JSONのkey順/空白を正規化し、安定ID・候補SHAと内容digestで追加/編集/削除/availability/head変更を区別する。古いSHAやAPPROVED/DISMISSED/reactionの文字列は保存事実であり、承認・scope・policy・tool指示を生成しない。

最新state、不変の過去snapshot、scanの一意receiptとpending差分outboxを一transactionで保存する。scan再送は同じreceiptを返し、内容不変の再取得では新しい通知要求を作らない。入力は2048 entities/2 MiB、保持stateも2048 entities/4 MiBを上限とし、取得不能による保持分も数える。過大なscanは全体を保留し、黙って切り捨てない。

検証: `go test ./internal/storage -run '^TestPRResource' -count=1`。編集・削除・review取消・好意的reaction、JSON順序差、履歴保持、取得不能と復帰、CAS/逆順/replay、失敗rollback、再openと容量を確認する。この分割は保存portであり、実API収集・署名済みraw deliveryとの回収・差分outboxの関連Task通知とdaemon配置は後続である。

## 資源差分の通知と現在の待機条件

`ReconcilePRResourceNotifications`はpending scan outboxを有界に処理し、元snapshot digest・種別/変更別の件数・PR資源key・scan IDを持つ通知を、binding済みの発行元購読へ保存する。外部本文を通知の指示欄へ挿入しない。event、fanout、queued状態は一transactionで確定し、snapshotが後から変わっても元versionを参照する。永続scan cursorはbad rowの後続を進め、再起動/再配送は同じevent/通知へ収束する。

通常更新は明示waitまでdeferredとし、閲覧/配送ACKで判断入力を消費しない。session_waitまたはmain/Taskの新しいwait集合で以前のPR待機を置き換え、外れた購読の未予約resource入力はdeferredへ戻す。mainのplan/acceptでも古いwaitを終了する。reportは既存契約どおり待機条件を変更しない。runに予約済みの入力は変更せず、共通の適用/再投入規則を使う。cancel/終端session/旧scopeはarchivedとし、通常の資源更新でmainを無条件に起動しない。

production KernelSourceのready処理から有界batchを回収する。この処理でAPI/modelを呼ばない。検証は`go test ./internal/storage -run 'TestPRResource|TestPRCompletion|TestPRWait' -count=1`。待機の前後・ACK・tool/main wait置換、失敗rollbackと再open、元version参照、cancel/実方向修正、bad row公平性、再配送の重複排除を確認する。API収集/raw deliveryの登録境界回収と、新しい関連Taskへの自動購読展開は後続である。

## 認証済みGitHub資源collector

`ResourceCollector`はready走査の外で、bindingと前state digestを読み、認証済みghのGETだけでsnapshotを収集する。PR headerのrepository数値ID/branch/番号を照合し、collectionを全page取得する。issue/inline comment、PRと各commentのreaction、review、check run、commit status、issue event履歴を別種別へ正規化する。check runはhead全体APIの直近1000 suites制限を避け、全suiteとsuiteごとのfilter=all全pageを取得する。suite走査が予算内に完了しない場合はcheck全体をunavailableとし、過去checkを削除にしない。review/inline/checkの観測SHAを保持し、statusは問合せた固定headへ対応付ける。PRのdefault branchが後から変わっても過去資源の読取を妨げない。

取得失敗・不完全なreaction親・不正/重複IDのcollectionは理由付きunavailableとなり、前snapshotの事実を消さない。取得中に主要PR headerが変わった場合とDBのCAS競合は保存せず再取得を要求する。GitHubの複数collectionは外部の原子的snapshotではなく、Webhook/継続再照合で補う。完了したscan IDの再送は保存receiptを返し、APIを再実行しない。

各gh応答は4 MiB、collector全体は16 MiB/128 collection読取呼出し/30秒を上限とする。paginationのHTTP回数を128回と主張するものではない。限界時のreaction等は取得不能として明示し、黙って完全取得にしない。保存側は大きなPR本文とJSON metadata/escapingを収めるため1 entityを512 KiBまでとし、scan全体2 MiB/保持4 MiBと2048 entitiesの上限を維持する。`ResourceGET/ResourcePages`は書込guardを持たず、GET以外を実行するAPIを公開しない。

検証: `go test ./internal/product ./internal/github -run TestResource -count=1`。全種別/親reaction/古いSHA、権限等のread失敗と保持、header変化/CAS競合、collection予算、全page/object collection、null/欠落collection拒否、subprocess出力上限と完了scan再送を確認する。daemonへの非同期配置、署名済みraw deliveryの登録境界回収と関連Task購読展開は後続である。

## 署名済みraw deliveryの先行保存

`PRInbox{Store, RepositoryID}`はcontroller専用の`github.Inbox`実装で、必ずHMAC検証を行う`github.Webhook`の後ろへ接続する。モデルtoolとして公開しない。設定したrepository数値IDとの一致、delivery/event識別子、UTF-8 JSONと1 MiB上限を検査し、PR・Wave・購読がまだなくても受信bytesを保存する。本文・action・好意的reaction等から操作権限や最新PR状態を生成しない。

元の署名対象bytesはbase64で正確に保持し、ID/event/repositoryとともにdigestへ含める。同じIDと同じ入力は一つの受信となり、異なる入力の再利用は拒否する。raw record・repository/sequence索引・独立の単調sequence counter・初回受信coverageを一transactionで確定する。保存失敗はHTTP成功にせずrollbackし、SQLiteの競合/IOエラーも返して呼出し全体の再試行へ委ねる。

`PRRawDeliveries`はcontroller用のrepository別cursor読取で、workerへの履歴grantではない。小さな索引をseekし、最大128件/2 MiBのページに制限する。`NextAfter`は後続ページ、`LastSequence`は現在の末尾で再接続・次の受信待ちに使える。rowidに依存せずVACUUM/reopen後も同じsequenceとなる。coverageは初回受信時刻と`history_before=unavailable`・`captured_deliveries_only`を保存し、受信前履歴やWebhook欠落を架空の完全取得にしない。

検証: `go test ./internal/storage -run '^TestPRInbox' -count=1`。実HMAC handlerの署名/設定repo/再送/衝突、binding前保存と権限非生成、元bytes、rollback、複数repo cursor、VACUUM/reopen、末尾後の新規受信、大きなpayloadのページ境界を確認する。この分割は受信portであり、daemon HTTP配置・raw履歴のPR登録境界回収・関連Task通知は後続である。

## 登録境界のWebhook履歴回収

`ReconcilePRWebhookNotifications`はbinding済み購読ごとのraw cursorで、intent保存後に受信した更新をPR番号へ照合する。PR作成応答とbindingの間に届いたcomment等も、binding確定後の走査で回収する。issue commentはPR issueであることを確認し、review/inline/PRとcheck-suite/run/workflowの明示PR番号を扱う。PR番号を持たないstatus・check等はraw履歴に保持し、collectorの差分照合で補う。この経路で不明なPRを推測してTaskを起動しない。

通知には元delivery ID/sequence/digest、種別/action、actor数値ID、PR headとreview/inline対象SHAを区別して保存する。本文は挿入せず、raw eventから最新state・candidate・承認・sender権限を変更しない。逆順deliveryも履歴事実として残し、現在状態は別のAPI snapshotで確認する。通常は明示waitまでdeferred、cancel/終端/旧scopeはarchivedとなる。

raw cursor・元scope付きevent・通知は一transactionで保存する。再送/再起動/共通fanout照合で重複せず、保存失敗はcursorも戻す。購読は有界かつ公平に走査し、無関係なraw履歴の後にも更新がある場合と未走査購読がある場合はproduction Sourceへ即時再走査を返す。末尾のidle購読ではbusy loopを作らない。HTTP/API/modelをこのready処理から起動しない。

検証: `go test ./internal/storage ./internal/control -run 'TestPRWebhook|TestPRInbox|TestPRResource|TestPRCompletion|TestPRWait|TestKernelSource' -count=1`。実署名handlerからbinding前後、同一再送、別PR除外、本文の権限非生成、ACK/wait、rollback/reopen、無関係な先行履歴、受信時刻の登録境界、cancel、古いreview SHA、購読ページとidle終了を確認する。daemonのHTTP/非同期collector配置と関連Taskへの購読展開、外部出所付き親message中継は後続である。

## 親子messageの外部出所追跡

`send_message`の引数やsender認可は変更しない。controllerが発行元runの保存input snapshotをdigest/容量で検証し、そのsnapshotに含まれて実runへboundされたexternal通知と受信messageから`Message.Sources`を自動付与する。モデルは出所・actor・sender等を引数で注入できない。Webhookは元の署名対象bytesまで、API差分は通知に実際に提示したscan ID/digest・決定的event ID・不変snapshotまで再照合し、resource・要求版・発行元session/run・通知/event ID・digest・PR head/対象SHA・actorを保持する。

同じrunの入力で受け取った中継messageと明示replyからは元の出所を継承し、新しいrun/actorへ付け替えない。受信側のwake入力にもSourcesを含める。mainの複数Wave入力の出所は元Waveのまま残し、宛先Waveへの権限とは分離する。Sourcesはrunの文脈にあった情報の監査記録であり、本文が全sourceを引用したとの意味ではない。sourceは最大64件で、過大な連鎖を黙って削らずmessage全体を保留する。出所付きmessageはreceiverのcontextを含む実際の単一wake入力も保存前に組み立て、64 KiBへ収まらなければ通知・message・eventを全rollbackする。通常のinput/manifest容量制限も維持する。

保存済み通知のIDだけを名乗る別runや、改ざんされたheader/digestではmessageを確定しない。既存の直属親子・返信・現在session/run/thread/turn/call・snapshot scopeを同じtransactionで検査する。外部本文・source metadataからsender権限、artifact grant、未登録Wave要求、criteria/policy変更や正式承認を生成しない。旧messageはSources省略のまま読み書きできる。

検証: `go test ./internal/storage -run '^TestExternalMessageSource|^TestParentChildMailboxes|^TestMessage|^TestWake' -count=1`。実PR受付/署名履歴/API差分から明示wait・wake/run・共通send_messageへ通し、古いreview SHA、返信、新runで受信messageだけからの再中継、wake入力/再open/再送を確認する。出所/sender引数の偽装、保存不整合、宛先Waveの権限、未登録要求とcriteria緩和も拒否する。関連Task購読展開とP daemon接続は後続である。

## WorkItem割当によるTaskの自動購読

P plan採用時、Taskの検証済みWorkItem割当ごとにread用購読intentと受信購読を同じtransactionで作る。`source_kind=assignment`はtool operationと分離し、存在しないrun/call/operationを生成しない。candidate projectionやPRが未確定ならpendingで保持し、trusted product登録と正のPR bindingで解決する。後から追加されたTaskは既存PRへ関連付ける。登録順序や再実行で重複購読を作らない。

operation由来とassignment由来の購読は、それぞれ元operationまたはTask/session/親/WorkItem割当を再検証する。PR資源とのWorkItem/Wave/repository/番号の所有関係も照合する。同じSourceのraw回収・API差分通知・明示wait処理を再利用し、Taskが別の兄弟の購読を待機することや、未割当WorkItemへのすり替えを拒否する。通常更新はwaitまでdeferred、旧Task/旧scopeはarchivedとし、購読だけで製品Taskの実行・publish・承認を許可しない。

plan、登録、PR bindingの途中で購読保存が失敗すれば全体をrollbackする。元の購読時刻を保持して、binding前に届いた更新も回収する。Taskのwaitのstorage試験は認可済みwaitを模擬したもので、native P実行の合格にはしない。`product_readiness_unavailable`はTask_11のcanonical接続まで維持する。

検証: `go test ./internal/storage -run '^TestTaskPRSubscriptions' -count=1`。planとの原子性、WorkItem所有登録後の遅延candidate projection、call非生成、binding前raw/後続snapshotとwait、再open/再送、binding rollback、兄弟/未割当/偽operation、既存PRと新plan・旧scope履歴を確認する。非同期P daemonと既存PR工程への接続は後続である。

## 外部読取の独立worker

`product.Observer`はcontroller lock下で使うread専用の独立loopで、KernelSourceのready処理へHTTPを持ち込まない。binding済み資源を有界cursorで読み、既定2・最大8 workerで異なるPRを並行収集する。同じ資源を重複実行せず、完了・失敗後は既定60秒の間隔を置く。一つの遅い読取や失敗で後続資源の枠を塞がない。API/collectorは共有cancelと30秒上限を尊重し、loopは全workerが戻るまでstore/controller ownerを保持する。

変化が保存された場合だけ非blockingなOnMutation hintで通常の通知回収へつなぐ。再起動時は保存stateへ照合し、同じ内容の再取得を新しい変更にしない。Report/OnMutationは短い非blocking callbackを使う。これはread workerであり、publish/review/mergeやモデル起動を行わない。

`PRObservationTargets`はbindingとprojectionを同じDB snapshotで検査し、観測対象のPR番号/branchを元資源へ固定する。pause/cancelや現在のbranch変更でも過去の外部事実を観測できるが、canonical projection自体を戻さず書込許可にも使わない。

検証: `go test ./internal/product ./internal/storage -run 'TestPRObserver|TestPRObservation|TestResource|TestTaskPRSubscriptions' -count=1`。外部読取をblockしたまま別PR/DB読取が進むこと、失敗の非starvation、cancel終了、poll間隔、restart重複排除、資源cursorとcancel後のheaderを確認する。追加資源はobserver scheduling専用の保存fixtureで、PR mutation/正式承認の実接続合格にはしない。daemon設定/HTTP配置、書込dispatchとcanonical工程の接続は後続である。

### daemon で観測を有効にする

既存の K 起動設定とは別に、同一ユーザー所有・0600 の JSON を明示する。

```json
{"schema_version":1,"workers":2,"every_seconds":60}
```

```sh
harness wave run --root /absolute/private/root --runtime /absolute/private/runtime.json --pr-observer /absolute/private/pr-observer.json
```

実際の CLI バイナリ名に合わせて実行する。workers は 1..8、every_seconds は 1..3600 で、いずれも省略せず指定する。設定を渡さない起動では観測 worker は動かない。読取には controller 側の `gh` 認証を使い、worker Codex home へ認証を追加しない。binding 済み資源だけを DB から選ぶため、新規の K DB に GitHub repository を必須にしない。

daemon は IPC、model dispatch、観測を別 goroutine で動かし、いずれかが異常終了したら共通 cancel で停止する。観測の失敗は個別資源の Report と次回 poll へ進む。daemon 終了時は全 worker の終了まで controller lock / store を保持する。Webhook HTTP 配置と外部書込 dispatch はこの設定に含まれず後続である。

検証: daemon worker 待機中の IPC 応答、cancel 中の lock 保持と join 後の解放、異常終了と予期しない正常 return の伝播、private/schema/上限の拒否、実 CLI の opt-in constructor 起動・停止を `go test ./internal/kernelcli -run 'TestPRObserver|TestKernelDaemon|TestKernelCLIRun' -count=3` で確認する。実 GitHub/model 接続はこの fixture の合格に含めない。

### daemon の Webhook HTTP 入口

`wave run` の任意 `--pr-webhook /absolute/private/pr-webhook.json` は、repository を固定した署名検証入口を起動する。観測の `--pr-observer` と同時に使える。Webhook が使えない構成では observer の定期照合だけを使える。

```json
{"schema_version":1,"repository_id":17,"listen":"127.0.0.1:8080","secret_file":"/absolute/private/webhook-secret"}
```

JSON と secret file は同一ユーザー所有・0600 の通常ファイルにする。secret は 1..4096 bytes の正確な内容を使い、末尾改行も除去しない。schema / repository ID と numeric loopback address:port を明示する。`localhost`、外向き address、port 0 を拒否する。reverse proxy を使う場合は `/github` を転送し、GitHub 側に同じ secret を設定する。secret を model/tool 入力へ渡さない。

`POST /github` は既存 HMAC handler で 1 MiB の上限と署名を検査し、configured repository の raw delivery を commit してから非blockingな wake hint を送る。同じ delivery の再送は既存 record を維持し、内容違い・別repository・保存失敗には 503、署名違いには 401 を返す。入口は内容から operation / approval / policy を生成しない。資源への通知回収は既存の kernel source が行う。

HTTP は header/body/idle timeout と同時 handler 上限32を持ち、過負荷時は503を返す。daemon cancel は request context に伝わり、5秒の graceful shutdown 後は接続を閉じる。controller は開始済み handler が戻るまで DB / lock を保持する。bind failure も全 daemon loop の停止へ伝播する。host のポート公開や実 GitHub 配送確認はこの fixture 検証とは別である。

検証: `go test ./internal/kernelcli -run 'TestPRWebhook|TestPRObserver|TestKernelDaemon|TestKernelCLIRun' -count=3`。実 loopback HTTP で正確な payload 保存後の hint、誤署名/foreign/conflicting replay 拒否、再送の非重複、handler cancel/join、終了後の listener close、bind failure、private secret と設定境界を確認する。書込 dispatch/canonical 工程の接続は後続である。

### 外部書込の非同期 dispatch

`product.Dispatcher` は canonical publication/review gate と transport を明示する controller 専用 worker pool である。`PRDispatchOperations` は同一 DB snapshot の accepted PR tool intent だけを 128 件以下の cursor page で返し、unknown / completed / canceled は送信 queue に戻さない。model dispatch の Ready では外部 I/O を待たない。

既定2・最大8 workerで異なる操作を並行処理し、各実行は共有cancelと1分のdeadlineを渡す。既存 adapter が候補・scope・pause/cancel・capacity・mutation lease・canonical gateを照合する。同じPRの書込は既存leaseで直列化する。失敗は次回間隔（既定1分）まで待ち、他の操作を選べる。callbackは非blockingで使い、cancel時は全子workerをjoinしてから返る。timer用cacheは期限切れを除去する。

準備済み・未送信の再試行はoperation IDから導く同じownerを使う。これで再起動後も prepared を照合できるが、物理停止の証拠にはしない。旧executorが残っていても送信直前の `MarkPRMayHaveSent` はtransactionで一度しか成功せず、成功後のunknownを再度送らない。異なるownerの既存claimを奪わない。結果不明のread-only回収には従来どおり別途停止証拠を要求する。

検証: `go test ./internal/product ./internal/storage -run 'TestPRDispatcher|TestPRDispatchOperations' -count=3`。共通gatewayで実際に受付した操作から、PR公開/reviewの既存guard付きadapter、preparedのDB再open後再開、応答欠落unknownの非再送、別PR/DBの並行進行、cancel join、cursor/状態filterを確認する。並行fixtureはmodel run開始前にPR枠2を明示し、既定値1を変更しない。APIはfixture transportであり実Cloud合格ではない。canonical工程のport実装とdaemon書込opt-inへの配置は後続である。

### native session への P tool 公開

private runtime JSON に `"product_tools": true` を明示すると、共通 `SessionDriver` が新規 thread/start の dynamicTools に既存の `pr_publish` / `pr_review_request` を追加する。既定falseでは従来の kernel tool set を使う。TaskにはWave toolを追加しない。tool公開は認可・gate・正式承認を作らず、呼出は従来の process owner / session / run / thread / turn / call を固定する共通gatewayを通る。

thread作成前に `PinSessionTools` が論理session / generationごとの kernel_v1 または product_v1 を保存する。保存失敗は準備予約 / RPCより前に返す。同じgenerationの構成変更や、profile記録がない既存threadのP化を拒否する。既存の無記録threadは従来K構成でのみ再開できる。変更にはtrusted `BeginMainThreadReplacement` portで停止確認・入力境界・監査を伴う明示更新を行い、新generation / 新threadを使う。既存Task threadを変更せず、新Task/sessionへ引き渡す。profileは履歴として残し、DB再open後も照合する。

この設定は外部操作の受付toolを公開する。canonical gate実装とdaemon書込dispatcherの配置、WorkItem割当Taskのreadiness解放は後続であり、受付を外部送信成功へ読み替えない。

検証: `go test ./internal/worker ./internal/control ./internal/storage ./internal/kernelcli -run 'TestProductTools|TestSessionToolProfile|TestRuntimeProductTools|TestRuntimeNativeProductTool|TestKernelCLIRun' -count=3`。main/Taskのrole別tool公開、profile保存失敗時のRPCゼロ、世代/再open/既存thread拒否/明示rotation後の構成変更、production Runtimeと実DBのprofile照合、private runtime opt-inの読込を確認する。RPC fixtureでP mainから実共通gatewayへpr_publishし、同じcallの再送がoperation/購読を増やさず、K Waveが同じnative tool要求を拒否することを確認した。fixtureの汎用最終出力は製品候補やWave受入の証拠にしない。実model/GitHub接続の検証とは区別する。

### 既存 canonical journal の gate adapter

`service.Controller.ProductPublicationGate` / `ProductReviewGate` を既存 `product.Publisher.Gate` / `ReviewRequester.Gate` に渡せる。これらはsourceの既存WorkItem journalを読み、kernelのprojectionそのものを検証証拠として扱わない。明示した信頼済みConfig、WorkItem全体/候補/PR番号の一致、保存policy snapshot、最新Issue本文/hold、検証計画のidentity/criterion coverageと固定commit内のtest参照、現在候補のrequired runner結果を確認する。部分範囲では残作業記録とcommitのclosing textも確認する。公開bodyは従来と同じ `publicationBody` から構成する。最後にWorkItemを読み直し、途中のpause等を拒否する。

review gateはさらに最新のPRのrepo/branch/base/candidate SHA/open/non-draftを照合する。CapabilitiesPathがあれば、上限1MiB・実行ユーザー所有・group/other書込不可の通常ファイルをsymlink/FIFOを避けて毎回読み、欠落したfieldを以前のverified値で埋めない。構成とrepositoryの一致、cloud_requestのverified evidenceとtrusted requester/triggerを要求する。cloud_native_approvalがunknownでも依頼capabilityとは分け、依頼可能という結果から正式承認を生成しない。

gate自体はdomainの状態変更・publish/review/mergeを行わず、DB/APIの失敗は呼出元へ返す。実際の送信は既存adapterの直前gate再評価・kernel capacity/lease・一度だけのunknown guardを通る。このadapterは既存source journal用で、新K DBにlegacy tableを暗黙作成しない。新P sourceの作成/同期とdaemonへの書込配置はTask_11接続で扱う。

検証: `go test ./internal/service -run TestProductCanonical -count=3`。既存のfake agent/runner workflowと実git worktreeでREVIEWINGまで到達したsourceから、公開資料の構成、外部書込ゼロ、projectionやsource/policy/Issue/validation/candidate/test参照/partial residual/PR番号の不一致拒否、capability取消と欠落reportの拒否を確認する。PR header変更とsymlink/FIFO/group・other書込/サイズ超過/別ユーザー所有も、有効な開始状態を確認してから拒否を検証する。foreign UIDのmappingまたは所有者変更を許されないtest環境では別所有者fixtureだけskipし、root環境の実file検証と区別する。fixtureのrequest capabilityは実Cloudの合格証拠にしない。

### 通知で提示された外部本文の有界読取

runtime JSONの既存項目へ `"product_tools": true` と `"product_tool_profile": "product_v2"` を指定すると、新規threadのcontext_read schemaにexternal sectionを公開する。profile省略時は従来のproduct_v1を使い、そのschemaとkernel_v1を変えない。既存のv1 threadはv1設定で再開できる。v2へ変更するときは保存profile照合と明示main rotation / 新Taskへの引き渡しを使い、既存threadのenumを変更した扱いにしない。unknown profileやbool opt-inを欠くprofile指定はRPC前に拒否する。

Pの現在runへ実際に渡した `external_update` 通知のIDを `context_read` の `reference_id` とし、sectionをexternal、offsetを0、limitを1..2048にする。返るnext_offsetで続きを読み、同じdigest/bytesを確認する。共通gatewayと現在Wave境界、保存run inputのhash/通知binding、署名rawまたは不変snapshotを再照合する。delivery IDやscan IDを知っているだけでは読めず、peer sessionや別runのmessageへ中継された出所だけから新しい読取grantを作らない。

Webhookは保存した正確なpayload bytesを返し、slice digestはpayload bytes自身のSHA-256になる。通知のdelivery digestはID/event/repository/payload全体のdigestなので別の値である。API観測はHead/Sectionsのcanonical JSONを返し、slice digestは通知のsnapshot digestと一致する。後続のscanで現在stateが更新されても、同じ通知は元の観測版を読む。本文は不信なJSONデータとして扱い、instructions/scope/policy/approvalへ変換しない。DB再open後も保存inputのgrantを照合し、破損したbacking dataを拒否する。SourceReadyでHTTPを待たず、読取自体も外部APIを呼ばない。

検証: `go test ./internal/contract ./internal/worker ./internal/control ./internal/storage ./internal/kernelcli -run 'TestExternalContext|TestExternalMessage|TestExternalContextSchema|TestProductToolProfile|TestRuntimeProductTools|TestKernelCLIRun' -count=3`。UTF-8本文の有界page/全bytes/digest、共通gateway読取、後続scan・再open後の同一版、peer/ID推測/relay-only/破損sourceの拒否、既存schema不変とv2のnative公開・profile保存、private runtimeからの設定伝播をfixtureで確認する。実model/GitHubや製品Wave受入の合格とは分ける。

## 候補作成前のWorkItem所属登録

Task_11 / #42 の `KernelStore.RegisterProductWorkItem` は trusted controller 専用の保存portで、P Waveのsource/source ID・scope/policy版にWorkItem ID、repository名/数値ID、criterion coverageを結び付ける。candidate作成前に所属を保存できる。モデルtoolやCLI/source入口はこの分割で追加しない。

criterionは登録済み要求の非空・重複なし部分集合とし、順序を正規化する。同じscope/policyの定義は不変で、次の版への更新には前digestのCASを要求する。repository数値IDとWave所属は移動できない。所属・現在定義・不変履歴を一transactionで保存し、版更新時は古いcandidate所属を解除する。定義は64 KiB、Waveごと128件を上限とし、pause/cancel/終端/期限切れは新規更新を保留する。完全一致の再送はread-onlyで同じsnapshotを返す。

保存入力には親の当該Wave全件とTask自身に割り当てたWorkItemだけを載せ、Task input digestへsource定義digestを含める。古いsource版は現在の定義として渡さず、不在理由を示す。Kの入力schemaは変更しない。候補projectionと登録済みrepositoryの不一致を拒否する。

検証は `go test ./internal/storage -run '^TestProductWorkItemRegistration' -count=1`。direct/GitHub source識別、候補なし所属、再open/replay、rollback、他Wave/K/未知criterion拒否、親/Taskの入力分離、scope更新CASとcandidate解除、repository照合・件数上限・pause中再送を確認する。metadata登録は実行・検証・公開権限を生成せず、製品Taskのreadiness保留を維持する。source入口、worktree、製品結果/候補の適用とdaemon書込workerの配置はTask_11の後続である。

## 初回sourceの原子的な受付

`KernelStore.CreateProductWave` は trusted source controller が作成した `ProductWaveRequest`（Wave要求と1〜128件のWorkItem定義）を受け付ける共通portである。`direct` と `github` は同じtransaction経路を使う。これは正規化済みsourceの保存であり、Issue poolからの取得やCLI入口、外部Issue本文からpolicyを決める機能ではない。

Wave作成・最初のevent/通知intent・全WorkItemの所属/定義/履歴・source受付receiptを一transactionで保存する。したがって、別controllerが最初のplanning入力を予約する時点で全所属が揃う。どのWorkItemやreceiptで失敗してもWaveと通知を含めてrollbackし、同じsource IDで改めて受付できる。既に見えているWaveへの初回登録の後付けは拒否し、明示的な版更新へ分ける。

WorkItem一覧とcriterion一覧の順序を正規化して受付digestを固定する。定義や要求を変えた再送は拒否し、完全一致の再送は保存済みWaveの現在状態をread-onlyで返す。pause/cancelや方向修正後も初回通知を再発行せず、古いsource定義や候補を復活させない。初回定義digestはreceiptに保持し、現在定義と履歴を区別する。source IDはUTF-8で4096 bytes以下、NUL/改行を禁止する。

検証は `go test ./internal/storage -run '^TestProduct(Admission|WorkItemRegistration)' -count=1`。両sourceの初回予約入力、2件目/receiptでの失敗rollback、再送の順序差・pause・方向修正・再open、定義変更・他Waveへの所属移動・既存Waveへの後付け・重複/未知criterion/K拒否を確認する。source名のfixture確認は実GitHub取得やP全体の受入ではない。

## GitHub Issue sourceの読取と明示受付

`product.IssueAdmitter` / `wave import-issue` は、trusted repository数値ID・ready/hold labelとcontrollerのmain/上限から、GitHub Issueを共通`CreateProductWave`へ渡す。既存`service.ParseScope`のfence parserを`verification.ParseScope`へ共通化し、旧入口は同じ契約を維持する。

GET専用ResourceAPIでrepositoryの数値ID/full_nameとIssueの番号/open/ready/hold/updated_atを検証し、Issueに似たPRを拒否する。本文/label/更新時刻とrepositoryを最後に読み直し、変化・権限/取得失敗を受付成功にしない。依存宣言は一つ、正数の非重複32件までとし、self/不正/曖昧な宣言を拒否する。依存はclosedを要求し、kernelで既に扱う依存は、source名だけでなく共通WorkItem所属から管理Waveを解決し、direct/GitHubの入口にかかわらず現在のP Waveの完了も要求する。所属の欠落・破損・旧scope/policyとrepository ID不一致は保留する。旧metadata未登録Waveはsource indexでも照合する。APIは128読取呼出し/16 MiB/30秒、gh各応答は4 MiB、CLIのwhole-mutation retryは10秒の枠内で動く。

初回のfull scope revision 1だけを扱い、部分/既merge/改訂済みのscopeは明示lineage importへ保留する。Issue source/WorkItem IDを数値repository IDとIssue番号から固定し、scopeの変化を単なる再送として上書きしない。Issueの選択条件は正式承認ではなく、外部書込・candidate・実行許可を作らない。保存前後のHTTP読取は外部サービスとのatomic snapshotではなく、公開gateでのfresh source再確認が後続に残る。

検証: `go test ./internal/product ./internal/kernelcli -run 'TestIssueAdmission|TestIssueImportCLI' -count=1`。共通受付/再送、repository/Issueの変化、hold/取得不能、依存・scope履歴の保留、キャンセル、認証済みghと同じargvのGET専用transport fixtureを確認する。初回pool workerの配置は次節を参照。実GitHub接続、初回以降のscope同期と製品runtime全体は未受入である。

## 初回Issue poolのbackground worker

`product.IssuePool`はready走査の外で番号・状態・labelだけを32件ずつ取得する。`github.Client.IssuePoolMetadata`は固定の読取専用GraphQL queryを持ち、POSTはqueryのtransportとしてだけ使用する。任意queryやmutationを受け付けず、本文は選択した候補のbounded GETだけで取得する。大きな本文を含む全ページを`--paginate --slurp`で一応答へまとめない。

priority labelごとのgroup、最後の優先度なしgroupの順に、作成順のmetadataページを読む。各ページはIssue番号で整列し、repository ID/name、完全なpageInfo、非空の前進cursor、番号/labelの重複や上限を検証する。既知のhold/PR/closedを除外し、partialなlabel情報から不在を推測しない。受付時は全labelから現在のpriority groupを再確認し、本文・依存・source確認を共通`IssueAdmitter`へ渡す。

repositoryごとにsource policy/priority/collection版digest、group、ページcursor、ページ内の最後の処理番号を保存する。部分ページ・試行数上限・budget・再openを跨いで未取得候補へ戻れる。古い内部cursor形式や選択policyの変更は非権限の進行位置だけを先頭へ戻し、既存Wave/WorkItemの履歴を変更しない。APIページ自体は外部のatomic snapshotではなく、全group完了後の再走査と受付直前のfresh確認で補う。

1走査1〜32試行、metadata 2048件、API data/transport output 16 MiB、128読取呼出し、45秒、gh各応答4 MiBを上限とする。失敗した過大応答やdecode失敗も親のbudgetへ計上する。予算切れは途中結果と診断を返し、未取得Issueのcursorを進めない。走査自身のdeadlineは読取終了後に、daemon contextに従う1秒の保存用contextで処理済み位置を保存する。daemonのcancel/deadlineを迂回した保存は行わない。

`wave run --issue-pool private.json`はschema1・private same-user設定をruntime構築前に検査し、同じmain/上限/Storeのworkerをcontroller-owned lifecycleへ配置する。既存source Waveの再open・初回event再発行は行わず、commit後のhintだけを送る。shutdownは読取の終了までjoinする。既存Issueのhold/close/編集の禁止は後述のsource observerで接続する。方向修正の自動採用と製品Task/候補/公開は別の接続として残る。

検証: `go test ./internal/product ./internal/github ./internal/kernelcli -run 'TestIssuePool|TestIssueMetadata' -count=1`。32件ページとページ内位置の再open、4 MiBを超える旧形式collectionでもmetadataから高priorityの後方Issueを先に受付できること、選択した本文だけのGET、fresh labelのpriority再確認、完全/不完全なページ境界を確認する。既存の容量/rollback、過大応答の累積budget、未取得5件目の再開、deadline/cancel、private設定とdaemon lifecycleの回帰も維持する。fixtureと実GitHub/実modelを含むP全体の受入は区別する。

## Sourceの観測と独立した保留

`KernelStore.ObserveProductSource`はtrusted controller専用の保存portである。canonicalなGitHub source ID、repository数値ID/Issue番号、source policy digest、GETで確認したopen/closed・ready/hold・元要求digest・更新時刻、または理由付き取得不能を受け付ける。current snapshot digestによるCASは外部読取の前に取得する。Sourceのscope/policyは照合し、同じsource内のplan変更は上書きしない。

観測は`ready / unavailable / closed / hold / not_ready / request_changed / scope_unconfirmed / policy_changed`を区別する。未確認・closed/hold・ready条件不成立・要求編集・WorkItem所属の旧版は`Wave.source_hold`として後続dispatchを保留する。`ready`は必要なsource条件を満たしたことだけを表し、検証・正式承認やWaveの成功を意味しない。編集を新scopeへ自動採用せず、版付き同期/後続工程へ残す。

source policy digestはWaveのpolicy revisionごとに固定する。Issue受付時に選択policyのdigestを同じtransactionで保存し、未登録の既存sourceは最初の観測で固定する。同じpolicy版でルールが変わった観測は`policy_changed`として保留し、scope/plan変更だけでは新ルールを採用しない。明示的なpolicy版更新が必要で、pinの欠落・破損は`state_invalid`にする。

source保留中でも、既存WorkItemを現在scope/policyへ更新するCAS付き登録は可能とし、保留そのものは次のready観測まで維持する。新所属の追加、manual pause、pending direction、終端の制限は維持する。

source保留、manual pause、pending directionは独立で、手動resumeはsource保留を解除しない。Sourceがreadyに戻ってもmanual pause/directionを消さない。Waveと小さいtool authority projectionを同じtransactionで保存し、既存のscheduler/共通gateway/PR送信直前guardへ反映する。cancelは終端の親向け観測を阻害しないよう実行用保留を外し、source観測だけで終端Waveを再open/再holdしない。終端であってもsnapshotの非ready状態とfactsは保存し、禁止の根拠を消さない。実行用SourceHoldは毎回retry判定より前に再計算するため、将来の明示的な再活性化後には同じfactsでも禁止を再設定する（Pの完了/再活性化port自体は未接続）。

現在snapshot・不変history・Wave/authority・watch eventを原子的に保存し、完全一致の再送はread-onlyにする。watch eventから通知intentを生成せず、source pollだけで無条件のmain turnを要求しない。破損/missing headの履歴は上書きして再構築せず、active Waveへ`state_invalid`の禁止だけを保存し、元の証拠を残す。SQL/保存失敗はrollbackする。

検証: `go test ./internal/storage -run '^TestProductSourceObservation' -count=1`。CAS/再open/履歴、手動保留との独立、状態別保留、scope/foreign/K拒否、再計画と競合する観測、history/watch失敗rollback、warm saved input/準備済みPRの禁止、通知/run非増加、破損証拠保持と終端観測を確認する。この分割は保存と共通禁止の接続であり、既存Issueをpollしてこのportへ渡すworker配置は次節を参照。版付き要求同期は後続である。

## 受付済みIssueの定期観測

`wave run --issue-source-observer private.json` は、初回受付とは独立したtrusted readerをcontrollerのbackground lifecycleへ配置する。private same-user設定はschema 1、`source_policy`（初回受付と同じrepository/repository_id/ready_label/hold_label）、`attempts` 1〜32、`every_seconds` 1〜3600を要求し、runtime生成前に検証する。初回受付も継続する場合は`--issue-pool`を併用する。

対象は数値repository IDに属する非終端のGitHub P sourceだけで、DBからID順に最大32件ずつ読む。repositoryごとにpolicy digestと最後のWave IDを保存し、再open後も継続する。末尾で先頭へ戻り、hold/closed/not_readyの既存sourceも再確認する。全Waveの読込やschedulerのready走査内でHTTPを行わない。

読取前にsnapshot CASと現在policy版のpinを取得する。設定のdigestがpinと異なる場合はGETせず、Issueの成功factsを含まない`policy_changed`の禁止を保存し、同一再送も診断上は失敗として報告する。pinそのものは更新しない。GET専用transportでrepository数値ID/name、Issue番号・非PR・状態・時刻を検査する。Issueとrepositoryを再読し、本文・label・更新時刻等の変化や不正な応答、取得失敗は`unavailable`として禁止する。安定した取得からready/holdと元要求digestを計算し、共通`ObserveProductSource`へ渡す。異なる選択policyは既存pinと衝突して保留し、外部本文からpolicyやscopeを更新しない。

1 scanは45秒・128 GET・16 MiB、各sourceの読取は5秒。応答失敗のbytesも共通budgetへ課金する。ローカル読取/scan期限が切れた試行は読取終了をjoinし、生きているcontroller contextで各1秒以内の保存とcursor checkpointを行う。daemonのcancel後は新たな保存を開始しない。未読のsourceはbudget到達後にcursorを進めない。観測またはstate_invalidの禁止が保存済みと確認できた試行だけcursorを進める。取得失敗をunavailableとして保存できた場合は次のsourceへ進むが、preflight期限切れ・CAS競合・SQL失敗等で未保存なら元のcursorから再試行する。

実行用禁止が変わった場合だけcommit後のwake hintを送る。pollは通知intent/無条件のmain turn、candidate、外部writeを作らない。実行中のreadはshutdownまでjoinする。pollとGitHubはatomic snapshotではなく、公開直前のfresh source gateと版付き要求同期・後続作業は引き続き後続の接続範囲である。

検証: `go test ./internal/product ./internal/storage ./internal/kernelcli -run 'TestIssueSourceObserver|TestProductSourceObservationTargets' -count=1`。状態別保留/復旧・manual pause維持、再送、古いreadと新しいholdのCAS競合、repo/PR/変化/取得不能の禁止、pagination/再open、read/scan期限checkpoint、出力budget、shutdown join、private設定とGET argv/commit後hintを確認する。transport fixtureの成功を実GitHub/モデル/Cloud承認やP全体の合格には数えない。

## worktree準備中の固定パス予約

`OperationCapacityRequest{Kind:"git", Repository:..., Worktree:...}` のoptional `Worktree` は、trusted controllerがGit checkoutを準備する際の作成先を表す。これはcapacity/resource予約であり、Task実行やcandidateの業務上の許可ではない。既存のWorktree未指定Git操作とJSON契約は維持する。

共通repository Git予約と元checkoutの排他に加え、作成前のcanonicalな作成先を`workspace_prepare:`資源へ固定する。作成前は通常directoryのpath、作成後はlinked worktreeのgit-dirに変わるため、Git discoveryだけの識別に依存しない。app-server準備・runner・Git予約は、この固定pathと同一/配下/包含directoryの新規利用を拒否する。既存のsymlink別名と、まだ参照先がない絶対/相対のsymlinkもcomponent順に解決する。link/..をリンク解決前にcleanせず、cycleや非directoryを通るpathは拒否する。予約要求のWorktreeにも解決済みpathを保存し、実行ownerは返された保存済みpathを使う。リンクの付替え後に同じoperation IDを再送して作成先を変えることは拒否する。保存済み固定pathはcheckout作成後や再open後も変えない。

準備を予約する側も、作成先と重なる既存のlive session・runner・Git操作を検査する。共通Git資源・作成先・旧worktreeの予約は一transactionで保存し、失敗はrollbackする。unknown/期限切れだけで解放せず、trusted ownerの`process_stopped`または確実な`not_started`の証拠が必要である。独立したworkspaceやrepositoryは引き続き利用できる。

検証: `go test ./internal/storage -run 'TestWorkspacePreparation|TestGitCapacity|TestRunnerCapacity|TestProcessCapacity' -count=1`。実temp Gitで作成前/作成後の識別変更、alias/child/包含directory、再open、unknown保持、停止証拠後の解放、既存利用者、別repoによる同じ作成先の並行予約、保存失敗rollbackを確認する。製品worktreeの作成worker・固定候補の登録・P Taskのreadinessは後続の接続であり、この予約だけで未接続の製品Taskを実行しない。

## 製品Taskの固定worktree準備

`wave prepare-product-task --root ROOT --file private.json` は、hostが作成したschema 1の`request`（`task_id`、`repository_id`、canonicalなlocal `repository`、固定40桁`base_sha`）から一つの製品work/fix Taskのworktreeを準備する。ファイルはprivate same-userを要求する。数値repository IDとlocal repositoryの対応はhost設定の責任で、Issue本文やmodel toolからこの要求を作らない。移動するbranch名をbaseとして受け付けず、remote fetch/pushやPR公開を行わない。

対象は現在scope/policy/planに属する未開始のP work/fix Task、一件の登録済みWorkItem、そのcriterionの範囲だけである。現在のcanonical入力digest・登録digest・親session・固定Task CWDを照合し、準備要求とGit/作成先の予約を同じtransactionで保存する。review/verification、K、旧scope、既存thread/run、pauseや出所不一致を新規準備に使わない。

保存したintentの`prepared -> launching`を一度だけ消費してから、`ROOT/workspaces/SESSION_ID`へ`harness/SESSION_ID`のcheckoutを作る。再送/再openがlaunchingを見ても二重開始しない。作成先が既に存在する場合は上書き/削除/暗黙採用せず失敗履歴を残す。親と作成先はprivateなsame-user directoryとして検査する。

Gitはcheckout前にbranch/gitdir条件付きfilter設定も検出するため、まず`--no-checkout`で作成し、新worktreeの設定を取得してから固定SHAへresetする。hook/filter/merge driver/fsmonitor、自動maintenance/GC、submodule再帰を無効化する。準備用profileではconfig probeを含む各Git commandのleaderをunreapedのまま保持し、所有groupの子を停止してabsenceを確認してからWaitする。停止未確認はlaunching/予約を保持し、通常の失敗と同じ停止証拠に変換しない。

固定HEAD、branch、clean状態、同じcommon Git directoryと実git-dirを検証したhostの停止証拠から、readyのworkspace記録・work/fixの`workspace-write` binding・予約解放・不変historyを原子的に保存する。記録全体のchecksumでmutableな状態/証拠の破損も拒否する。scopeやcanonical入力が途中で変わればstale履歴だけを残し、writable bindingを作らず、確認済み停止の資源を解放する。未開始のcancelと開始済みの停止を別の証拠にし、model run/Task attempt/candidate/正式承認を作らない。

`wave product-workspace --root ROOT --id TASK_ID` は記録をread-onlyで確認する。失敗/unknownを同じTaskで自動再実行せず、停止確認と明示的な復旧または新Taskへ渡す。readyの完全一致再送も歴史的なreceiptを返すだけで、新しい実行許可ではない。CLIのownerは同期Gitと有界な停止証拠保存の終了までStoreを保持する。

検証: `go test ./internal/storage ./internal/product ./internal/kernelcli ./internal/workspace -run 'TestProductWorkspace|TestStoppedGit' -count=1`。一度開始・並行claim・再open/unknown保持、SQL失敗rollback、未開始/停止/取消/入力変化の履歴、proof破損拒否、実temp Gitの固定/private checkout、条件付きfilter/hookの非実行、既存data保持、子group停止、private CLI/閲覧/再送とmodel/candidate非生成を確認する。自動準備workerの配置、P Task実行/readiness、runner・candidate結果登録と正式gateは引き続き後続であり、準備だけで`product_readiness_unavailable`を外さない。

## 製品work/fix成果のschema v2

Pのwork/fix TaskでWorkItemを扱う場合は、共通session driverへ`product-task-result-v2.json`を選ぶ。K・汎用Task・独立review/verificationは既存のtask-result-v1を使う。選択は保存済みTaskとWave policyからcontrollerが行い、modelがschema番号を変えてroleを取得することはできない。v1のJSON/schemaは維持する。

v2の`product_changes`は、worktree準備後のTask専用manifestの`product_workspace.reference`（work_item_id・workspace_id・workspace_digest）をそのまま返す変更準備完了の申告である。固定base SHAとbranchも入力に保存する。parentやreview roleに実装Taskのworkspace参照を渡さず、外部messageで伝わった参照も別Taskの成果として受け付けない。元要求の登録が不明/破損なら新しい入力を作らない。

outcome=resultは参照一件、waiting/blockedは空のproduct_changesとする。work_item_refsは空、review/verificationはnullであり、modelは未知の新commit SHAやCandidate IDを申告しない。共通driverはraw finalを保持し、controllerが停止後に確定するCandidateへ書き換えない。criterionのclaimed/unverifiedと独立した品質・正式承認を区別する。

保存側はP work/fix roleと単一WorkItem割当、現在の登録、Task固有のworkspace ID/digest・scope/sessionを再照合する。不正な参照はmodel成果の不正として扱い、SQL/cancelは運用エラーのまま返す。保存済み証拠の欠落・破損は保留にし、Codexの再実行や成功へ変換しない。

この契約だけではmutableなworktreeをCandidateにせず、通常の`ApplyTaskResult`は`product candidate adoption requires the trusted host`として成果適用を保留する。停止後のsealingとhostによる採用は後述のportで行う。model runの許可や製品readinessの禁止も外さない。検証はcontract/worker/storage/controlのversion/role境界、既存v1 schema一致、SHA/自己review拒否、共通driverのraw final保持、Task専用入力・peer参照拒否、Kの自己分類拒否を対象とし、製品Candidate/Cloud受入の証拠には数えない。

## 準備完了時のwork/fix入力固定

単一WorkItemのP work/fixでは、workspaceのready証拠と不変historyを保存するtransaction内で、未開始Taskと現在plan bindingの入力digestを固定する。固定入力は準備開始時の元入力digest、workspaceの値・停止証拠・固定baseを含み、そのTaskが後で生成するCandidateのmutableなmembership更新には依存しない。準備中の入力変化は引き続きstaleにし、ready化しない。

元要求の登録・Wave scope/policy・Task/sessionの所属を再検査し、ready記録またはhistoryの欠落/破損時は可変Candidateへのfallbackを許さない。source result、Artifactの実bytes、依存Taskの成果は引き続き現在の証拠を検証する。K、未準備Task、review/verificationの入力契約は維持する。使用済みsnapshot/runを書き換えず、完全一致の準備完了再送も元receiptを返すだけである。

検証: `go test ./internal/storage -run 'TestFixedProductInput|TestProductWorkspace|TestProductChange' -count=1`。work/fixの入力・plan・manifest一致、Candidate更新と再open後の安定、reviewの従来挙動、scope/policy/登録/historyの拒否、Artifact/依存証拠の失効、plan更新失敗の全面rollbackを確認する。これは固定入力の保存接続であり、この保存自体はCandidate sealing、P実行readiness、品質/正式承認を許可しない。sealingと採用のhost portは後述する。

## 固定checkoutのcommit確定処理

`workspace.FreezePinned` は、hostが保存したrepository・CWD・branch・正確なlinked git-dir・common-dir・base SHAを入力とする同期Git処理である。callerには、workerと子processの停止証拠、Git/worktreeの排他予約が必要であり、この関数だけでTask実行・Candidate登録や公開を許可しない。

すべてのdirectoryが保存時のcanonical pathであること、元repositoryとcheckoutのcommon-dir、正確なgit-dir、branch、HEADが固定baseの子孫であることを、staging前に検査する。common-dirとbranchだけを模倣した`.git`の差し替えは拒否する。strict stopped-command profileで変更をstage/commitし、host指定identityと署名無効化、既存のhook/filter等の無効化を適用する。replace refを無効化し、legacy graftsは拒否して、実commitの親子関係を検査する。commit後も同じ所属・base ancestry・clean状態を再確認する。停止不明・取消・検査失敗には成功receiptを返さず、callerがintent/予約を保持して照合する。

検証: `go test ./internal/workspace -run 'TestFreezePinned|TestStoppedGit' -count=1`。実temp Gitの不変commitとclean再送、hook/filter/signing非実行、common-dir/branchを模倣した別git-dir、branch/path差し替え、無関係なbase、取消時のstage/commit非実行を確認する。製品Taskの停止済みrunとのDB照合とsealing intent/履歴、Candidate採用のhost portは後述する。この関数の成功をP全体の受入には数えない。

## 停止済みTaskからのlocal Candidate sealing

`product.CandidateSealer.Seal(ctx, taskID, runID)` は、trusted hostが停止済みrunの変更を固定する同期ownerである。現在Pの単一WorkItem work/fix、同じTask/session/target、正常終了した非baseline turn、runとsession双方の停止、未適用のactive runを要求する。pause/hold/取消/期限切れ/interruptionは開始を許可しない。

raw finalのschema v2とTask固有workspace参照、保存済みsnapshotの実bytes、run budget binding、snapshot内manifest参照とread grant、manifestのchecksum・Task入力・workspaceを照合する。現在の固定入力に加え、準備開始前の元入力も比較し、別Candidateに追い越されたcheckoutを新しい成果として確定しない。source/Artifact/依存成果の証拠検査を維持する。model finalをcommit SHAへ書き換えない。

`ReserveProductSeal` はこの不変入力とGit/作成先資源の予約を原子的に保存する。`ClaimProductSeal` は再照合した後、commit成功時だけ一度の開始を返す。launchingの再送/再openは開始を返さず、停止不明も予約を保持する。ownerはprivate directoryと保存済みGit証拠から`FreezePinned`を呼び、有界3秒の停止証拠保存をjoinする。callerはその終了前にStoreを閉じない。

`FinishProductSeal` は保存済みcheckoutと一致する停止証拠から、固定Candidate・receipt・不変history・予約解放を一transactionで保存する。途中で入力/出所が変わればstaleの歴史的commitとして保持する。失敗/not_startedはCandidateを作らず、使用済みintentから自動再試行しない。SQL/運用エラーはstale成功に変換せずrollbackし、commit済みでもreceipt保存に失敗した場合はlaunching/予約を残して明示照合を要求する。元receiptの再送はread-onlyで、失われたheadをhistoryから新規操作として再生成しない。

検証: `go test ./internal/storage ./internal/product -run 'TestProductSeal|TestCandidateSealer' -count=1`。正常/非baseline/物理停止/snapshot・manifest・元入力の境界、並行claim、再open、未開始/失敗、SQL rollback、stale history、実Git commit・raw final保持と保存失敗後の二重実行拒否を確認する。SQLite fixtureのmodel停止factsを使用しており、実model dispatchの受入には数えない。

このportはlocal commitの確定までである。active ProductTargetへの採用とTask成果適用は次節のhost portで行い、製品readiness、runner/独立review/正式承認への配置は引き続き後続である。`sealed`の歴史的receiptを、現在の実行・品質・公開の許可として使わない。


## sealed CandidateとTask成果の原子的な採用

`KernelStore.AdoptProductTaskResult(ctx, sessionID, runID, baseBranch)` はhost専用の採用portで、baseBranchはtrusted repository設定から渡す。model、Issue本文、dynamic toolからbaseを指定しない。通常の`ApplyTaskResult`は製品Candidateを自動採用せず保留する。

sealed receiptのchecksum・不変history・Git停止/予約解放・固定Candidateとcheckout証拠を再検査し、現在のTask/run/snapshot/元入力にも照合する。既存のcriterion割当・Artifact実bytes/許可等の成果検査を通ってから、ProductTargetと不変Candidate、controllerの`product_task_candidate` sidecar、Task/result/application、planの完了参照を同じtransactionで保存する。modelのraw finalとschema v2は書き換えない。SQL失敗はprojectionを含む全体をrollbackし、新しいmodel attemptを消費せず同じ停止済み成果から再試行できる。

新CandidateはWorkItemの`VERIFYING`へ置き、独立品質評価・正式承認・PR公開を生成しない。既存WorkItemの公開branch、PR番号、試行/修正回数、merge holdを維持し、古いcompletion/verdictの投影を引き継がない。WorkItemのpause/終端/scope不一致はworkspace準備・sealing・採用を禁止する。merge holdはmerge専用として保持し、local作業を妨げない。終端WorkItemを新scopeの作業へ再解釈することも許さない。

sidecarは元Task/run/result、入力・raw final digest、sealing参照と固定Candidateを結び付ける。完了成果の再利用と`TaskResultInput.product_candidate`は、sidecar・歴史的receipt/history・不変Candidate・正常適用済みrun・raw finalと保存済み構造化成果の一致を要求する。現在のmutableなCandidateへ古い成果を付け替えず、証拠欠落/破損時は完了成果として使わない。K/v1のJSONはoptional field未出力で維持する。

検証: `go test ./internal/storage ./internal/product -run 'TestProductCandidate|TestProductWorkItemPause|TestCandidateSealer' -count=1`。原子的な採用と再open/再送、base変更再送の拒否、sidecar/plan更新失敗rollback、history/元入力/出所保留、criterion/Artifact不正による非採用、証拠欠落と保存成果改変の拒否、既存PR/branch/merge hold/予算回数の保持、実temp Gitから固定SHAの採用を確認する。model停止factsはfixtureで、Pの実model dispatch・runner/独立review・Cloud/mergeの受入には数えない。製品実行readinessと品質/正式gateの配置は後続である。

## 独立評価Taskの固定Candidate入力

host専用`BindProductCandidateSubject(ctx, taskID)`は、単一WorkItemのP review/verification Taskについて、宣言済みの適用済み成果から固定Candidateを選ぶ。caller/modelがSHAを渡す入力はない。直接の依存Taskが出したCandidateを優先し、それがなければ明示的なSourceResultIDsから一意の対象を選ぶ。SourceResultIDsの文脈は直接依存の評価対象を置き換えない。全依存・元成果の証拠を検査し、未完了・別WorkItemしかない・複数の対象が混在する場合は固定しない。

初回は現在のactive Candidateと一致することを要求する。scope/policy/plan、登録とcriterionの範囲、未使用の独立Task/session、Wave/WorkItemの禁止を確認し、subject・不変history・Taskとplanの入力digestを原子保存する。依存成果の完成前に計画された未開始Taskの入力も、保存済みの成果から実行前に固定する。thread/run/attemptを作らず、read-only bindingを維持する。明示的に持ち越した旧planの成果は元の出所のまま参照でき、PR dispatch用のProductTargetを新planへ暗黙更新しない。

subjectにはCandidate、code/base/scope/policyから計算する`candidate_digest`、producerのsidecar参照と元入力digestを保存する。以後のTask入力はmutableなWorkItem.CandidateIDを使わず、この固定subjectを参照する。Source/Artifact/依存成果と登録の検査は継続し、head/historyやproducer/Candidate証拠の欠落・破損からmutableな値へfallbackしない。active Candidateが後で更新されても、既存Taskの評価対象を付け替えない。完全一致の再送は歴史的receiptの読取だけである。

Task専用manifestのoptional `product_subject`と、役割で絞った`product_subjects`へ保存する。parentは対象一覧を読めるが、Task専用の出力identityを持たない。reviewへの入力はschema v1と固定`candidate_digest`をreview.target_digestに使うことを指示する。subjectのdigestは入力参照、candidate_digestは評価対象を識別する別の値である。K・未固定Taskには新fieldを出力しない。

検証: `go test ./internal/storage -run '^TestProductSubject' -count=1`。review/verificationの固定入力とTask/plan/manifestの一致、Candidate更新・再open後の安定、head/history/producer/登録破損の拒否、保存失敗rollback、未使用sessionと宣言済み成果の境界、明示carryとSourceResultIDからの対象選択を確認する。model停止factsはfixtureである。この保存は実行・checkout・書込・runner成功・品質/正式承認の許可ではない。独立した固定checkout、verificationのテスト追加契約と正式runner、review成果の品質gateへの接続は後続であり、製品readinessの禁止を外さない。

## review専用の固定read-only checkout

固定subjectを持つP review Taskにも、既存のhost `WorkspacePreparer` / `prepare-product-task`を使える。未開始Task、現在のsubject/Candidate、登録と元producerのcanonical repository・数値IDを検査し、要求base SHAがsubjectのCandidate SHAと完全一致する場合だけ準備する。model/callerが別SHAや同じSHAを持つ別repositoryへ評価対象を切り替えることを許さない。

review用workspaceの値にはoptional subject ID/digestを固定し、一度だけの準備claim、privateな独立CWD/branch/git-dir、strict stopped Gitと証拠/history/資源解放のtransactionを共有する。ready化してもsession bindingは`read-only`で、work/fixの変更申告identityをmanifestへ出さない。Task/plan入力にも固定workspace証拠を追加し、subjectとcheckoutの両方を結び付ける。準備中にactive Candidateが更新された場合はstale履歴にし、元subjectを付け替えない。

検証: `go test ./internal/storage ./internal/product -run 'TestProductReview|TestProductWorkspace|TestFixedProductInput' -count=1`。subject参照/read-only/入力digest、SHA/repository/未固定role拒否、準備中のCandidate変更、証拠欠落、実temp Gitで実装担当とは異なるCWD/git-dirに固定commitと製品codeがあること、再送とproducer状態維持を確認する。model factsはfixtureであり、read-only sandbox下の実model reviewやP受入には数えない。

verificationのテスト追加は別の書込範囲/出力契約を必要とするため、このreview用checkoutへ自己分類しない。製品readiness、review成果の品質gate、verification/runnerへの接続は引き続き後続である。

## 独立検証の変更申告schema v3

Pの単一WorkItem verification Taskは専用`product-verification-task-result-v3.json`を選ぶ。K/汎用Taskとreviewはv1、製品work/fixはv2を維持し、modelがschema番号でroleを取得することはできない。元のv1/v2 JSON/schemaは変更しない。

outcome=resultは、Task固有のworkspace変更参照一件と`product_verification`（固定subject ID/digest、今回の成果に宣言した計画Artifact ID）を要求する。計画は既存`verification.Plan`として既知の入力Candidateに対して作成し、元要求・契約・criterionと期待値の根拠を記録する。新しいcommit SHA/Candidateやrunner成功、独立review/正式承認を自己申告しない。既存のreview/verification成功referenceはnull、WorkItemRefsは空とする。waiting/blockedでは変更参照を空にし、product_verificationをnullにする。

共通driverはv3を独立して検証し、raw finalの計画参照を保持する。controllerは固定Task/subject/workspaceとtrusted P verification roleを再照合し、他Taskの参照を拒否する。テスト書込範囲、正常終了・停止後のsealing、計画Artifactの内容/許可と最終Candidateへのrebindは後続の接続であり、この契約だけでは成果を適用しない。元の計画Artifact/raw finalを書き換えず、hostが既存の計画検証・rebindを使って正規化した証拠を別保存する。

検証: contract/worker/control/storageのv1/v2/v3境界、既存schema一致、subject/plan必須、SHA/自己review/runner evidence拒否、共通driverのraw final保持、trusted role選択、Kの自己分類拒否を確認する。verificationの実行・書込・sealing/plan採用が未接続の間は成果を保留し、製品readinessを外さない。

## host verification policyの固定

`RegisterProductVerificationPolicy(target, value)`は、Wave policy版と登録済みrepositoryごとに、canonical local repository、変更許可パス、`policy.Verification`の必須/登録profileを固定するhost専用portである。model toolや候補内configから登録しない。verification policyの版はWave policy版と一致させる。

許可パスはcanonicalな相対exact pathまたは末尾`/`のdirectory prefixとし、親参照・root・glob・Git metadata・重複を拒否する。profileは既存の必須集合/実行証拠検査に加え、最大64件、argv最大128要素、各文字列の上限、timeout最大24時間、policy全体1 MiBに制限する。pathは最大128件。正規化はcallerのmap/sliceを共有しない。

current headと不変historyを同じtransactionで保存し、完全一致の再送はread-onlyにする。同じpolicy版への異なる設定はconflictとし、head/historyの欠落・破損を新規pinとして上書きしない。scope/plan変更でpolicy版の内容を暗黙変更しない。保存失敗は両方rollbackする。

verification Taskのmanifestにはoptional `product_verification_policy`としてID/digest・許可パス・必須/利用可能profile名だけを渡す。完全な実行argvやprofile内部設定をmodel入力へ出さない。K、work/fix、review、parentにはこのTask用fieldを出力しない。

検証: `go test ./internal/storage -run '^TestProductVerificationPolicy' -count=1`。immutable pin/再open/再送、異なる設定・scope外repository・不正path/profile/版の拒否、callerからの分離、argv非公開、証拠欠落とhistory保存失敗rollbackを確認する。この設定は書込/実行/runner成功の許可ではなく、検証workspace・sealing/plan採用への接続は後続である。

## policyに結び付いたverification workspace

固定subjectに加え、同じWave policy版・数値repository・canonical local repositoryのverification policyをpin済みのP verification Taskは、hostのWorkspacePreparerで独立した作業領域を準備できる。subjectの入力Candidate SHAへcheckoutし、値にsubjectとpolicyのID/digestを固定する。準備/claim/停止証拠保存の各境界で照合し、policy欠落/破損・別repositoryは書込bindingへ移さない。

verificationのready証拠からworkspace-write binding、Task/planの固定入力、historyと予約解放を原子保存する。manifestはv3専用のworkspace変更参照とsubject、publicな変更許可パス/profile名を渡し、v2 work/fixやread-only reviewへ自己分類しない。入力のcheckout anchorは今回のsource Candidate SHA、最終Candidateの比較元はsource Candidate.BaseSHAとして別に保持する。旧work/fix記録のoptional未出力とdigestは維持する。

許可パスはtrusted policyで固定してmodelへ指示する。製品コード変更の拒否、テスト変更の停止後sealing・計画検証/採用は後続の範囲検査に接続するため、このworkspace準備だけで製品readinessやmodel turnを許可しない。runner成功・品質/正式承認も生成しない。

検証: `go test ./internal/storage ./internal/product -run 'TestProductVerificationWorkspace|TestProductVerificationPreparer' -count=1`。subject/policy/baseの固定、書込bindingとv3入力参照、policy不在/破損/別repository、証拠欠落後の拒否、実temp Gitの独立領域・固定HEAD・テストファイル追加と実装領域非変更を確認する。model factsとファイル書込はfixtureであり、実modelによる検証やP受入には数えない。

## verificationの差分・計画を検査するlocal sealing

`CandidateSealer`はschema v3の停止済みverification runにも対応する。通常のrun/snapshot/manifest/Task固有workspaceの照合に加え、固定subjectと現在Candidate、policyと変更許可パス、今回のTask/sessionが所有するcommittedな計画Artifactの実bytes/digestを検査する。計画は1 MiB以内で、元要求とWorkItemの全included criterion、入力Candidate、trusted policyのprofileに対して既存`verification.DecodePlan`を適用する。blocker/未検証criterion、未登録profileや対象不一致を残す成功申告は受け付けない。

一度だけのGit claim後、strict stopped-command profileで入力Candidate SHAからの差分をtrusted許可パスと照合し、計画のテスト参照がCWD内のregular fileであることを確認してからFreezePinnedへ進む。commit後にも同じ差分を検査し、計画参照が固定commitのtracked regular fileであることを確認する。差分/tree検査ではreplace refを無効化する。製品コード変更や存在しないテスト参照は成功receiptにせず、元の作業領域と失敗履歴を残す。

hostの停止証拠はsubject/policy/計画Artifactのdigestと実施した検査を含む。保存側は元入力を再照合し、新CandidateのBaseSHAに保存済みcomparison baseを使う。freshな場合だけ、元計画を最終Candidateに`verification.Rebind`した計画を別保存する。scopeはdigestで固定して再照合し、大きな元要求本文をsealing recordへ重複保存しない。途中で出所が変わった場合はstaleのcommit履歴にし、SQL/運用失敗はrollbackして照合用予約を保持する。元のArtifact bytesとraw finalは書き換えない。

検証: `go test ./internal/storage ./internal/product ./internal/verification -run 'TestProductVerificationSeal|TestProductSeal|TestCandidateSealer|TestCheckChanges|TestPlan' -count=1`。元計画・comparison base・正常停止proof・final plan rebind・再送、対象/profile/coverage/blocker拒否、実temp Gitのテスト追加commitと製品コード/欠落参照のcommit前拒否を確認する。modelと停止factsはfixtureで、正式runnerの実行成功やP受入には数えない。local sealingだけではactive ProductTarget/Task成功へ採用せず、v3成果/計画の原子的採用・runner品質gateへの配置は後続である。

## v3成果・Candidate・canonical planの原子的採用

host専用`AdoptProductVerificationTaskResult(sessionID, runID, baseBranch)`は、schema v3とverification role、freshなsealed receipt、元計画Artifact/subject/policy、現在の入力と出所を検査し、追加テスト込みCandidate・canonical plan・元のTask結果を一transactionで採用する。通常の`ApplyTaskResult`はhost採用を要求して保留し、work/fix用のhost profileはv3を採用しない。

canonical planは最終Candidateと元Artifact ID/digest、入力Candidate、policy/sealのID/digest、Task/run/result/targetに結び付け、head/historyへ原子保存する。`product_task_candidate` sidecarには小さなplan ID/digest参照を追加し、private実行argvは含めない。計画とsidecar、ProductTarget、不変Candidate、Task/result/application/planの完了参照は全部保存されるか全部rollbackする。WorkItemは引き続きVERIFYINGで、runner/独立review/正式承認の成功は生成しない。

`ReadProductVerificationPlan`は元のsealed canonical planとhistoryを検証する。v3の完了成果とSourceResultInputもplan参照を照合し、計画の欠落・破損を成功のまま使わない。後続Taskの固定subject選択は、実装時のCandidateではなく、明示依存の検証担当が採用した最終Candidateを参照する。元のraw finalと計画Artifactは不変で、v2 sidecarの新optional fieldは出力しない。

検証: `go test ./internal/storage ./internal/product -run 'TestProductVerificationAdoption|TestProductVerificationFinalCandidate|TestProductVerificationSealer' -count=1`。host境界、計画/Task/候補の一致、history/Task更新失敗rollback、完了証拠欠落、元結果維持、後続reviewの最終対象選択、実temp Gitからv3採用までを確認する。model/停止factsはfixtureで、runner実行・品質/正式承認やP受入には数えない。runner配置と品質gateへの接続は後続である。
