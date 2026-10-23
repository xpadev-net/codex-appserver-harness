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

`wave run --issue-pool private.json`はschema1・private same-user設定をruntime構築前に検査し、同じmain/上限/Storeのworkerをcontroller-owned lifecycleへ配置する。既存source Waveの再open・初回event再発行は行わず、commit後のhintだけを送る。shutdownは読取の終了までjoinする。既存Issueの方向修正・hold/close同期と製品Task/候補/公開は別の接続として残る。

検証: `go test ./internal/product ./internal/github ./internal/kernelcli -run 'TestIssuePool|TestIssueMetadata' -count=1`。32件ページとページ内位置の再open、4 MiBを超える旧形式collectionでもmetadataから高priorityの後方Issueを先に受付できること、選択した本文だけのGET、fresh labelのpriority再確認、完全/不完全なページ境界を確認する。既存の容量/rollback、過大応答の累積budget、未取得5件目の再開、deadline/cancel、private設定とdaemon lifecycleの回帰も維持する。fixtureと実GitHub/実modelを含むP全体の受入は区別する。
