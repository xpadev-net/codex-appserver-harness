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
