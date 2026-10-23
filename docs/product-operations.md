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

`PreparePRDispatch(operationID, ownerID)` はcontroller用の保存portで、HTTP送信を行わない。最新のWave/Task/WorkItem projection、scope/候補digest、pause/cancel/方向修正/期限を照合し、executorの一意claim、PRまたはCloudの枠、同じWorkItemのPR書き込み排他を一つのtransactionで取得する。PRの存続枠は複数plan版・更新操作で共有し、書き込み排他は操作ごとに取得する。Cloud開始には既存PRの枠が必要で、同一candidateのreview予算はrepository/PR番号/SHAに結び付ける。

実P adapterはcanonical WorkItemと検証/configのgateを確認し、その最後の`BeforeWrite`で`MarkPRMayHaveSent`を呼ぶ必要がある。このportが保存した最新projectionと優先操作・claim/leaseをもう一度照合し、preparedを一度だけunknownへ変更する。変更後にHTTP送信へ進む。呼び出し直後・実HTTP前にcrashしても、再起動はunknownを保持し、読取専用の外部照合なしで再送しない。この変更自体は検証成功・Cloud承認の証拠ではない。

`CancelUnsentPRDispatch`は未取得またはpreparedの操作だけを取消できる。遅延した送信callbackをfenceし、新規PRの未送信枠・Cloud枠と書き込みleaseを原子的に解放する。既存のopen PRの存続枠は更新を取消しても保持する。先に外部close/mergeを確認して枠を解放した場合も、未送信claimのcleanupは可能である。unknownの取消・枠解放は拒否する。同一owner/reasonの取消再送は同じ監査結果となり、監査保存失敗は全解放をrollbackする。

この分割はstorage状態機械だけを追加する。実HTTP adapter、canonical sourceの直前guard、marker照合、resource binding/通知、native P tool公開は後続であり、現在のruntimeは外部送信を開始しない。SQLite BUSY/LOCKEDはdomain holdに変換せず返し、callerがrollback後の操作全体を再試行する。未知HTTP結果の再送と混同しない。

追加検証: `go test ./internal/storage -run 'TestPRDispatch|TestPRBeforeSend|TestPRClaim|TestPRPrepared|TestPRReviewClaim|TestPRMark|TestConcurrentPRClaim|TestPRUnsent' -count=1`。別接続でのclaim競合と再試行、送信前の優先操作、prepared/unknownの一度消費、再起動・cancel後のunknown保持、同じPRの排他と存続枠、Cloudの別枠/一度予算、既知未送信の取消、保存失敗rollbackを確認する。
