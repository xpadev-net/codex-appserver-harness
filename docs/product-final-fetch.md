# 固定した最終 HEAD の取得

`FinalRunner.FetchRemotes` に repository ID と信頼されたリモートの対応を渡すと、最終計画が選んだ commit を host が取得してから正式 runner を開始する。モデルの tool や Issue の値から URL/refspec を作らない。計画の source/capability gate と GitHub の repository/default HEAD/merge ancestry を開始時と送信直前に観測し、取得後にも現在の計画を再確認する。

取得は通常の `git` capacity を使い、同じ common Git directory と作業ツリーを他の所有者から排他する。保存する値は immutable plan/digest、対象 scope、repository/policy に対応するローカルパス、canonical Git metadata、固定 SHA、明示した transport と実行権である。`prepared` から一度だけ claim し、実際の fetch 前に durable send fence を一度だけ消費する。pause、古い HEAD/観測、失われた lease は新しい送信を許可しない。同じ HEAD を再観測した場合は、未着手の準備だけを現在の観測で開始できる。

host の Git は固定 SHA の object だけを明示した credential-free HTTPS URL（offline fixture は canonical absolute repository path）から取り込み、URL rewrite、partial clone の暗黙 fetch、hooks、外部 filter/merge driver、再帰 submodule と自動 maintenance を抑止する。通常 branch、checkout、`FETCH_HEAD` を更新しない。runner の private clone にはその SHA を固定ローカル repository から file transport だけで明示的に取り込み、checkout/clean/SHA を検査する。これにより、branch を動かさずに取得した object も正式 runner に届く。

同期 Git 所有者は子 process group を join してから停止証拠を保存する。停止未確認では lease を保持する。claim/send/terminal/history の欠落は証拠喪失として再実行を保留する。再openや再呼出しは unknown な command を再開しない。停止が確認できた結果は、cancel や HEAD 変更後も元の操作の履歴として保存し、同じ証拠の replay は副作用を繰り返さない。失敗した取得の自動 retry はこの port では許可しない。

この変更は固定 commit の取得と `FinalRunner.Start` への接続である。daemon の最終工程 dispatch、親結果適用直前の source/HEAD 照合、正式 Cloud APPROVED と P 全体受入は後続へ残る。取得できたことはテスト成功・公開・merge・Wave 完了の証拠ではない。

検査:

- `TestFetchPinnedCommitImportsExactObjectWithoutRefsOrCheckoutChanges` / `TestFetchPinnedCommitRejectsChangedAuthorityAndMetadata`: 一時 Git で未知 SHA の実取得、origin 非使用、ref/HEAD/private file/FETCH_HEAD 不変、URL 書換え・遅い remote/pin 変更・資格情報付き URL・不正 object の拒否。
- `TestProductFinalFetch*`: 一度限りの claim/send、共有 Git 排他、source 失効、直前の fence、遅い停止結果/reopen/replay、欠落証拠の再生成拒否、transaction rollback。これらの停止証拠と GitHub 観測は fixture である。
- `TestProductVerificationSealerChecksRealDiffAndTrackedPlanBeforeReceipt/publication_http_worker_alias/fetched_final_runner`: 合成 GitHub merge/ancestry 観測と実 temp Git を組み合わせ、元 repository にない commit を実取得し、その同じ SHA の正式 profile を実行して実停止を記録する。host checkout が動かず、完了 replay が fetch/runner を繰り返さない。native Cloud/P の実接続には数えない。
- `TestUnconfirmedGitStopDuringCancelNeverPublishesTerminalResult/fetch`: runner の local import でも停止未確認を terminal result に変換しない。
