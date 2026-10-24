# P 最終検証の daemon 接続

Go の product executor で publication/merge を明示設定すると、`FinalVerificationWorker` が現在の P Wave を有界ページで走査する。完全な merged coverage、現在の host verification policy、source と capability report を確認し、GitHub で default HEAD と全 merge receipt の ancestry を観測して固定計画を選ぶ。未着手の正式 profile を共有 runner の予算・lease で起動し、既存 execution worker が停止結果を回収する。merge 未設定では最終受入を保留する。

取得先は host の repository 設定と一致する GitHub repository metadata の clone URL から解決する。固定 SHA の取得と runner の clone は既存の送信 fence・停止証明を使う。unknown な起動や停止を再実行せず、破損した Wave は診断を残して後続ページを進める。

現在の最終 profile がすべて停止したら、成功・失敗のいずれも既存の親通知経路に digest 付き入力を一度だけ保存する。この処理は model を呼ばず、Wave を完了しない。同じ結果の再走査は通知を追加せず、現在の計画や結果が変わった通知は親 wake の直前に保留する。

親の normally stopped MainResult に P accept がある場合、`ApplyMainResult` は実行中 daemon に登録した gate で source・host policy・capabilities・default HEAD/ancestry を再観測する。DB transaction の外で外部読取を行い、その後、同じ runtime 登録を保持したまま通常の原子的受入を行う。保存済み親入力の coverage/plan/evidence digest と現在の完全な検証結果、pause・deadline・予算・残存操作も既存 gate で照合する。HEAD が進んだ場合は古い pass で完了しない。

登録は controller lock の取得後に行い、全 worker の終了後に解除する。DB の reopen は callback を復元しない。別 owner への交代・取消し・未設定は新しい受入を保留し、適用済み結果の replay は保存済み結果を返す。完了証拠には実 runtime の epoch/generation/config digest と実 session/run/output/input snapshot の対応を監査用に保存する。

検査は以下の component evidence である。native Cloud の能力を証明したものではなく、P 全体と #20 は未達のままとする。

- 実 temp Git/SQLite/runner の既存 publication fixture で、新しい commit の観測・実取得・同じ SHA の正式 profile 実行・停止回収・親通知・重複防止を検査する。GitHub の merge/ancestry/capability 観測は fixture。
- 現在の host policy と merged projection の照合、各必要 capability の unknown、および 現在の source の保留による保留を検査する。
- 実保存済み親結果で gate 保留・途中の pause・登録取消し・reopen を検査し、成功時の監査対応と replay を確認する。
- 成功/失敗の通知は model turn を増やさず、現在の選択喪失で wake を保留する。破損 Wave に続く有効 Wave をページ走査できる。
- daemon がバックグラウンド処理の join 中も登録を保持し、停止後に解放することを検査する。
