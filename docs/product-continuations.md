# merge 済み作業からの版付き後続登録

`RegisterProductContinuation` は信頼された source controller が、同じ Wave の新 scope/policy 版に新しい WorkItem を登録する Go/SQLite port である。Issue 本文や model の指示は policy の権限を持たず、この port は model tool として公開しない。

元作業は、登録された HTTP owner の実停止を含む authenticated merge receipt と、実際に MERGED へ更新された元 projection を必要とする。元 receipt の digest、元 registration/target、candidate SHA、merge SHA、projection digest を保存する。現在の projection が元 receipt と異なる場合は保留する。新しい WorkItem ID を使い、同じ repository/Wave/source の新 scope または新 policy revision を必要とする。古い PR、候補、WorkItem、scope/policy の履歴を変更せず、新作業へ品質・正式承認を継承しない。

新しい registration/membership/version と、lineage の head/history/admission/request-key を一 transaction で保存する。Wave 内の key は別の後続 ID を作る再送に流用できず、同時要求でも一つだけ登録する。完全一致の replay は元の保存結果を返す。head/history/request-key/registration version/元 merge 証拠の欠落は再構成せず保留する。pause/cancel、方向修正待ち、期限は新規登録を止める。source hold 中は、認証した元 merge と新版に対応する metadata の継続登録だけを許可し、hold は解除しない。通常の新規登録と model/製品操作は引き続き保留する。

この登録は Task/session、候補、model/Git/runner/外部操作を作らず、Wave の期限や累積予算を変更しない。読取は現在の registration の所属・source・後退しない版と、その不変 version、保存時の元 registration version、元 merge 証拠を再照合する。正当な次版への metadata 更新後も、元の系譜は同じ監査記録として読める。これは旧登録・候補・品質を現在の実行へ流用する許可ではない。旧形式の型・JSON/checksum は変更しない。

実 SQLite の検査は、既知 merge→親 run の既存 turn を保持した停止・監査 archive→通常の方向修正による新版→新規登録、replay/reopen、元 PR/projection と予算の保持、同じ版・別 repo・不正条件・変更された元 projection・pause の拒否、保存途中の rollback、証拠喪失、同時 request-key 競合を確認する。HTTP/model の事実は fixture であり native P の受入ではない。

source 登録と読取に加え、後述の Task 入力・workspace admission を接続している。旧 WorkItem を historical とする coverage の切替は後述する。親 tool/CLI、GitHub source 改訂の信頼された採用は後続で接続する。登録だけで全範囲 merge 後の製品 Wave を復旧済みとは判定しない。動的な Issue 分割・子 Issue 作成とも区別する。

## Task 入力・workspace 準備への接続

新版 Task の入力 digest は、現在の registration に加えて保存系譜の ID/digest を含む。元作業自体が後続であれば、その元作業の系譜も最大128件の有界な集合として照合する。現在の Task は現在版の registration を必要とし、過去 node は保存された不変 version と元 merge の監査事実として扱う。古い node の存在だけで現在版の入力を許可しない。

親・担当 Task の manifest には、work/parent ID、元 target、merge/receipt ID と digest、固定 SHA の要約を含める。private checkout、raw proof、旧品質・正式承認を渡さない。系譜の証拠欠落は unavailable/hold として扱い、旧形式に fallback して実行しない。

後続 work/fix の workspace reserve/claim は、系譜を入力に結び付けた固定 base 計画を必須とする。base の ancestry 必須集合には各元作業の merge SHA を加える。古い host 初期 SHA だけを渡すと Git owner を予約せず保留する。base 計画を準備済みでも、claim 前に系譜を失えば開始しない。検証・review は通常の固定候補と独立品質 gate を使う。

実 SQLite の検査は、新 plan の新 Task/session、manifest と入力 digest の対応、安全な要約、証拠欠落時の準備/scheduler 保留と復旧、base 計画なしの reserve 拒否、元 merge SHA の ancestry 指定、計画後の証拠喪失による claim 拒否、過去 node の監査と現在入力の区別を確認する。base 観測・物理取得の事実は trusted fixture であり、実 GitHub の新 HEAD/後続実装や native P の受入には含めない。coverage の旧版切替は次節、CLI/tool/source 改訂の採用は引き続き別に接続する。

## 現在 coverage と旧版の退役

coverage は現在版の登録から全系譜を照合し、元の登録 digest が残る旧版 WorkItem だけを `retired` に列挙する。各退役は現在の後続 WorkItem、根拠となる continuation ID/digest、元登録 digest を持つ。旧 merge は元 target/receipt/SHA のまま `historical` に残り、現在の `merged` や criterion の達成に加算しない。新 WorkItem は自分自身の候補・独立品質・正式 gate・merge が必要である。

旧登録自体が現在版へ更新された場合は、新たな現在の義務として残し、系譜を根拠に消さない。元登録 head、どちらかの不変 registration version、系譜の head/history/admission/request-key、元 merge receipt/history/projection の欠落は退役を許可せず、旧作業を unavailable/unmerged、criterion を residual として保留する。系譜がない旧版も自動退役しない。読取と再起動は Wave、予算、元 PR、保存済み入力を変更しない。後続のない既存 JSON では任意の `retired` を省略する。

実 SQLite fixture で、通常の方向修正→後続登録→退役、元 merge の履歴保持、親 manifest、残作業・予算・旧 projection の保持、再起動、各証拠の喪失・復旧、旧登録を現在版へ更新した場合の退役拒否を確認する。これも metadata の component 検査であり、新候補の実 GitHub merge、source 改訂採用、native P の受入を意味しない。

## GitHub source の現在版確認

元 Issue の WorkItem 登録が旧版でも、現在の後続系譜がその登録を認証付きで退役させていれば、source の現在版への所属確認に使える。元登録を現在版へ書き換えない。`ObserveProductSource` は従来どおり host の再読取、repository/Issue、open/ready/hold、元要求 digest、policy pin を照合する。continuation 登録自体は source hold を解除せず、次の正しい source 観測だけが解除する。系譜を失えば `scope_unconfirmed` に戻る。

実 Git・SQLite の統合 fixture は、最初から GitHub Issue として採用した Wave の候補→独立検証/runner/review→登録 HTTP owner の停止→merge receipt→親の停止・archive→通常の方向修正→新 WorkItem 登録→4回の GET bookend による source 確認を通す。元の PR/登録を保持し、新作業を unmerged/residual とする。Issue close、要求変更、系譜の喪失・復旧でも各 read の gate を照合する。正式承認・CI/rules・リモート merge・Issue GET は fixture であり、後続の実候補や native P を合格にしない。変更された Issue 本文の採用は別の明示操作が必要であり、この接続では `request_changed` を維持する。
