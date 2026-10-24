# merge 済み作業からの版付き後続登録

`RegisterProductContinuation` は信頼された source controller が、同じ Wave の新 scope/policy 版に新しい WorkItem を登録する Go/SQLite port である。Issue 本文や model の指示は policy の権限を持たず、この port は model tool として公開しない。

元作業は、登録された HTTP owner の実停止を含む authenticated merge receipt と、実際に MERGED へ更新された元 projection を必要とする。元 receipt の digest、元 registration/target、candidate SHA、merge SHA、projection digest を保存する。現在の projection が元 receipt と異なる場合は保留する。新しい WorkItem ID を使い、同じ repository/Wave/source の新 scope または新 policy revision を必要とする。古い PR、候補、WorkItem、scope/policy の履歴を変更せず、新作業へ品質・正式承認を継承しない。

新しい registration/membership/version と、lineage の head/history/admission/request-key を一 transaction で保存する。Wave 内の key は別の後続 ID を作る再送に流用できず、同時要求でも一つだけ登録する。完全一致の replay は元の保存結果を返す。head/history/request-key/registration version/元 merge 証拠の欠落は再構成せず保留する。pause/cancel、方向修正待ち、期限は新規登録を止める。source hold 中は、認証した元 merge と新版に対応する metadata の継続登録だけを許可し、hold は解除しない。通常の新規登録と model/製品操作は引き続き保留する。

この登録は Task/session、候補、model/Git/runner/外部操作を作らず、Wave の期限や累積予算を変更しない。読取は現在の registration の所属・source・後退しない版と、その不変 version、保存時の元 registration version、元 merge 証拠を再照合する。正当な次版への metadata 更新後も、元の系譜は同じ監査記録として読める。これは旧登録・候補・品質を現在の実行へ流用する許可ではない。旧形式の型・JSON/checksum は変更しない。

実 SQLite の検査は、既知 merge→親 run の既存 turn を保持した停止・監査 archive→通常の方向修正による新版→新規登録、replay/reopen、元 PR/projection と予算の保持、同じ版・別 repo・不正条件・変更された元 projection・pause の拒否、保存途中の rollback、証拠喪失、同時 request-key 競合を確認する。HTTP/model の事実は fixture であり native P の受入ではない。

source 登録と読取に加え、後述の Task 入力・workspace admission を接続している。旧 WorkItem を historical とする coverage の切替、親 tool/CLI、GitHub source 改訂の信頼された採用は後続で接続する。登録だけで全範囲 merge 後の製品 Wave を復旧済みとは判定しない。動的な Issue 分割・子 Issue 作成とも区別する。

## Task 入力・workspace 準備への接続

新版 Task の入力 digest は、現在の registration に加えて保存系譜の ID/digest を含む。元作業自体が後続であれば、その元作業の系譜も最大128件の有界な集合として照合する。現在の Task は現在版の registration を必要とし、過去 node は保存された不変 version と元 merge の監査事実として扱う。古い node の存在だけで現在版の入力を許可しない。

親・担当 Task の manifest には、work/parent ID、元 target、merge/receipt ID と digest、固定 SHA の要約を含める。private checkout、raw proof、旧品質・正式承認を渡さない。系譜の証拠欠落は unavailable/hold として扱い、旧形式に fallback して実行しない。

後続 work/fix の workspace reserve/claim は、系譜を入力に結び付けた固定 base 計画を必須とする。base の ancestry 必須集合には各元作業の merge SHA を加える。古い host 初期 SHA だけを渡すと Git owner を予約せず保留する。base 計画を準備済みでも、claim 前に系譜を失えば開始しない。検証・review は通常の固定候補と独立品質 gate を使う。

実 SQLite の検査は、新 plan の新 Task/session、manifest と入力 digest の対応、安全な要約、証拠欠落時の準備/scheduler 保留と復旧、base 計画なしの reserve 拒否、元 merge SHA の ancestry 指定、計画後の証拠喪失による claim 拒否、過去 node の監査と現在入力の区別を確認する。base 観測・物理取得の事実は trusted fixture であり、実 GitHub の新 HEAD/後続実装や native P の受入には含めない。coverage の旧版切替と CLI/tool/source 改訂の採用は引き続き別に接続する。
