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

## 変更された Issue 本文の明示採用

`AdoptProductSourceRequest` は trusted operator/source controller 専用の metadata port であり、model tool ではない。現在の scope/policy に適用済みの方向修正全文を、新しい Issue の `title + "\n\n" + body` と一致させる。保存済み host 観測の head/history、open/ready/hold、policy head/history、現在の後続系譜と元 Issue の退役を照合し、一致した要求 digest だけを採用する。対象 ID/digest を明示し、観測後に別の観測が入れば旧 CAS で採用しない。

一つの scope/policy 版につき head/history/admission を一 transaction で保存し、同じ要求は同じ記録を返す。その版の別本文を採用するには新たな方向修正が必要である。元要求、旧 PR、旧 scope/policy、旧入力、予算は変更しない。保存だけで source hold を解除せず、新しい host GET の成功後に新 plan/Task を採用する。Task 入力 digest、準備、manifest には採用 ID/digest と根拠の要約を結び付ける。後の scope/policy でも変更本文を使う場合は、その版の方向修正と観測で改めて明示採用する。過去の採用があるのに現在版の採用がない状態では、新 Task/plan の入力を保留し、source 観測を `request_changed` にする。元本文へ戻す変更も、同じ明示操作で採用する。

採用記録の head/history/admission、方向修正、元観測、policy、系譜を失えば原要求へ fallback せず保留する。source 観測は `state_invalid` を保存し、Task の入力・準備も保留する。記録がない旧形式の JSON/digest は変更しない。この port は Issue 本文を policy として実行せず、品質・正式承認・候補や新 model run を作らない。

実 Git/SQLite fixture は、元 Issue の merge と方向修正後、新本文の `request_changed`、明示採用、採用だけでは hold 維持、次の4 GETで ready、新 plan の新 Task 入力、途中失敗の全 rollback、exact replay、各証拠の喪失・復旧、reopen を確認する。親 tool/CLI による呼出しと、後続の実候補・正式 GitHub gate の受入は別途必要である。

次 scope の検査では、旧採用を保持した通常の方向修正と registration CAS の後、暗黙の原要求への fallback による plan 採用を拒否し、新 scope の観測・明示採用・ready を再度通す。追加モデルや外部書込みは行わない。

## Operator CLI

保存済みの観測と coverage はモデル・GitHub 呼出しなしで読める。

```sh
./harness wave product-source --root /absolute/private/kernel --id WAVE_ID
./harness wave product-coverage --root /absolute/private/kernel --id WAVE_ID
```

`product-source` は GitHub Issue の最新保存観測（ID/digest、対象 scope/policy、status、要求 digest）を返す。direct source や未観測では観測を生成しない。`product-coverage` は現在版の merged/historical/retired/residual と元 merge ID/receipt digest を返す。いずれも読取時点の事実であり、後の操作は改めて現在版・証拠を照合する。

```sh
./harness wave continue-product --root /absolute/private/kernel --file continuation.json
./harness wave adopt-product-source --root /absolute/private/kernel --file source-adoption.json
```

両ファイルは所有者だけが読める通常ファイル（例: `chmod 600`）、1 MiB 以下とし、未知 field・余分な CLI flag は拒否する。continuation.json は `ProductContinuationRequest` の JSON（`target`、`key`、`parent_merge_id`、`parent_receipt_digest`、`definition`）。definition は新しい `id`、元と同じ `repository` / `repository_id`、現在版の `criteria` を持つ。source-adoption.json は `ProductSourceRequestAdoptionRequest`（`target`、`direction_id`、`observation_id`、`observation_digest`、新しい `work_item_id`）。target は `wave_id` / `scope_revision` / `plan_revision` / `policy_revision` を全て明示する。値は実際の `wave inspect`、source、coverage、方向修正や登録の返答から取得する。

最初に通常の `wave steer --file` で新 scope を適用し、停止境界の完了を確認する。元 merge と現在 target を明示して `continue-product` を行う。本文変更がある場合は方向修正全文を新しい Issue の title/本文と一致させ、source observer が保存した `request_changed` 観測を指定して `adopt-product-source` を行う。次の正しい source 観測で ready となった後、親が新 plan/Task を採用する。CLI 自体は model/Git/外部書込みを起動せず、保存後に既存 daemon へ best-effort hint を送る。操作は同じ Go port の再送・rollback・source/merge 証拠 gate を使う。

CLI の SQLite 検査は private/未知 field/不正 flag/必須ファイル、未観測 merge・未適用 source の拒否、正しい保存済み coverage 読取、Wave/Task/run/tool operation の非変更を確認する。成立する merge/方向修正/採用・再送の検査は前節の Go port 統合で確認している。CLI の検査だけで後続候補や native P の受入を主張しない。これらは operator 専用であり、model の本文を trusted source 権限へ変換する tool は公開しない。

## 新しい固定 base からの物理候補

`publication_http_versioned_work` の統合 fixture は、ローカル repository を別の remote fixture へ clone し、元候補を通常の `merge --no-ff` で取り込む。実在する二親 merge SHA を HTTP 応答の fixture と authenticated receipt に使い、後続系譜の親 merge SHA と一致させる。存在しない仮 SHA を fetch 成功として扱わない。

通常の方向修正・後続登録・4 GET の source 再確認後、別 ID の plan/Task/session を作る。production `BasePlanner` と source gate、固定 commit の実 fetch/checkout、停止した writer fixture、host の seal/adoption を通し、新候補の base が元 merge SHA、新候補 SHA と WorkItem/PR が元と異なることを確認する。元の登録・系譜・PR、Wave の期限と累積予算を保持する。

新候補は `VERIFYING` で、元の品質による公開を拒否する。新 verifier は別 session/checkout に新候補を固定し、元の検証計画を継承しない。独立検証モデルの実行・新 runner/review・新 PR/正式 merge・親受入はこの単位では完了させない。model の run/stop、Issue GET、正式承認/CI/rules、remote HTTP 応答は明示的な fixture であり、実 Git/SQLite の検査を native P の受入へ読み替えない。

## policy revision の明示切替

operator の wave steer 用 JSON に任意の next_policy_revision を指定できる。省略は従来どおり、指定する場合は現在版 + 1 だけを許可する。direction の現在 target と同じ CAS を使い、旧 process の停止と結果適用の安全境界を待って scope と policy revision を進める。巻戻し・飛越し・overflow は拒否する。

変更するのは policy の版だけで、stage/product、権限、上限 snapshot、期限・累積予算を保持する。実際の source/検証設定は trusted host 設定から読込み、新版に別の immutable pin を作る。Issue 本文や model に policy の指定権を渡さず、旧 pin/結果/Task を新版へ書き換えない。新しい plan/Task/session と、merge 済み作業からの新 WorkItem が必要である。変更本文を採用済みなら、新 scope/policy にもその版の明示 source 採用を要求する。

SQLite では、次版以外の拒否、旧 JSON の任意項目省略、stop/result 適用待ち、旧 Task/result の保持、権限・予算・期限の保持、replay/reopen を確認する。product の物理 Git fixture は、policy 1 の merge→通常の direction で policy 2→新 source pin/後続→変更した host 検証設定の独立 pin→実 base 取得/新候補/新 verifier 準備を通し、policy 1 の検証設定が同一 digest のまま残ることを照合する。profile/run/正式 gate は fixture であり、新品質・native P の合格には数えない。
