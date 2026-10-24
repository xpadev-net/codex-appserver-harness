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
