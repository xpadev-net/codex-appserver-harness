# Kernel Wave の動的分割

分割の深さ・子数は新規 Wave の WaveOperationLimits に固定する。既定値は split_depth=2 / split_children=8 で、trusted WaveRequest.OperationLimits で明示した値だけを新規受付時に採用する。範囲は深さ 1..128、子数 2..128。全体の WorkItem bound も128であり、上限値がその数の作成や実行を許可する意味ではない。

省略された旧保存記録の 0/0 は分割無効で、新しい既定値を補充しない。片側だけ 0、負数、子数1、上限超過は拒否する。policy/scope の方向修正、受付再送、再起動は lifetime snapshot を変えない。Task/Phase/main/token 等の既存の Wave 予算とも別に照合する必要がある。

この単位は kernel に不足していた上限 snapshot の接続である。分割 proposal の controller 採用、criterion と各 part の lossless mapping、子 WorkItem/Issue の作成・系譜・冪等 outbox、coverage の切替、親 gateway は後続で接続する。子の Wave を Task から作る許可や、旧 DB の自動 migration ではない。

SQLite 検査は、新規 snapshot、方向修正後の保持、旧形式の読取と JSON 省略、旧 source 再送と reopen 時の非補充、不正値の受付 transaction 全 rollback を確認する。外部 Issue/実 model の受入は含めない。
