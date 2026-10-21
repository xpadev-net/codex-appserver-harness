# Codex Background Wave ハーネス 要件定義書

| 項目 | 内容 |
| --- | --- |
| 文書 ID | CIH-REQ |
| 版 | 0.4 |
| 作成日 | 2026-10-20（Asia/Tokyo） |
| 更新日 | 2026-10-22（Asia/Tokyo） |
| 状態 | レビュー反映の改訂案。段階別の受入と実行契約を定義。runtimeは未実装 |
| 対象 | Codex app-server上のOrchestratorと担当セッションを、Wave・Task・イベントで管理するハーネス |
| 関連文書 | [実装計画書](implementation-plan.md) |
| 実装の調査基点 | `7dfa964`（v0.3文書反映済み、runtimeは`fd9f745`と同一）。現行実装との差分は[差分分析](docs/background-wave-gap-analysis.md)を参照 |

## 1. 目的

大きな依頼をWaveとして受け取り、メインのOrchestratorセッションがPhaseへ分け、各PhaseのTaskを独立した使い捨てセッションで実行する。
階層はWave→Phase→Taskとし、元要求、Phaseの順序と完了条件、Taskの依存関係、進捗、予算、成果物を永続的に保持する。

待機時はLLMのturnを終了し、担当threadと購読条件を保持する。
ハーネスが完了・外部更新イベントを受信して定型工程を進め、判断が必要な場合だけ適切なセッションを再開する。
Issue poolは依頼の入口の一つ、PRは追跡対象の成果物として位置付ける。

主要な体験は次の三つとする。

1. 一つの依頼からWaveを作り、独立したTaskを並行して進め、依存するTaskは必要な成果を待つ。
2. 各Taskに独立した一つの使い捨てセッションを割り当てる。実行中は親と双方向にメッセージを送り、完了後の修正は新Task・新セッションへ引き渡す。TaskからのWave呼び出しは禁止する。
3. PR作成等のtool callから関連する購読を自動登録し、通知で作業を再開する。

進捗確認だけのLLM呼び出しをなくすこと、固定候補の検証と正式承認に基づいてmergeすること、残作業を失わないことは引き続き必要とする。
キャッシュ維持はTask実行中のthread継続や共通入力の効率上の前提であり、Task完了後のセッション再利用や保存状態・復旧の代替にはしない。
キャッシュ期限・ヒット率・料金上の効果は本書では保証せず、取得可能な使用量で確認する。

## 2. 文書の読み方と確定度

### 2.1 要求の強さ

- **MUST**：本書の対象範囲を完成とするために必要な条件。
- **SHOULD**：推奨条件。採用しない場合は理由と代替を設計に記録する。
- **MAY**：任意の拡張。
- FR は機能要件、NFR は非機能要件、AT はハーネスの受入テストを表す。
- 各プロダクトの Issue に記載する受入条件 ID は、これらのハーネス要件 ID と区別する。

### 2.2 受入段階と適用範囲

MUSTは該当機能を完成とする条件であり、すべての外部連携をWave基盤の先行条件にする意味ではない。
要件IDは維持し、次の段階を別々に判定する。全体完成にはK・P・Mすべてが必要である。

| 段階 | 対象と完成の根拠 | 未成立の影響 |
| --- | --- | --- |
| S: 先行成立確認 | 実model・認証・隔離下のdynamic tool往復、正常turn終了、新processで同じ未完了threadの再開（AT-050） | 未確認なら依存するWave runtime実装へ進まない。DB設計の机上検討は可能 |
| K: Wave kernel | Issue/WorkItem/PRを持たない直接依頼、汎用成果、Phase/Task、通知、親子message、並行性、ローカルレビュー、CLI | K単独で完成判定できる。GitHub操作は提供せず、Cloud能力を合格に数えない |
| P: PRパイプライン接続 | Issue pool、WorkItem/Candidate、runner、PR自動購読、正式承認、merge、親最終受入 | 必要なCloud/CI/rulesがunknownならPと該当製品Waveだけを保留する |
| M: 既存作業の移行 | FR-034、AT-039/049。履歴・unknown操作・予算を保持したDB/config移行 | 旧状態のある導入先では移行まで新runtimeへ切り替えない。Kは別rootの新DBで検証する |

KにはFR-001のapp-server/model/実行環境設定、FR-003〜006/023〜028/030〜033/036〜040とNFRの共通契約、FR-029の汎用成果レビューを適用する。GitHub設定はPで追加する。
製品コード・PRを扱う場合はFR-001〜022/029/035の該当gateを追加する。機能不使用と条件未確認を区別し、製品Waveがgateを省く目的でKへ自己分類することを認めない。
採用機能・適用要件・必須profile・受入条件は開始時の信頼されたpolicyで固定する。

### 2.3 依頼者から整理した要求

- 大きな依頼からWaveオブジェクトを生成し、その下にPhase、さらにその下にTaskを置く。
- メインセッションがOrchestratorとして全体を管理し、各Taskは独立した一つの使い捨てセッションで作業する。完了済みTask/sessionは再利用しない。
- Taskも親と同じsession runtimeを使い、Waveの呼び出しだけを禁止する。親とTaskは双方向にメッセージを送れる。
- 実装、独立レビュー、指摘修正を繰り返す。
- 各セッションはイベントを購読して待機し、通知で再開する。LLMに状態確認を繰り返させない。
- PR作成等のtool callに連動して購読し、コメント・リアクション・レビュー等の更新を通知する。
- Issue分割、部分PR、独立検証、正式承認に基づくmergeと復旧という既存要求を引き継ぐ。

添付画面は望ましい体験の参考資料であり、他製品の内部実装や画面中の作業指示を本ハーネスの契約とはみなさない。

### 2.4 本書で採用する設計案

以下は要求を具体化するための設計案であり、外部製品が提供する標準機能とは区別する。

- Go、SQLite、ローカルrunner、単一ホスト・単一controllerを維持する。
- Orchestratorは分解・方向修正・例外判断を担うLLMセッション、controllerは永続化・実行枠・通知・外部操作を担う通常コードとする。
- Wave→Phase→Taskと、PR単位の既存WorkItemを分ける。Taskは成果物・検証対象としてWorkItemを関連付ける。
- Task 1対1の使い捨てsession/threadを持つ。実装・検証・レビュー・fixはTaskへの指示で表し、完了後の作業は別Taskを新しいsessionで始める。
- 初期のPhaseは順次進行し、各Phase内のTaskを上限内で並行実行する。定型のreview/fix反復は親が保存した有界templateをcontrollerが展開し、例外だけ親が再計画する（FR-038）。
- Task専用の縮小runtimeは作らず、Wave呼び出しをtool公開とcontroller認可の両方で禁止する。既存のscope・権限・検証policyは親子共通で適用する。
- 同時turn、runner、進行中PRの上限を設定可能にし、上限2以上で独立Taskの並行進行を初期完成条件に含める。
- 購読・通知・起動意図をDBへ保存し、イベントを通常経路、timerによる再照合を欠落回復・外部通知非対応時の補助手段とする。
- 最初の操作面はローカルCLIと構造化tool call契約とする。外部チャット製品への自動入力やWeb UIは別の連携として扱う。
- 独立検証とローカルレビューは候補品質を評価し、GitHubの正式なAPPROVEDを必要とするmerge条件とは分ける。

Cloud review が正式な APPROVED を返せることや、GitHub App の bot によるレビュー依頼が受理されることは未検証である。成立を確認するまで自動 merge を有効化しない。承認条件を暗黙に弱めて実装上の都合を合わせない。

## 3. 対象範囲

### 3.1 段階的に完成させるもの（K・P・M）

- リポジトリ単位の設定、直接依頼の受付とIssue pool。
- Wave→Phase→Task、Phaseの進行条件とTask依存、メインOrchestrator、Taskごとの使い捨てセッションの管理。
- 永続購読、tool callに連動する購読、通知配送、通知からの再開。
- 親とTaskの双方向メッセージ、永続受信箱、返信と通知による再開。
- 上限と排他を守る複数Task・複数PRの同時進行。
- Issue の取得、実装単位の作成、依存関係の管理。
- app-server を使う実装・検証 thread の開始、終了、再開。
- 独立検証担当によるテスト設計・追加・失敗の切り分け、および独立したローカルコードレビュー。
- 通常プログラムによるテスト runner。
- テストを含む不変の候補 commit。
- PR 作成・更新、Cloud review 依頼、指摘修正、merge。
- Issue 分割、部分 PR、親 Issue の完了管理。
- 永続化、イベントの重複処理防止、再起動後の再照合。
- 保留、中止、再試行、予算、観測情報を扱う操作手段。

### 3.2 後続拡張

- 複数ホスト、分散scheduler。
- 外部チャット製品をホストとする会話連携。
- Phase間pipeline、quorum/any-success、任意の動的fan-out。初期の条件schemaでは拒否する。
- stacked PR、merge queue。
- 外部 runner、特定の実行環境を必要とする検証 profile。
- Web UI、追加の Issue provider / Review provider。

### 3.3 本書の対象外

- プロダクト要求を根拠なく新規作成・削減すること。
- Cloud の非公開 API を前提にした連携。
- Cloud review の代わりにハーネスが承認を生成すること。
- リリース、公開、商用環境への deploy。これらは別の明示的な workflow とする。
- テスト成功だけを根拠に、あらゆる仕様や品質が保証されたと判断すること。

## 4. 用語と管理単位

| 用語 | 定義 |
| --- | --- |
| Orchestrator session | メインの継続会話。依頼を分解し、Waveを作成・補正し、必要な例外判断を行う |
| Wave | 元要求、要求の版、Phase群、予算、進捗、完了条件を束ねる永続単位 |
| Phase | Wave内でTaskを束ねる進行単位。既存WorkItemの`phase`とは別の型・ID。Task群、順序、開始・完了条件を持つ |
| Task | 一つのPhaseに所属する作業。1対1の使い捨てセッションを持ち、完了後は再開せず、Wave呼び出しを禁止する |
| Attempt / Run | 未完了Task内の一回の起動とその結果。turnの失敗とTaskの終端を区別する。完了後の修正は同じTaskの追加Attemptにはしない |
| Artifact | controller管理の不変な汎用成果。ID・digest・サイズ・出所・要求版を持ち、Task間では参照を渡す |
| Plan revision | 要求のscope revisionと別の実行プラン版。Phase/Task構成とtemplate展開の条件・履歴を固定する |
| Parent / Task message | 親Orchestratorと直属Task間の永続メッセージ。質問・回答・指示・blocker等を運び、必要に応じて受信側を再開する |
| Subscription | 対象・イベント種別・購読者・有効な要求の版・再開条件を保持する購読 |
| Notification | 購読に対応する保存済み通知。配送確認とモデルturnの開始は別に追跡する |
| Tool operation | モデルの構造化要求を検証し、controllerが実行する操作。権限を伴うGitHub操作も含む |
| Issue | 元要求を記録するGitHub上の任意の入口。直接依頼には必須でない |
| Issue pool | 選択条件、優先順位、依存関係を満たす Issue / WorkItem の集合 |
| WorkItem | 原則として一つの PR でレビュー・merge できる実装範囲 |
| 実装 session | 実装を指示されたTaskの会話。実行中の待機・復旧には同じthreadを使い、Task完了後は再利用しない |
| 検証 session | 受入条件を基にテストを設計・追加し、検証結果を解釈する会話 |
| Thread | 継続する会話履歴。再開に使う識別子を保存する |
| Turn | 一回の入力と、それによって行われるモデルの作業 |
| Candidate | 製品コードとテストを含む、commit SHA で固定された検証・レビュー対象 |
| Scope revision | 直接依頼またはIssueの受入条件と、WorkItemで扱う対象範囲の版 |
| Verification plan | 受入条件と検証方法、必要な profile、未検証項目、成功後の処理の対応 |
| Validation run | 一つの候補に対する、runner / CI の実行記録 |
| Review round | 一つの候補に対するレビュー依頼と結果のまとまり |
| Gate | merge などの遷移に必要な条件 |
| Inbox / Outbox | 受信イベント、および実行予定の外部操作を永続化する記録 |
| Reconcile | 外部サービスとローカル状態を通常コードで再照合する処理 |

管理関係は、Orchestrator session 1対N Wave、Wave 1対N Phase、Phase 1対N Task、Task 1対1 session/thread、WorkItem 1対1 主PRとする。
TaskはWorkItemを0件以上参照できる。調査・分析・報告だけのTask/WaveにIssue・Git commit・PRを要求しない。
TaskとWorkItemは成果物・検証対象の関連として結び付ける。同じWorkItemを実装Task・独立検証Task・レビューTaskが参照でき、一つのTaskも部分PRの複数WorkItemを扱える。
部分PRの後続は別WorkItem、完了済みTaskのfixは同じWorkItemを対象とする新Task・新sessionとする。Task内へ隠れたsessionを追加しない。
新Taskは元要求・対象範囲・候補SHA・前Taskの成果と指摘を引き継ぎ、過去の会話やcacheが利用できることを必須にしない。
Issue経由の場合は従来のIssueとWorkItemの対応も保持する。会話履歴だけを進行状態の原本にしない。

PR の close / 再作成等が必要な例外では、過去の PR を履歴として保持し、同じ WorkItem に複数の主 PR を同時に関連付けない。

## 5. 役割と通常フロー

### 5.1 役割

| 担当 | 主な責務 | 成否の根拠 |
| --- | --- | --- |
| Orchestrator | 元要求の分解、依存関係・方向修正・例外の判断 | 元要求、Task成果、証拠、運用者の入力 |
| ハーネス本体 | 状態遷移、購読・通知、実行枠、候補固定、外部操作、再照合 | 永続状態と取得した外部事実 |
| 実装担当 | 製品コードの実装・修正 | 要求、変更、検証結果 |
| 検証担当 | 検証計画、テスト追加、判断を要する失敗の切り分け | 元の要求、既存契約、実行証拠 |
| ローカルReviewer | 固定候補のコードレビューと再レビュー | 元要求、契約、候補差分、検証証拠 |
| Runner | 固定候補でのコマンド実行と結果記録 | 実際の終了状態、終了コード、成果物 |
| Codex Cloud review | PR のコードレビュー | 対象候補に結び付いた取得可能なレビュー結果 |
| 運用者 | 設定、未確定仕様の判断、再試行・中止 | 表示された理由と証拠 |

### 5.2 通常フロー

以下は製品変更を含むPの例である。Kでは固定候補を汎用Artifactへ置き換えて独立評価し、PR/merge工程を適用しない（§11.1）。

1. 直接依頼またはIssueを受け取り、Orchestratorの分解案を検証してWave・Phase群・Task・要求の版を保存する。
2. 実行中Phaseの開始条件・Task依存・実行枠を満たすTaskごとに新しいsession/threadを作成する。
3. 実装Taskが対象WorkItemの候補・成果・残作業を構造化出力し、Taskを終了する。
4. Phaseの必要Taskと完了条件を満たしてから、独立検証・テスト・レビュー等の次Phaseへ進む。
5. 独立検証Taskが元要求・候補を確認し、テストと検証計画を作る。runnerが固定候補を検証する。
6. レビューPhaseの新Taskが固定候補を評価し、結果・指摘を返して終了する。
7. 修正が必要なら、保存済みtemplateに一致する指摘はcontrollerがfix Phaseの新Taskへ渡す。分類不能・範囲変更は親の再計画へ戻す。新Taskは元要求・指摘・成果参照・候補・実行証拠を受け取る。
8. 必要に応じて検証・レビューのPhaseを追加し、新Taskで更新候補を確認する。
9. PR操作toolと自動購読を使い、CI・Cloud review・外部更新を待つ。未完了Taskは親とメッセージを送り、通知から同じthreadの次turnを開始できる。
10. PRを持つWaveは最新候補の全gateと正式承認からmergeし、実際の結果と親最終受入を確認する。汎用Waveは成果とcriterionの対応・独立評価・残作業を照合して完了する。

Phase内では独立Taskを並行実行する。次Phaseへの遷移は保存した完了条件に従い、失敗・中止・終了した数だけで成功としない。
待機条件と親子メッセージは保存してturnを終了し、未完了Taskだけを通知で再開する。
Task完了後の追加作業は新Taskへ引き渡し、親Orchestratorの継続sessionは保持する。
通常のturn終了、Task完了、session processの解放、履歴・証拠のGC、実行中断は別の操作として扱う。

## 6. 機能要件

### FR-001 リポジトリ設定と能力検証 — MUST

- 対象 repository ID、base branch、Issue pool 条件、検証 policy、review policy を登録できること。
- main / master、モデル、実行環境、reviewer の識別子をハードコードしないこと。
- app-server、GitHub、Cloud review の採用経路を事前検証できること。
- 必須の連携能力が未確認または不成立なら、その能力が必要な操作を保留すること。

### FR-002 Issue pool の選択と取得 — MUST

- ready 条件、除外条件、優先順位、依存関係から次の対象を決められること。
- 同じ管理 DB の中で、同じ対象を重複取得しないこと。
- GitHub の label は表示・入力に使えるが、排他の唯一の根拠にしないこと。
- Issue が閉じられた、保留された、仕様が更新された場合に再照合すること。

### FR-003 受入条件と対象範囲の版管理 — MUST

- 直接依頼またはIssueの受入条件に識別子を付け、今回の範囲と残作業を記録すること。
- Scope revision を Candidate、検証計画、レビュー依頼に関連付けること。
- 直接依頼の方向修正またはIssue編集で受入条件が変わった場合、古い完了・承認判定をそのまま流用しないこと。
- 要求不足を、実装の現状を追認する期待値で補わないこと。

### FR-004 WorkItem、worktree、thread の対応 — MUST

- WorkItemごとのbranch・worktreeと、関連するTask・Task固有threadを追跡すること。実装・独立検証・レビューのTaskを識別すること。
- 同じ worktree または担当 thread を同時に複数の処理から変更しないこと。
- 未完了Taskの待機・復旧では同じthreadを継続し、完了後の修正では新Taskへ構造化した成果・指摘・証拠を引き渡すこと。
- thread の履歴だけに依存せず、候補コードと進行状態を別途保持すること。

### FR-005 app-server のライフサイクル — MUST

- 接続初期化、thread の開始・再開、turn の開始と終了イベントを扱うこと。
- 正常終了、失敗、中断を区別し、終了しただけで候補完成と判定しないこと。
- 待機時は次の turn を開始せず、再開理由が生じた時だけ起動すること。
- 承認要求、入力要求、認証・利用制限を含む停止原因を放置しないこと。
- 再接続後に同じ処理を無条件に再送せず、保存した識別子と実行状態を照合すること。

### FR-006 構造化された引き渡し — MUST

- 実装・検証担当の最終結果を、版付きの schema で検証すること。
- 共通結果にはschema版、Wave/Phase/Task、scope/plan版、outcome、今回の成果参照、残作業、必要な検証、blockerを含めること。
- WorkItem参照は0件以上とし、製品変更の結果ではWorkItem・Candidateを追加で必須にすること。汎用成果の自己申告を製品gateへ代用しないこと。
- 汎用成果にはartifact ID・digest・サイズ・形式・出所・要求版を付け、controllerが内容と参照権限を検証すること。大きな成果は参照渡しとし、上限・保持・失効時の扱いを固定すること（実装計画§2.2）。
- candidate、分割案、途中検証要求、保留を区別すること。
- 自然文の「完了」、自己申告の SHA / テスト成功 / review 成功だけで外部操作を決めないこと。

### FR-007 独立検証担当 — MUST

- 実装Taskとは別のTask/sessionで、テスト設計・追加・失敗原因の判断を行えること。
- 入力の基準は元の要求、今回の範囲、既存契約・テストとすること。
- 実装担当の報告は手掛かりとして利用し、期待値の唯一の根拠にしないこと。
- 製品コードの修正は原則として実装担当へ戻すこと。
- 独立検証の起動・省略条件は信頼された policy で定義し、担当 agent の自己判断だけで省略しないこと。

### FR-008 検証計画 — MUST

- 各受入条件について、既存テスト、新規テスト、結果の解釈、未検証の状態を記録すること。
- 実行 profile、期待する結果の根拠、成功後の遷移を定義すること。
- 必須の検証集合を担当 agent が縮小できないこと。
- 既存テストで満たせる条件に重複したテストを追加する必要はないこと。
- 部分 PR では、今回の対象と将来の残作業を混同しないこと。

### FR-009 テスト込みの不変 Candidate — MUST

- 正式な検証・レビュー対象は、製品コードと追加テストを含む commit に固定すること。
- head SHA、比較対象の base 情報、Scope revision、検証 policy の版を記録すること。
- 実行中に対象ファイルを書き換える処理が残った状態で候補を確定しないこと。
- 製品コードまたはテストの変更後は、新しい Candidate として必要な gate を評価すること。
- 検証結果の引き継ぎは明示された有効性条件を満たす場合だけ許可すること。初期は新候補で必要な検証を取り直す。

### FR-010 外部テスト runner — MUST

- LLM を呼ばずに、検証 job の起動、終了検知、timeout、cancel、結果保存を行えること。
- 候補 SHA に固定した checkout 等で実行し、作業中のファイルと分離すること。
- 短い探索的テストを実装担当が行うことは許容するが、長時間の必須検証は runner が担当すること。
- 待機中の進捗確認のために実装・検証 turn を開始しないこと。

### FR-011 実行証拠とログ — MUST

- job ID、候補、検証対象 SHA、profile、環境情報、開始・終了時刻、終了コード、ログを保存すること。
- 成功、テスト失敗、実行環境障害、cancel、timeout、結果不明を区別すること。
- 必須テストの未実行、空の結果、不明な対象 SHA を成功として扱わないこと。
- head SHA と test merge / merge group 等の検証対象 SHA を区別すること。
- LLM の結果要約から runner の成功記録を捏造しないこと。

### FR-012 失敗の分類と差し戻し — MUST

- 製品コード、テスト・fixture、実行環境、仕様不明、未分類を区別すること。
- 機械的に判別できる障害は通常コードで処理し、判断が必要な失敗だけを検証担当へ渡すこと。
- 製品コードの不具合は実装担当、テストの不具合は検証担当へ戻すこと。
- 分類の根拠と参照した証拠を保存すること。
- テストの削除、skip、期待値変更は理由と仕様上の根拠を記録し、未検証条件を隠さないこと。

### FR-013 新Taskへの修正引き渡し — MUST

- 有効な指摘・失敗と必要な情報をまとめ、親の保存済みplan/templateまたは新たな構造化判断から、新しい修正Taskへ渡すこと。
- 完了済みの実装・検証・レビューTaskを再開せず、新Taskと新sessionを作成すること。
- 元要求、scope/policy版、前Task、成果参照・指摘・実行証拠・残作業を関連付けること。製品変更では対象WorkItem・候補SHA・review roundも必須とすること。
- 指摘IDで重複したfix Taskの生成を抑止し、inline comment一件ごとにTaskを増やさないこと。評価済みIDと解決済みIDを対応させ、部分的なno_actionで未評価の指摘を解決済みにしないこと。
- 有効な検証計画は再利用し、Task・Phaseをまたいでも修正予算をリセットしないこと。
- 成功したテストの不要なLLM再承認を追加しないこと。

### FR-014 PR の作成・更新 — MUST

- ハーネスが候補を公開し、PR を作成または更新すること。
- PR本文に、関連するWave・Task・任意の親Issue、今回の範囲、残作業、検証証拠、WorkItem識別情報を含めること。
- テスト追加を含む最終候補を Cloud review の対象とすること。
- PR 作成の応答が不明な場合、既存 PR を照合してから再試行すること。
- 部分 PR の本文や commit message によって親 Issue が意図せず閉じないようにすること。

### FR-015 Cloud review の依頼 — MUST

- 検証済みの連携経路から、対象 PR・候補に対するレビューを依頼すること。
- 依頼主体、review round、依頼時の head / base 情報、時刻、依頼コメント等を記録すること。
- 正常完了、未完了、失敗、利用制限、結果不明を識別できる範囲で管理すること。
- 同じ依頼の受理が不明な場合、無条件に重複依頼しないこと。
- app-server 内の review 実行を、GitHub の Cloud review 依頼の代替として扱わないこと。

### FR-016 明示承認の判定 — MUST

- 初期方式では、信頼された reviewer の有効な正式 APPROVED review を必要とすること。
- review の対象 commit が現在の候補 head と一致すること。
- review の取り消し、後続の有効な修正要求、対象範囲の版を確認すること。
- COMMENTED、指摘なし、反応なし、経過時間、一般的な好意的コメント、reaction を暗黙に承認へ変換しないこと。
- 初期方式が成立しない場合、自動 merge を保留し、方式変更を明示的な設計判断として扱うこと。
- ハーネスや実装担当が、Cloud に代わる approval を作成しないこと。

### FR-017 gate の失効と再評価 — MUST

- head 更新、base の変更による差分の変化、Scope revision 更新、検証 policy 更新を検知し、影響する gate を失効させること。
- 古い candidate に遅れて届いた review / CI 結果で、現在の候補を承認しないこと。
- merge 直前に最新の head、review、checks、PR 状態、要求の版を再取得・照合すること。
- 外部イベントは再照合の契機とし、到着順序だけで最終判断しないこと。

### FR-018 merge gate と完了確認 — MUST

- open / 非 draft、候補一致、明示承認、必須検証成功、blocking review の解消、対象範囲の整合を確認すること。
- 今回の必須受入条件について、検証計画上の未検証項目が残っていないこと。自動化できない条件は policy が認める証拠・評価で確認し、残作業へ移す場合は対象範囲の版と残作業を更新すること。
- 部分 PR では残作業の永続化を merge 条件に含めること。
- repository rules、required approvals / checks、必要な会話解決を満たすこと。
- merge の送信直前に現在の操作許可状態を確認し、CANCELED または明示的な merge hold の場合は未送信の merge を実行しないこと。
- 期待する head SHA を merge 要求に付け、保護ルールを bypass しないこと。
- 受付、queue 投入、merge 完了を区別すること。
- 実際に merged になったことを確認してから WorkItem を完了すること。

### FR-019 Issue の分割 — MUST

- 分割案に、子 Issue の要求、受入条件、親条件との対応、依存関係を含めること。
- 要求の取りこぼし、不要な重複、依存の循環、分割数・深さの上限を検証すること。
- 明示的に許可された分割は毎回の人手承認なしで進められること。
- GitHub 上の作成はハーネスが行い、一部成功後の再開で子 Issue を二重作成しないこと。
- 分割済みの親を、同じ未分割タスクとして再取得し続けないこと。

### FR-020 部分 PR と親 Issue の完了 — MUST

- 一つの Issue を、独立して検証・merge できる複数 WorkItem に分けられること。
- 各部分 PR は既存動作を壊さず、今回の範囲に必要な検証を満たすこと。
- 残作業を merge 前に DB と追跡可能な Issue / WorkItem として保存すること。
- 親 Issue は、今回の部分 PR の merge だけでは完了にしないこと。
- 親条件に対応する作業が merge 済みで、最終受入検証が成立したときに親を完了すること。
- 先行PRの成果を必要とする後続は、merge後の最新baseを基点に開始すること。独立Taskの同時進行をこの順序制約で妨げないこと。

### FR-021 Webhook と再照合 — MUST

- 必要な GitHub イベントを受信し、署名を検証して永続化すること。
- 重複する delivery を安全に扱い、イベント受信処理と外部操作を分離すること。
- 再起動時、および設定された低頻度の処理で、進行中の外部状態を再照合すること。
- Webhook を受けられない構成では、通常コードの定期照合で動作できること。
- これらの処理を LLM turn で実装しないこと。

### FR-022 永続化と不明応答からの復旧 — MUST

- 状態更新と次の操作の意図を、同じ transaction に保存すること。
- Issue 取得、turn 開始、候補固定、PR 作成、レビュー依頼、merge の途中停止から再開できること。
- API timeout や応答欠落を、操作未実行と同一視しないこと。
- 外部側の結果を照合し、既存の操作・資源に関連付けてから再試行すること。
- JSON-RPC の request ID 等を、確認なしに外部の冪等性保証として扱わないこと。

### FR-023 運用操作、保留、中止 — MUST

- 状態、起動理由、保留理由、必要な入力、直近の証拠を確認できること。
- 新規 turn の停止、明示的な実行中断、WorkItem の中止、限定的な再試行を区別できること。
- CANCELED の WorkItem に残った未送信の PR 公開・レビュー依頼・merge 操作は実行しないこと。明示的な merge hold は未送信の merge を保留し、操作の送信直前にも許可状態を確認すること。
- pause / cancel 後に、既に送信済みの外部操作が完了する場合も追跡すること。
- app-server の承認・入力要求は設定済み policy に従って応答し、未対応要求は理由付きで保留すること。
- 完成していない作業を停止した場合も、差分と再開情報を保持すること。
- pause、通常message、版付き方向修正、cancel、明示interruptの優先・送信前照合・安全境界を定義すること。初期は実行中への自動steerを行わないこと（実装計画§4.5）。

### FR-024 予算、上限、再試行 — MUST

- LLM 使用量、実行時間、修正回数、review 依頼回数、検証 job 数、分割数の上限を設定できること。
- 実装・テストの担当間を往復しても、WorkItem 全体の修正予算をリセットしないこと。
- 通信・環境障害の再試行と、コードを変える修正を区別すること。
- 利用制限や上限到達時は理由付きで保留し、承認や成功として扱わないこと。
- 待機 timeout を理由に意味のない実装修正 turn を起動しないこと。

### FR-025 観測と効果測定 — MUST

- WorkItem、candidate、thread、turn、validation run、review round、外部操作を関連付けること。
- モデル呼び出し理由、入出力使用量、修正回数、テスト実行回数、待機時間、merge までの時間を記録すること。
- 待機中の LLM 呼び出し件数を独立して確認できること。
- ログの要点を差し戻し、必要な完全ログへアクセスできること。
- Cloud 側の token 使用量等を取得できない場合、取得不能と明示し、架空の合算をしないこと。

### FR-026 設定・検証 policy の版と信頼境界 — MUST

- 採用した設定、検証 profile、review policy の版を実行に関連付けること。
- agent が必須検証、承認条件、実行権限を変更して gate を通せないこと。
- リポジトリに policy を置く場合も、候補の変更によって今回の必須条件が自動的に弱まらないこと。
- 正当な policy 更新は別の設定更新手順で取り込み、影響する gate を再評価すること。
- 認証情報、モデル設定、検証コマンド等を文書の例だけで有効化しないこと。

### FR-027 Waveと直接依頼 — MUST

- GitHub Issueの事前作成を要求せず、元要求・受入条件を登録してWaveを作れること。
- Wave、Phaseの順序・開始/完了条件・所属Task、Task依存、要求の版、成果物、残作業、予算、完了条件を永続化すること。
- 分解案のcoverage・Phase/Task所属・変更範囲・上限を検証すること。Phase順序とTask/成果依存を合わせて循環や後続Phaseへの不可能な待機を拒否すること。
- Phase内のTaskを並行進行し、次Phaseへの遷移はFR-038の明示条件に基づくこと。親のplanには結果に応じた次のレビュー/fix Phaseの有界templateを保存できること。
- 方向修正では要求の版を更新し、旧版の通知・成果・承認を現行版へ無条件に流用しないこと。
- Wave完了はTaskの自己申告数で決めず、全対象範囲の検証、必要なmerge、最終受入を照合すること。

### FR-028 メインOrchestrator — MUST

- メインthreadで依頼の分解、方向修正、Task横断の例外判断、結果の説明を行えること。
- 通常は同じOrchestrator threadを保持し、待機中はturnを終了して実行枠を解放すること。履歴利用不能や文脈容量の保守ではFR-040の監査付きbinding更新を行い、論理Orchestrator sessionを保持すること。
- 新規入力、意味のあるblocker、再計画の必要、Wave終了などの起動理由を保存すること。
- 定型工程の成功や状態確認だけで不要なOrchestrator turnを開始しないこと。
- Orchestratorの提案もcontrollerによる構造・権限・予算・版の検証を経て適用すること。

### FR-029 独立した検証・レビューTask — MUST

- 独立検証・ローカルレビューのTask/thread/runと、対象artifactまたはWorkItem・候補SHAを追跡すること。汎用成果も独立したTaskで評価できること。
- 独立した検証・レビューは親が作成する別Taskで行い、Task内で役割別sessionを自動生成しないこと。
- Reviewerは不変のartifactまたは固定候補を読み取り専用で評価し、構造化された指摘・根拠・対象digest/SHA・判定を返すこと。
- 成果修正は新fix Taskへ渡すこと。製品不具合は実装fix、テスト不具合は検証fixとして指示し、再レビューも新Taskで更新成果/候補に対して行うこと。
- ローカルレビューを正式なGitHub承認へ変換しないこと。

### FR-030 永続購読と通知 — MUST

- 対象、イベント種別、購読者、要求の版、cursor、待機条件、配送状態を保存できること。
- 購読登録と完了が前後しても、保存済み結果との照合で通知を取りこぼさないこと。
- イベントの重複・逆順・再配送を扱い、通知済みという理由だけで成果やgateを成立させないこと。
- Task完了、失敗、入力要求、runner終了、CI・PR更新を関連する購読へ配送すること。
- 再接続時に未確認通知を再取得でき、無関係な全ログを毎回セッションへ投入しないこと。

### FR-031 通知による停止・再開 — MUST

- 待機条件と再開に必要な情報を保存してからturnを正常終了すること。待機を実行中turnへのinterruptで表現しないこと。
- 通知群から起動意図を永続化し、同じ再開理由による二重turn開始を防ぐこと。
- turn開始応答が不明なら既存runを照合し、未確認の再送を行わないこと。
- 同じthreadの同時turn、新規入力と通知の競合、pause・cancel・旧版の通知を扱うこと。
- 通知の配送確認、runとの関連付け、判断の適用を分け、途中停止で判断入力を失わないこと。

### FR-032 イベント起点の実行枠と排他 — MUST

- Task完了・操作・通知を契機に実行可能な工程をdispatchし、全操作をtimer一巡につき一件へ制限しないこと。
- LLM turn、runner job、進行中PRの枠を独立に制御し、待機TaskはLLM枠を消費しないこと。
- 独立Taskの並行実行と依存Taskの待機、同一worktree・thread・共有Git操作の排他を両立すること。
- Wave/Taskの予算・全体の実行枠を原子的に予約し、再起動・修正・分割でも予算をリセットしないこと。
- timer再照合は欠落・障害回復と通知非対応の外部サービスに残すこと。通常コードのpollingまで禁止しないこと。

### FR-033 Waveの操作と観測 — MUST

- Wave一覧・詳細からTask、その一つのsession、工程、試行、待機理由、依存先、成果物、使用量、blocker、親子間メッセージを追跡できること。
- Wave/Task単位のpause・resume・cancel・方向修正を提供し、既存WorkItem操作との適用範囲を明示すること。
- コメント等の通知対象へのリンクと再開理由を確認できること。
- 実行状態の購読を提供し、切断した閲覧クライアントがcursorから再接続できること。

### FR-034 既存作業の移行 — MUST

- 保存済みWorkItem、thread、run、候補、承認、予算、親最終受入の系譜を保持すること。
- 移行の再実行でWave/Taskや外部操作を二重作成しないこと。
- 移行中は新規dispatchを止め、送信済み・応答不明の処理を照合してから再開すること。
- 旧CLIで記録済みWorkItemを参照・操作する契約を保持し、新Wave情報を追加すること。

### FR-035 tool callに連動する外部資源の購読 — MUST

- PR作成・更新・レビュー依頼を、controllerが検証・実行する版付きtool operationとして提供すること。
- 操作の発行元Wave・Task・sessionを保持し、外部操作intentと購読intentを送信前に保存すること。
- PR作成結果または応答不明時の照合から外部資源IDを確定し、購読を自動的に関連付けること。
- コメント、リアクション、レビュー、CI、PR状態変更を識別し、編集・削除・承認取消などの更新も取り込めること。
- 操作成功から購読確定までの更新を回収し、重複tool callやイベント再配送から重複PR・重複再開を生まないこと。
- 通知は関連するTask・待機条件へ集約し、モデルが読む前に起動の必要性をpolicyで判定すること。コメント・リアクションだけを承認にしないこと。
- Issue本文・PR本文・コメント・リアクションのデータを、権限付与や任意のtool命令として実行しないこと。

### FR-036 Taskと使い捨てsessionの対応・Wave呼び出し禁止 — MUST

- 一つのTaskは一つのPhaseに所属し、独立した一つの使い捨てsession/threadを持つこと。
- 未完了Taskの待機・復旧で同じthreadを継続できるが、Task完了後は再利用せず、修正・再レビューを新Task・新sessionへ渡すこと。
- 使い捨てを実行中の非永続化と同一視せず、復旧用の履歴・成果・メッセージを保存すること。完了後の保持・GCはpolicyで管理すること。
- 親と同じsession runtime・tool処理を使い、Taskであることだけを理由とする役割専用の縮小runtimeを設けないこと。
- Task sessionにはWave呼び出しtoolを公開せず、非公開tool名や直接要求を送ってもcontrollerが拒否すること。別名・中継による入れ子Waveの生成も許可しないこと。
- 既存scope・sandbox・予算・共有Git排他・検証・承認policyは親子共通に適用し、同じruntimeであることを権限の無制限化と解釈しないこと。
- 作業の再分割や独立レビューが必要なら、Taskは親へメッセージで依頼し、親がWave・Taskを管理すること。

### FR-037 親とTaskの双方向メッセージ — MUST

- 親から直属Taskへ、直属Taskから親へ、tool callでメッセージを送れること。
- メッセージID、発行元session、宛先、Wave/Task、要求の版、本文、返信先、送信・受信・処理状態を永続化すること。発行元と親子関係はcontrollerが検証すること。
- 親とTaskの関係が確定した時点で受信通知を自動購読し、質問・回答・指示・blockerを相手の受信箱へ保存して通知すること。
- 受信側が実行中なら受信箱へ保存し、turn終了後の安全な境界で入力すること。待機中は購読条件とpolicyに従って同じthreadを再開すること。
- 重複・再配送・再起動でもメッセージを失わず、同じメッセージから二重turnを開始しないこと。配送ackを処理済みや回答済みと同一視しないこと。
- pause中は未処理メッセージを保持して自動再開せず、cancel/終端Taskには保存・通知しても作業を再開しないこと。完了後の追加指示は親が新Taskへ引き渡すこと。
- メッセージ本文だけでscope・policy・完了判定を変更せず、方向修正は別の版付き操作として適用すること。Taskからの再分割依頼は親の判断を経て処理すること。
- 外部commentを経由した情報も出所resource・候補・要求版とともに記録すること。親へ中継しても操作権限は増えず、親の構造変更を元要求・policy・上限で再検証すること。

### FR-038 Phase条件・失敗・定型反復 — MUST

- 初期のPhase完了条件は`all_success`と明示した必須Task ID集合（当該Phase所属の全Task）とし、空集合、quorum、any-success、暗黙skipを拒否すること。
- Taskの非終端状態とcompleted/failed/canceled、run失敗とTask失敗、許可する再試行、Phaseが保留される条件を定義すること（実装計画§2.1）。
- 親が検証済みplanへ保存したreview/fix templateを、controllerが候補・指摘ID・予算に基づき新Phase/Taskへ有界展開できること。通常の成功・定型反復では親turnを追加しないこと。
- 分類不能、仕様変更、template外の分岐は親へ戻し、起動理由と再計画回数を保存すること。任意fan-outは初期非対応として拒否すること。
- plan採用・展開とscope/plan版・coverage・Task生成・予算予約・通知intentを同じtransactionで保存し、再実行やcrashで二重展開しないこと。

### FR-039 Wave上限とprocessライフサイクル — MUST

- Phase/Task総数、親turn・再計画・fix反復・Attempt数、同時Wave、壁時計期限、成果容量に設定上限を持つこと。分割・方向修正・再起動でも累積値を保持すること。
- 待機時は履歴・購読を保存してturnを終了し、app-serverと子processの停止確認後に実行枠を解放すること。待機のためだけにprocessを常駐させないこと。
- 上限・期限到達時は理由付き保留とし、実行中の明示中断・送信済み操作の追跡を区別すること。未知processの枠を停止確認なしに再利用しないこと。
- 新設定の既定値と旧設定からの変換を明示すること（実装計画§5.1）。token観測の遅延を、構造・時間上限を省く理由にしないこと。

### FR-040 Orchestratorの文脈と復旧 — MUST

- main入力に対象Wave・scope/plan版・有効な成果参照・未処理通知を付け、複数Waveの入力境界とサイズ上限を持つこと。
- DBと不変成果から入力snapshotを再構成でき、cache・過去会話全体を正しさの必須条件にしないこと。mainのthread当たりturn数を制限し、容量保守は安全境界での監査付きthread更新とし、Wave予算・通知適用履歴を保持すること。
- thread履歴が欠落した場合は旧run・未知起動・通知の適用状態を照合し、旧process停止とbinding履歴を保持して新main threadへ復旧できること。未照合なら保留すること。
- mainの復旧でTaskの1対1 session制約を緩めず、Task threadが復旧不能なら旧Taskを終端化して新Taskへ引き渡すこと。

## 7. 状態と共通の不変条件

Waveは`planning / running / waiting / blocked / completed / canceled`を持ち、pauseはdispatch抑止として別に保持する。
Phaseは所属Taskと進行条件、Taskは一つのsession・試行・依存待ち/外部待ち/入力待ちの理由を保持する。
表示上のWave集約から実行可否を決めず、PhaseとTaskの保存した条件を照合する。
WorkItemの状態はPR処理の契約として維持する。

### 7.1 WorkItem の主要状態

| 状態 | 意味 |
| --- | --- |
| READY | 依存条件を満たし、実行枠を待っている |
| IMPLEMENTING | 実装担当の turn が進行中 |
| VERIFYING | 検証担当が計画・テストを作成、または判断を要する結果を切り分けている |
| VALIDATING | runner / CI の検証を実行中または待機中 |
| REVIEWING | PR のレビュー・関連する必須チェックを待機中 |
| MERGE_PENDING | merge 要求の実行・結果確認中 |
| MERGED | merge 完了を確認済み |
| SPLIT | 元の範囲を子 WorkItem に移管済み。製品要求の完了を意味しない |
| BLOCKED | 必要条件が成立せず、理由を保存して保留中 |
| CANCELED | 明示的に中止された |

状態と gate は分ける。例えば review と CI の完了順序が変わっても、すべての組み合わせを別状態として増やさない。

### 7.2 共通の不変条件

1. 同じ担当 thread / worktree に、競合する変更処理を同時に実行しない。
2. 正常な turn 終了と、妥当な引き渡し結果の両方が揃ってから次の工程へ進む。
3. 必須の検証と承認は、現在の候補・要求の版・policy に対応する。
4. 承認の欠如、結果不明、timeout は、承認や成功を意味しない。
5. 実装・検証担当は、必須 gate の成否を自己申告で確定できない。
6. 外部操作は受理と完了を区別する。
7. 部分実装の残作業は merge より先に保存する。
8. 親 Issue の完了は、子の close 数だけで判断しない。
9. 待機、再照合、通常のバックオフのために LLM を呼ばない。
10. threadの再開は未完了Taskの継続であり、完了済みTaskの再利用や過去の成果・承認の有効性を保証しない。

## 8. 非機能要件

| ID | 要件 | 検証方法 |
| --- | --- | --- |
| NFR-001 | 通常の VALIDATING / REVIEWING / MERGE_PENDING 待機中の、新規 LLM 呼び出しをゼロにする。必要な判断による turn 開始は状態・理由を明示して行う | 起動記録と状態履歴の照合 |
| NFR-002 | 再起動、重複イベント、不明応答から復旧し、二重実装・二重 PR・無確認 merge を防ぐ | 障害注入と再照合テスト |
| NFR-003 | 製品コードを実行する環境と、GitHub 書き込み・merge の資格情報を分離する。worktree の分離だけを権限隔離とみなさない | 実行環境・credential 到達性の確認 |
| NFR-004 | 進行、判断、証拠、外部操作を関連付けて追跡できる。秘密情報はログや差し戻しに含めない | 記録の追跡、ログ検査 |
| NFR-005 | Codex バイナリ、生成型、必要な API / adapter を固定し、更新時に契約テストを行う | バージョン照合と再生テスト |
| NFR-006 | LLM、runner、review、進行中 PR の枠を別々に制御する。待機は LLM 実行枠を消費しない | 実行枠と排他のテスト |
| NFR-007 | 検証計画・既存テスト・構造化成果を引き継ぎ、未完了Taskのthreadを継続する。完了Taskを再利用せず、不要な再設計や大量ログ再投入を抑える | 起動理由、Task/session ID、引き渡し、使用量の比較 |
| NFR-008 | 状態遷移、外部 API、Codex adapter、runner を分離し、通常の単体テストと限定的な接続テストで検証可能にする | モジュール境界とテスト構成の確認 |
| NFR-009 | 購読・通知・再開の永続性を保ち、通知の重複から同じthreadの並行turnを開始しない | crash・順序・競合の障害注入 |
| NFR-010 | 上限2以上の設定で独立Taskを並行進行し、待機中も別Taskを進める | 決定的schedulerテストとrace検査 |
| NFR-011 | 閲覧・通知配送・再接続はモデルを呼ばずに行い、cache情報が取得不能でも正しく動作する | 呼び出し理由・cache有無の比較 |
| NFR-012 | **SHOULD**: 単一ホスト・独立Task 2件・空き枠あり・外部I/Oをstubにした参照試験で、通知保存から再開intent予約までp95 1秒以内を目標とする。100件以上を測定し、cold start/model応答/外部待ちを別に報告する | 時刻・環境・件数・p95、未達理由と代替を記録 |
| NFR-013 | **MUST**: controller管理app-server数を同時turn上限以下に保ち、待機時の停止を確認する。processのRSS・起動時間・終了時間を観測し、負荷試験の環境と設定を保存する | process数・子process残存・設定上限・実測値の照合 |

## 9. 保存・公開・更新する情報

- 実行に必要な状態は DB に保存し、会話文や label だけに依存しない。
- 候補のコードは Git commit に固定する。
- job の完全ログと構造化された結果は再起動後も参照できる場所に保存する。
- PR には対象範囲、残作業、検証の要約と参照先を掲載する。
- 子 Issue 作成、PR 作成、review 依頼には再照合に使える固有識別情報を持たせる。
- 認証情報や非公開ログを、PR 本文や外部コメントへ自動掲載しない。
- ログの保存期間・容量上限・削除条件は運用設定で決める。
- Issue / PR が外部から更新された場合は、その事実を取り込み、古いローカル状態で上書きしない。

## 10. 受入テスト

以下はハーネス自身の受入条件であり、対象プロダクトのテストとは別である。

| ID | ケース | 期待結果 | 主な対応要件 |
| --- | --- | --- | --- |
| AT-001 | 必須の Cloud 承認形式または依頼経路が未確認 | 自動 merge を有効化しない。他の開発・検証は進められる | FR-001, FR-015, FR-016 |
| AT-002 | 同じ Issue を同時に取得しようとする | 管理 DB 内で二重に取得されない | FR-002, FR-004 |
| AT-003 | 小さな Issue を正常実装する | 正常終了と schema 検証を経て検証工程へ進む | FR-005, FR-006 |
| AT-004 | 実装 turn が中断・失敗する | 完了と扱わず、差分と再開情報を保存する | FR-005, FR-023 |
| AT-005 | 受入条件に対応するテストが不足している | 独立検証担当が根拠付きでテストを追加する | FR-007, FR-008 |
| AT-006 | 部分 PR に含めない条件がある | 対象範囲だけを検証し、残作業を保持する | FR-003, FR-008, FR-020 |
| AT-007 | 実装後にテストを追加する | テスト込みの候補 SHA を正式検証・review 対象とする | FR-009, FR-014 |
| AT-008 | 長時間テスト・review・merge 待機を発生させる | 待機のための LLM 呼び出しがゼロ | FR-010, FR-021, NFR-001 |
| AT-009 | テストが実装不具合を検出する | 証拠をまとめ、新しいfix Task/sessionへ引き渡して同じWorkItemの候補を修正する | FR-011, FR-012, FR-013 |
| AT-010 | テストの期待値が仕様と異なる | 根拠付きで検証担当へ戻し、仕様に合わせて修正する | FR-007, FR-012 |
| AT-011 | 一時的な実行環境障害が発生する | policy の範囲で通常コードが再試行する | FR-010, FR-012, FR-024 |
| AT-012 | 仕様から期待値を確定できない | 推測で成功させず、理由を保存して保留する | FR-003, FR-012, FR-023 |
| AT-013 | 必須テストが未実行、結果が空、対象 SHA が不明、今回の必須受入条件に未検証項目がある | merge gate を成立させない | FR-008, FR-011, FR-018 |
| AT-014 | 有効な明示承認と全検証結果が揃う | 期待 SHA 付きで merge し、実際の完了を確認する | FR-016, FR-018 |
| AT-015 | COMMENTED、指摘なし、reaction、timeout のみ | 承認に変換せず、待機または保留する | FR-015, FR-016, FR-024 |
| AT-016 | 一巡の review に複数の指摘がある | 重複を除きまとめて担当 thread へ返す | FR-012, FR-013 |
| AT-017 | 承認後に製品コードまたはテストが更新される | 古い候補の承認・結果を現候補に使わない | FR-009, FR-017 |
| AT-018 | review が取り消される、base 差分・要求の版が変わる | 影響する gate を失効させ、再評価する | FR-003, FR-017 |
| AT-019 | head と CI の test merge SHA が異なる | 正しい候補との対応を確認し、古い結果を採用しない | FR-011, FR-017 |
| AT-020 | Webhook が重複・逆順・欠落する | 二重処理せず、再照合で現在の状態に戻る | FR-021, FR-022 |
| AT-021 | turn 開始直後または PR 作成直後に応答を失う | 外部・実行状態を照合してから再開する | FR-005, FR-014, FR-022 |
| AT-022 | merge 受付後に停止する | 復旧後に実際の状態を確認し、二重完了しない | FR-018, FR-022 |
| AT-023 | 子 Issue の一部作成後に失敗する | 作成済みを重複させず、分割処理を再開する | FR-019, FR-022 |
| AT-024 | 部分 PR を merge する | 残作業が既に保存され、親 Issue は継続する | FR-014, FR-020 |
| AT-025 | 親の全作業と最終検証が完了する | 親の要求の版を確認して Issue を完了する | FR-003, FR-020 |
| AT-026 | 実装とテスト修正が繰り返される | WorkItem 全体の予算で上限を適用する | FR-013, FR-024 |
| AT-027 | agent が必須検証や承認条件を弱めようとする | 今回の gate の policy は変更されない | FR-008, FR-026, NFR-003 |
| AT-028 | 保留・中止または app-server の入力要求が発生する | 理由と必要な操作が確認でき、意図せず再開しない。cancel / merge hold 後に古い outbox の禁止操作を送信しない | FR-005, FR-018, FR-023 |
| AT-029 | 検証成功後に判断が不要である | LLM に成功を再確認させず、定義済みの次工程へ進む | FR-008, FR-013, NFR-007 |
| AT-030 | 運用記録から一つの WorkItem を追跡する | turn、候補、検証、review、merge と使用量を関連付けられる | FR-025, NFR-004 |
| AT-031 | Issueなしの依頼からWaveを作る | 元要求・Phase群・所属Task・依存・coverageが保存され、欠落・循環・複数Phaseへの所属を拒否する | FR-027, FR-028 |
| AT-032 | 独立Task A/BとA依存のCを開始 | A/Bが重なって実行され、Cは有効なA成果まで待つ | FR-032, NFR-010 |
| AT-033 | 同じPhase内のTask Aが外部レビューを待つ | 独立Task Bが進み、待機中に確認用turnを起動しない。次Phaseは完了条件まで待つ | FR-027, FR-031, FR-032 |
| AT-034 | 購読前・同時・後に対象が完了 | いずれも通知を取得し、同じ再開理由のturnは一つだけ | FR-030, FR-031 |
| AT-035 | 完了保存後・通知配送前、および配送後・確認前に停止 | 再起動で通知・判断入力を回収し、二重起動しない | FR-030, FR-031, NFR-009 |
| AT-036 | レビューPhaseの指摘を修正 | 親がfix Phaseの新Task/sessionへ指摘を渡し、更新SHAを新しいレビューTaskで評価する。過去Taskを再開せずGitHub承認も生成しない | FR-013, FR-029, FR-016 |
| AT-037 | pause/cancel/方向修正後に旧完了通知が届く | 履歴を保持し、禁止されたdispatchや旧版成果の採用をしない | FR-027, FR-031, FR-033 |
| AT-038 | 新規入力と複数Waveの通知が同じmain threadへ届く | 一つのturnへ集約または順次処理し、入力・通知を失わない | FR-028, FR-031 |
| AT-039 | 既存DBを移行し再実行する | ID・thread・予算・PR・親最終受入を保持し、二重登録・再送がない | FR-034 |
| AT-040 | 上限到達、修正分割、共有Git操作の競合 | 原子的な枠・予算・排他を守り、解除後に進行可能なTaskを再開する | FR-024, FR-032 |
| AT-041 | Wave詳細を購読し切断・再接続 | cursorから再開し、Task・工程・理由・使用量をモデル呼び出しなしで表示する | FR-033, NFR-011 |
| AT-042 | PR作成tool call直後にコメント等が届く | 資源への購読を自動確定し、登録前後の更新を回収して関連Taskへ通知する | FR-035, FR-030 |
| AT-043 | PR作成応答欠落、tool再送、重複通知 | markerで既存PRへ関連付け、PR・購読・再開turnを二重作成しない | FR-035, FR-022, FR-031 |
| AT-044 | コメント・リアクション・review更新を受信 | 種別を識別して通知・集約し、古いSHA・取消を照合する。好意的反応を正式承認にしない | FR-035, FR-016, FR-017 |
| AT-045 | 実装→レビュー→fix Phaseを進める | 各Taskは一つのPhase・一つのsessionに属し、Phase内では並行実行する。完了条件で次Phaseへ進み、修正Taskは新sessionを使う。完了Taskを再利用しない | FR-027, FR-036 |
| AT-046 | TaskがWave toolを要求する | tool非公開とcontroller認可で拒否し、別名や中継でも子Waveを生成しない。再分割依頼は親へ送れる | FR-036 |
| AT-047 | 親子が質問・回答・指示を送る | 双方向に保存・通知し、返信と発行元を追跡する。別Wave/Taskへの宛先偽装を拒否する | FR-037 |
| AT-048 | 実行中・待機中・pause/cancel後にメッセージが届く | 実行中は受信箱へ保持し、待機中は同じthreadを一度だけ再開する。pause/cancel/終端・旧版から禁止された再開や範囲更新をしない | FR-031, FR-037 |
| AT-049 | 既存の実装・検証threadを移行する | 履歴を保持し、実行中のthreadは対応するPhase/Taskに結び付ける。終了済みthreadを新Taskへ再利用せず、親子受信箱と返信も復旧できる | FR-034, FR-036, FR-037 |
| AT-050 | dynamic tool成立確認と未対応環境 | 実tool往復・正常終了・同一未完了threadの再開を先行確認し、未対応/認証不足は依存工程を保留。outputSchema方式へ黙って置換しない | FR-001/005, NFR-005 |
| AT-051 | 汎用TaskだけのWaveと大きな成果 | Issue/WorkItem/PRなしで調査→独立レビュー→報告を完了し、成果参照のdigest/容量/権限/失効を検証する | FR-006/027/029 |
| AT-052 | Phase条件とTaskの各終了結果 | all_successの必要成果だけで進み、失敗/cancel/blocked/unknownでは進まない。復旧可能Attemptと終端Taskを区別する | FR-038 |
| AT-053 | 定型fix/reviewと親判断の境界 | 保存済みtemplateを新Task/sessionへ展開し、定型反復の親turnはゼロ。分類不能だけ理由付きで親を再開する | FR-013/028/038 |
| AT-054 | plan採用・展開の途中でcrash | scope/plan版・Task・coverage・予算・通知が一緒に確定し、再実行で二重展開しない | FR-022/038 |
| AT-055 | Phase/Task/再計画/Attempt/期限/容量の超過 | 同時追加・再起動・方向修正でも累積上限と期限を保持し、理由付き保留と送信済み追跡を行う | FR-024/032/039 |
| AT-056 | 同一repoの複数Waveと同じファイルのPR | Git操作を排他し、先行mergeによる競合を新fix Taskへ返す。検証/承認を失効し、期待SHAでmergeする | FR-009/017/032/036 |
| AT-057 | 外部commentを親messageへ中継 | 出所を追跡し、sender/宛先偽装、scope/policy緩和、子Wave生成を拒否する | FR-026/035/036/037 |
| AT-058 | main文脈上限・履歴欠落と旧run | 入力を上限内で再構成し、旧run停止確認と通知照合後にmainを復旧する。未照合なら新turnを作らない | FR-028/031/040 |
| AT-059 | 待機process・優先操作・通知遅延 | 待機processを停止し、pause/cancel/steer競合で禁止dispatchを行わない。上限・p95・cold startを測定する | FR-023/039, NFR-012/013 |

単体・component テストで再現できる状態遷移を毎回実サービスで検証する必要はない。外部の成立条件は限定した接続テスト、通常の制御ロジックは再生可能な fixture と障害注入で確認する。

## 11. 段階別の完了条件

### 11.1 K: Wave kernel

- S（AT-050）の実接続が成立し、Kで使用する共通要件に実装・証拠の対応がある。
- AT-031〜038/040/041/045〜048/051〜055/057〜059のK適用部分を確認する。AT-056の共有Git排他はローカルGitで確認し、実PR経路はPで確認する。
- Issue/PRなしの調査→独立レビュー→報告、Task 2件の並行実行、親子message、待機後の再開、crash回復、定型fix/reviewを実演する。
- 必須criterionと成果の対応、独立評価、未解決指摘・残作業の不存在から完了する。LLMの完了主張数で判定しない。
- 通常待機・閲覧・定型遷移による親turnはゼロ。CLIから状態と理由・使用量・成果へ辿れる。

### 11.2 P: PRパイプライン接続

- Kを満たし、製品変更のFR-001〜029/035とAT-001〜030/042〜044/056の全経路を確認する。
- 小さなIssueを実装→独立検証→修正→Cloud正式承認→merge→親最終受入まで通し、部分PR・分割・失敗復旧も確認する。
- 必要なCloud/CI/rulesがunknownなら、Pと該当する製品Waveの完了は保留する。Kの独立した合格は維持できる。

### 11.3 M: 既存作業の移行と全体完成

- FR-034、AT-039/049を確認し、旧CLI・thread履歴・unknown操作・予算・親最終受入を保持して移行する。
- 実DBがなくてもfixtureでMを検証する。実DB不存在を理由に履歴保持要件を削除しない。
- K・P・Mが揃い、FR-001〜040、NFR-001〜013、AT-001〜059の適用と証拠を説明できた時に全体完成とする。
- 導入先のrepository・model・認証主体・profile・上限・保留復帰を設定し、未成立の承認経路を自動merge可能と表示しない。

## 12. 未確定事項と解消方法

| ID | 未確定事項 | 解消方法 | 未解消時の扱い |
| --- | --- | --- | --- |
| OQ-001 | Cloud が正式な APPROVED review を返せるか | 問題なし・指摘ありの検証用 PR で実際の結果を取得 | 自動 merge を無効化 |
| OQ-002 | 採用する GitHub App / user 主体で review が起動するか | コメント依頼または設定済み自動 review を実測 | 依頼経路を未設定として保留 |
| OQ-003 | review の結果を対象 head / round に十分に関連付けられるか | 再 push、遅延結果、再レビューのケースを検証 | 判定不能として保留 |
| OQ-004 | 対象リポジトリ、base branch、Issue pool、認証方法 | 導入時の repository 設定で決定 | 対象を取得しない |
| OQ-005 | 採用する Codex バイナリ、モデル、履歴保存先、sandbox | app-server 接続・停止・再開の検証で決定 | 実行環境未設定 |
| OQ-006 | 必須検証 profile と、その成功が意味する範囲 | プロダクトの受入条件と既存 CI から定義 | 必須検証を完了としない |
| OQ-007 | GitHub の rules / CI と、採用 token の連携 | 実際の PR 作成・更新による起動と gate を検証 | PR 処理または merge を保留 |
| OQ-008 | 導入先に適した予算・性能・運用通知先 | 実装計画§5.1の提案既定値を基点に明示設定・計測 | 未設定の機能を保留。値の妥当性を実測前に保証しない |
| OQ-009 | 採用環境のdynamic tool往復と再開 | S / Task_0で実model・認証・隔離下の限定spike | 依存runtimeを保留。outputSchema要求配列は別契約として再計画 |

OQ-001〜OQ-003が未解消でも、Wave、担当セッション、購読・通知、runner、状態管理、分割など独立した実装は進められる。Kの合格とPの未確認を別に報告し、厳密な承認条件を満たす製品E2Eや全体完成とは報告しない。

## 13. 外部仕様の根拠

参照確認日：2026-10-20。以下は接続方法や外部の状態の根拠であり、本書独自の workflow、要件 ID、policy、状態名はハーネスの設計である。採用版・実環境で再確認する。

- [S1: Codex App Server](https://learn.chatgpt.com/docs/app-server) — thread / turn、イベント、構造化出力、生成 schema。
- [S2: Review GitHub pull requests with Codex](https://learn.chatgpt.com/docs/third-party/github) — GitHub での review 依頼と設定。正式な APPROVED の返却保証としては扱っていない。
- [S3: GitHub Pull request reviews API](https://docs.github.com/en/rest/pulls/reviews) — review の state、reviewer、commit_id。
- [S4: GitHub Pull requests API](https://docs.github.com/en/rest/pulls/pulls) — PR と merge、期待 SHA、非同期結果。
- [S5: GitHub Webhook events and payloads](https://docs.github.com/en/webhooks/webhook-events-and-payloads) — 購読するイベント。
- [S6: Best practices for using webhooks](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks) — 署名、応答、delivery ID。
- [S7: Handling failed webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries) — 失敗時の再配信・復旧。
- [S8: Troubleshooting required status checks](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks) — 最新候補と checks、test merge commit の扱い。
- [S9: About protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches) — required reviews / checks、base 更新、承認の失効。
- [S10: Triggering a workflow](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow) — token とイベントによる CI 起動の確認。
- [S11: REST API endpoints for reactions](https://docs.github.com/en/rest/reactions/reactions) — reaction を検証する場合の情報。初期の承認判定には採用しない。

