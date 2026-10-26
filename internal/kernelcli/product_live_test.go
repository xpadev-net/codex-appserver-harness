package kernelcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// This opt-in exercises local P model roles and the real detached runner through
// the production CLI. It does not request GitHub resources or accept P globally.
func TestLiveProductLocalModelVerificationReview(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_PRODUCT_LOCAL") != "1" {
		t.Skip("explicit native local product model exercise only")
	}
	runLiveProductRoles(t, false)
}

func TestLiveProductScopedModelVerificationReview(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_PRODUCT_SCOPED") != "1" {
		t.Skip("explicit native scoped product exercise only")
	}
	runLiveProductRoles(t, true)
}

func runLiveProductRoles(t *testing.T, scoped bool) {
	t.Helper()
	binary, model, root, output, cli := os.Getenv("HARNESS_LIVE_CODEX_BINARY"), os.Getenv("HARNESS_LIVE_MODEL"), os.Getenv("HARNESS_LIVE_PRODUCT_ROOT"), os.Getenv("HARNESS_LIVE_PRODUCT_EVIDENCE"), os.Getenv("HARNESS_LIVE_HARNESS_BINARY")
	if !filepath.IsAbs(binary) || model == "" || !filepath.IsAbs(root) || !filepath.IsAbs(output) || !filepath.IsAbs(cli) {
		t.Fatal("absolute native/production harness binaries, NEW private root, model and evidence required")
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatal("native local product requires a new root")
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatal("native evidence must not overwrite a prior observation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	versionCtx, stopVersion := context.WithTimeout(ctx, 5*time.Second)
	version, err := verifiedNativeVersion(versionCtx, binary)
	stopVersion()
	if err != nil {
		t.Fatal("configured native version unsupported")
	}
	auth := os.Getenv("HARNESS_LIVE_SESSION_AUTH_FILE")
	if auth == "" {
		home := os.Getenv("CODEX_HOME")
		if home == "" {
			home, err = os.UserHomeDir()
			if err != nil {
				t.Fatal("resolve session auth source")
			}
			home = filepath.Join(home, ".codex")
		}
		auth = filepath.Join(home, "auth.json")
	}
	auth, err = filepath.EvalSymlinks(auth)
	if err != nil {
		t.Fatal("authorized current session authentication unavailable")
	}
	tokenLimit := int64(500000)
	if value := os.Getenv("HARNESS_LIVE_PRODUCT_TOKENS"); value != "" {
		tokenLimit, err = strconv.ParseInt(value, 10, 64)
		if err != nil || tokenLimit < 1 {
			t.Fatal("positive initial token limit required")
		}
	}
	if err := Run(ctx, []string{"init", "--root", root, "--model", model}, io.Discard, io.Discard); err != nil {
		t.Fatal("initialize fresh native root")
	}
	k, main, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal("open fresh native root")
	}
	t.Cleanup(func() {
		if err := k.Close(); err != nil {
			t.Error("close native store")
		}
	})
	limits := main.Limits
	limits.Tokens = tokenLimit
	repo := t.TempDir()
	files := map[string]string{"go.mod": "module example.invalid/harness-local-product\n\ngo 1.23\n", "math.go": "package product\n\nfunc Multiply(a, b int) int { return a + b }\n"}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0600); err != nil {
			t.Fatal("write fixture source")
		}
	}
	for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "native local fixture"}, {"config", "user.email", "native@example.invalid"}, {"add", "go.mod", "math.go"}, {"-c", "commit.gpgsign=false", "commit", "-m", "initial fixture"}} {
		if _, err := workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal("initialize fixture Git repository")
		}
	}
	base, err := workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal("resolve fixture base")
	}
	w, err := k.CreateProductWave(ctx, storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: main.SessionID, Source: "direct", SourceID: "native-local-product", OriginalRequest: "In Go, Multiply(a,b int) must return a*b for non-overflowing integers, including positive, negative and zero values. Independently add tests and review the exact tested candidate. Local workflow only: do not publish PRs, contact GitHub or create extra agents.", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "Multiply returns the exact integer product for positive, negative and zero cases without overflow."}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: limits}}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "work", Repository: "fixture/local-product", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal("admit native product fixture")
	}
	workID := "work"
	var split *liveProductSplit
	if scoped {
		prepared := prepareLiveProductSplit(t, ctx, k, w, cli, base)
		split = &prepared
		workID = prepared.Binding.Value.Child.Value.WorkItemID
	}
	instructions := []string{
		"Implement the original Multiply requirement in math.go, package product. Do not edit go.mod or add tests; a separate verification Task owns tests. Read the complete manifest and product_workspace reference. Publish short implementation notes as your own artifact. A claimed criterion_result must reference that owned published artifact in evidence_ids and list it in artifacts; without evidence use status=unverified and evidence_ids=[]. Complete your assignment using the provided schema_version=2 output with the exact workspace change reference; do not claim a new SHA or quality approval. Later verification/review is not remaining work in this assignment. Do not publish PRs, contact GitHub, spawn agents or create sessions.",
		`Independently verify the original Multiply requirement against the fixed product_subject. Change only tests/ paths. Add tests/behavior_test.go in package tests importing product "example.invalid/harness-local-product" and a TestMultiply covering positive, negative and zero cases without overflow. Publish a verification_plan JSON artifact and return its ID using product_verification with subject_id=product_subject.id, subject_digest=product_subject.digest (not value.candidate_digest), and the exact product_workspace change reference in schema_version=3. Plan schema_version=1 requires work_item_id=work, current scope_revision/policy_revision, input_candidate_id equal to product_subject.value.candidate.id, plan_revision=1, criteria_tests=[{criterion_id:AC,test_ref:tests/behavior_test.go::TestMultiply,expectation_basis:original AC integer product requirement,profile_id:unit}], requested_profiles=[], unverified_criteria=[], blockers=[], on_success=publish_pr. Use actual JSON strings/keys; read all supplied policy and subject data. Your exploratory checks do not replace the formal host runner. Complete this assignment without waiting for later review. Do not publish PRs, contact GitHub, spawn agents or create sessions.`,
		"Independently review the exact read-only product_subject candidate, implementation and added tests against original AC. Read product_machine_verification for the authenticated host unit result; it must already have passed and match this candidate. Prior verifier Task statements describe its earlier pre-run state. Keep work_item_refs=[] and verification=null. Publish your own review_report artifact and use schema_version=1 review.target_digest equal to the exact product_subject candidate_digest. Report truthful clean/findings/inconclusive; claimed original criterion evidence must reference your own report. Do not modify files, fabricate a verdict, approve GitHub, publish PRs, spawn agents or create sessions. Complete the review assignment without claiming Wave acceptance.",
	}
	if scoped {
		localID := split.Binding.Value.Scope.Criteria[0].ID
		instructions = []string{
			"Implement Multiply in math.go for the complete local obligation in your manifest.product_split_scopes entry (positive and zero integer multiplication). Do not edit go.mod or add tests; a separate verification Task owns tests. Read the full manifest and product_workspace reference. Publish your own implementation notes Artifact, list it in artifacts, and return the exact workspace change reference in schema_version=2. The Task criterion AC names the original whole requirement; leave AC unverified with no evidence because the sibling part is separate. Your local assignment can be completed independently of later roles. Do not publish PRs, contact GitHub, spawn agents or create sessions.",
			fmt.Sprintf("Independently verify the complete local obligation in your manifest.product_split_scopes entry against the fixed product_subject. Change only tests/ paths. Add tests/behavior_test.go in package tests importing product \"example.invalid/harness-local-product\" and TestMultiply for positive and zero cases. Publish a verification_plan JSON Artifact. Plan schema_version=1, work_item_id=%s, current scope_revision/policy_revision, input_candidate_id=product_subject.value.candidate.id, plan_revision=1, criteria_tests=[{criterion_id:%s,test_ref:tests/behavior_test.go::TestMultiply,expectation_basis:the complete positive and zero local requirement,profile_id:unit}], requested_profiles=[], unverified_criteria=[], blockers=[], on_success=publish_pr. Return schema_version=3 with product_verification subject_id=product_subject.id, subject_digest=product_subject.digest (not candidate_digest), plan_artifact_id=your actual published plan ID, and the exact product_workspace change reference. Read all supplied subject and policy data. Leave original whole Task criterion AC unverified with no evidence; local criterion %s belongs inside the plan. Do not publish PRs, contact GitHub, spawn agents or create sessions.", workID, localID, localID),
			fmt.Sprintf("Independently review the exact read-only product_subject candidate and tests against every local criterion in your manifest.product_split_scopes entry. Read product_machine_verification and require passed formal runner evidence for this exact SHA. Publish your own review_report Artifact and list it in artifacts. Return schema_version=4, work_item_refs=[], verification=null, review.target_digest=product_subject.value.candidate_digest and a truthful clean/findings/inconclusive verdict. Populate product_local_review.scope_digest with the exact scope_digest from that manifest entry and product_local_review.criteria with %s and every other included LOCAL criterion, each with truthful claimed/unverified status, basis and evidence_ids referencing your own report. Leave the original whole Task criterion AC unverified with no evidence. Your local claim is not whole-criterion or Wave acceptance. Do not modify files, fabricate a verdict, approve GitHub, publish PRs, spawn agents or create sessions.", localID),
		}
	}
	kinds, ids := []string{"work", "verification", "review"}, []string{"implement", "verify", "review"}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{}, Templates: []contract.FixReviewTemplate{}}
	for i, kind := range kinds {
		deps := []contract.TaskDependency{}
		if i > 0 {
			deps = append(deps, contract.TaskDependency{TaskID: ids[i-1]})
		}
		spec := contract.TaskSpec{ID: ids[i], Kind: kind, Instruction: instructions[i], Criteria: []string{"AC"}, WorkItemIDs: []string{workID}, DependsOn: deps, InputArtifactIDs: []string{}}
		plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: ids[i], Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{ids[i]}}, FailurePolicy: "drain_and_block"})
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "native-local-plan", plan); err != nil {
		t.Fatal("adopt local product role plan")
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Clean(filepath.Join(cwd, "../../scripts/isolate-linux"))
	template := t.TempDir()
	if err := os.Chmod(template, 0700); err != nil {
		t.Fatal(err)
	}
	toolProfile := "product_v2"
	if scoped {
		toolProfile = "product_v5"
	}
	runtimeFile := operatorFile(t, RuntimeConfig{SchemaVersion: 1, ProductTools: true, ProductToolProfile: toolProfile, CodexBinary: binary, WorkerCodexHome: template, SessionAuthFile: auth, Launcher: []string{launcher}, LauncherContract: "argv_passthrough_v1", IsolationVerified: true, GitDirectories: []string{filepath.Join(repo, ".git")}})
	executorConfig := ProductExecutorConfig{
		SchemaVersion: 1, EverySeconds: 1, IsolationVerified: true,
		Repositories: []product.ExecutionRepository{{
			Name: "fixture/local-product", BaseBranch: "trunk", InitialSHA: base,
			Policy: storage.ProductVerificationPolicy{
				RepositoryID: 17, Repository: repo, VerifierPaths: []string{"tests/"},
				Verification: policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{
					"unit": {Revision: 1, Argv: []string{"/usr/local/go/bin/go", "test", "-v", "./..."}, TimeoutSeconds: 90, SuccessPattern: "(?m)^--- PASS: TestMultiply"},
				}},
			},
			Runner: storage.ProductRunnerRuntime{Root: filepath.Join(root, "runner-jobs"), LauncherContract: "argv_passthrough_v1", Launcher: []string{launcher}, ForbiddenPaths: []string{}},
		}},
	}
	if scoped {
		executorConfig.Publication = &ProductPublicationConfig{Workers: 1, ReviewRoutes: []ProductReviewRouteConfig{}, SourcePolicies: []product.IssueSourcePolicy{split.Policy}}
	}
	executorFile := operatorFile(t, executorConfig)
	log, err := os.OpenFile(filepath.Join(root, "native-daemon.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal("private daemon log")
	}
	defer log.Close()
	cmd := exec.Command(cli, "wave", "run", "--root", root, "--runtime", runtimeFile, "--product-executor", executorFile)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal("start production product daemon")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	evidence := map[string]any{"stage": "native local product implementation/verification/runner/review", "binary_version": version, "model": model, "executed_tests": 1, "token_limit": tokenLimit, "local_execution": "not_accepted", "full_p_acceptance": "not_accepted", "cloud_native_approval": "unknown_not_observed", "github_operations": 0, "repository": "fixture/local-product", "base_sha": base, "plan_origin": "host_authored_local_fixture", "credential_values_recorded": false, "private_paths_recorded": false}
	if scoped {
		evidence["stage"] = "native registered split-child implementation/verification/runner/local review"
		evidence["source_origin"] = "controlled local GitHub adapter through production publication source observer"
		evidence["split_children"] = len(split.Registered.Value.Children)
		evidence["scope_digest"] = split.Binding.Value.ScopeDigest
		evidence["split_depth"] = split.Binding.Value.Child.Value.Depth
		evidence["parent_acceptance"] = "not_accepted_peer_unmerged"
	}
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case stopErr := <-done:
				exited = true
				if stopErr != nil {
					t.Error("native daemon shutdown failed")
				}
			case <-time.After(45 * time.Second):
				t.Error("native daemon shutdown unconfirmed")
				_ = cmd.Process.Kill()
				select {
				case <-done:
					exited = true
				case <-time.After(5 * time.Second):
					t.Error("native daemon termination unconfirmed")
				}
			}
		}
		clean := context.Background()
		var live, runs, mainRuns, taskRuns int
		if err := k.DB.QueryRowContext(clean, "SELECT count(*) FROM records WHERE kind='managed_process' AND json_extract(payload,'$.state')<>'stopped'").Scan(&live); err != nil {
			t.Error("read native physical stop evidence")
			live = -1
		}
		if err := k.DB.QueryRowContext(clean, "SELECT count(*) FROM records WHERE kind='agent_run'").Scan(&runs); err != nil {
			t.Error("read native model run evidence")
		}
		if err := k.DB.QueryRowContext(clean, "SELECT count(*) FROM records WHERE kind='agent_run' AND work_item_id=?", main.SessionID).Scan(&mainRuns); err != nil {
			t.Error("read native parent run evidence")
		}
		if err := k.DB.QueryRowContext(clean, "SELECT count(*) FROM records r JOIN kernel_sessions s ON s.id=r.work_item_id WHERE r.kind='agent_run' AND s.kind='task'").Scan(&taskRuns); err != nil {
			t.Error("read native Task model run evidence")
		}
		evidence["model_runs"], evidence["main_runs"], evidence["all_managed_processes_stopped"] = runs, mainRuns, live == 0
		evidence["task_model_runs"] = taskRuns
		if scoped {
			rows, err := k.DB.QueryContext(clean, "SELECT payload FROM records WHERE kind='agent_run' AND work_item_id=?", main.SessionID)
			if err != nil {
				t.Error("read scoped parent wait evidence")
			} else {
				actions := []string{}
				for rows.Next() {
					var raw []byte
					if err := rows.Scan(&raw); err != nil {
						t.Error("read scoped parent run")
						continue
					}
					var run contract.SessionRun
					var result contract.MainResult
					if json.Unmarshal(raw, &run) != nil || run.State != "completed" || contract.DecodeStrict([]byte(run.Final), &result) != nil {
						t.Error("scoped parent did not finish a verifiable wait")
						continue
					}
					for _, decision := range result.Decisions {
						actions = append(actions, decision.Action)
						if decision.Action != "wait" {
							t.Error("scoped parent changed the host fixture plan")
						}
					}
				}
				if err := rows.Err(); err != nil {
					t.Error("read scoped parent waits")
				}
				rows.Close()
				evidence["main_decisions"] = actions
			}
			var management int
			if err := k.DB.QueryRowContext(clean, "SELECT count(*) FROM kernel_tool_operations WHERE session_id=? AND name IN ('wave_create','wave_plan','product_split','product_split_nested','pr_publish','pr_review_request')", main.SessionID).Scan(&management); err != nil || management != 0 {
				t.Error("scoped parent performed an unexpected management operation")
			}
		}

		if live != 0 {
			t.Error("native physical stopping not confirmed")
		}
		if taskRuns != 3 || runs != taskRuns+mainRuns || !scoped && mainRuns != 0 {
			t.Error("unexpected native role run count")
		}
		budget, budgetErr := k.WaveRunBudget(clean, w.ID)
		if budgetErr != nil {
			t.Error("read native token observations")
		} else {
			evidence["observed_tokens_lower_bound"] = budget.ObservedTokens
		}
		evidence["observed_at"] = time.Now().UTC()
		if t.Failed() {
			evidence["local_execution"] = "not_accepted"
		}
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil || os.WriteFile(output, append(raw, '\n'), 0600) != nil {
			t.Error("write sanitized native product evidence")
		}
	})
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("native product deadline reached")
		case err := <-done:
			exited = true
			_ = err
			t.Fatal("native daemon exited before local quality completed")
		case <-ticker.C:
		}
		tasks, err := k.Tasks(ctx, w.ID)
		if err != nil {
			t.Fatal("read native product tasks")
		}
		complete := len(tasks) == 3
		for _, task := range tasks {
			if task.State == contract.TaskFailed || task.State == contract.TaskCanceled {
				t.Fatal("native product Task failed or canceled")
			}
			complete = complete && task.State == contract.TaskCompleted
		}
		if !complete {
			continue
		}
		quality, err := k.CheckProductQuality(ctx, w.Target(), workID)
		if err != nil {
			t.Fatal("native local product quality lacks authenticated proof")
		}
		if scoped {
			if quality.Value.ScopeDigest != split.Binding.Value.ScopeDigest || len(quality.Value.Criteria) != len(split.Binding.Value.Scope.Included) {
				t.Fatal("native local quality lost complete qualified scope")
			}
			if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); err == nil {
				t.Fatal("child quality completed delegated Root")
			}
			coverage, err := k.ProductCoverage(ctx, w.Target())
			if err != nil || len(coverage.Value.Merged) != 0 || len(coverage.Value.UnmergedWorkItems) != 2 {
				t.Fatal("native part completed parent coverage")
			}
			evidence["controlled_issue_posts"] = liveProductSplitPosts(t, split.Directory)
			if evidence["controlled_issue_posts"] != 2 {
				t.Fatal("split retried controlled Issue creation")
			}
		}
		sessions, threads := map[string]bool{}, map[string]bool{}
		account := ""
		for _, task := range tasks {
			if scoped {
				var result contract.TaskResult
				if err := k.Record(ctx, "task_result", task.ResultID, &result); err != nil {
					t.Fatal("native scoped Task result proof missing")
				}
				for _, criterion := range result.CriterionResults {
					if criterion.CriterionID != "AC" || criterion.Status != "unverified" {
						t.Fatal("native local part claimed whole criterion")
					}
				}
				if task.Spec.Kind == "review" && (result.SchemaVersion != 4 || result.ProductLocalReview == nil || result.ProductLocalReview.ScopeDigest != split.Binding.Value.ScopeDigest) {
					t.Fatal("native local review used the wrong scope/output version")
				}
			}
			b, err := k.SessionBinding(ctx, task.SessionID)
			if err != nil || b.ThreadID == "" {
				t.Fatal("native Task thread missing")
			}
			sessions[b.ID], threads[b.ThreadID] = true, true
			if task.Spec.Kind == "review" && b.Sandbox != "read-only" {
				t.Fatal("native review was writable")
			}
			var authBinding storage.SessionAuthentication
			if err := k.Record(ctx, "session_authentication", b.ID, &authBinding); err != nil || authBinding.AccountID == "" {
				t.Fatal("native Task authentication proof missing")
			}
			if account == "" {
				account = authBinding.AccountID
			} else if account != authBinding.AccountID {
				t.Fatal("native role authentication changed")
			}
			if task.Spec.Kind == "verification" {
				source, err := k.ReadProductTaskCandidate(ctx, task.ResultID)
				if err != nil || source.Value.VerificationPlan == nil {
					t.Fatal("native verifier adoption missing")
				}
				formal, err := k.ReadProductVerificationEvidence(ctx, source.Value.VerificationPlan.ID)
				if err != nil || !formal.Value.Complete || !formal.Value.Passed {
					t.Fatal("native formal execution did not pass")
				}
				for _, job := range formal.Value.Jobs {
					if job.Completion == nil || job.Completion.Result.TestedSHA != quality.Value.Candidate.SHA || job.Completion.Result.Status != "passed" {
						t.Fatal("native tested SHA mismatch")
					}
				}
				evidence["formal_jobs"] = len(formal.Value.Jobs)
			}
		}
		if len(sessions) != 3 || len(threads) != 3 {
			t.Fatal("native roles reused a completed session/thread")
		}
		var admissions int
		if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='product_model_admission'").Scan(&admissions); err != nil || admissions != 3 {
			t.Fatal("native role admission evidence missing")
		}
		evidence["model_admissions"] = admissions
		var external, completions int
		if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_tool_operations WHERE name LIKE 'pr_%'").Scan(&external); err != nil || external != 0 {
			t.Fatal("local-only native test requested GitHub work")
		}
		if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind IN ('product_wave_completion','wave_completion','product_cloud_completion')").Scan(&completions); err != nil || completions != 0 {
			t.Fatal("local quality became P completion or native Cloud response")
		}
		sum := sha256.Sum256([]byte(account))
		evidence["authentication_subject_sha256"] = hex.EncodeToString(sum[:])
		evidence["task_sessions_unique"], evidence["task_threads_unique"] = true, true
		evidence["quality_digest"], evidence["tested_sha"] = quality.Digest, quality.Value.Candidate.SHA
		evidence["local_execution"] = "completed_with_verified_local_quality"
		evidence["task_kinds"] = kinds
		return
	}
}
