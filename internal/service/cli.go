package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func CLI(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("commands: init, serve, status, inspect, metrics, pause, resume, hold, release, cancel, retry, interrupt, associate, split, adopt, webhook")
	}
	command := args[0]
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	configPath := fs.String("config", "", "trusted config JSON outside repository")
	id := fs.String("id", "", "WorkItem ID")
	reason := fs.String("reason", "", "operator reason")
	file := fs.String("file", "", "proposal or scope JSON")
	operation := fs.String("operation", "", "operation/run ID")
	evidence := fs.String("evidence", "", "reconciled absence evidence reference")
	turn := fs.String("turn", "", "saved external turn ID")
	listen := fs.String("listen", "127.0.0.1:8080", "webhook bind address")
	repo := fs.String("repo", "", "GitHub OWNER/REPOSITORY")
	repoPath := fs.String("repository-path", "", "local clone path")
	model := fs.String("model", "", "model available to dedicated worker auth")
	binary := fs.String("codex-binary", "", "native Codex executable")
	launcher := fs.String("launcher", "", "absolute isolation launcher")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}
	if command == "init" {
		return initConfig(ctx, *configPath, *repo, *repoPath, *model, *binary, *launcher)
	}
	cfg, err := Load(*configPath)
	if err != nil {
		return err
	}
	controller, err := New(cfg)
	if err != nil {
		return err
	}
	defer controller.Store.Close()
	s := controller.Store
	encode := func(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }
	switch command {
	case "serve":
		return controller.Run(ctx)
	case "status":
		items, err := s.List(ctx)
		if err != nil {
			return err
		}
		return encode(items)
	case "inspect":
		w, err := s.Get(ctx, *id)
		if err != nil {
			return err
		}
		out := map[string]any{"work_item": w}
		for _, kind := range []string{"scope", "candidate", "plan", "agent_run", "validation", "review_round", "gate", "operation_error", "assessment", "source", "residual", "job_intent", "parent_completion", "parent_final_error", "continuation_hold"} {
			records, err := s.Records(ctx, kind, w.ID)
			if err != nil {
				return err
			}
			out[kind] = records
		}
		return encode(out)
	case "gc":
		return controller.GC(ctx)
	case "metrics":
		metrics, err := controller.Metrics(ctx, *id)
		if err != nil {
			return err
		}
		return encode(metrics)
	case "webhook":
		secret := os.Getenv("HARNESS_WEBHOOK_SECRET")
		if secret == "" {
			return errors.New("HARNESS_WEBHOOK_SECRET is required")
		}
		server := &http.Server{Addr: *listen, Handler: github.Webhook([]byte(secret), s), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		}()
		err = server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	w, err := s.Get(ctx, *id)
	if err != nil {
		return err
	}
	event := domain.Event{ID: fmt.Sprintf("operator:%s:%d", command, time.Now().UnixNano()), WorkItemID: w.ID, Kind: command, Reason: *reason, Attempt: w.Attempt, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Candidate: w.Candidate}
	switch command {
	case "pause", "resume", "hold", "release", "cancel":
		return s.Apply(ctx, event)
	case "retry":
		if *reason == "" {
			return errors.New("--reason is required; budgets are retained")
		}
		if *operation != "" {
			if *evidence == "" {
				return errors.New("--evidence proving unexecuted is required")
			}
			return s.RetryUnexecutedBounded(ctx, *operation, *evidence+": "+*reason, cfg.Limits.NetworkRetries)
		}
		ops, err := s.Operations(ctx)
		if err != nil {
			return err
		}
		for _, op := range ops {
			if op.WorkItemID == w.ID && (op.State == "sending" || op.State == "unknown") {
				return errors.New("reconcile outstanding operation before retry")
			}
		}
		runs, err := s.Records(ctx, "agent_run", w.ID)
		if err != nil {
			return err
		}
		for _, data := range runs {
			var run worker.Run
			if err = json.Unmarshal(data, &run); err != nil {
				return err
			}
			var quiesced bool
			if run.State != "completed" && s.Record(ctx, "agent_quiesced", run.ID, &quiesced) != nil {
				return errors.New("turn process quiescence is unverified; associate and reconcile first")
			}
		}
		if w.RecoveryPhase == domain.Validating {
			raw, err := s.Records(ctx, "job_intent", w.ID)
			if err != nil {
				return err
			}
			for _, data := range raw {
				var intent JobIntent
				if err = json.Unmarshal(data, &intent); err != nil {
					return err
				}
				if _, err = controller.Jobs.Read(intent.Spec); err != nil {
					return errors.New("orphan runner must complete or be explicitly canceled before retry")
				}
			}
		}
		if w.RecoveryPhase != domain.Validating && w.RepairRounds >= cfg.Limits.RepairRounds {
			return errors.New("repair budget exhausted")
		}
		event.Kind = "operator_retry"
		return s.Apply(ctx, event)
	case "cancel-job":
		if *operation == "" || *reason == "" {
			return errors.New("--operation and --reason are required")
		}
		raw, err := s.Records(ctx, "job_intent", w.ID)
		if err != nil {
			return err
		}
		found := false
		for _, data := range raw {
			var intent JobIntent
			if err = json.Unmarshal(data, &intent); err != nil {
				return err
			}
			if intent.Spec.JobID == *operation {
				found = true
			}
		}
		if !found {
			return errors.New("job is not associated with this work item")
		}
		if err = s.Put(ctx, "operator_audit", event.ID, w.ID, event); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(cfg.Root, "jobs", *operation+".cancel"), []byte(*reason), 0600)
	case "interrupt":
		if *operation == "" || *reason == "" {
			return errors.New("--operation and --reason are required")
		}
		var run worker.Run
		if err = s.Record(ctx, "agent_run", *operation, &run); err != nil {
			return err
		}
		return s.Put(ctx, "interrupt", *operation, w.ID, map[string]string{"reason": *reason})
	case "reconcile":
		if *operation == "" || *reason == "" {
			return errors.New("--operation and --reason are required")
		}
		return s.RestoreReconciliation(ctx, w.ID, *operation, *reason)
	case "associate":
		if *operation == "" || *turn == "" || *reason == "" {
			return errors.New("--operation, --turn, --reason are required")
		}
		var run worker.Run
		if err = s.Record(ctx, "agent_run", *operation, &run); err != nil {
			return err
		}
		if run.TurnID != "" && run.TurnID != *turn {
			return errors.New("run already associated with another turn")
		}
		run.TurnID = *turn
		run.State = "unknown"
		if err = s.Put(ctx, "agent_run", run.ID, w.ID, run); err != nil {
			return err
		}
		return s.Put(ctx, "operator_audit", event.ID, w.ID, map[string]any{"action": "associate", "run": run.ID, "turn": *turn, "reason": *reason})
	case "rereview":
		return controller.Rereview(ctx, w.ID, *reason)
	case "split":
		if *file == "" {
			return errors.New("--file is required")
		}
		body, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		scope, err := controller.current(ctx, w)
		if err != nil {
			return err
		}
		depth := 0
		err = s.Record(ctx, "split_depth", w.ID, &depth)
		if err != nil && !missing(err) {
			return err
		}
		proposal, err := splitting.Decode(body, w.ID, scope, depth, cfg.Limits.SplitDepth, cfg.Limits.SplitChildren)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(body)
		ref := fmt.Sprintf("split-%x", digest[:16])
		if err = s.Put(ctx, "split_proposal", ref, w.ID, proposal); err != nil {
			return err
		}
		event.Kind = "plan_split"
		event.Ref = ref
		return s.Apply(ctx, event)
	case "adopt":
		var previousDigest string
		if err = s.Record(ctx, "policy_snapshot", w.ID, &previousDigest); err != nil {
			return err
		}
		if previousDigest != controller.policyDigest() && cfg.Verification.Revision <= w.PolicyRevision {
			return errors.New("policy changes require an increased verification.revision")
		}
		if err = controller.ensurePolicy(ctx); err != nil {
			return err
		}
		if *file == "" || *reason == "" {
			return errors.New("--file and --reason are required")
		}
		body, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		var scope verification.Scope
		if err = contract.DecodeStrict(body, &scope); err != nil {
			return err
		}
		if scope.Revision != w.ScopeRevision+1 {
			return errors.New("scope revision must increment exactly once")
		}
		if err = scope.Validate(); err != nil {
			return err
		}
		issue, err := controller.GitHub.GetIssue(ctx, w.Issue)
		if err != nil {
			return err
		}
		if w.Phase == domain.Merged {
			return controller.AdoptContinuation(ctx, w, scope, *reason)
		}
		return s.Adopt(ctx, w.ID, scope.Revision, cfg.Verification.Revision, map[string]any{"scope": scope, "source": Source{digestIssue(issue)}, "adoption_reason": *reason, "policy_snapshot": controller.policyDigest()})
	}
	return fmt.Errorf("unknown command %q", command)
}
func digestIssue(issue github.Issue) string { return digest(issue) }
func initConfig(ctx context.Context, path, repository, repositoryPath, model, binary, launcher string) error {
	if repository == "" || repositoryPath == "" || model == "" || binary == "" || launcher == "" {
		return errors.New("init requires --repo, --repository-path, --model, --codex-binary, --launcher")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	repositoryPath, err = filepath.Abs(repositoryPath)
	if err != nil {
		return err
	}
	b, err := exec.CommandContext(ctx, "gh", "api", "repos/"+repository).Output()
	if err != nil {
		return err
	}
	var metadata struct {
		ID            int64  `json:"id"`
		DefaultBranch string `json:"default_branch"`
	}
	if err = json.Unmarshal(b, &metadata); err != nil {
		return err
	}
	root := filepath.Join(filepath.Dir(absolute), "state")
	cfg := Config{LauncherContract: "argv_passthrough_v1", CIRefMode: "head", PriorityLabels: []string{"priority:high", "priority:normal", "priority:low"}, LogRetentionDays: 30, MaxArtifactBytes: 1 << 30, Version: 1, Repository: repository, RepositoryID: metadata.ID, RepositoryPath: repositoryPath, BaseBranch: metadata.DefaultBranch, Root: root, Model: model, CodexBinary: binary, WorkerCodexHome: filepath.Join(filepath.Dir(absolute), "worker-codex"), Launcher: []string{launcher}, ReadyLabel: "harness:ready", HoldLabel: "harness:hold", Verification: policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"go", "test", "-v", "./..."}, TimeoutSeconds: 900, SuccessPattern: policy.GoTestSuccessPattern}}}, VerifierPaths: []string{"tests/"}, Review: review.Policy{RequiredApprovals: 1, TrustedReviewerIDs: []int64{}}, ReviewTrigger: "@codex review", RequiredChecks: []gate.RequiredCheck{}, MergeMethod: "squash", Limits: Limits{NetworkRetries: 3, ValidationJobs: 100, RepairRounds: 3, RunnerRetries: 1, ReviewRequests: 2, SplitDepth: 2, SplitChildren: 8, Tokens: 1000000, TurnSeconds: 1800, ReviewSeconds: 3600}, ReconcileSeconds: 60}
	if err = cfg.Validate(); err != nil {
		return err
	}
	if within(repositoryPath, absolute) {
		return errors.New("config must be outside repository")
	}
	if err = os.MkdirAll(filepath.Dir(absolute), 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(absolute, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}
func (c *Controller) Metrics(ctx context.Context, id string) (map[string]any, error) {
	raw, err := c.Store.Records(ctx, "agent_run", id)
	if err != nil {
		return nil, err
	}
	calls := map[string]int{}
	tokens := int64(0)
	threadTotals := map[string]int64{}
	unknown := 0
	waiting, phaseUnknown, acknowledged := 0, 0, 0
	for _, data := range raw {
		var run worker.Run
		if err = json.Unmarshal(data, &run); err != nil {
			return nil, err
		}
		calls[run.Reason]++
		if run.TurnID != "" {
			acknowledged++
		}
		var phase domain.Phase
		if err = c.Store.Record(ctx, "invocation_phase", run.ID, &phase); err != nil {
			phaseUnknown++
		} else if phase == domain.Validating || phase == domain.Reviewing || phase == domain.MergePending {
			waiting++
		}
		var usage struct {
			Total struct {
				TotalTokens *int64 `json:"totalTokens"`
			} `json:"total"`
		}
		if json.Unmarshal(run.Usage, &usage) != nil || usage.Total.TotalTokens == nil {
			unknown++
		} else {
			if *usage.Total.TotalTokens > threadTotals[run.ThreadID] {
				threadTotals[run.ThreadID] = *usage.Total.TotalTokens
			}
		}
	}
	for _, total := range threadTotals {
		tokens += total
	}
	items, err := c.Store.List(ctx)
	if err != nil {
		return nil, err
	}
	durations := map[string]int64{}
	lifecycle := map[string]any{}
	for _, w := range items {
		if id != "" && w.ID != id {
			continue
		}
		for phase, nanos := range w.PhaseDurations {
			durations[phase] += nanos
		}
		if !w.Terminal() && !w.PhaseStartedAt.IsZero() {
			duration := time.Since(w.PhaseStartedAt)
			if duration >= 0 {
				durations[string(w.Phase)] += duration.Nanoseconds()
			}
		}
		entry := map[string]any{"created_at": w.CreatedAt, "merge_observed_at": w.MergeObservedAt, "repair_rounds": w.RepairRounds}
		if !w.CreatedAt.IsZero() && !w.MergeObservedAt.IsZero() {
			entry["time_to_merge_observation_seconds"] = w.MergeObservedAt.Sub(w.CreatedAt).Seconds()
		}
		lifecycle[w.ID] = entry
	}
	return map[string]any{"turn_start_attempts_by_reason": calls, "acknowledged_turns": acknowledged, "runs_without_phase_evidence": phaseUnknown, "observed_tokens": tokens, "runs_with_unavailable_usage": unknown, "cloud_usage": "unavailable", "turn_starts_during_wait": waiting, "phase_duration_nanoseconds": durations, "lifecycle": lifecycle}, nil
}
