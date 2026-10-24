package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func workspacePreparerFixture(t *testing.T, configure ...func(*contract.PlanProposal)) (WorkspacePreparer, storage.ProductWorkspaceRequest, domain.Wave, domain.Task) {
	t.Helper()
	return workspacePreparerFixtureWithAdmission(t, nil, configure...)
}

func workspacePreparerFixtureWithAdmission(t *testing.T, request *storage.ProductWaveRequest, configure ...func(*contract.PlanProposal)) (WorkspacePreparer, storage.ProductWorkspaceRequest, domain.Wave, domain.Task) {
	t.Helper()
	ctx := context.Background()
	a, _ := issueAdmissionFixture(t)
	var w domain.Wave
	var err error
	if request == nil {
		w, err = a.Admit(ctx, 42)
	} else {
		w, err = a.Store.CreateProductWave(ctx, *request)
	}
	if err != nil {
		t.Fatal(err)
	}
	work := contract.TaskSpec{ID: "implementation", Kind: "work", Instruction: "implement the registered acceptance criterion", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{w.SourceID}}
	review := contract.TaskSpec{ID: "review", Kind: "review", Instruction: "independently review the fixed candidate", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: work.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{w.SourceID}}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "implementation", Tasks: []contract.TaskSpec{work}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{work.ID}}, FailurePolicy: "drain_and_block"}, {ID: "review", Tasks: []contract.TaskSpec{review}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{review.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	for _, apply := range configure {
		apply(&plan)
	}
	admitted, err := a.Store.AdoptPlan(ctx, w.Target(), "prepare-product", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := a.Store.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	w, err = a.Store.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "workspace test"}, {"config", "user.email", "workspace@example.invalid"}} {
		if _, err := workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "base.txt"), []byte("base content\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".gitattributes"), []byte("*.txt filter=untrusted\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "--all"}, {"-c", "commit.gpgsign=false", "commit", "-m", "base"}} {
		if _, err := workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	base, err := workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return WorkspacePreparer{Store: a.Store}, storage.ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: 17, Repository: repo, BaseSHA: base}, w, task
}
func TestProductWorkspacePreparerCreatesFixedPrivateCheckoutWithoutRunOrCandidate(t *testing.T) {
	ctx := context.Background()
	p, request, w, task := workspacePreparerFixture(t)
	marker := filepath.Join(t.TempDir(), "unexpected-controller-code")
	hooks := t.TempDir()
	if err := os.WriteFile(filepath.Join(hooks, "post-checkout"), []byte("#!/bin/sh\ntouch '"+marker+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"config", "core.hooksPath", hooks}, {"config", "filter.untrusted.smudge", "touch '" + marker + "'; cat"}, {"config", "filter.untrusted.clean", "touch '" + marker + "'; cat"}, {"config", "filter.untrusted.required", "true"}} {
		if _, err := workspace.Git(ctx, request.Repository, args...); err != nil {
			t.Fatal(err)
		}
	}
	ready, err := p.Prepare(ctx, request)
	if err != nil || ready.State != "ready" || ready.Proof == nil || ready.Proof.HeadSHA != request.BaseSHA {
		t.Fatal(ready, err)
	}
	if ready.Value.CWD != filepath.Join(p.Store.Root, "workspaces", task.SessionID) || ready.Value.Branch != "harness/"+task.SessionID {
		t.Fatal(ready.Value)
	}
	info, err := os.Lstat(ready.Value.CWD)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal(info, err)
	}
	data, err := os.ReadFile(filepath.Join(ready.Value.CWD, "base.txt"))
	if err != nil || string(data) != "base content\n" {
		t.Fatal(string(data), err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("repository hooks/filters executed", err)
	}
	if err := workspace.Verify(ctx, request.Repository, ready.Value.CWD, ready.Value.Branch); err != nil {
		t.Fatal(err)
	}
	b, err := p.Store.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "workspace-write" || b.ThreadID != "" {
		t.Fatal(b, err)
	}
	reservation, err := p.Store.OperationReservation(ctx, ready.ID)
	if err != nil || reservation.State != "released" || reservation.Proof == nil || reservation.Proof.Disposition != "process_stopped" {
		t.Fatal(reservation, err)
	}
	again, err := p.Prepare(ctx, request)
	if err != nil || !reflect.DeepEqual(ready, again) {
		t.Fatal("exact replay ran another checkout", again, err)
	}
	tasks, err := p.Store.ReadyTasks(ctx, storage.ReadyRequest{Target: w.Target()})
	if err != nil || len(tasks.Tasks) != 0 || len(tasks.Held) == 0 || tasks.Held[0].Reason != "product_readiness_unavailable" {
		t.Fatal("checkout proof skipped product result gates", tasks, err)
	}
	var runs, candidates int
	if err := p.Store.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal(runs, err)
	}
	if err := p.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate')").Scan(&candidates); err != nil || candidates != 0 {
		t.Fatal("checkout became candidate", candidates, err)
	}
}
func TestProductWorkspacePreparerCannotRestartUnknownLaunch(t *testing.T) {
	ctx := context.Background()
	p, request, _, _ := workspacePreparerFixture(t)
	reserved, err := p.Store.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := p.Store.ClaimProductWorkspace(ctx, request.TaskID, reserved.Digest)
	if err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	observed, err := p.Prepare(ctx, request)
	if !errors.Is(err, storage.ErrProductWorkspaceHeld) || observed.State != "launching" {
		t.Fatal(observed, err)
	}
	if _, err := os.Stat(reserved.Value.CWD); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unknown launch started another Git mutation", err)
	}
	branches, err := workspace.Git(ctx, request.Repository, "for-each-ref", "--format=%(refname)", "refs/heads/harness/")
	if err != nil || strings.TrimSpace(branches) != "" {
		t.Fatal("unknown retry created branch", branches, err)
	}
	reservation, err := p.Store.OperationReservation(ctx, reserved.ID)
	if err != nil || reservation.State != "held" {
		t.Fatal("unknown retry released owner resources", reservation, err)
	}
}
func TestProductWorkspacePreparerPreservesPreexistingDataAndFailedHistory(t *testing.T) {
	ctx := context.Background()
	p, request, _, task := workspacePreparerFixture(t)
	cwd := filepath.Join(p.Store.Root, "workspaces", task.SessionID)
	if err := os.MkdirAll(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(cwd, "keep-user-data")
	if err := os.WriteFile(keep, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	failed, err := p.Prepare(ctx, request)
	if err == nil || failed.State != "failed" || failed.Proof == nil || failed.Proof.Outcome != "failed" {
		t.Fatal(failed, err)
	}
	data, err := os.ReadFile(keep)
	if err != nil || string(data) != "preserve" {
		t.Fatal("preexisting destination changed", string(data), err)
	}
	replay, err := p.Prepare(ctx, request)
	if !errors.Is(err, storage.ErrProductWorkspaceHeld) || !reflect.DeepEqual(failed, replay) {
		t.Fatal("failed preparation silently retried", replay, err)
	}
	reservation, err := p.Store.OperationReservation(ctx, failed.ID)
	if err != nil || reservation.State != "released" {
		t.Fatal("known stopped failure retained resources", reservation, err)
	}
	b, err := p.Store.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "read-only" {
		t.Fatal("failed checkout widened worker permission", b, err)
	}
}

func TestProductWorkspacePreparerDisablesNewBranchConditionalFiltersBeforeCheckout(t *testing.T) {
	ctx := context.Background()
	p, request, _, _ := workspacePreparerFixture(t)
	marker := filepath.Join(t.TempDir(), "conditional-filter-ran")
	if err := os.WriteFile(filepath.Join(request.Repository, ".gitattributes"), []byte("*.txt filter=conditional\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "--all"}, {"-c", "commit.gpgsign=false", "commit", "-m", "conditional attributes"}} {
		if _, err := workspace.Git(ctx, request.Repository, args...); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	request.BaseSHA, err = workspace.Resolve(ctx, request.Repository, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	conditional := filepath.Join(t.TempDir(), "conditional-git-config")
	config := "[filter \"conditional\"]\n required = true\n smudge = touch '" + marker + "'; cat\n clean = touch '" + marker + "'; cat\n"
	if err := os.WriteFile(conditional, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.Git(ctx, request.Repository, "config", "includeIf.onbranch:harness/**.path", conditional); err != nil {
		t.Fatal(err)
	}
	ready, err := p.Prepare(ctx, request)
	if err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("new-branch conditional filter escaped source config probe", err)
	}
	data, err := os.ReadFile(filepath.Join(ready.Value.CWD, "base.txt"))
	if err != nil || string(data) != "base content\n" {
		t.Fatal(string(data), err)
	}
}
