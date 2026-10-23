package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

func productRegistrationFixture(t *testing.T, source string) (*KernelStore, domain.Wave, ProductWorkItemDefinition) {
	t.Helper()
	k, request := waveStoreFixture(t)
	request.Source, request.SourceID = source, "registered-source"
	request.Criteria = append(request.Criteria, domain.WaveCriterion{ID: "extra", Requirement: "additional behavior"})
	request.Policy.Stage, request.Policy.Product = "P", true
	w, err := k.CreateWave(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, ProductWorkItemDefinition{ID: "work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"extra", "AC"}}
}
func registrationManifest(t *testing.T, k *KernelStore, session string, target contract.Target) sessionManifest {
	t.Helper()
	snapshot, err := k.BuildSessionInput(context.Background(), session, []contract.Target{target})
	if err != nil {
		t.Fatal(err)
	}
	ref := inputContext(t, snapshot).Waves[0].Manifest
	var saved savedInputManifest
	if err := k.Record(context.Background(), "session_input_manifest", ref.ID, &saved); err != nil {
		t.Fatal(err)
	}
	var manifest sessionManifest
	if err := json.Unmarshal(saved.Data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestProductWorkItemRegistrationBeforeCandidateIsAtomicIdempotentAndSourceOwned(t *testing.T) {
	for _, source := range []string{"direct", "github"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			k, w, definition := productRegistrationFixture(t, source)
			saved, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition)
			if err != nil {
				t.Fatal(err)
			}
			definition.Criteria = []string{"AC", "extra"}
			replay, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition)
			if err != nil || !reflect.DeepEqual(saved, replay) || saved.Value.Source != source || saved.Value.SourceID != w.SourceID {
				t.Fatal(saved, replay, err)
			}
			var membership struct {
				Target      contract.Target `json:"target"`
				CandidateID string          `json:"candidate_id"`
			}
			if err := k.Record(ctx, "wave_work_item", definition.ID, &membership); err != nil || membership.Target != w.Target() || membership.CandidateID != "" {
				t.Fatal(membership, err)
			}
			for _, table := range []string{"kernel_run_bindings", "kernel_tool_operations", "kernel_dispatch_intents"} {
				var count int
				if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatal("registration admitted execution", table, count, err)
				}
			}
			var projections int
			if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate')").Scan(&projections); err != nil || projections != 0 {
				t.Fatal("registration manufactured a candidate", projections, err)
			}
			path := k.Root + "/state.db"
			k.Close()
			reopened, err := OpenExistingKernel(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if again, err := reopened.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil || !reflect.DeepEqual(saved, again) {
				t.Fatal(again, err)
			}
		})
	}
}

func TestProductWorkItemRegistrationFeedsRoleFilteredInputsButKeepsReadinessHeld(t *testing.T) {
	ctx := context.Background()
	k, w, first := productRegistrationFixture(t, "direct")
	first.Criteria = []string{"AC"}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "other-work"
	second.Criteria = []string{"extra"}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", second); err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].WorkItemIDs = []string{first.ID}
	plan.Phases[1].Tasks[0].WorkItemIDs = []string{second.ID}
	plan.Phases[1].Tasks[0].Criteria = []string{"extra"}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "product-plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	mainSnapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	reserveInputRun(t, k, "main", mainSnapshot)
	parent := readSavedInput(t, k, "main", inputContext(t, mainSnapshot).Waves[0].Manifest)
	if len(parent.ProductWorkItems) != 2 || len(parent.UnavailableProductWorkItems) != 0 {
		t.Fatal(parent.ProductWorkItems, parent.UnavailableProductWorkItems)
	}
	for _, id := range admitted.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		input := registrationManifest(t, k, task.SessionID, w.Target())
		want := task.Spec.WorkItemIDs[0]
		if len(input.ProductWorkItems) != 1 || input.ProductWorkItems[0].Value.Definition.ID != want {
			t.Fatal("Task input includes a peer registration", input.ProductWorkItems)
		}
	}
	ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(ready.Tasks) != 0 || len(ready.Held) != 1 || ready.Held[0].Reason != "product_readiness_unavailable" {
		t.Fatal("metadata authorized product dispatch", ready, err)
	}
}

func TestProductWorkItemRegistrationRollbackRejectsUnknownScopeAndCannotMoveWave(t *testing.T) {
	ctx := context.Background()
	k, w, definition := productRegistrationFixture(t, "direct")
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_product_registration BEFORE INSERT ON records WHEN NEW.kind='product_work_item_registration_version' BEGIN SELECT RAISE(FAIL,'registration rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err == nil || !strings.Contains(err.Error(), "registration rollback") {
		t.Fatal("rollback injection not reached", err)
	}
	var count int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item')").Scan(&count); err != nil || count != 0 {
		t.Fatal("partial ownership committed", count, err)
	}
	if _, err := k.DB.ExecContext(ctx, "DROP TRIGGER fail_product_registration"); err != nil {
		t.Fatal(err)
	}
	bad := definition
	bad.Criteria = []string{"outside"}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", bad); err == nil {
		t.Fatal("unregistered criterion admitted")
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal(err)
	}
	other, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: w.OrchestratorSessionID, Source: w.Source, SourceID: "another-source", OriginalRequest: w.OriginalRequest, Criteria: w.Criteria, Policy: w.Policy})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, other.Target(), "", definition); err == nil {
		t.Fatal("WorkItem moved to a sibling Wave")
	}
	policy := w.Policy
	policy.Stage, policy.Product = "K", false
	request := WaveRequest{OrchestratorSessionID: w.OrchestratorSessionID, Source: w.Source, SourceID: "kernel-source", OriginalRequest: w.OriginalRequest, Criteria: w.Criteria, Policy: policy}
	kernel, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	bad = definition
	bad.ID = "kernel-work"
	if _, err := k.RegisterProductWorkItem(ctx, kernel.Target(), "", bad); err == nil {
		t.Fatal("K registration admitted")
	}
}

func TestProductWorkItemRegistrationScopeRefreshRequiresCASAndClearsCandidate(t *testing.T) {
	ctx := context.Background()
	k, w, p := productTargetFixture(t)
	definition := ProductWorkItemDefinition{ID: p.Value.WorkItem.ID, Repository: p.Value.Repository, RepositoryID: p.Value.WorkItem.RepositoryID, Criteria: []string{"AC"}}
	saved, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition)
	if err != nil {
		t.Fatal(err)
	}
	direction, err := k.RequestWaveDirection(ctx, directionRequest(w))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	input := registrationManifest(t, k, "main", w.Target())
	if len(input.ProductWorkItems) != 0 || len(input.UnavailableProductWorkItems) != 1 {
		t.Fatal("old source scope relabeled", input.ProductWorkItems, input.UnavailableProductWorkItems)
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "stale", definition); err == nil {
		t.Fatal("scope refresh skipped CAS")
	}
	updated, err := k.RegisterProductWorkItem(ctx, w.Target(), saved.Digest, definition)
	if err != nil || updated.Digest == saved.Digest {
		t.Fatal(updated, err)
	}
	var membership struct {
		Target      contract.Target `json:"target"`
		CandidateID string          `json:"candidate_id"`
	}
	if err := k.Record(ctx, "wave_work_item", definition.ID, &membership); err != nil || membership.Target != w.Target() || membership.CandidateID != "" {
		t.Fatal("old candidate survived source revision", membership, err)
	}
	var count int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='product_work_item_registration_version'").Scan(&count); err != nil || count != 2 {
		t.Fatal("source history lost", count, err)
	}
}

func TestProductWorkItemRegistrationChecksProjectionRepositoryAndBoundsCount(t *testing.T) {
	ctx := context.Background()
	k, w, definition := productRegistrationFixture(t, "direct")
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal(err)
	}
	p := ProductTarget{Target: w.Target(), Repository: definition.Repository, BaseBranch: "master", WorkItem: domain.WorkItem{ID: definition.ID, RepositoryID: 18, ScopeRevision: 1, PolicyRevision: 1, Version: 1, Phase: domain.Reviewing, Candidate: "candidate", Branch: "harness/work"}, Candidate: runner.Candidate{ID: "candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: 1, PolicyRevision: 1}}
	if _, err := k.SetProductTarget(ctx, "", p); err == nil {
		t.Fatal("projection changed registered repository")
	}
	p.WorkItem.RepositoryID = definition.RepositoryID
	if _, err := k.SetProductTarget(ctx, "", p); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < maxProductWorkItems; i++ {
		next := definition
		next.ID = fmt.Sprintf("work-%03d", i)
		if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", next); err != nil {
			t.Fatal(err)
		}
	}
	next := definition
	next.ID = "overflow"
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", next); err == nil {
		t.Fatal("unbounded source catalog admitted")
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal("bounded catalog blocked exact replay", err)
	}
}

func TestProductWorkItemRegistrationPausedReplayDoesNotGrantNewOwnership(t *testing.T) {
	ctx := context.Background()
	k, w, definition := productRegistrationFixture(t, "direct")
	saved, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition)
	if err != nil {
		t.Fatal(err)
	}
	mustControl(t, k, operatorRequest(w, "pause-registration", "pause", ""))
	replay, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition)
	if err != nil || !reflect.DeepEqual(saved, replay) {
		t.Fatal(replay, err)
	}
	definition.ID = "new-work"
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err == nil {
		t.Fatal("paused Wave admitted new ownership")
	}
	var count int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}
