package storage

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductTemplateRemapsIntermediateFutureTasksAndTemplates(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, source := productTemplateFixture(t, "implementation_defect", func(plan *contract.PlanProposal) {
		verifier := plan.Phases[1].Tasks[0].ID
		future := contract.TaskSpec{ID: "future-review", Kind: "review", Instruction: "required subsequent product review", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: verifier}, {TaskID: "quality-review"}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{"work"}}
		plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "future-phase", Tasks: []contract.TaskSpec{future}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{future.ID}}, FailurePolicy: "drain_and_block"})
		plan.Templates = append(plan.Templates, contract.FixReviewTemplate{ID: "future-template", InputTaskID: future.ID, Classifications: []string{"implementation_defect"}, FixInstruction: "repair subsequent findings", ReviewInstruction: "repeat later role", MaxRounds: 2})
	})
	oldID := structureID("task-", w.ID, "future-review")
	old, err := k.Task(ctx, oldID)
	if err != nil {
		t.Fatal(err)
	}
	expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: source.ID})
	if err != nil || expansion.Product.FutureClones[oldID] == "" {
		t.Fatal(expansion, err)
	}
	clone, err := k.Task(ctx, expansion.Product.FutureClones[oldID])
	if err != nil {
		t.Fatal(err)
	}
	if clone.ID == old.ID || clone.SessionID == old.SessionID || clone.Attempts != 0 || clone.Spec.Instruction != old.Spec.Instruction || !reflect.DeepEqual(clone.Spec.Criteria, old.Spec.Criteria) {
		t.Fatal("future role changed or reused session", clone)
	}
	deps := []string{}
	for _, dep := range clone.Spec.DependsOn {
		deps = append(deps, dep.TaskID)
		if dep.TaskID == source.LocalID {
			t.Fatal("old report remained a dependency")
		}
	}
	if !slices.Contains(deps, expansion.Product.Verifier) {
		t.Fatal("new Candidate verifier not selected", clone.Spec.DependsOn)
	}
	nextReview, err := k.Task(ctx, expansion.Product.Replacements[source.ID][0])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(deps, nextReview.LocalID) {
		t.Fatal("new review report not selected", clone.Spec.DependsOn)
	}
	found := false
	for _, template := range expansion.Admission.Plan.Templates {
		if template.ID == "future-template" {
			found = template.InputTaskID == clone.LocalID
		}
	}
	if !found {
		t.Fatal("future template root was not cloned")
	}
	old, err = k.Task(ctx, oldID)
	if err != nil || old.State != contract.TaskCanceled {
		t.Fatal("old unused consumer stayed runnable", old, err)
	}
	latest, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bound, readErr := templateTaskBoundTx(ctx, tx, latest, old.ID, map[string]bool{})
	tx.Rollback()
	if readErr != nil || !bound {
		t.Fatal("consumer clone lineage was lost", bound, readErr)
	}
}

func TestProductTemplatePreservesPeerReviewDependencyAndPhaseBarrier(t *testing.T) {
	for _, separate := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_phase", true: "later_phase"}[separate], func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, _, first := productTemplateFixture(t, "implementation_defect", func(p *contract.PlanProposal) {
				second := p.Phases[len(p.Phases)-1].Tasks[0]
				second.ID = "peer-review"
				second.Instruction = "review peer report and fixed Candidate"
				second.DependsOn = append(second.DependsOn, contract.TaskDependency{TaskID: "quality-review"})
				if separate {
					p.Phases = append(p.Phases, contract.PhaseSpec{ID: "peer-phase", Tasks: []contract.TaskSpec{second}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{second.ID}}, FailurePolicy: "drain_and_block"})
					owner := p.Templates[0]
					owner.ID = "peer-owner"
					owner.InputTaskID = second.ID
					p.Templates = append(p.Templates, owner)
				} else {
					phase := &p.Phases[len(p.Phases)-1]
					phase.Tasks = append(phase.Tasks, second)
					phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, second.ID)
				}
			})
			second, err := k.Task(ctx, structureID("task-", w.ID, "peer-review"))
			if err != nil {
				t.Fatal(err)
			}
			second, run := seedProductQualityReview(t, k, w, plan, second)
			if separate {
				var result contract.TaskResult
				if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
					t.Fatal(err)
				}
				result.Review.Verdict = "findings"
				result.Review.Findings = []contract.ReviewFinding{{ID: "peer-defect", Classification: "implementation_defect", TargetDigest: result.Review.TargetDigest, Basis: "AC", Description: "additional peer finding"}}
				raw, err := marshalTaskResultFixture(result)
				if err != nil {
					t.Fatal(err)
				}
				run.Final = string(raw)
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := kernelRecordTx(ctx, tx, "agent_run", run.ID, second.SessionID, run); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ApplyTaskResult(ctx, second.SessionID, run.ID); err != nil {
				t.Fatal(err)
			}
			sourceID := first.ID
			if separate {
				sourceID = second.ID
			}
			expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: sourceID})
			if err != nil {
				t.Fatal(expansion, err)
			}
			newFirst, err := k.Task(ctx, expansion.Product.Replacements[first.ID][0])
			if err != nil {
				t.Fatal(err)
			}
			if separate {
				// The second review was a completed later consumer. An explicit
				// request at the later barrier re-reviews both original roles.
				if expansion.Product.FutureClones[second.ID] != "" {
					t.Fatal("completed peer was treated as unused")
				}
			}
			newSecond, err := k.Task(ctx, expansion.Product.Replacements[second.ID][0])
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, dep := range newSecond.Spec.DependsOn {
				if dep.TaskID == newFirst.LocalID {
					found = true
				}
				if dep.TaskID == first.LocalID {
					t.Fatal("old peer report remained granted")
				}
			}
			if !found {
				t.Fatal("peer role ordering was dropped", newSecond.Spec.DependsOn)
			}
			if separate && newSecond.PhaseID == newFirst.PhaseID {
				t.Fatal("original phase barrier was collapsed")
			}
		})
	}
}

func TestProductTemplateRefusesProgressedFutureConsumer(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, source := productTemplateFixture(t, "implementation_defect", func(plan *contract.PlanProposal) {
		future := contract.TaskSpec{ID: "later-work", Kind: "work", Instruction: "later generic consumer", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: "quality-review"}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
		plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "later", Tasks: []contract.TaskSpec{future}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{future.ID}}, FailurePolicy: "drain_and_block"})
	})
	future, err := k.Task(ctx, structureID("task-", w.ID, "later-work"))
	if err != nil {
		t.Fatal(err)
	}
	future.Attempts = 1
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveTask(ctx, tx, future); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: source.ID}); !errors.Is(err, ErrTemplateNeedsMain) {
		t.Fatal("progressed consumer was rewritten", err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.PlanRevision != w.PlanRevision || current.FixRounds != w.FixRounds {
		t.Fatal("failed remap changed plan/budget", current, err)
	}
}

func TestProductTemplateExplicitPeerSelectorRequiresParentAndRollsBack(t *testing.T) {
	ctx := context.Background()
	k, w, plan, _, first := productTemplateFixture(t, "implementation_defect", func(p *contract.PlanProposal) {
		phase := &p.Phases[len(p.Phases)-1]
		peer := phase.Tasks[0]
		peer.ID = "explicit-peer"
		peer.Instruction = "peer role with explicit report selector"
		peer.DependsOn = append(peer.DependsOn, contract.TaskDependency{TaskID: "quality-review"})
		phase.Tasks = append(phase.Tasks, peer)
		phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, peer.ID)
	})
	peer, err := k.Task(ctx, structureID("task-", w.ID, "explicit-peer"))
	if err != nil {
		t.Fatal(err)
	}
	peer, run := seedProductQualityReview(t, k, w, plan, peer)
	if _, err := k.ApplyTaskResult(ctx, peer.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	// Feed the remapper an explicit old-report selector. The adopted task is
	// already immutable, so this probes compilation without altering its facts.
	peer, err = k.Task(ctx, peer.ID)
	if err != nil {
		t.Fatal(err)
	}
	firstInput, err := k.ReadProductTaskReview(ctx, first.ResultID)
	if err != nil {
		t.Fatal(err)
	}
	peerInput, err := k.ReadProductTaskReview(ctx, peer.ResultID)
	if err != nil {
		t.Fatal(err)
	}
	var result contract.TaskResult
	if err := k.Record(ctx, "task_result", first.ResultID, &result); err != nil {
		t.Fatal(err)
	}
	peer.Spec.DependsOn[len(peer.Spec.DependsOn)-1].ArtifactID = result.Artifacts[0]
	binding := &ProductTemplateBinding{WorkItemID: "work", Candidate: plan.Value.Candidate, Verifier: "new-verifier", Replacements: map[string][]string{first.ID: {structureID("task-", w.ID, "new-first")}, peer.ID: {structureID("task-", w.ID, "new-peer")}}}
	phase := contract.PhaseSpec{Tasks: []contract.TaskSpec{{ID: "new-first", Kind: "review", DependsOn: []contract.TaskDependency{{TaskID: "new-verifier"}}}, {ID: "new-peer", Kind: "review", DependsOn: []contract.TaskDependency{{TaskID: "new-verifier"}}}}}
	current := contract.PlanProposal{Phases: []contract.PhaseSpec{{Tasks: []contract.TaskSpec{first.Spec, peer.Spec}}}}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, compileErr := k.remapProductReviewPhasesTx(ctx, tx, w, current, "explicit-test", binding, []domain.Task{first, peer}, phase)
	tx.Rollback()
	if !errors.Is(compileErr, ErrTemplateNeedsMain) {
		t.Fatal("explicit stale selector was silently widened", compileErr, firstInput.Digest, peerInput.Digest)
	}
	currentWave, err := k.Wave(ctx, w.ID)
	if err != nil || currentWave.PlanRevision != w.PlanRevision || currentWave.FixRounds != w.FixRounds {
		t.Fatal("rejected remap changed state", currentWave, err)
	}
}
