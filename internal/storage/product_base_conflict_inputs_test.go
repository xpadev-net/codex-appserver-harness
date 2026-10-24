package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func stoppedBaseConflictFixture(t *testing.T) (*KernelStore, ProductWorkspace, contract.ProductBaseConflictReference, contract.PlanProposal) {
	t.Helper()
	ctx := context.Background()
	k, plan, saved, pin, source := existingBaseWorkspaceFixture(t)
	if _, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err != nil {
		t.Fatal(err)
	}
	stopped, err := k.FinishProductWorkspace(ctx, saved.Value.Request.TaskID, existingBaseProof(t, k, plan, saved, pin, source, "conflict"))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := k.NotifyProductPreparationAttention(ctx, saved.Value.Request.TaskID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	notes := preparationNotices(t, k, saved.Value.Request.TaskID)
	if len(notes) != 1 || notes[0].Reason != "product_base_merge_conflict" || notes[0].Preparation == nil || notes[0].Preparation.Conflict == nil || notes[0].Preparation.BaseMerge == nil || notes[0].Preparation.BaseMerge.Outcome != "conflict" {
		t.Fatal("parent lacks authenticated conflict handoff", notes)
	}
	raw, err := json.Marshal(notes[0].Preparation)
	if err != nil || strings.Contains(string(raw), k.Root) || strings.Contains(string(raw), plan.Value.Head.Remote) || strings.Contains(string(raw), "git_dir") {
		t.Fatal("private conflict proof leaked", string(raw), err)
	}
	ref := *notes[0].Preparation.Conflict
	proposal := storagePlanFixture()
	proposal.Phases[0].ID, proposal.Phases[1].ID = "conflict-repair", "conflict-evaluation"
	fix := &proposal.Phases[0].Tasks[0]
	fix.ID, fix.Kind, fix.Instruction, fix.WorkItemIDs, fix.ProductBaseConflict = "resolve-base-conflict", "fix", "resolve the host-stopped base conflict in the new assigned checkout", []string{saved.Value.WorkItemID}, &ref
	proposal.Phases[0].Completion.RequiredTaskIDs = []string{fix.ID}
	proposal.Phases[1].Tasks[0].ID = "review-base-conflict"
	proposal.Phases[1].Tasks[0].DependsOn[0].TaskID = fix.ID
	proposal.Phases[1].Completion.RequiredTaskIDs = []string{"review-base-conflict"}
	return k, stopped, ref, proposal
}

func TestBaseConflictCreatesFreshFixInputAndCarriesOnlyAuthenticatedProduct(t *testing.T) {
	ctx := context.Background()
	k, source, ref, proposal := stoppedBaseConflictFixture(t)
	w, err := k.Wave(ctx, source.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	before := w
	admitted, err := k.AdoptPlan(ctx, w.Target(), "explicit-base-conflict-repair", proposal)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil || w.FixRounds != before.FixRounds+1 || w.Replans != before.Replans+1 || w.TasksCreated != before.TasksCreated+2 {
		t.Fatal("conflict repair lost cumulative budgets", w, err)
	}
	fresh, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil || fresh.ID == ref.TaskID || fresh.SessionID == source.Value.SessionID || fresh.Attempts != 0 || fresh.Spec.ProductBaseConflict == nil || *fresh.Spec.ProductBaseConflict != ref {
		t.Fatal("old Task/session reused", fresh, err)
	}
	old, err := k.Task(ctx, ref.TaskID)
	if err != nil || old.State != contract.TaskCanceled || old.Attempts != 0 {
		t.Fatal("source Task was not retired", old, err)
	}
	input, err := k.ReadProductBaseInput(ctx, fresh.ID)
	if err != nil || input.Value.Preparation.Product == nil || input.Value.Preparation.Product.Value.Target != admitted.Target || input.Value.Preparation.Product.Value.Candidate.SHA != source.Value.Request.BaseSHA || input.Value.ConflictBaseSHA != source.Proof.BaseMerge.BaseSHA {
		t.Fatal("new input lost original product/conflict", input, err)
	}
	ancestors, err := input.RequiredAncestors(input.Value.Preparation.Product.Value.Candidate.BaseSHA)
	if err != nil || !slices.Contains(ancestors, source.Proof.BaseMerge.BaseSHA) {
		t.Fatal("new base may omit the conflict's fixed predecessor", ancestors, err)
	}
	replay, err := k.AdoptPlan(ctx, before.Target(), "explicit-base-conflict-repair", proposal)
	if err != nil || !reflect.DeepEqual(replay, admitted) {
		t.Fatal("plan replay duplicated fix", replay, err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(after, w) {
		t.Fatal("replay consumed budget", after, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	read, err := k.ReadProductBaseInput(ctx, fresh.ID)
	if err != nil || !reflect.DeepEqual(read, input) {
		t.Fatal("restart lost conflict provenance", read, err)
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("input reference started model", runs, err)
	}
}

func TestBaseConflictAdmissionRejectsWrongOrMissingSourceWithoutBudgetOrProjectionChange(t *testing.T) {
	for _, mode := range []string{"digest", "workspace", "proof", "unknown_owner", "lost_terminal", "lost_send", "lost_candidate", "candidate", "paused"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, source, _, proposal := stoppedBaseConflictFixture(t)
			w, err := k.Wave(ctx, source.Value.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			ref := proposal.Phases[0].Tasks[0].ProductBaseConflict
			switch mode {
			case "digest":
				ref.WorkspaceDigest = strings.Repeat("a", 64)
			case "workspace":
				ref.WorkspaceID += "-other"
			case "proof":
				ref.ProofDigest = strings.Repeat("b", 64)
			case "unknown_owner":
				if _, err := k.DB.Exec("UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", source.Value.SessionID); err != nil {
					t.Fatal(err)
				}
			case "lost_terminal", "lost_send":
				kind := "product_base_workspace_terminal"
				if mode == "lost_send" {
					kind = "product_workspace_base_merge_send"
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, source.ID); err != nil {
					t.Fatal(err)
				}
			case "lost_candidate":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_candidate'"); err != nil {
					t.Fatal(err)
				}
			case "candidate":
				p, err := k.ReadProductTarget(ctx, w.Target(), source.Value.WorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				next := p.Value
				next.Candidate.ID, next.Candidate.SHA, next.WorkItem.Candidate = "moved-after-conflict", strings.Repeat("e", 40), "moved-after-conflict"
				next.WorkItem.Version++
				if _, err := k.SetProductTarget(ctx, p.Digest, next); err != nil {
					t.Fatal(err)
				}
			case "paused":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-conflict", Target: w.Target(), Action: "pause", Reason: "fixture"}); err != nil {
					t.Fatal(err)
				}
				w, err = k.Wave(ctx, w.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			p, err := k.ReadProductTarget(ctx, w.Target(), source.Value.WorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.AdoptPlan(ctx, w.Target(), "invalid-conflict-repair", proposal); err == nil {
				t.Fatal("invalid conflict source accepted", mode)
			}
			after, err := k.Wave(ctx, w.ID)
			if err != nil || !reflect.DeepEqual(after, w) {
				t.Fatal("rejected source consumed plan/budget", after, err)
			}
			current, err := k.ReadProductTarget(ctx, w.Target(), source.Value.WorkItemID)
			if err != nil || !reflect.DeepEqual(current, p) {
				t.Fatal("rejected source carried candidate", current, err)
			}
		})
	}
}

func TestBaseConflictLostSourceAfterAdmissionHoldsFreshPreparation(t *testing.T) {
	ctx := context.Background()
	k, source, _, proposal := stoppedBaseConflictFixture(t)
	w, err := k.Wave(ctx, source.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "conflict-repair-before-loss", proposal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_workspace_base_merge_send' AND id=?", source.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductBaseInput(ctx, admitted.CreatedTaskIDs[0]); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("lost conflict evidence became preparation input", err)
	}
}

func TestBaseConflictCarryFailureRollsBackPlanAndCanRetrySameProof(t *testing.T) {
	ctx := context.Background()
	k, source, _, proposal := stoppedBaseConflictFixture(t)
	w, err := k.Wave(ctx, source.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), source.Value.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_conflict_carry BEFORE INSERT ON records WHEN NEW.kind='product_target_observation' BEGIN SELECT RAISE(FAIL,'conflict carry rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "retry-conflict-carry", proposal); err == nil || !strings.Contains(err.Error(), "conflict carry rollback") {
		t.Fatal("carry failure not reached", err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(after, w) {
		t.Fatal("carry failure consumed plan/budget", after, err)
	}
	current, err := k.ReadProductTarget(ctx, w.Target(), source.Value.WorkItemID)
	if err != nil || !reflect.DeepEqual(current, p) {
		t.Fatal("carry rollback changed candidate", current, err)
	}
	old, err := k.Task(ctx, source.Value.Request.TaskID)
	if err != nil || old.State != contract.TaskReady {
		t.Fatal("carry rollback retired source", old, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_conflict_carry"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "retry-conflict-carry", proposal); err != nil {
		t.Fatal("positive stopped source could not be retried", err)
	}
}

func TestBaseConflictRepairPreservesNewMergeHold(t *testing.T) {
	ctx := context.Background()
	k, source, _, proposal := stoppedBaseConflictFixture(t)
	w, err := k.Wave(ctx, source.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), source.Value.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	controlled := p.Value
	controlled.WorkItem.MergeHold = true
	if _, err := k.SetProductTarget(ctx, p.Digest, controlled); err != nil {
		t.Fatal(err)
	}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "base-conflict-with-merge-hold", proposal)
	if err != nil {
		t.Fatal("merge-only hold blocked local fix", err)
	}
	input, err := k.ReadProductBaseInput(ctx, admitted.CreatedTaskIDs[0])
	if err != nil || input.Value.Preparation.Product == nil || !input.Value.Preparation.Product.Value.WorkItem.MergeHold {
		t.Fatal("fix carry discarded merge hold", input, err)
	}
}
