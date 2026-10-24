package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func preparationNotices(t *testing.T, k *KernelStore, taskID string) []Notification {
	t.Helper()
	ctx := context.Background()
	task, err := k.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReconcileNotifications(ctx, 128); err != nil {
		t.Fatal(err)
	}
	all, err := k.PendingWakeNotifications(ctx, task.ParentSessionID, 128)
	if err != nil {
		t.Fatal(err)
	}
	out := []Notification{}
	for _, n := range all {
		if preparationAttentionReason(n.Reason) {
			out = append(out, n)
		}
	}
	return out
}
func TestProductPreparationAttentionPersistsSafeExceptionOnlyOnce(t *testing.T) {
	for _, mode := range []string{"failed", "owner_unresolved", "evidence_unavailable"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, request, _ := productBaseWorkspaceFixture(t)
			saved, err := k.ReserveProductWorkspace(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if claim, err := k.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest); err != nil || !claim.Start {
				t.Fatal(claim, err)
			}
			reason := "product_workspace_owner_unresolved"
			if mode == "failed" {
				if _, err := k.FinishProductWorkspace(ctx, request.TaskID, ProductWorkspaceProof{ProofID: "known-stopped-failed-preparation", Outcome: "failed", Reason: "fixture local owner stopped"}); err != nil {
					t.Fatal(err)
				}
				reason = "product_workspace_failed"
			} else if mode == "evidence_unavailable" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_base_workspace_claim' AND id=?", saved.ID); err != nil {
					t.Fatal(err)
				}
				reason = "product_workspace_evidence_unavailable"
			}
			for i := 0; i < 2; i++ {
				changed, err := k.NotifyProductPreparationAttention(ctx, request.TaskID)
				if err != nil || changed != (i == 0) {
					t.Fatal(changed, err)
				}
			}
			notes := preparationNotices(t, k, request.TaskID)
			if len(notes) != 1 || notes[0].Reason != reason || notes[0].Preparation == nil {
				t.Fatal(notes)
			}
			if mode != "evidence_unavailable" && notes[0].Preparation.BasePlan.ID != plan.ID {
				t.Fatal("lost base provenance", notes[0])
			}
			raw, err := json.Marshal(notes[0])
			if err != nil || strings.Contains(string(raw), k.Root) || strings.Contains(string(raw), plan.Value.Head.Remote) {
				t.Fatal("private preparation data leaked", string(raw), err)
			}
			note := notes[0]
			if err := k.AcknowledgeNotification(ctx, note.SessionID, note.ID); err != nil {
				t.Fatal(err)
			}
			if changed, err := k.NotifyProductPreparationAttention(ctx, request.TaskID); err != nil || changed {
				t.Fatal("ACK/poll repeated exception", changed, err)
			}
			b, err := k.SessionBinding(ctx, note.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			checkErr := k.checkParentAttentionWakeTx(ctx, tx, b, note)
			tx.Rollback()
			if checkErr != nil {
				t.Fatal(checkErr)
			}
			altered := note
			copy := *note.Preparation
			copy.State = "invented-success"
			altered.Preparation = &copy
			tx, err = k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			checkErr = k.checkParentAttentionWakeTx(ctx, tx, b, altered)
			tx.Rollback()
			if checkErr == nil {
				t.Fatal("notification relabeled source observation")
			}
			task, err := k.Task(ctx, request.TaskID)
			if err != nil || task.Attempts != 0 {
				t.Fatal(task, err)
			}
			var runs int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
				t.Fatal("notification started model", runs, err)
			}
		})
	}
}
func TestProductBaseChangedNoticeStopsWakingAfterOriginalPlanIsCurrent(t *testing.T) {
	ctx := context.Background()
	k, plan, request, pin := productBaseWorkspaceFixture(t)
	completeBaseWorkspace(t, k, plan, request, pin)
	input, err := k.ReadProductBaseInput(ctx, request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	head := plan.Value.Head
	head.HeadSHA = strings.Repeat("e", 40)
	next, err := k.PrepareProductBasePlan(ctx, request.TaskID, input.Digest, ProductBaseObservation{Head: head, ObservedAt: k.clock()})
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := k.NotifyProductPreparationAttention(ctx, request.TaskID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	notes := preparationNotices(t, k, request.TaskID)
	if len(notes) != 1 || notes[0].Reason != "product_base_changed" || notes[0].Preparation.CurrentPlan.ID != next.ID {
		t.Fatal(notes)
	}
	if _, err := k.PrepareProductBasePlan(ctx, request.TaskID, input.Digest, ProductBaseObservation{Head: plan.Value.Head, ObservedAt: k.clock()}); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, notes[0].SessionID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held := k.checkParentAttentionWakeTx(ctx, tx, b, notes[0])
	tx.Rollback()
	if !errors.Is(held, ErrWakeHeld) {
		t.Fatal("resolved base notice woke parent", held)
	}
	if changed, err := k.NotifyProductPreparationAttention(ctx, request.TaskID); err != nil || changed {
		t.Fatal("resolved cause emitted notification", changed, err)
	}
}
func TestChangedUnstartedWorkspaceReleasesCapacityWithoutGitOrTurn(t *testing.T) {
	ctx := context.Background()
	k, plan, request, _ := productBaseWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	input, err := k.ReadProductBaseInput(ctx, request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	head := plan.Value.Head
	head.HeadSHA = strings.Repeat("e", 40)
	if _, err := k.PrepareProductBasePlan(ctx, request.TaskID, input.Digest, ProductBaseObservation{Head: head, ObservedAt: k.clock()}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-unstarted", Target: saved.Value.Target, Action: "pause", Reason: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.FinishChangedUnstartedWorkspace(ctx, request.TaskID); err != nil || changed {
		t.Fatal("pause advanced profile", changed, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "resume-unstarted", Target: saved.Value.Target, Action: "resume", Reason: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.FinishChangedUnstartedWorkspace(ctx, request.TaskID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	done, err := k.ProductWorkspace(ctx, request.TaskID)
	if err != nil || done.State != "failed" || done.Proof == nil || done.Proof.Outcome != "not_started" || done.BaseClaim != nil || done.BaseFetchAt != nil {
		t.Fatal(done, err)
	}
	r, err := k.OperationReservation(ctx, saved.ID)
	if err != nil || r.State != "released" || r.Proof == nil || r.Proof.Disposition != "not_started" {
		t.Fatal(r, err)
	}
	if changed, err := k.FinishChangedUnstartedWorkspace(ctx, request.TaskID); err != nil || changed {
		t.Fatal("stop replay changed profile", changed, err)
	}
	replay, err := k.ProductWorkspace(ctx, request.TaskID)
	if err != nil || !reflect.DeepEqual(replay, done) {
		t.Fatal(replay, err)
	}
	if changed, err := k.NotifyProductPreparationAttention(ctx, request.TaskID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	notes := preparationNotices(t, k, request.TaskID)
	if len(notes) != 1 || notes[0].Preparation.Outcome != "not_started" {
		t.Fatal(notes)
	}
}
