package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func sealingNotices(t *testing.T, k *KernelStore, taskID string) []Notification {
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
	var notes []Notification
	for _, n := range all {
		if sealingAttentionReason(n.Reason) {
			notes = append(notes, n)
		}
	}
	return notes
}

func checkSealingWake(t *testing.T, k *KernelStore, n Notification) error {
	t.Helper()
	ctx := context.Background()
	b, err := k.SessionBinding(ctx, n.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	return k.checkParentAttentionWakeTx(ctx, tx, b, n)
}

func TestProductSealingAttentionSafeOnceAndAuthenticated(t *testing.T) {
	for _, mode := range []string{"failed", "owner_unresolved", "missing_head", "missing_history"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, run := productSealFixture(t)
			saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
				t.Fatal(claim, err)
			}
			reason := "product_seal_owner_unresolved"
			switch mode {
			case "failed":
				_, err = k.FinishProductSeal(ctx, task.ID, run.ID, ProductSealProof{ProofID: "known-stopped", Outcome: "failed", Reason: "private error contains /private/secret and credential material"})
				reason = "product_seal_failed"
			case "missing_head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_seal' AND id=?", saved.ID)
				reason = "product_seal_evidence_unavailable"
			case "missing_history":
				if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, ProductSealProof{ProofID: "known-stopped", Outcome: "failed", Reason: "fixture"}); err != nil {
					t.Fatal(err)
				}
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_seal_history' AND id=?", saved.ID)
				reason = "product_seal_evidence_unavailable"
			}
			if err != nil {
				t.Fatal(err)
			}
			reservation, err := k.OperationReservation(ctx, saved.ID)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if changed, err := k.NotifyProductSealingAttention(ctx, task.ID); err != nil || changed != (i == 0) {
					t.Fatal(changed, err)
				}
			}
			notes := sealingNotices(t, k, task.ID)
			if len(notes) != 1 || notes[0].Reason != reason || notes[0].Sealing == nil || notes[0].Sealing.RunID != run.ID || notes[0].Sealing.Seal.ID != saved.ID {
				t.Fatal(notes)
			}
			n := notes[0]
			raw, err := json.Marshal(n)
			if err != nil || strings.Contains(string(raw), k.Root) || strings.Contains(string(raw), "/private/secret") || strings.Contains(string(raw), "credential material") || strings.Contains(string(raw), run.Final) {
				t.Fatal("private sealing data leaked", string(raw), err)
			}
			if err := checkSealingWake(t, k, n); err != nil {
				t.Fatal(err)
			}
			altered := n
			copy := *n.Sealing
			copy.State = "invented-success"
			altered.Sealing = &copy
			if err := checkSealingWake(t, k, altered); err == nil {
				t.Fatal("altered summary accepted")
			}
			if err := k.AcknowledgeNotification(ctx, n.SessionID, n.ID); err != nil {
				t.Fatal(err)
			}
			if changed, err := k.NotifyProductSealingAttention(ctx, task.ID); err != nil || changed {
				t.Fatal("ACK repeated cause", changed, err)
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil || !reflect.DeepEqual(current, task) {
				t.Fatal("attention changed task", current, err)
			}
			after, err := k.OperationReservation(ctx, saved.ID)
			if err != nil || !reflect.DeepEqual(after, reservation) {
				t.Fatal("attention changed capacity", after, err)
			}
			var runs, applied int
			if err := k.DB.QueryRow("SELECT count(*),sum(applied) FROM kernel_run_bindings").Scan(&runs, &applied); err != nil || runs != 1 || applied != 0 {
				t.Fatal("attention ran/applied model", runs, applied, err)
			}
		})
	}
}

func TestProductSealingAttentionRetiresResolvedOwner(t *testing.T) {
	ctx := context.Background()
	k, _, task, run := productSealFixture(t)
	saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	if changed, err := k.NotifyProductSealingAttention(ctx, task.ID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	notes := sealingNotices(t, k, task.ID)
	if len(notes) != 1 {
		t.Fatal(notes)
	}
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, sealProofFixture(saved)); err != nil {
		t.Fatal(err)
	}
	if err := checkSealingWake(t, k, notes[0]); !errors.Is(err, ErrWakeHeld) {
		t.Fatal("resolved owner still wakes", err)
	}
	if changed, err := k.NotifyProductSealingAttention(ctx, task.ID); err != nil || changed {
		t.Fatal("ordinary adoption wait notified", changed, err)
	}
}

func TestProductSealingAttentionLeavesOrdinaryWaitsAndLiveRunsSilent(t *testing.T) {
	for _, mode := range []string{"unadmitted", "prepared", "run_live", "session_live", "applied", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run := productSealFixture(t)
			if mode != "unadmitted" {
				saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if mode != "prepared" {
					if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
						t.Fatal(claim, err)
					}
				}
			}
			var err error
			switch mode {
			case "run_live":
				_, err = k.DB.Exec("UPDATE kernel_run_bindings SET process_stopped=0")
			case "session_live":
				_, err = k.DB.Exec("UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", task.SessionID)
			case "applied":
				_, err = k.DB.Exec("UPDATE kernel_run_bindings SET applied=1")
			case "pause":
				_, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-seal-notice", Target: w.Target(), Action: "pause", Reason: "fixture"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := k.NotifyProductSealingAttention(ctx, task.ID); err != nil || changed {
				t.Fatal("ordinary wait/control notified", changed, err)
			}
			if notes := sealingNotices(t, k, task.ID); len(notes) != 0 {
				t.Fatal(notes)
			}
		})
	}
}
