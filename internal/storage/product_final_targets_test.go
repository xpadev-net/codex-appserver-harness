package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductFinalNotificationIsDurableIdempotentAndRejectsStaleEvidence(t *testing.T) {
	for _, status := range []string{"passed", "failed"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			k, plan, _ := completeFinalEvidenceFixture(t, status)
			target := plan.Value.Target
			w, err := k.Wave(ctx, target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			before := w.MainTurns
			for i := 0; i < 2; i++ {
				changed, err := k.NotifyProductFinalVerification(ctx, target)
				if err != nil || changed != (i == 0) {
					t.Fatal(changed, err)
				}
			}
			if _, err := k.ReconcileNotifications(ctx, 128); err != nil {
				t.Fatal(err)
			}
			notes, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 128)
			if err != nil {
				t.Fatal(err)
			}
			var final *Notification
			for i := range notes {
				if notes[i].Reason == "product_final_verification" {
					if final != nil {
						t.Fatal("duplicate final notice")
					}
					final = &notes[i]
				}
			}
			if final == nil {
				t.Fatal("missing final judgment notice", notes)
			}
			b, err := k.SessionBinding(ctx, w.OrchestratorSessionID)
			if err != nil {
				t.Fatal(err)
			}
			check := func() error {
				tx, err := k.kernelTx(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				return k.checkParentAttentionWakeTx(ctx, tx, b, *final)
			}
			if err := check(); err != nil {
				t.Fatal(err)
			}
			w, err = k.Wave(ctx, w.ID)
			if err != nil || w.MainTurns != before || w.State == domain.WaveCompleted {
				t.Fatal("notice spent a model turn or completed Wave", w, err)
			}
			// Removing current selection must invalidate delivery, even though the
			// immutable successful/failed execution and notice remain in history.
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_final_plan_current' AND id=?", w.ID); err != nil {
				t.Fatal(err)
			}
			if err := check(); !errors.Is(err, ErrWakeHeld) {
				t.Fatal("stale final evidence woke parent", err)
			}
		})
	}
}

func TestProductFinalTargetsBoundedPagingSurvivesUnavailableWave(t *testing.T) {
	ctx := context.Background()
	k, plan, _, _ := finalRunnerFixture(t)
	target := plan.Value.Target
	if _, err := k.ProductFinalExecutionTargets(ctx, "", 0); err == nil {
		t.Fatal("unbounded scan accepted")
	}
	// The damaged row sorts before the valid fixture and must not starve it.
	if _, err := k.DB.Exec("INSERT INTO kernel_waves(id,source,source_id,input_digest,payload) SELECT '000-corrupt',source,'corrupt-final-fixture',input_digest,? FROM kernel_waves LIMIT 1", []byte("{")); err != nil {
		t.Fatal(err)
	}
	page, err := k.ProductFinalExecutionTargets(ctx, "", 1)
	if err != nil || len(page.Unavailable) != 1 || page.NextAfter != "000-corrupt" {
		t.Fatal(page, err)
	}
	page, err = k.ProductFinalExecutionTargets(ctx, page.NextAfter, 1)
	if err != nil || len(page.Targets) != 1 || page.Targets[0] != target || page.NextAfter != "" {
		t.Fatal(page, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "final-target-pause", Target: target, Action: "pause", Reason: "fixture"}); err != nil {
		t.Fatal(err)
	}
	page, err = k.ProductFinalExecutionTargets(ctx, "000-corrupt", 1)
	if err != nil || len(page.Targets) != 0 {
		t.Fatal("paused Wave dispatched", page, err)
	}
}
