package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestParentAttentionIsDurableAckIndependentAndAppliedOnce(t *testing.T) {
	ctx := context.Background()
	k, w, review, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"specification_change"})
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	n := pending[0]
	if n.Kind != "parent_attention" || n.Reason != "template_requires_main" || n.TaskID != review.ID || n.ResultID == "" || n.Target != w.Target() || n.SourceEventID == "" {
		t.Fatal(n)
	}
	source, err := parentAttentionTx(ctx, k.DB, n.AttentionID)
	if err != nil || source.Task.ResultID != n.ResultID || source.Task.ID != review.ID {
		t.Fatal(source, err)
	}
	if err = k.AcknowledgeNotification(ctx, w.OrchestratorSessionID, n.ID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	if err = k.Close(); err != nil {
		t.Fatal(err)
	}
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for i := 0; i < 3; i++ {
		if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || changed {
			t.Fatal("duplicate attention", changed, err)
		}
	}
	pending, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(pending) != 1 || pending[0].ID != n.ID || !pending[0].DeliveryAcknowledged {
		t.Fatal(pending, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: w.OrchestratorSessionID, Key: "exception", NotificationIDs: []string{n.ID}, IncludeSessionContext: true})
	if err != nil || !strings.Contains(string(wake.Snapshot.Input), "template_requires_main") {
		t.Fatal(wake, err)
	}
	b := prepareWakeSession(t, k, w.OrchestratorSessionID)
	run := wakeStartingRun(k, b, wake)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	final := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: w.Target(), Action: "wait", SubscriptionIDs: []string{n.SubscriptionID}, Summary: "specification exception requires operator input", Remaining: []string{"resolve specification question"}, Blockers: []string{"specification change"}}}}
	saveMainCompletion(t, k, b, run, final, true)
	if receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID); err != nil || receipt.State != "applied" {
		t.Fatal(receipt, err)
	}
	for i := 0; i < 3; i++ {
		if changed, err := k.ReconcileSessionTemplates(ctx, b.ID); err != nil || changed {
			t.Fatal(changed, err)
		}
		pending, err = k.PendingWakeNotifications(ctx, b.ID, 16)
		if err != nil || len(pending) != 0 {
			t.Fatal("applied attention woke main again", pending, err)
		}
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.MainTurns != 1 || current.Replans != 0 || current.FixRounds != 0 {
		t.Fatal(current, err)
	}
}

func TestParentAttentionHonorsWaveAndTaskControls(t *testing.T) {
	for _, mode := range []string{"wave_pause", "cancel", "scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, _, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"specification_change"})
			if _, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil {
				t.Fatal(err)
			}
			pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
			if err != nil || len(pending) != 1 {
				t.Fatal(pending, err)
			}
			switch mode {
			case "wave_pause":
				mustControl(t, k, operatorRequest(w, "pause", "pause", ""))
			case "cancel":
				mustControl(t, k, operatorRequest(w, "cancel", "cancel", ""))
			case "scope":
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				w.ScopeRevision++
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			current, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
			if err != nil || len(current) != 0 {
				t.Fatal("obsolete/held attention remained eligible", current, err)
			}
			if _, err = k.ReserveWake(ctx, WakeRequest{SessionID: w.OrchestratorSessionID, Key: "held", NotificationIDs: []string{pending[0].ID}, IncludeSessionContext: true}); err == nil {
				t.Fatal("direct reserve bypassed current controls")
			}
			if mode == "wave_pause" {
				mustControl(t, k, operatorRequest(w, "resume", "resume", ""))
				current, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
				if err != nil || len(current) != 1 || current[0].ID != pending[0].ID {
					t.Fatal(current, err)
				}
			}
		})
	}
}
func TestParentAttentionEventFailureIsAtomicAndReconciled(t *testing.T) {
	ctx := context.Background()
	k, w, _, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"specification_change"})
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_attention BEFORE INSERT ON kernel_notifications WHEN json_extract(NEW.payload,'$.kind')='parent_attention' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err == nil || changed {
		t.Fatal(changed, err)
	}
	var count int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='parent_attention'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.DB.ExecContext(ctx, "DROP TRIGGER fail_attention"); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := k.ReconcileNotifications(ctx, 128); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	var payload map[string]any
	raw, err := json.Marshal(pending[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	payload["attention_id"] = "invented"
	raw, err = json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.ExecContext(ctx, "UPDATE kernel_notifications SET payload=? WHERE id=?", raw, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16); err == nil {
		t.Fatal("forged attention source accepted")
	}
}

func TestParentAttentionBlockedTaskPauseAndResolvedUnknownRun(t *testing.T) {
	for _, mode := range []string{"blocked_pause", "resolved_unknown"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, b, run := interruptionFixture(t)
			run.State = "unknown"
			if mode == "blocked_pause" {
				run.State = "failed"
				run.TurnID = "failed-turn"
			}
			if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
				t.Fatal(err)
			}
			if mode == "blocked_pause" {
				if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "group_stopped", Identity: managedIdentity(42)}); err != nil {
					t.Fatal(err)
				}
				if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "classified transient failure"); err != nil {
					t.Fatal(err)
				}
			}
			if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
				t.Fatal(changed, err)
			}
			pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
			if err != nil || len(pending) != 1 {
				t.Fatal(pending, err)
			}
			if mode == "blocked_pause" {
				if pending[0].Reason != "task_blocked" {
					t.Fatal(pending)
				}
				mustControl(t, k, operatorRequest(w, "pause-task", "pause", b.TaskID))
			} else {
				if pending[0].Reason != "task_run_unresolved" {
					t.Fatal(pending)
				}
				run.State = "completed"
				run.TurnID = "known-turn"
				if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
					t.Fatal(err)
				}
			}
			pending, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
			if err != nil || len(pending) != 0 {
				t.Fatal("resolved/changed source still woke parent", pending, err)
			}
		})
	}
}

func TestParentAttentionExpiredEvidenceIsHeldAndRestorationPreventsWake(t *testing.T) {
	ctx := context.Background()
	k, w, _, artifact := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, nil)
	expired := artifact
	expired.State, expired.ExpiredAt = "expired", k.clock()
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveArtifact(ctx, tx, expired); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(pending) != 1 || pending[0].Reason != "task_evidence_unavailable" {
		t.Fatal(pending, err)
	}
	if _, err = k.BuildSessionInput(ctx, w.OrchestratorSessionID, []contract.Target{w.Target()}); err != nil {
		t.Fatal("held evidence prevented parent context reconstruction", err)
	}
	set := mustReady(t, k, w)
	if len(set.Tasks) != 0 {
		t.Fatal("expired evidence advanced Wave", set)
	}
	tx, err = k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveArtifact(ctx, tx, artifact); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	pending, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(pending) != 0 {
		t.Fatal("restored evidence caused stale wake", pending, err)
	}
}

func TestParentAttentionInconclusiveAndExhaustedFixBudget(t *testing.T) {
	for _, mode := range []string{"inconclusive", "fix_budget"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, review, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
			if mode == "inconclusive" {
				task, err := k.Task(ctx, review.ID)
				if err != nil {
					t.Fatal(err)
				}
				var result contract.TaskResult
				if err = k.Record(ctx, "task_result", task.ResultID, &result); err != nil {
					t.Fatal(err)
				}
				result.Review.Verdict = "inconclusive"
				result.Review.Findings = []contract.ReviewFinding{}
				if err = k.Put(ctx, "task_result", task.ResultID, task.ID, result); err != nil {
					t.Fatal(err)
				}
			} else {
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				w.FixRounds = w.Policy.Limits.FixRounds
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
				t.Fatal(changed, err)
			}
			pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
			if err != nil || len(pending) != 1 {
				t.Fatal(pending, err)
			}
			if mode == "inconclusive" && pending[0].Reason != "review_inconclusive" || mode == "fix_budget" && !strings.Contains(pending[0].Detail, "budget exhausted") {
				t.Fatal(pending)
			}
			current, err := k.Wave(ctx, w.ID)
			if err != nil || current.Target() != w.Target() || current.MainTurns != 0 || current.Replans != 0 {
				t.Fatal(current, err)
			}
		})
	}
}
