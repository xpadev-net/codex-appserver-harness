package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func rotationFixture(t *testing.T) (*KernelStore, domain.Wave) {
	t.Helper()
	k, request := waveStoreFixture(t)
	request.Policy.Limits.ThreadTurns = 2
	w, err := k.CreateWave(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return k, w
}
func rotationKnownRun(t *testing.T, k *KernelStore, w domain.Wave, id string) {
	t.Helper()
	ctx := context.Background()
	b := prepareWakeSession(t, k, "main")
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: id, ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	result := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: w.Target(), Action: "report", Summary: "saved observation", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}}}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	run.State = "completed"
	run.TurnID = "turn-" + id
	run.Final = string(raw)
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
}
func TestMainThreadMaintenancePreservesBudgetAndCompletesBindingAudit(t *testing.T) {
	ctx := context.Background()
	k, w := rotationFixture(t)
	rotationKnownRun(t, k, w, "one")
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := k.EnsureMainThreadCapacity(ctx, "main", snapshot); err != nil || changed {
		t.Fatal("premature rotation", changed, err)
	}
	rotationKnownRun(t, k, w, "two")
	before, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := k.EnsureMainThreadCapacity(ctx, "main", snapshot); err != nil || !changed {
		t.Fatal(changed, err)
	}
	after, err := k.SessionBinding(ctx, "main")
	if err != nil || after.ThreadID != "" || after.PreparationID != "" || after.Generation != before.Generation+1 || after.ID != before.ID || after.CWD != before.CWD || after.Model != before.Model || after.Sandbox != before.Sandbox {
		t.Fatal(after, err)
	}
	budget, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || budget.MainTurns != 2 || budget.UnobservedRuns != 2 {
		t.Fatal("maintenance changed cumulative budget", budget, err)
	}
	var pending string
	if err = k.Record(ctx, "main_rotation_pending", "main", &pending); err != nil {
		t.Fatal(err)
	}
	var rotation MainThreadRotation
	if err = k.Record(ctx, "main_thread_rotation", pending, &rotation); err != nil || rotation.State != "pending" || rotation.OldThreadID != before.ThreadID || rotation.SnapshotID != snapshot.ID || rotation.SnapshotDigest != snapshot.Digest {
		t.Fatal(rotation, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, k, b)
	if err = k.BindSessionThread(ctx, b, before.ThreadID); err == nil {
		t.Fatal("old thread reused for maintenance")
	}
	if err = k.BindSessionThread(ctx, b, "new-main-thread"); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	if err = k.Record(ctx, "main_thread_rotation", pending, &rotation); err != nil || rotation.State != "bound" || rotation.NewThreadID != "new-main-thread" || rotation.CompletedAt.IsZero() {
		t.Fatal(rotation, err)
	}
	var oldThread, newThread string
	if err = k.DB.QueryRow("SELECT old_thread,new_thread FROM kernel_rebindings WHERE session_id=? AND generation=?", b.ID, b.Generation).Scan(&oldThread, &newThread); err != nil || oldThread != before.ThreadID || newThread != "new-main-thread" {
		t.Fatal(oldThread, newThread, err)
	}
	rotationKnownRun(t, k, w, "three")
	budget, err = k.WaveRunBudget(ctx, w.ID)
	if err != nil || budget.MainTurns != 3 || budget.UnobservedRuns != 3 {
		t.Fatal("new thread reset Wave budget", budget, err)
	}
}
func TestMainThreadReplacementRejectsUnappliedOrLiveRuns(t *testing.T) {
	ctx := context.Background()
	k, b, waves, _, _ := mainResultFixture(t)
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{waves[0].Target()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginMainThreadReplacement(ctx, b, snapshot, "history is unavailable"); err == nil {
		t.Fatal("unapplied live run was abandoned")
	}
}
func TestMainRotationAuditAndThreadBindingCommitTogether(t *testing.T) {
	ctx := context.Background()
	k, w := rotationFixture(t)
	rotationKnownRun(t, k, w, "one")
	before, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	rotation, err := k.BeginMainThreadReplacement(ctx, before, snapshot, "recover known unavailable history")
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, k, b)
	if _, err = k.DB.Exec("CREATE TRIGGER fail_rotation_audit BEFORE UPDATE ON kernel_rebindings BEGIN SELECT RAISE(ABORT,'audit update failure'); END"); err != nil {
		t.Fatal(err)
	}
	if err = k.BindSessionThread(ctx, b, "new-main-thread"); err == nil {
		t.Fatal("unaudited replacement thread bound")
	}
	saved, err := k.SessionBinding(ctx, b.ID)
	if err != nil || saved.ThreadID != "" {
		t.Fatal("audit failure partially bound a thread", saved, err)
	}
	var pending MainThreadRotation
	if err = k.Record(ctx, "main_thread_rotation", rotation.ID, &pending); err != nil || pending.State != "pending" {
		t.Fatal(pending, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER fail_rotation_audit"); err != nil {
		t.Fatal(err)
	}
	if err = k.BindSessionThread(ctx, b, "new-main-thread"); err != nil {
		t.Fatal(err)
	}
}
func TestMainReplacementRejectsUnsavedStaleOrForeignInput(t *testing.T) {
	for _, mode := range []string{"unsaved", "stale", "wrong-session", "paused", "task"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w := rotationFixture(t)
			rotationKnownRun(t, k, w, "one")
			b, err := k.SessionBinding(ctx, "main")
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unsaved":
				snapshot.ID = "unknown"
			case "stale":
				w.ScopeRevision++
			case "wrong-session":
				b.ID = "foreign"
			case "paused":
				w.Paused = true
			case "task":
				b.Kind = "task"
			}
			if mode == "stale" || mode == "paused" {
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = k.BeginMainThreadReplacement(ctx, b, snapshot, "explicit recovery"); err == nil {
				t.Fatal("untrusted/stale replacement accepted")
			}
		})
	}
}

func TestLostReplacementCreationRequiresStopAndKeepsRotationAudit(t *testing.T) {
	ctx := context.Background()
	k, w := rotationFixture(t)
	rotationKnownRun(t, k, w, "one")
	rotationKnownRun(t, k, w, "two")
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := k.EnsureMainThreadCapacity(ctx, "main", snapshot); err != nil || !changed {
		t.Fatal(changed, err)
	}
	var rotationID string
	if err = k.Record(ctx, "main_rotation_pending", "main", &rotationID); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, b); err != nil {
		t.Fatal(err)
	}
	lost, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.RecoverUnboundMainThread(ctx, lost, snapshot, "lost replacement thread response"); err == nil {
		t.Fatal("unconfirmed creation was abandoned")
	}
	if err = k.MarkSessionStopped(ctx, lost.ID, lost.PreparationID); err != nil {
		t.Fatal(err)
	}
	if err = k.RecoverUnboundMainThread(ctx, lost, snapshot, "lost replacement thread response"); err != nil {
		t.Fatal(err)
	}
	current, err := k.SessionBinding(ctx, lost.ID)
	if err != nil || current.Generation != lost.Generation+1 || current.ThreadID != "" {
		t.Fatal(current, err)
	}
	if err = k.BindSessionThread(ctx, lost, "late-old-generation-thread"); err == nil {
		t.Fatal("late old-generation response bound after recovery")
	}
	current = reserveSessionFixture(t, k, current)
	if err = k.BindSessionThread(ctx, current, "recovered-new-main"); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, current.ID, current.PreparationID); err != nil {
		t.Fatal(err)
	}
	var rotation MainThreadRotation
	if err = k.Record(ctx, "main_thread_rotation", rotationID, &rotation); err != nil || rotation.Generation != current.Generation || rotation.State != "bound" || rotation.NewThreadID != "recovered-new-main" {
		t.Fatal(rotation, err)
	}
	budget, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || budget.MainTurns != 2 {
		t.Fatal("lost creation recovery reset/spent model budget", budget, err)
	}
	var count int
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='main_rotation_recovery' AND work_item_id=?", current.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}
