package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func publicationCall(t *testing.T, b contract.SessionBinding, run contract.SessionRun, target contract.Target, content string) contract.ToolInvocation {
	t.Helper()
	return kernelCall(t, b, run, "artifact_publish", contract.PublishArtifactRequest{SchemaVersion: 1, Target: target, Content: content, ContentType: "text/plain", Kind: "report", InputArtifactIDs: []string{}})
}
func blockArtifactDirectory(t *testing.T, k *KernelStore) func() {
	t.Helper()
	path, held := filepath.Join(k.Root, "objects"), filepath.Join(k.Root, "objects-held")
	if err := os.Rename(path, held); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(held, path); err != nil {
			t.Fatal(err)
		}
	}
}
func savePublicationTaskOutcome(t *testing.T, k *KernelStore, b contract.SessionBinding, run contract.SessionRun, turn string, result contract.TaskResult) ResultApplication {
	t.Helper()
	ctx := context.Background()
	raw, err := marshalTaskResultFixture(result)
	if err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", turn, string(raw)
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
func TestKernelPublicationIsImmutableScopedAndReplayable(t *testing.T) {
	for _, kind := range []string{"task", "orchestrator"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, wave, _, b, run := kernelToolFixture(t, kind)
			gateway := contract.KernelGateway{Backend: k}
			call := publicationCall(t, b, run, wave.Target(), "native report")
			var group sync.WaitGroup
			group.Add(2)
			replies := make([]contract.ToolReply, 2)
			errs := make([]error, 2)
			for i := range replies {
				go func(i int) { defer group.Done(); replies[i], errs[i] = gateway.Handle(ctx, call) }(i)
			}
			group.Wait()
			if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(replies[0], replies[1]) || replies[0].State != "completed" {
				t.Fatal(replies, errs)
			}
			reply := replies[0]
			a, err := k.Artifact(ctx, reply.ResourceID)
			if err != nil || a.State != "committed" || a.Digest != fmt.Sprintf("%x", sha256.Sum256([]byte("native report"))) || a.SessionID != b.ID {
				t.Fatal(a, err)
			}
			origin := domain.ArtifactOrigin{Kind: "task", Reference: b.TaskID}
			if kind == "orchestrator" {
				origin = domain.ArtifactOrigin{Kind: "session", Reference: b.ID}
			}
			if a.Origin != origin || a.Target() != wave.Target() {
				t.Fatal("model publication invented provenance", a)
			}
			_, data, err := k.ReadArtifact(ctx, b.ID, wave.Target(), a.ID, 0, 100)
			if err != nil || string(data) != "native report" {
				t.Fatal(string(data), err)
			}
			var artifacts, ledger int
			k.DB.QueryRow("SELECT count(*) FROM kernel_artifacts").Scan(&artifacts)
			k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind='artifact_bytes'").Scan(&ledger)
			if artifacts != 1 || ledger != 1 {
				t.Fatal("replay duplicated quota or objects", artifacts, ledger)
			}
			var pub kernelPublication
			if err = k.Record(ctx, "kernel_publication", reply.OperationID, &pub); err != nil || pub.Content != "" {
				t.Fatal("committed plaintext outlived immutable object", pub.Content, err)
			}
			items, err := k.Notifications(ctx, b.ID, 0, 10)
			if err != nil || len(items) != 1 || items[0].State != "bound_to_tool" || items[0].RunID != run.ID {
				t.Fatal(items, err)
			}
			if pending, err := k.PendingWakeNotifications(ctx, b.ID, 10); err != nil || len(pending) != 0 {
				t.Fatal("synchronous success created another wake", pending, err)
			}
			altered := publicationCall(t, b, run, wave.Target(), "different report")
			if _, err = gateway.Handle(ctx, altered); err == nil {
				t.Fatal("call ID overwrote committed publication")
			}
		})
	}
}
func TestKernelPublicationUnknownWaitAndRecoveryNeedNoModelRetry(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	restore := blockArtifactDirectory(t, k)
	call := publicationCall(t, b, run, wave.Target(), "recoverable bytes")
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || reply.State != "unknown" || len(reply.SubscriptionIDs) != 1 || reply.ResourceID == "" {
		t.Fatal(reply, err)
	}
	a, err := k.Artifact(ctx, reply.ResourceID)
	if err != nil || a.State != "staged" {
		t.Fatal(a, err)
	}
	var charged int64
	if err = k.DB.QueryRow("SELECT sum(bytes) FROM kernel_artifacts WHERE state<>'expired'").Scan(&charged); err != nil || charged != int64(len("recoverable bytes")) {
		t.Fatal(charged, err)
	}
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "waiting", SubscriptionIDs: reply.SubscriptionIDs}
	receipt := savePublicationTaskOutcome(t, k, b, run, call.TurnID, result)
	if receipt.State != contract.TaskWaiting {
		t.Fatal(receipt)
	}
	restore()
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if report, err := k.ReconcileKernelPublications(ctx, 10); err != nil || len(report.Completed) != 1 || len(report.Pending) != 0 {
		t.Fatal(report, err)
	}
	if report, err := k.ReconcileKernelPublications(ctx, 10); err != nil || len(report.Completed) != 0 || len(report.Pending) != 0 {
		t.Fatal(report, err)
	}
	pending, err := k.PendingWakeNotifications(ctx, b.ID, 10)
	if err != nil || len(pending) != 1 || pending[0].OperationID != reply.OperationID || pending[0].ArtifactID != a.ID || pending[0].OperationState != "completed" {
		t.Fatal(pending, err)
	}
	_, data, err := k.ReadArtifact(ctx, b.ID, b.Target, a.ID, 0, 100)
	if err != nil || string(data) != "recoverable bytes" {
		t.Fatal(string(data), err)
	}
	if _, err = k.ReserveWake(ctx, WakeRequest{SessionID: b.ID, Key: "publication-ready", NotificationIDs: []string{pending[0].ID}}); err != nil {
		t.Fatal(err)
	}
	current, err := k.Task(ctx, b.TaskID)
	if err != nil || current.Attempts != 1 {
		t.Fatal("reconciliation spent a model attempt", current, err)
	}
}
func TestKernelPublicationManifestResultNotificationCommitTogether(t *testing.T) {
	for _, failure := range []string{"operation", "notification"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			k, wave, _, b, run := kernelToolFixture(t, "task")
			statement := "CREATE TRIGGER inject_publication BEFORE UPDATE ON kernel_tool_operations WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'publication result failure'); END"
			if failure == "notification" {
				statement = "CREATE TRIGGER inject_publication BEFORE INSERT ON kernel_notifications BEGIN SELECT RAISE(ABORT,'publication notification failure'); END"
			}
			if _, err := k.DB.Exec(statement); err != nil {
				t.Fatal(err)
			}
			reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, publicationCall(t, b, run, wave.Target(), "atomic publication"))
			if err != nil || reply.State != "unknown" {
				t.Fatal(reply, err)
			}
			a, err := k.Artifact(ctx, reply.ResourceID)
			if err != nil || a.State != "staged" {
				t.Fatal("manifest committed without result/notification", a, err)
			}
			var events, notifications int
			k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind IN ('artifact_committed','kernel_operation_completed')").Scan(&events)
			k.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&notifications)
			if events != 0 || notifications != 0 {
				t.Fatal("partial completion survived", events, notifications)
			}
			if _, err = k.DB.Exec("DROP TRIGGER inject_publication"); err != nil {
				t.Fatal(err)
			}
			if report, err := k.ReconcileKernelPublications(ctx, 10); err != nil || len(report.Completed) != 1 || len(report.Pending) != 0 {
				t.Fatal(report, err)
			}
			items, err := k.Notifications(ctx, b.ID, 0, 10)
			if err != nil || len(items) != 1 || items[0].State != "pending" {
				t.Fatal(items, err)
			}
		})
	}
}
func TestKernelPublicationAdmissionRollsBackQuotaAndRejectsOriginSpoof(t *testing.T) {
	for _, failure := range []string{"quota", "operation", "origin"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			k, wave, _, b, run := kernelToolFixture(t, "task")
			call := publicationCall(t, b, run, wave.Target(), "publication")
			if failure == "quota" {
				k.ControllerArtifactBytes = 2
			}
			if failure == "operation" {
				if _, err := k.DB.Exec("CREATE TRIGGER inject_publication_admission BEFORE INSERT ON kernel_tool_operations BEGIN SELECT RAISE(ABORT,'admission failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "origin" {
				var raw map[string]any
				if err := json.Unmarshal(call.Arguments, &raw); err != nil {
					t.Fatal(err)
				}
				raw["origin"] = map[string]string{"kind": "repository", "reference": "forged"}
				call.Arguments, _ = json.Marshal(raw)
			}
			if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
				t.Fatal("invalid publication admitted")
			}
			var artifacts, operations, ledger int
			k.DB.QueryRow("SELECT count(*) FROM kernel_artifacts").Scan(&artifacts)
			k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&operations)
			k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind='artifact_bytes'").Scan(&ledger)
			files, err := os.ReadDir(filepath.Join(k.Root, "objects"))
			if err != nil || artifacts != 0 || operations != 0 || ledger != 0 || len(files) != 0 {
				t.Fatal("unreserved artifact effect survived", artifacts, operations, ledger, files, err)
			}
		})
	}
}
func TestKernelPublicationDirectCompletionAppliesWithoutSpuriousWake(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	call := publicationCall(t, b, run, wave.Target(), "already delivered")
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || reply.State != "completed" {
		t.Fatal(reply, err)
	}
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "blocked", Blockers: []string{"need parent clarification"}}
	receipt := savePublicationTaskOutcome(t, k, b, run, call.TurnID, result)
	if receipt.State != contract.TaskBlocked {
		t.Fatal(receipt)
	}
	items, err := k.Notifications(ctx, b.ID, 0, 10)
	if err != nil || len(items) != 1 || items[0].State != "applied" {
		t.Fatal(items, err)
	}
	if pending, err := k.PendingWakeNotifications(ctx, b.ID, 10); err != nil || len(pending) != 0 {
		t.Fatal("delivered publication consumed another attempt", pending, err)
	}
}
func TestKernelPublicationLateRecoveryRetainsOriginalScope(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	restore := blockArtifactDirectory(t, k)
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, publicationCall(t, b, run, wave.Target(), "old scope bytes"))
	if err != nil || reply.State != "unknown" {
		t.Fatal(reply, err)
	}
	wave, err = k.Wave(ctx, wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	wave.ScopeRevision++
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveWave(ctx, tx, wave); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	restore()
	if report, err := k.ReconcileKernelPublications(ctx, 10); err != nil || len(report.Completed) != 1 || len(report.Pending) != 0 {
		t.Fatal(report, err)
	}
	a, err := k.Artifact(ctx, reply.ResourceID)
	if err != nil || a.Target() != b.Target {
		t.Fatal("late artifact adopted new scope", a, err)
	}
	items, err := k.Notifications(ctx, b.ID, 0, 10)
	if err != nil || len(items) != 1 || items[0].Target != b.Target || items[0].Target == wave.Target() {
		t.Fatal(items, err)
	}
	if pending, err := k.PendingWakeNotifications(ctx, b.ID, 10); err != nil || len(pending) != 0 {
		t.Fatal("old publication woke new scope", pending, err)
	}
}
func TestKernelPublicationDirectResultSurvivesFailedSourceRun(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	call := publicationCall(t, b, run, wave.Target(), "keep across failed source")
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID = "failed", call.TurnID
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "failed source"); err != nil {
		t.Fatal(err)
	}
	if pending, err := k.PendingWakeNotifications(ctx, b.ID, 10); err != nil || len(pending) != 1 || pending[0].OperationState != "completed" {
		t.Fatal("known result lost on retry", pending, err)
	}
}

func TestKernelPublicationCannotClaimUngrantedInputAndPendingIsReported(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	private := mustImport(t, k, ArtifactImport{SessionID: "main", Target: wave.Target(), Key: "private-input", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "private source"}}, "not assigned to Task")
	call := kernelCall(t, b, run, "artifact_publish", contract.PublishArtifactRequest{SchemaVersion: 1, Target: wave.Target(), Content: "derivative", Kind: "report", ContentType: "text/plain", InputArtifactIDs: []string{private.ID}})
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("publication claimed an ungranted input")
	}
	var artifacts, operations int
	k.DB.QueryRow("SELECT count(*) FROM kernel_artifacts").Scan(&artifacts)
	k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&operations)
	if artifacts != 1 || operations != 0 {
		t.Fatal(artifacts, operations)
	}
	restore := blockArtifactDirectory(t, k)
	call = publicationCall(t, b, run, wave.Target(), "still pending")
	call.CallID = "pending"
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || reply.State != "unknown" {
		t.Fatal(reply, err)
	}
	report, err := k.ReconcileKernelPublications(ctx, 10)
	if err != nil || len(report.Completed) != 0 || len(report.Pending) != 1 || report.Pending[0] != reply.OperationID {
		t.Fatal("unrecovered publication looked completed", report, err)
	}
	restore()
}
