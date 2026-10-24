package product

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestExecutionWorkerReportsSealingExceptionsWithoutRetryOrAdoption(t *testing.T) {
	for _, mode := range []string{"failed", "owner_unresolved", "adoption_held"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			e, task, ready, _, _ := conflictFixFixture(t)
			task, run := stoppedProductWriterRunFixture(t, e.Store, ready, task)
			reason := "product_seal_failed"
			if mode == "owner_unresolved" {
				saved, err := e.Store.ReserveProductSeal(ctx, task.ID, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if claim, err := e.Store.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
					t.Fatal(claim, err)
				}
				reason = "product_seal_owner_unresolved"
			} else if mode == "adoption_held" {
				// Existing integration helper seals real resolved files before moving
				// the selected candidate; it must never overwrite that selection.
				if err := os.WriteFile(filepath.Join(ready.Value.CWD, "base.txt"), []byte("resolved both changes\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if sealed, err := (CandidateSealer{Store: e.Store}).Seal(ctx, task.ID, run.ID); err != nil || sealed.State != "sealed" {
					t.Fatal(sealed, err)
				}
				moveConflictCandidate(t, e, task, ready)
				reason = "product_seal_adoption_held"
			}
			before, err := e.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			worker := NewExecutionWorker(e, time.Second)
			if changed, err := worker.task(ctx, task.ID); !changed || err == nil {
				t.Fatal("exception was not observed", changed, err)
			}
			first, err := e.Store.ProductSeal(ctx, task.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := e.Store.ReconcileNotifications(ctx, 128); err != nil {
				t.Fatal(err)
			}
			notes, err := e.Store.PendingWakeNotifications(ctx, task.ParentSessionID, 128)
			if err != nil {
				t.Fatal(err)
			}
			var found []storage.Notification
			for _, n := range notes {
				if n.TaskID == task.ID && n.Sealing != nil {
					found = append(found, n)
				}
			}
			if len(found) != 1 || found[0].Reason != reason || found[0].Sealing.RunID != run.ID {
				t.Fatal(found)
			}
			raw, err := json.Marshal(found[0])
			if err != nil || strings.Contains(string(raw), ready.Value.CWD) || strings.Contains(string(raw), ready.Value.Request.Repository) {
				t.Fatal("private checkout leaked", string(raw), err)
			}
			if err := e.Store.AcknowledgeNotification(ctx, found[0].SessionID, found[0].ID); err != nil {
				t.Fatal(err)
			}
			if changed, err := worker.task(ctx, task.ID); changed || err == nil {
				t.Fatal("poll repeated mutation", changed, err)
			}
			after, err := e.Store.ProductSeal(ctx, task.ID, run.ID)
			if err != nil || !reflect.DeepEqual(first, after) {
				t.Fatal("poll retried sealing", after, err)
			}
			current, err := e.Store.Task(ctx, task.ID)
			// The existing adoption gate may persist a blocked Task. This is
			// still the original unapplied run, never accepted Task success.
			if err != nil || current.State != contract.TaskRunning && current.State != contract.TaskBlocked || current.ResultID != "" {
				t.Fatal("exception applied task", current, err)
			}
			current.State, current.Reason = task.State, task.Reason
			if !reflect.DeepEqual(current, task) {
				t.Fatal("exception changed Task identity/budget", current)
			}
			p, err := e.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
			if err != nil || !reflect.DeepEqual(p, before) {
				t.Fatal("exception changed candidate", p, err)
			}
			reservation, err := e.Store.OperationReservation(ctx, first.ID)
			if err != nil || (mode == "owner_unresolved" && reservation.State != "held") {
				t.Fatal("unknown owner lost capacity", reservation, err)
			}
		})
	}
}
