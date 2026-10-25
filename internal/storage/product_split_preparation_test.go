package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestProductSplitPreparationCommitsCompleteSetAndReplays(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductSplitExecution(ctx, r)
	if err != nil || len(saved.Value.Creations) != 2 {
		t.Fatal(saved, err)
	}
	for _, ref := range saved.Value.Creations {
		job, err := k.ReadProductSplitCreate(ctx, ref.ID)
		if err != nil || job.Digest != ref.Digest || job.State != "prepared" || job.HTTP != nil || job.Value.Child.Value.Depth != 1 || job.Value.Request.Target != w.Target() {
			t.Fatal("incomplete initial journal", job, err)
		}
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("execution budget reset", after, err)
	}
	var count, amount int
	if err := k.DB.QueryRow("SELECT count(*),sum(amount) FROM kernel_budget_ledger WHERE kind='split_children'").Scan(&count, &amount); err != nil || count != 1 || amount != 2 {
		t.Fatal("whole allocation not charged once", count, amount, err)
	}
	for _, q := range []string{"SELECT count(*) FROM kernel_tasks", "SELECT count(*) FROM kernel_run_bindings", "SELECT count(*) FROM records WHERE kind='product_target'"} {
		var count int
		if err := k.DB.QueryRow(q).Scan(&count); err != nil || count != 0 {
			t.Fatal("preparation granted execution", q, count, err)
		}
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&count); err != nil || count != 1 {
		t.Fatal("child registration inferred", count, err)
	}
	replay, err := k.PrepareProductSplitExecution(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, replay) {
		t.Fatal("replay changed complete set", replay, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-complete-split", Target: w.Target(), Action: "pause", Reason: "audit only"}); err != nil {
		t.Fatal(err)
	}
	replay, err = k.PrepareProductSplitExecution(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, replay) {
		t.Fatal("restart/pause changed complete set", replay, err)
	}
}

func TestProductSplitPreparationLateFailuresRollBackEveryStage(t *testing.T) {
	for _, kind := range []string{"product_split_create_admission", "product_split_preparation_creation"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, _, r := productSplitProposalFixture(t)
			if _, err := k.DB.Exec("CREATE TRIGGER fail_second_split BEFORE INSERT ON records WHEN NEW.kind='" + kind + "' AND (SELECT count(*) FROM records WHERE kind='" + kind + "')=1 BEGIN SELECT RAISE(ABORT,'second child failure'); END"); err != nil {
				t.Fatal(err)
			}
			got, err := k.PrepareProductSplitExecution(ctx, r)
			if err == nil || got.ID != "" {
				t.Fatal("partial preparation returned", got, err)
			}
			for _, q := range []string{"SELECT count(*) FROM records WHERE kind LIKE 'product_split_%'", "SELECT count(*) FROM kernel_budget_ledger WHERE kind='split_children'", "SELECT count(*) FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"} {
				var count int
				if err := k.DB.QueryRow(q).Scan(&count); err != nil || count != 0 {
					t.Fatal("partial transaction committed", q, count, err)
				}
			}
			if _, err := k.DB.Exec("DROP TRIGGER fail_second_split"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.PrepareProductSplitExecution(ctx, r); err != nil {
				t.Fatal("failed attempt could not retry", err)
			}
		})
	}
}

func TestProductSplitPreparationEvidenceLossHoldsAndExactRestore(t *testing.T) {
	ctx := context.Background()
	k, _, r := productSplitProposalFixture(t)
	saved, err := k.PrepareProductSplitExecution(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_split_preparation", splitProposalHead(r)},
		{"product_split_preparation_history", saved.ID},
		{"product_split_preparation_admission", splitProposalHead(r)},
		{"product_split_preparation_creation", saved.Value.Creations[0].ID},
	} {
		t.Run(loss.kind, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			if got, err := k.PrepareProductSplitExecution(ctx, r); got.ID != "" || !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("receipt reconstructed", got, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			if got, err := k.PrepareProductSplitExecution(ctx, r); err != nil || !reflect.DeepEqual(got, saved) {
				t.Fatal("restore changed receipt", got, err)
			}
		})
	}
}

func TestProductSplitPreparationReceiptAlonePreventsRecreatingKnownJournalsAndSeats(t *testing.T) {
	for _, keep := range []string{"product_split_preparation", "product_split_preparation_history", "product_split_preparation_admission", "product_split_preparation_creation"} {
		t.Run(keep, func(t *testing.T) {
			ctx := context.Background()
			k, w, r := productSplitProposalFixture(t)
			_, err := k.PrepareProductSplitExecution(ctx, r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", keep); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_children'"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			changed := r
			changed.Key = "replacement-key"
			if _, err := k.PrepareProductSplitProposal(ctx, changed); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("remaining receipt allowed replacement proposal", err)
			}
			if _, err := k.PrepareProductSplitExecution(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("remaining receipt allowed preparation", err)
			}
			if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", ProductWorkItemDefinition{ID: "cannot-refund", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("missing child seats silently refunded", err)
			}
		})
	}
}

func TestProductSplitPreparationCurrentPauseDoesNotConsumeAnyIntent(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-split", Target: w.Target(), Action: "pause", Reason: "no new children"}); err != nil {
		t.Fatal(err)
	}
	if got, err := k.PrepareProductSplitExecution(ctx, r); got.ID != "" || !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("pause admitted preparation", got, err)
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_split_%'").Scan(&n); err != nil || n != 0 {
		t.Fatal("pause left intents", n, err)
	}
}

func TestProductSplitPreparationReceiptPreventsResetOfAnEntireMissingInitialJournal(t *testing.T) {
	for _, keep := range []string{"product_split_preparation", "product_split_preparation_history", "product_split_preparation_admission", "product_split_preparation_creation"} {
		t.Run(keep, func(t *testing.T) {
			ctx := context.Background()
			k, _, r := productSplitProposalFixture(t)
			saved, err := k.PrepareProductSplitExecution(ctx, r)
			if err != nil {
				t.Fatal(err)
			}
			job, err := k.ReadProductSplitCreate(ctx, saved.Value.Creations[0].ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_create%' OR (kind LIKE 'product_split_preparation%' AND kind<>?)", keep); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.PrepareProductSplitCreate(ctx, job.Value.Request, job.Value.Child.ID, job.Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("known initial journal was recreated", err)
			}
			if _, err := k.ReadLatestProductSplitCreate(ctx, job.Value.Child.ID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("known lost journal appeared absent", err)
			}
		})
	}
}

func TestProductSplitPreparationConcurrentConnectionsShareOneCompleteSet(t *testing.T) {
	ctx := context.Background()
	k, _, r := productSplitProposalFixture(t)
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	type result struct {
		saved ProductSplitPreparation
		err   error
	}
	start := make(chan struct{})
	done := make(chan result, 2)
	for _, store := range []*KernelStore{k, other} {
		go func(store *KernelStore) {
			<-start
			saved, err := store.PrepareProductSplitExecution(ctx, r)
			done <- result{saved, err}
		}(store)
	}
	close(start)
	first, second := <-done, <-done
	if first.err != nil && second.err != nil {
		t.Fatal("no complete-set winner", first.err, second.err)
	}
	for _, outcome := range []result{first, second} {
		if outcome.err != nil {
			var busy sqlite3.Error
			if outcome.saved.ID != "" || !errors.As(outcome.err, &busy) || busy.Code != sqlite3.ErrBusy && busy.Code != sqlite3.ErrLocked {
				t.Fatal("unexpected concurrent failure", outcome)
			}
		}
	}
	winner := first.saved
	if first.err != nil {
		winner = second.saved
	}
	for _, store := range []*KernelStore{k, other} {
		replayed, err := store.PrepareProductSplitExecution(ctx, r)
		if err != nil || !reflect.DeepEqual(replayed, winner) {
			t.Fatal("retry changed winning complete set", replayed, err)
		}
	}
	var journals, allocations int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_create'").Scan(&journals); err != nil || journals != 2 {
		t.Fatal(journals, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind='split_children'").Scan(&allocations); err != nil || allocations != 1 {
		t.Fatal(allocations, err)
	}
}
