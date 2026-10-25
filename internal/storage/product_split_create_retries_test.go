package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func abortedSplitRetryFixture(t *testing.T) (*KernelStore, ProductSplitChildPlanRequest, ProductSplitCreate) {
	t.Helper()
	k, _, r, job := splitCreationFixture(t)
	aborted, err := k.AbortUnactivatedProductSplitCreate(context.Background(), job.ID, job.Value.OwnerID, job.Digest, "initial-no-send")
	if err != nil {
		t.Fatal(err)
	}
	return k, r, aborted
}
func TestProductSplitUnsentRetryKeepsChildSeatsMarkerAndPhysicalFences(t *testing.T) {
	ctx := context.Background()
	k, r, prior := abortedSplitRetryFixture(t)
	allocation, err := k.ReadProductSplitAllocation(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	wave, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "restarted-controller")
	if err != nil {
		t.Fatal(err)
	}
	if retry.ID == prior.ID || retry.Value.RetryOf != prior.ID || retry.Value.Attempt != 1 || retry.Lease.Generation <= prior.Lease.Generation || !reflect.DeepEqual(retry.Value.Child, prior.Value.Child) || retry.Value.AllocationID != prior.Value.AllocationID || retry.Value.Request != prior.Value.Request {
		t.Fatal("retry changed child/scope/seats", retry)
	}
	latest, err := k.ReadLatestProductSplitCreate(ctx, prior.Value.Child.ID)
	if err != nil || !reflect.DeepEqual(latest, retry) {
		t.Fatal("latest retry not bound", latest, err)
	}
	replay, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "restarted-controller")
	if err != nil || !reflect.DeepEqual(replay, retry) {
		t.Fatal("retry charged twice", err)
	}
	old, err := k.ReadProductSplitCreate(ctx, prior.ID)
	if err != nil || !reflect.DeepEqual(old, prior) {
		t.Fatal("original intent rewritten", err)
	}
	afterAllocation, err := k.ReadProductSplitAllocation(ctx, r)
	if err != nil || !reflect.DeepEqual(afterAllocation, allocation) {
		t.Fatal("retry reset WorkItem reservation", err)
	}
	afterWave, err := k.Wave(ctx, wave.ID)
	if err != nil || !reflect.DeepEqual(afterWave, wave) {
		t.Fatal("retry reset execution budgets", err)
	}
	identity := startSplitCreationOwner(t, k, retry)
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, retry.ID, "unknown-owner"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("active unknown owner permitted a retry", err)
	}
	stop, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := process.StopGroup(stop, identity); err != nil {
		t.Fatal(err)
	}
	stopped, err := k.FinishProductSplitCreateHTTP(ctx, retry.ID, retry.Value.OwnerID, retry.Digest, strings.Repeat("a", 64), "retry-physical-stop")
	if err != nil || stopped.State != "stopped" {
		t.Fatal(stopped, err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, retry.ID, "not-allowed"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("unknown result allowed another POST attempt", err)
	}
	observedBudget, err := k.WaveOperationBudget(ctx, wave.ID)
	if err != nil || observedBudget.SplitCreateRetries != 1 || observedBudget.Limits.SplitCreateRetries != 1 {
		t.Fatal("retry use not observable", observedBudget, err)
	}
	var amount, count int
	if err := k.DB.QueryRow("SELECT count(*),sum(amount) FROM kernel_budget_ledger WHERE kind='split_create_retry' AND wave_id=?", wave.ID).Scan(&count, &amount); err != nil || count != 1 || amount != 1 {
		t.Fatal("retry ledger", count, amount, err)
	}
}

func TestProductSplitUnsentRetryLifetimeBudgetAcrossChildrenAndReopen(t *testing.T) {
	ctx := context.Background()
	k, r, prior := abortedSplitRetryFixture(t)
	retry, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, retry.ID, retry.Value.OwnerID, retry.Digest, "retry-no-send"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, retry.ID, "third-owner"); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("retry cap reset along chain", err)
	}
	plan, err := k.ReadProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	second, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[1].ID, "another-child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, second.ID, second.Value.OwnerID, second.Digest, "other-no-send"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, second.ID, "another-retry"); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("another child reset Wave lifetime budget", err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, second.ID, "after-reopen"); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("reopen reset retry budget", err)
	}
}

func TestProductSplitUnsentRetryLateLedgerFailureRollsBackAllFactsAndLease(t *testing.T) {
	ctx := context.Background()
	k, _, prior := abortedSplitRetryFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_split_retry BEFORE INSERT ON kernel_budget_ledger WHEN NEW.kind='split_create_retry' BEGIN SELECT RAISE(FAIL,'late retry ledger failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner"); err == nil {
		t.Fatal("fault not reached")
	}
	id := splitCreateRetryID(prior.ID)
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE id=? OR work_item_id=?", id, id).Scan(&n); err != nil || n != 0 {
		t.Fatal("partial retry committed", n, err)
	}
	original, err := k.ReadProductSplitCreate(ctx, prior.ID)
	if err != nil || !reflect.DeepEqual(original, prior) {
		t.Fatal("failed retry changed predecessor lease/history", err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_split_retry"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner"); err != nil {
		t.Fatal("rollback prevented safe retry", err)
	}
}

func TestProductSplitUnsentRetryLostProofNeverFallsBackOrRecreates(t *testing.T) {
	ctx := context.Background()
	k, r, prior := abortedSplitRetryFixture(t)
	saved, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner")
	if err != nil {
		t.Fatal(err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_split_create", saved.ID}, {"product_split_create_history", saved.ID}, {"product_split_create_admission", saved.ID},
		{"product_split_create_event", splitCreateEventID(saved.ID, 0)}, {"product_split_create_retry_budget", saved.ID},
		{"product_split_create", prior.ID}, {"product_split_create_event", splitCreateEventID(prior.ID, 1)},
	} {
		t.Run(loss.kind+"/"+loss.id, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadLatestProductSplitCreate(ctx, prior.Value.Child.ID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("latest fell back after proof loss", err)
			}
			if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner"); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost proof replaced retry", err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
		})
	}
	var raw []byte
	if err := k.DB.QueryRow("SELECT payload FROM kernel_budget_ledger WHERE kind='split_create_retry' AND operation_key=?", saved.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_create_retry'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("lost ledger refunded retry", err)
	}
	if _, err := k.DB.Exec("INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'split_create_retry',1,0,?)", r.Target.WaveID, saved.ID, raw); err != nil {
		t.Fatal(err)
	}
	restored, err := k.ReadLatestProductSplitCreate(ctx, prior.Value.Child.ID)
	if err != nil || !reflect.DeepEqual(restored, saved) {
		t.Fatal("trusted restore changed intent", err)
	}
}

func TestProductSplitRetryLimitsValidateAndLegacyJSONDoesNotGainPermission(t *testing.T) {
	for _, value := range []int{-1, 33} {
		k, r := waveStoreFixture(t)
		limits := DefaultWaveOperationLimits()
		limits.SplitCreateRetries = value
		r.OperationLimits = &limits
		if _, err := k.CreateWave(context.Background(), r); err == nil {
			t.Fatal("unbounded split retry admission", value)
		}
		var n int
		if err := k.DB.QueryRow("SELECT count(*) FROM kernel_waves").Scan(&n); err != nil || n != 0 {
			t.Fatal("invalid limits partially admitted", n, err)
		}
	}
	ctx := context.Background()
	k, _, proposalRequest := productSplitProposalFixture(t)
	limits, err := k.WaveOperationLimits(ctx, proposalRequest.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	limits.SplitCreateRetries = 0
	raw, err := json.Marshal(limits)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "split_create_retries") {
		t.Fatal("legacy disabled encoding gained field")
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='wave_operation_limits' AND id=?", raw, proposalRequest.Target.WaveID); err != nil {
		t.Fatal(err)
	}
	proposal, err := k.PrepareProductSplitProposal(ctx, proposalRequest)
	if err != nil {
		t.Fatal(err)
	}
	r := ProductSplitChildPlanRequest{Target: proposalRequest.Target, ParentWorkItemID: proposalRequest.ParentWorkItemID, ProposalID: proposal.ID, ProposalDigest: proposal.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	job, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "legacy-owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "legacy-no-send"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, job.ID, "not-authorized"); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("missing legacy limit acquired retry permission", err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	saved, err := k.WaveOperationLimits(ctx, r.Target.WaveID)
	if err != nil || saved != limits {
		t.Fatal("reopen overlaid retry default", saved, err)
	}
}

func TestProductSplitRetryBudgetWitnessAlonePreventsInitialAndUpstreamRecreation(t *testing.T) {
	for _, witness := range []string{"record", "ledger"} {
		t.Run(witness, func(t *testing.T) {
			ctx := context.Background()
			k, r, prior := abortedSplitRetryFixture(t)
			if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner"); err != nil {
				t.Fatal(err)
			}
			// Keep proposal/plan/allocation, but lose every creation index and lease.
			keep := "product_split_create_retry_budget"
			if witness == "ledger" {
				keep = "none"
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_create%' AND kind<>?", keep); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			if witness == "record" {
				if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_create_retry'"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.PrepareProductSplitCreate(ctx, r, prior.Value.Child.ID, prior.Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("retry witness permitted initial resend", err)
			}
			if _, err := k.ReadLatestProductSplitCreate(ctx, prior.Value.Child.ID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("retry witness became absent", err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", keep); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_children'"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("retry witness recreated child plan", err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("retry witness recreated child seats", err)
			}
			if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "new-after-loss", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("retry witness freed catalog seats", err)
			}
		})
	}
}

func TestProductSplitUnsentRetryKeepsLifetimeSnapshotAcrossDirection(t *testing.T) {
	ctx := context.Background()
	k, r, prior := abortedSplitRetryFixture(t)
	retry, err := k.ReserveProductSplitCreateUnsentRetry(ctx, prior.ID, "retry-owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, retry.ID, retry.Value.OwnerID, retry.Digest, "second-no-send"); err != nil {
		t.Fatal(err)
	}
	before, err := k.WaveOperationLimits(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "retry-direction", Target: r.Target, Instruction: "new bounded scope", NextPolicyRevision: r.Target.PolicyRevision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	after, err := k.WaveOperationLimits(ctx, r.Target.WaveID)
	if err != nil || after != before || after.SplitCreateRetries != 1 {
		t.Fatal("direction reset retry policy", after, err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, retry.ID, "new-scope-owner"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("old split gained new scope authority", err)
	}
	latest, err := k.ReadLatestProductSplitCreate(ctx, prior.Value.Child.ID)
	if err != nil || latest.ID != retry.ID || latest.State != "aborted" {
		t.Fatal("direction erased audit chain", latest, err)
	}
}

func TestProductSplitUnsentRetryBudgetIsAtomicAcrossConnections(t *testing.T) {
	ctx := context.Background()
	k, r, first := abortedSplitRetryFixture(t)
	plan, err := k.ReadProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	second, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[1].ID, "second-child")
	if err != nil {
		t.Fatal(err)
	}
	second, err = k.AbortUnactivatedProductSplitCreate(ctx, second.ID, second.Value.OwnerID, second.Digest, "second-child-no-send")
	if err != nil {
		t.Fatal(err)
	}
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		_, err := k.ReserveProductSplitCreateUnsentRetry(ctx, first.ID, "retry-A")
		done <- err
	}()
	go func() {
		<-start
		_, err := other.ReserveProductSplitCreateUnsentRetry(ctx, second.ID, "retry-B")
		done <- err
	}()
	close(start)
	a, b := <-done, <-done
	if (a == nil) == (b == nil) {
		t.Fatal("exactly one retry must reserve final Wave budget", a, b)
	}
	budget, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || budget.SplitCreateRetries != 1 {
		t.Fatal("concurrent retries overcommitted", budget, err)
	}
	var rows int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_create_retry_budget' AND work_item_id=?", r.Target.WaveID).Scan(&rows); err != nil || rows != 1 {
		t.Fatal("partial retry admission", rows, err)
	}
}
