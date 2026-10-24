package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func splitCreationFixture(t *testing.T) (*KernelStore, ProductSplitProposalRequest, ProductSplitChildPlanRequest, ProductSplitCreate) {
	t.Helper()
	k, parent, r, plan := splitAllocationFixture(t)
	if _, err := k.ReserveProductSplitChildren(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	job, err := k.PrepareProductSplitCreate(context.Background(), r, plan.Children[0].ID, "split-controller")
	if err != nil {
		t.Fatal(err)
	}
	return k, parent, r, job
}

func TestProductSplitHTTPHelper(t *testing.T) {
	root := os.Getenv("HARNESS_SPLIT_HTTP_TEST_ROOT")
	if root == "" {
		t.Skip("subprocess only")
	}
	k, err := OpenExistingKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	id := os.Getenv("HARNESS_SPLIT_HTTP_TEST_ID")
	job, err := k.ReadProductSplitCreate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("HARNESS_SPLIT_HTTP_TEST_CLOCK_ROLLBACK") == "1" {
		var gate splitCreateGateRecord
		if err := k.Record(context.Background(), "product_split_create_gate", job.ID, &gate); err != nil {
			t.Fatal(err)
		}
		calls := 0
		k.Now = func() time.Time {
			calls++
			if calls >= 3 {
				return gate.Value.ObservedAt.Add(-time.Nanosecond)
			}
			return gate.Value.ObservedAt.Add(time.Millisecond)
		}
	}
	first, err := k.ActivateProductSplitCreateHTTP(context.Background(), id, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64))
	if os.Getenv("HARNESS_SPLIT_HTTP_TEST_EXPECT_HELD") == "1" {
		if !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("delayed sealed wrapper was admitted", first, err)
		}
		return
	}
	if err != nil || !first.Start {
		t.Fatal("activation", first, err)
	}
	replay, err := k.ActivateProductSplitCreateHTTP(context.Background(), id, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64))
	if err != nil || replay.Start {
		t.Fatal("duplicate send admitted", replay, err)
	}
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "split-http-started.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	// No HTTP is sent by this physical admission fixture.
	for {
		time.Sleep(time.Second)
	}
}

func startSplitCreationOwner(t *testing.T, k *KernelStore, job ProductSplitCreate) process.Identity {
	t.Helper()
	sourceID, sourceDigest := "", ""
	w, err := k.Wave(context.Background(), job.Value.Request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	if w.Source == "github" {
		snapshot, err := k.ReadProductSourceObservation(context.Background(), w.ID)
		if err != nil {
			t.Fatal(err)
		}
		sourceID, sourceDigest = snapshot.ID, snapshot.Digest
	}
	if _, err := k.ObserveProductSplitCreateGate(context.Background(), job.ID, job.Value.OwnerID, job.Digest, sourceID, sourceDigest); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestProductSplitHTTPHelper$")
	cmd.Env = append(os.Environ(), "HARNESS_SPLIT_HTTP_TEST_ROOT="+k.Root, "HARNESS_SPLIT_HTTP_TEST_ID="+job.ID)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	identity := process.Identify(cmd.Process.Pid)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = process.StopGroup(ctx, identity)
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(k.Root, "split-http-started.json")); err == nil {
			saved, err := k.ReadProductSplitCreate(context.Background(), job.ID)
			if err != nil || saved.HTTP == nil || saved.HTTP.Process != identity || saved.State != "unknown" {
				t.Fatal("unbound owner", saved, err)
			}
			return identity
		}
		if !process.Alive(identity) {
			t.Fatal("helper exited before activation")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("activation timeout")
	return identity
}

func TestProductSplitCreationPhysicalOwnerStopsBeforeObservationAndNeverResends(t *testing.T) {
	ctx := context.Background()
	k, _, r, job := splitCreationFixture(t)
	identity := startSplitCreationOwner(t, k, job)
	pending, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID)
	if err != nil || pending.State != "unknown" {
		t.Fatal("unknown intent replaced", pending, err)
	}
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "false-abort"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("live owner falsely aborted", err)
	}
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	_, err = k.FinishProductSplitCreateHTTP(short, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "physical-stop")
	cancel()
	if err == nil {
		t.Fatal("live process accepted as stopped")
	}
	observation := ProductSplitCreatedIssue{RepositoryID: 17, ID: 101, Number: 11, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: time.Now().UTC()}
	if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, observation); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("Issue bound before physical stopping", err)
	}
	stopCtx, stopCancel := context.WithTimeout(ctx, 3*time.Second)
	defer stopCancel()
	if err := process.StopGroup(stopCtx, identity); err != nil {
		t.Fatal(err)
	}
	activated, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return activated.HTTP.ActivatedAt.Add(-time.Second) }
	if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "physical-stop"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("clock rollback committed invalid stopped state", err)
	}
	k.Now = time.Now
	unchanged, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || unchanged.State != "unknown" || unchanged.Stop != nil || unchanged.Sequence != 1 {
		t.Fatal("failed stop transition changed journal/lease", unchanged, err)
	}
	stopped, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "physical-stop")
	if err != nil || stopped.State != "stopped" || stopped.Stop == nil || stopped.Issue != nil {
		t.Fatal("stop inferred outcome", stopped, err)
	}
	observation.ObservedAt = time.Now()
	for _, mode := range []string{"marker", "title", "repo", "pull", "state", "stale"} {
		bad := observation
		switch mode {
		case "marker":
			bad.Body = strings.ReplaceAll(bad.Body, job.Value.Child.Value.Marker, "")
		case "title":
			bad.Title = "edited request"
		case "repo":
			bad.RepositoryID++
		case "pull":
			bad.IsPullRequest = true
		case "state":
			bad.State = "closed"
		case "stale":
			bad.ObservedAt = stopped.Stop.VerifiedAt.Add(-time.Second)
		}
		if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, bad); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("invalid issue observation accepted", mode, err)
		}
	}
	known, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, observation)
	if err != nil || known.State != "observed" || known.Issue.Number != 11 {
		t.Fatal("known child not bound", known, err)
	}
	readBack, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(readBack, known) {
		t.Fatal("host time representation changed after serialization", err)
	}
	localObservation := observation
	localObservation.ObservedAt = observation.ObservedAt.In(time.FixedZone("fixture-local", 9*60*60))
	if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, localObservation); err != nil {
		t.Fatal("local time observation replay rejected", err)
	}
	replay, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, observation)
	if err != nil || !reflect.DeepEqual(replay, known) {
		t.Fatal("observation not idempotent", err)
	}
	after, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID)
	if err != nil || !reflect.DeepEqual(after, known) {
		t.Fatal("known Issue got new intent", err)
	}
	var registrations int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&registrations); err != nil || registrations != 1 {
		t.Fatal("observation granted child execution", registrations, err)
	}
	key := "github:17:issue:11"
	var bindingOwner string
	var bindingRaw []byte
	if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind='product_split_created_resource' AND id=?", key).Scan(&bindingOwner, &bindingRaw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_created_resource' AND id=?", key); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, observation); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("missing resource binding reconstructed", err)
	}
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_created_resource',?,?,?)", key, bindingOwner, bindingRaw); err != nil {
		t.Fatal(err)
	}
	// A completed stop replay must not release a newer generation's owner.
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := claimResourceTx(ctx, tx, job.Lease.Resource, "successor", r.Target.WaveID)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "physical-stop"); err != nil {
		t.Fatal(err)
	}
	var owner, state string
	var generation int64
	if err := k.DB.QueryRow("SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", lease.Resource).Scan(&owner, &state, &generation); err != nil || owner != "successor" || state != "held" || generation != lease.Generation {
		t.Fatal("replay released successor", owner, state, generation, err)
	}
}

func TestProductSplitCreationAbortSealsDelayedWrapperAndRetainsBudgets(t *testing.T) {
	ctx := context.Background()
	k, _, r, job := splitCreationFixture(t)
	allocation, err := k.ReadProductSplitAllocation(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	aborted, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "atomic-seal")
	if err != nil || aborted.State != "aborted" || aborted.HTTP != nil {
		t.Fatal(aborted, err)
	}
	replay, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID)
	if err != nil || !reflect.DeepEqual(replay, aborted) {
		t.Fatal("aborted intent recreated", err)
	}
	after, err := k.ReadProductSplitAllocation(ctx, r)
	if err != nil || !reflect.DeepEqual(after, allocation) {
		t.Fatal("abort refunded lifetime seats", err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestProductSplitHTTPHelper$")
	cmd.Env = append(os.Environ(), "HARNESS_SPLIT_HTTP_TEST_ROOT="+k.Root, "HARNESS_SPLIT_HTTP_TEST_ID="+job.ID, "HARNESS_SPLIT_HTTP_TEST_EXPECT_HELD=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err != nil {
		t.Fatal("delayed wrapper did not observe seal", err)
	}
	// The caller is not a group leader; no caller-supplied PID is accepted.
	if _, err := k.ActivateProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64)); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("parent activated sealed intent", err)
	}
}

func TestProductSplitCreationHistoryEventAndLeaseLossCannotRecreate(t *testing.T) {
	ctx := context.Background()
	k, _, r, job := splitCreationFixture(t)
	for _, loss := range []struct{ kind, id string }{
		{"product_split_create", job.ID}, {"product_split_create_history", job.ID}, {"product_split_create_admission", job.ID}, {"product_split_create_event", splitCreateEventID(job.ID, 0)},
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
			if _, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("missing creation proof reconstructed", err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
		})
	}
	restored, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(restored, job) {
		t.Fatal("restoration changed outbox", err)
	}
}

func TestProductSplitCreationLateEventFailureRollsBackLeaseAndAllFacts(t *testing.T) {
	ctx := context.Background()
	k, _, r, plan := splitAllocationFixture(t)
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_split_create_event BEFORE INSERT ON records WHEN NEW.kind='product_split_create_event' BEGIN SELECT RAISE(FAIL,'late event failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "owner"); err == nil {
		t.Fatal("fault not reached")
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_split_create%'").Scan(&n); err != nil || n != 0 {
		t.Fatal("partial outbox committed", n, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'").Scan(&n); err != nil || n != 0 {
		t.Fatal("lease leaked after rollback", n, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_split_create_event"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "owner"); err != nil {
		t.Fatal("cannot prepare after rollback", err)
	}
}

func TestProductSplitCreationSurvivingWitnessBlocksParentPlanAndBudgetReplacement(t *testing.T) {
	for _, witness := range []string{"product_split_create", "product_split_create_history", "product_split_create_admission", "product_split_create_event", "lease"} {
		t.Run(witness, func(t *testing.T) {
			ctx := context.Background()
			k, parent, r, job := splitCreationFixture(t)
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", witness); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_children'"); err != nil {
				t.Fatal(err)
			}
			if witness != "lease" {
				if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
					t.Fatal(err)
				}
			}
			parent.Key = "replacement-key"
			if _, err := k.PrepareProductSplitProposal(ctx, parent); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("outbox witness allowed new proposal", err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("outbox witness allowed new child plan", err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("outbox witness allowed budget reset", err)
			}
			if _, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("outbox witness allowed new creation", err)
			}
			if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "would-free-seat", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("outbox witness freed lifetime seat", err)
			}
		})
	}
}

func TestProductSplitCreationPauseHoldsNewButExistingAuditSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	k, _, r, job := splitCreationFixture(t)
	plan, err := k.ReadProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-create", Target: r.Target, Action: "pause", Reason: "stop new mutation"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[1].ID, job.Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("pause prepared second mutation", err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID)
	if err != nil || !reflect.DeepEqual(replay, job) {
		t.Fatal("restart/pause recreated existing outbox", err)
	}
	if _, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, "another-owner"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("controller restart stole send ownership", err)
	}
}

func TestProductSplitCreationRejectsRolledBackHead(t *testing.T) {
	ctx := context.Background()
	k, _, _, job := splitCreationFixture(t)
	var originalRaw []byte
	if err := k.DB.QueryRow("SELECT payload FROM records WHERE kind='product_split_create' AND id=?", job.ID).Scan(&originalRaw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "abort-chain"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='product_split_create' AND id=?", originalRaw, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitCreate(ctx, job.ID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("rolled back head ignored later event", err)
	}
}

func TestProductSplitCreationFreshGateForUnchangedGitHubSource(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	old := k.clock().Add(-time.Hour)
	k.Now = func() time.Time { return old }
	observed, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := k.ReadProductWorkItemRegistration(ctx, w.SourceID)
	if err != nil {
		t.Fatal(err)
	}
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: w.SourceID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{
		{Key: "first", Title: "first part", Requirement: "first part of request", Criteria: []verification.Criterion{{ID: "first-ac", Requirement: "first behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "first-ac", PartID: "one", PartCount: 2, Basis: "first half"}}, DependsOn: []string{}},
		{Key: "second", Title: "second part", Requirement: "remaining part of request", Criteria: []verification.Criterion{{ID: "second-ac", Requirement: "remaining behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "second-ac", PartID: "two", PartCount: 2, Basis: "remaining half"}}, DependsOn: []string{"first"}},
	}}
	saved, err := k.PrepareProductSplitProposal(ctx, ProductSplitProposalRequest{Target: w.Target(), Key: "github-split", ParentWorkItemID: w.SourceID, RegistrationDigest: registration.Digest, Proposal: proposal})
	if err != nil {
		t.Fatal(err)
	}
	r := ProductSplitChildPlanRequest{w.Target(), w.SourceID, saved.ID, saved.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	job, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "split-controller")
	if err != nil {
		t.Fatal(err)
	}
	k.Now = time.Now
	repeated, err := k.ObserveProductSource(ctx, w.Target(), observed.Snapshot.Digest, facts)
	if err != nil || !repeated.Snapshot.ObservedAt.Equal(old) {
		t.Fatal("source content no longer deduplicated", repeated, err)
	}
	// Cached ready state alone has no one-use send authority.
	cmd := exec.Command(os.Args[0], "-test.run=^TestProductSplitHTTPHelper$")
	cmd.Env = append(os.Environ(), "HARNESS_SPLIT_HTTP_TEST_ROOT="+k.Root, "HARNESS_SPLIT_HTTP_TEST_ID="+job.ID, "HARNESS_SPLIT_HTTP_TEST_EXPECT_HELD=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err != nil {
		t.Fatal("cached source admitted a send", err)
	}
	// This is a trusted fresh-GET result fixture; no external GET/POST is claimed.
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "wrong-source", repeated.Snapshot.Digest); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("unbound fresh-read source accepted", err)
	}
	identity := startSplitCreationOwner(t, k, job)
	stopCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := process.StopGroup(stopCtx, identity); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "github-stop"); err != nil {
		t.Fatal(err)
	}
}

func TestProductSplitCreationGateLossNeverRecreatesFreshness(t *testing.T) {
	ctx := context.Background()
	k, _, _, job := splitCreationFixture(t)
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_create_gate'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("lost freshness proof recreated", err)
	}
}

func TestProductSplitCreationGateWitnessProtectsAllLostUpstreamIndexes(t *testing.T) {
	for _, witness := range []string{"product_split_create_gate", "product_split_create_gate_history"} {
		t.Run(witness, func(t *testing.T) {
			ctx := context.Background()
			k, parent, r, job := splitCreationFixture(t)
			if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", witness); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_children'"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			parent.Key = "new-after-gate-only"
			if _, err := k.PrepareProductSplitProposal(ctx, parent); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("gate allowed proposal replacement", err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("gate allowed child replacement", err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("gate allowed budget replacement", err)
			}
			if _, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("gate allowed send replacement", err)
			}
		})
	}
}

func TestProductSplitCreationClockRollbackCannotCommitAbortOrActivation(t *testing.T) {
	ctx := context.Background()
	k, _, _, job := splitCreationFixture(t)
	k.Now = func() time.Time { return job.Value.CreatedAt.Add(-time.Second) }
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "clock-abort"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("clock rollback committed invalid abort", err)
	}
	k.Now = time.Now
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestProductSplitHTTPHelper$")
	cmd.Env = append(os.Environ(), "HARNESS_SPLIT_HTTP_TEST_ROOT="+k.Root, "HARNESS_SPLIT_HTTP_TEST_ID="+job.ID, "HARNESS_SPLIT_HTTP_TEST_EXPECT_HELD=1", "HARNESS_SPLIT_HTTP_TEST_CLOCK_ROLLBACK=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err != nil {
		t.Fatal("clock rollback activated an invalid journal", err)
	}
	unchanged, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(unchanged, job) {
		t.Fatal("failed temporal transition changed journal/lease", unchanged, err)
	}
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "clock-abort"); err != nil {
		t.Fatal("clock recovery could not retry abort", err)
	}
}
