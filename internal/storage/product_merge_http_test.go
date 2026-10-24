package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

type mergeHTTPHelperReply struct {
	Start bool   `json:"start"`
	Again bool   `json:"again"`
	Error string `json:"error,omitempty"`
	PID   int    `json:"pid"`
}

func TestMergeHTTPActivationHelper(t *testing.T) {
	root := os.Getenv("MERGE_HTTP_TEST_ROOT")
	if root == "" {
		return
	}
	k, err := OpenExistingKernel(root + "/state.db")
	reply := mergeHTTPHelperReply{PID: os.Getpid()}
	if raw := os.Getenv("MERGE_HTTP_TEST_NOW"); raw != "" && err == nil {
		var now time.Time
		now, err = time.Parse(time.RFC3339Nano, raw)
		if err == nil {
			k.Now = func() time.Time { return now }
		}
	}
	if err == nil {
		id, owner, digest := os.Getenv("MERGE_HTTP_TEST_ID"), os.Getenv("MERGE_HTTP_TEST_OWNER"), os.Getenv("MERGE_HTTP_TEST_DIGEST")
		var claim ProductMergeClaim
		claim, err = k.ActivateProductMergeHTTP(context.Background(), id, owner, digest, strings.Repeat("a", 64))
		reply.Start = claim.Start
		if err == nil {
			var again ProductMergeClaim
			again, err = k.ActivateProductMergeHTTP(context.Background(), id, owner, digest, strings.Repeat("a", 64))
			reply.Again = again.Start
		}
		k.Close()
	}
	if err != nil {
		reply.Error = err.Error()
	}
	_ = json.NewEncoder(os.Stdout).Encode(reply)
	os.Exit(0)
}
func mergeHTTPActivationCommand(t *testing.T, k *KernelStore, job ProductMerge) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestMergeHTTPActivationHelper$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), "MERGE_HTTP_TEST_ROOT="+k.Root, "MERGE_HTTP_TEST_ID="+job.ID, "MERGE_HTTP_TEST_OWNER="+job.Value.OwnerID, "MERGE_HTTP_TEST_DIGEST="+job.Digest)
	return cmd
}
func activateHTTPChild(t *testing.T, k *KernelStore, job ProductMerge) mergeHTTPHelperReply {
	t.Helper()
	cmd := mergeHTTPActivationCommand(t, k, job)

	raw, err := cmd.Output()
	if err != nil {
		t.Fatal("HTTP helper failed", err, string(raw))
	}
	var reply mergeHTTPHelperReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatal(string(raw), err)
	}
	return reply
}
func registeredMergeHTTPFixture(t *testing.T) (*KernelStore, ProductMerge) {
	t.Helper()
	ctx := context.Background()
	k, id := mergeJournalFixture(t)
	job, err := k.PrepareProductMergeHTTP(ctx, id, "http-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	if job.Value.HTTPProtocol != productMergeHTTPProtocol {
		t.Fatal("protocol not pinned")
	}
	if _, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest); err != nil {
		t.Fatal(err)
	}
	job, err = k.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, mergeGateObservationFixture(k, job))
	if err != nil {
		t.Fatal(err)
	}
	return k, job
}

func TestProductMergeHTTPPinsProtocolAndRegistersRealChildOnce(t *testing.T) {
	ctx := context.Background()
	k, job := registeredMergeHTTPFixture(t)
	if _, err := k.PrepareProductMerge(ctx, job.Value.RequestID, job.Value.OwnerID, job.Value.Method); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("registered protocol downgraded", err)
	}
	reply := activateHTTPChild(t, k, job)
	if reply.Error != "" || !reply.Start || reply.Again {
		t.Fatal("HTTP child admission not one-use", reply)
	}
	saved, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil || saved.HTTP == nil || saved.HTTP.Process.PID != reply.PID || saved.HTTP.Process.PID == os.Getpid() || !saved.HTTP.Process.Valid() || saved.HTTP.SpecDigest != strings.Repeat("a", 64) {
		t.Fatal("real child not journaled", saved, err)
	}
	if second := activateHTTPChild(t, k, job); second.Start || second.Error == "" {
		t.Fatal("another child gained native send permission", second)
	}
	if _, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "false-unsent", "already activated"); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("activated request treated as unsent", err)
	}
	proof := ProductMergeStop{MergeID: job.ID, ProofID: "parent-stop", Reason: "parent cannot certify registered HTTP stop", StartedAt: job.ClaimedAt, FinishedAt: k.clock(), Disposition: "outcome_unknown"}
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, proof); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("parent epoch substituted for actual HTTP owner", err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	read, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(saved, read) {
		t.Fatal("HTTP identity lost after reopen", read, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_http_owner' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if third := activateHTTPChild(t, k, job); third.Start || third.Error == "" {
		t.Fatal("lost HTTP owner metadata regenerated", third)
	}
}

func TestProductMergeHTTPAbortSealsDelayedWrapperAndProtectsNextLease(t *testing.T) {
	ctx := context.Background()
	k, job := registeredMergeHTTPFixture(t)
	aborted, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "atomic-unsent", "registered admission sealed before activation")
	if err != nil || aborted.State != "aborted_http" || aborted.HTTP != nil || aborted.HTTPAbort == nil {
		t.Fatal(aborted, err)
	}
	if late := activateHTTPChild(t, k, job); late.Start || late.Error == "" {
		t.Fatal("late wrapper sent after abort", late)
	}
	seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
	if err != nil || seat.State != "held" {
		t.Fatal("unsent abort released PR capacity", seat, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next, err := claimResourceTx(ctx, tx, job.MutationLease.Resource, "next-http-owner", job.Value.Product.Value.Target.WaveID)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if next.Generation <= job.MutationLease.Generation {
		t.Fatal("lease generation reused")
	}
	again, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "atomic-unsent", "registered admission sealed before activation")
	if err != nil || !reflect.DeepEqual(aborted, again) {
		t.Fatal("abort replay changed fact/next owner", again, err)
	}
	var owner, state string
	if err := k.DB.QueryRow("SELECT run_id,state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&owner, &state); err != nil || owner != "next-http-owner" || state != "held" {
		t.Fatal(owner, state, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_http_abort' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductMerge(ctx, job.ID); err == nil {
		t.Fatal("lost abort proof accepted")
	}
}

func TestProductMergeHTTPLegacyAbsenceControlsAndRollbackNeverGrant(t *testing.T) {
	for _, mode := range []string{"legacy", "cancel", "stale_gate", "rollback_activation", "rollback_abort"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			if mode == "legacy" {
				k, job, _ := mergeResultFixture(t, true)
				if _, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "unproven", "legacy has no activation contract"); !errors.Is(err, ErrProductMergeHeld) {
					t.Fatal("legacy unknown inferred unsent", err)
				}
				if child := activateHTTPChild(t, k, job); child.Start || child.Error == "" {
					t.Fatal("legacy upgraded", child)
				}
				return
			}
			k, job := registeredMergeHTTPFixture(t)
			switch mode {
			case "cancel":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-before-http", Target: job.Value.Product.Value.Target, Action: "cancel", Reason: "native client not admitted"}); err != nil {
					t.Fatal(err)
				}
			case "stale_gate":
				t.Setenv("MERGE_HTTP_TEST_NOW", job.MayHaveSentAt.Add(31*time.Second).Format(time.RFC3339Nano))

			case "rollback_activation":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_http_activation BEFORE INSERT ON records WHEN NEW.kind='product_merge_http_owner' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
					t.Fatal(err)
				}
			case "rollback_abort":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_http_abort BEFORE INSERT ON records WHEN NEW.kind='product_merge_http_abort' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "abort-rollback", "injected failure"); err == nil {
					t.Fatal("failed abort succeeded")
				}
				read, err := k.ReadProductMerge(ctx, job.ID)
				if err != nil || read.State != "unknown" || read.HTTPAbort != nil {
					t.Fatal(read, err)
				}
				return
			}
			if child := activateHTTPChild(t, k, job); child.Start || child.Error == "" {
				t.Fatal("unsafe child activation", mode, child)
			}
			read, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || read.HTTP != nil || read.State != "unknown" {
				t.Fatal("failed activation changed journal", read, err)
			}
		})
	}
}

func TestProductMergeHTTPActivationAndAbortHaveSingleWinner(t *testing.T) {
	ctx := context.Background()
	k, job := registeredMergeHTTPFixture(t)
	cmd := mergeHTTPActivationCommand(t, k, job)
	var raw bytes.Buffer
	cmd.Stdout = &raw
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	aborted, abortErr := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "race-abort", "atomic race before native client admission")
	if err := cmd.Wait(); err != nil {
		t.Fatal(err, raw.String())
	}
	var reply mergeHTTPHelperReply
	if err := json.Unmarshal(raw.Bytes(), &reply); err != nil {
		t.Fatal(err, raw.String())
	}
	saved, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Start {
		if !errors.Is(abortErr, ErrProductMergeHeld) || saved.HTTP == nil || saved.HTTPAbort != nil || saved.State != "unknown" {
			t.Fatal("activation and abort both won", reply, abortErr, saved)
		}
	} else {
		if abortErr != nil || aborted.State != "aborted_http" || saved.HTTP != nil || saved.HTTPAbort == nil || reply.Error == "" {
			t.Fatal("neither admission nor abort committed", reply, abortErr, saved)
		}
	}
}
