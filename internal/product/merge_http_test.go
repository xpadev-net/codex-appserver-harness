package product

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkMergeHTTPWrapper(t *testing.T, g KernelMergeGate, saved storage.ProductTargetSnapshot, requestID, mode string, responseSHA ...string) {
	t.Helper()
	ctx := context.Background()
	k := g.Cloud.Store
	workerMode := ""
	if strings.HasPrefix(mode, "worker_") {
		workerMode = strings.TrimPrefix(mode, "worker_")
		mode = "group"
		if workerMode == "alias" {
			mode = "peer"
		}
	}
	driverMode := ""
	if strings.HasPrefix(mode, "driver_") {
		driverMode = strings.TrimPrefix(mode, "driver_")
		mode = "group"
	}
	transportMode := ""
	if strings.HasPrefix(mode, "transport_") {
		transportMode = strings.TrimPrefix(mode, "transport_")
		mode = transportMode
		if mode == "success" {
			mode = "group"
		}
		if mode == "missing" {
			mode = "no_request"
		}
	}
	var job storage.ProductMerge
	var err error
	if workerMode == "" {
		job, err = k.PrepareProductMergeHTTP(ctx, requestID, "kernel-http-owner", "merge")
		if err != nil {
			t.Fatal(err)
		}
	}
	if driverMode == "" && workerMode == "" {
		if _, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest); err != nil {
			t.Fatal(err)
		}
		if transportMode != "unfenced" {
			gate, err := g.Check(ctx, saved, requestID)
			if err != nil {
				t.Fatal(err)
			}
			job, err = k.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, gate)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var authCalls atomic.Int64
	if mode != "no_authority" {
		authority, err := ListenMergeHTTPAuthority(k, func(ctx context.Context, job storage.ProductMerge) (storage.ProductMergeGateObservation, error) {
			authCalls.Add(1)
			if mode == "denied" {
				if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-immediately-before-http", Target: job.Value.Product.Value.Target, Action: "pause", Reason: "fresh authorization must observe operator pause"}); err != nil {
					return storage.ProductMergeGateObservation{}, err
				}
			}
			return g.Check(ctx, job.Value.Product, job.Value.RequestID)
		})
		if err != nil {
			t.Fatal(err)
		}
		if other, err := ListenMergeHTTPAuthority(k, func(context.Context, storage.ProductMerge) (storage.ProductMergeGateObservation, error) {
			return storage.ProductMergeGateObservation{}, nil
		}); err == nil {
			other.Close()
			t.Fatal("second authority replaced live socket")
		}
		authCtx, cancelAuthority := context.WithCancel(ctx)
		authorityDone := make(chan error, 1)
		go func() { authorityDone <- authority.Serve(authCtx) }()
		defer func() { cancelAuthority(); authority.Close(); <-authorityDone }()
	}

	bin := t.TempDir()
	p := saved.Value
	repo := Repository{ID: p.WorkItem.RepositoryID, Name: p.Repository, DefaultBranch: p.BaseBranch}
	if mode == "no_request" {
		repo.ID++
	}
	raw, _ := json.Marshal(repo)
	if err := os.WriteFile(filepath.Join(bin, "repository.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	mergeSHA := strings.Repeat("e", 40)
	if len(responseSHA) > 1 {
		t.Fatal("one merge response SHA required")
	}
	if len(responseSHA) == 1 {
		mergeSHA = responseSHA[0]
	}
	mergeResponse, err := json.Marshal(map[string]any{"merged": true, "sha": mergeSHA})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "merge-response.json"), mergeResponse, 0600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$HTTP_WRAPPER_DATA/calls"
case "$*" in
 'api --method GET repos/'*)
   if [ "$HTTP_WRAPPER_MODE" = peer ]; then while [ ! -f "$HTTP_WRAPPER_DATA/release" ]; do /bin/sleep 0.01; done; fi
   /bin/cat "$HTTP_WRAPPER_DATA/repository.json" ;;
 'api --method PUT repos/'*'/merge --input -')
   /bin/cat > "$HTTP_WRAPPER_DATA/request.json"
   case "$HTTP_WRAPPER_MODE" in
     unknown) printf 'lost response'; exit 51 ;;
     cancel) /bin/sleep 30 ;;
     group) /bin/sleep 30 & ;;
   esac
   /bin/cat "$HTTP_WRAPPER_DATA/merge-response.json" ;;
 *) exit 49 ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if workerMode != "" {
		checkMergeDispatchWorker(t, g, saved, requestID, bin, mode, workerMode)
		return
	}
	if driverMode != "" {
		checkMergeDriver(t, g, job, bin, mode, driverMode)
		return
	}
	if transportMode != "" {
		checkMergeHTTPTransport(t, k, job, bin, mode, transportMode)
		return
	}
	spec, err := WriteMergeHTTPSpec(ctx, k, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteMergeHTTPSpec(ctx, k, job.ID); !os.IsExist(err) {
		t.Fatal("spec was not immutable", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := func() *exec.Cmd {
		cmd := exec.Command(exe, "_merge-http", spec.Path())
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Env = append(os.Environ(), "PATH="+bin, "HTTP_WRAPPER_DATA="+bin, "HTTP_WRAPPER_MODE="+mode)
		return cmd
	}
	child := command()
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	identity := process.Identify(child.Process.Pid)
	waited := false
	defer func() {
		if !waited {
			stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = process.StopGroup(stop, identity)
			cancel()
			_ = child.Wait()
		}
	}()
	if mode == "peer" {
		deadline := time.Now().Add(5 * time.Second)
		for {
			observed, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if observed.HTTP != nil {
				job = observed
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("wrapper did not register")
			}
			time.Sleep(5 * time.Millisecond)
		}
		denied, cancel := context.WithTimeout(ctx, time.Second)
		_, err := requestMergeHTTPAuthority(denied, k, job, job.HTTP.SpecDigest)
		cancel()
		if err == nil || authCalls.Load() != 0 {
			t.Fatal("same-UID foreign PID obtained authority", err, authCalls.Load())
		}
		fresh, err := g.Check(ctx, job.Value.Product, job.Value.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := k.MarkProductMergeHTTPMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, fresh); !errors.Is(err, storage.ErrProductMergeHeld) {
			t.Fatal("controller PID consumed registered client's native fence", err)
		}

		if err := os.WriteFile(filepath.Join(bin, "release"), []byte("continue"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	ready := time.NewTicker(10 * time.Millisecond)
	defer ready.Stop()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(filepath.Join(spec.directory(), "result.json")); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-ready.C:
		case <-deadline.C:
			t.Fatal("wrapper did not persist transport outcome")
		}
	}
	job, err = k.ReadProductMerge(ctx, job.ID)
	if err != nil || job.HTTP == nil {
		t.Fatal(job, err)
	}
	result, resultDigest, err := ReadMergeHTTPResult(ctx, k, job.ID)
	if err != nil || result.Process != job.HTTP.Process || resultDigest == "" {
		t.Fatal(result, err)
	}
	calls, err := os.ReadFile(filepath.Join(bin, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	expectedCalls := 2
	expectedDisposition := "response_received"
	expectedState := "merged"
	if mode == "no_request" || mode == "denied" || mode == "no_authority" {
		expectedCalls = 1
		expectedDisposition = "client_not_started"
		expectedState = "stopped_before_http"
	}
	if mode == "unknown" {
		expectedDisposition = "outcome_unknown"
		expectedState = "stopped_unknown"
	}
	if strings.Count(string(calls), "\n") != expectedCalls || result.Disposition != expectedDisposition {
		t.Fatal("unexpected native calls/result", string(calls), result)
	}
	if expectedCalls == 2 {
		body, err := os.ReadFile(filepath.Join(bin, "request.json"))
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]string
		if err := json.Unmarshal(body, &fields); err != nil || len(fields) != 2 || fields["sha"] != p.Candidate.SHA || fields["merge_method"] != "merge" {
			t.Fatal("expected-head native body changed", string(body), err)
		}
	}
	if (job.HTTPWrite != nil) != (expectedCalls == 2) {
		t.Fatal("native request lacked its separate fresh write fence", job.HTTPWrite)
	}
	if job.HTTPWrite != nil && job.HTTPWrite.Gate.ObservedAt.Before(job.HTTP.ActivatedAt) {
		t.Fatal("pre-activation authorization reused")
	}

	if _, err := command().CombinedOutput(); err == nil {
		t.Fatal("second wrapper received another send grant")
	}
	replayCalls, err := os.ReadFile(filepath.Join(bin, "calls"))
	if err != nil || string(replayCalls) != string(calls) {
		t.Fatal("replayed wrapper repeated native API", string(replayCalls), err)
	}
	// Even a validly parsed spec with altered bytes loses its DB-pinned digest.
	original, err := os.ReadFile(spec.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(spec.Path(), append([]byte(" "), original...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadMergeHTTPResult(ctx, k, job.ID); !errors.Is(err, storage.ErrTaskEvidence) {
		t.Fatal("changed spec authenticated old result", err)
	}
	if err := os.WriteFile(spec.Path(), original, 0600); err != nil {
		t.Fatal(err)
	}
	if job.HTTPWrite != nil {
		// Lost native-write metadata cannot be regenerated by another wrapper.
		originalWrite := *job.HTTPWrite
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_http_write' AND id=?", job.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ReadProductMerge(ctx, job.ID); err == nil {
			t.Fatal("missing native-write evidence accepted")
		}
		if _, err := command().CombinedOutput(); err == nil {
			t.Fatal("wrapper regenerated lost native-write evidence")
		}
		afterLoss, err := os.ReadFile(filepath.Join(bin, "calls"))
		if err != nil || string(afterLoss) != string(calls) {
			t.Fatal("lost metadata caused resend", string(afterLoss), err)
		}
		// Restore only the exact known fixture record to finish group cleanup.
		if err := k.Put(ctx, "product_merge_http_write", job.ID, p.Target.WaveID, originalWrite); err != nil {
			t.Fatal(err)
		}
	}

	proof := storage.ProductMergeStop{MergeID: job.ID, ProofID: "merge-http-result-" + resultDigest, Reason: "trusted fixed native merge wrapper result", StartedAt: result.StartedAt, FinishedAt: result.FinishedAt, Disposition: result.Disposition, Response: result.Response}
	// The wrapper remains a registered group leader until its supervisor stops
	// the whole group; a result file alone never proves physical completion.
	check, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	_, err = k.FinishProductMergeHTTP(check, job.ID, job.Value.OwnerID, job.Digest, result.SpecDigest, proof)
	cancel()
	if err == nil {
		t.Fatal("response file certified a live group")
	}
	stop, cancel := context.WithTimeout(ctx, 3*time.Second)
	if err := process.StopGroup(stop, job.HTTP.Process); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	_ = child.Wait()
	waited = true

	stopped, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, result.SpecDigest, proof)
	if err != nil || stopped.State != expectedState {
		t.Fatal(stopped, err)
	}
	seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
	if err != nil || (seat.State == "released") != (expectedState == "merged") {
		t.Fatal(seat, err)
	}
	if mode == "unknown" {
		var leaseState string
		if err := k.DB.QueryRow("SELECT state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&leaseState); err != nil || leaseState != "held" {
			t.Fatal("lost response released ownership", leaseState, err)
		}
	}
}

func TestMergeHTTPFilesArePrivateImmutableAndBounded(t *testing.T) {
	for _, mode := range []string{"valid", "public", "symlink", "hardlink", "fifo", "oversize", "wrong_path"} {
		t.Run(mode, func(t *testing.T) {
			spec := MergeHTTPSpec{Version: 1, StoreRoot: t.TempDir(), MergeID: "safe", OwnerID: "host", MergeDigest: strings.Repeat("a", 64), GateDigest: strings.Repeat("b", 64)}
			if err := os.Chmod(spec.StoreRoot, 0700); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{filepath.Join(spec.StoreRoot, "merge-http"), spec.directory()} {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := writeMergeHTTPFile(spec.Path(), spec); err != nil {
				t.Fatal(err)
			}
			if err := writeMergeHTTPFile(spec.Path(), spec); !os.IsExist(err) {
				t.Fatal("spec overwritten", err)
			}
			path := spec.Path()
			switch mode {
			case "public":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				original := filepath.Join(spec.directory(), "original.json")
				if err := os.Rename(path, original); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(original, path); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, filepath.Join(spec.directory(), "other.json")); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := os.WriteFile(path, []byte(strings.Repeat("x", mergeHTTPBytes+1)), 0600); err != nil {
					t.Fatal(err)
				}
			case "wrong_path":
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(spec.directory(), "copy.json")
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, digest, err := ReadMergeHTTPSpec(path)
			if mode == "valid" {
				if err != nil || got != spec || !mergeHTTPFixedDigest(digest) {
					t.Fatal(got, digest, err)
				}
			} else if err == nil {
				t.Fatal("untrusted spec accepted", mode)
			}
		})
	}
	var output mergeHTTPOutput
	if _, err := io.Copy(&output, strings.NewReader(strings.Repeat("x", mergeHTTPBytes+1))); err == nil || output.buffer.Len() > mergeHTTPBytes {
		t.Fatal("native command output cap bypassed through io.Copy", output.buffer.Len(), err)
	}
}
