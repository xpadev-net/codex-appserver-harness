package product

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkMergeHTTPTransport(t *testing.T, k *storage.KernelStore, job storage.ProductMerge, bin, httpMode, mode string) {
	t.Helper()
	ctx := context.Background()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("HTTP_WRAPPER_DATA", bin)
	t.Setenv("HTTP_WRAPPER_MODE", httpMode)
	transport := MergeHTTPTransport{Store: k, Executable: exe}
	expected := "merged"
	var stopped storage.ProductMerge
	switch mode {
	case "unactivated":
		// Reopen with a different controller epoch and no spec/process/result.
		reopened, err := storage.OpenExistingKernel(filepath.Join(k.Root, "state.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		stopped, err = (MergeHTTPTransport{Store: reopened}).Recover(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		expected = "aborted_http"
		if _, err := transport.Start(ctx, job.ID); !errors.Is(err, storage.ErrProductMergeHeld) {
			t.Fatal("sealed intent relaunched", err)
		}
	case "launch_failure":
		transport.Executable = filepath.Join(bin, "missing-executable")
		stopped, err = transport.Start(ctx, job.ID)
		if err == nil || stopped.HTTPAbort == nil {
			t.Fatal("launch failure did not atomically seal admission", stopped, err)
		}
		expected = "aborted_http"
	case "missing", "invalid", "recover":
		spec, err := WriteMergeHTTPSpec(ctx, k, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(exe, "_merge-http", spec.Path())
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		identity := process.Identify(cmd.Process.Pid)
		defer func() {
			cleanup, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			_ = process.StopGroup(cleanup, identity)
			_ = cmd.Wait()
		}()
		wait, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = transport.waitResult(wait, job.ID, identity)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if mode == "missing" {
			before, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || before.HTTPWrite != nil {
				t.Fatal("fixture unexpectedly sent a native mutation", before, err)
			}
			if err := os.Remove(filepath.Join(spec.directory(), "result.json")); err != nil {
				t.Fatal(err)
			}
			expected = "stopped_unknown"
		}
		if mode == "invalid" {
			if err := os.WriteFile(spec.Path(), []byte("corrupt"), 0600); err != nil {
				t.Fatal(err)
			}
			expected = "stopped_unknown"
		}
		reopened, err := storage.OpenExistingKernel(filepath.Join(k.Root, "state.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		stopped, err = (MergeHTTPTransport{Store: reopened}).Recover(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
	default:
		run, cancel := context.WithCancel(ctx)
		defer cancel()
		if mode == "cancel" {
			expected = "stopped_unknown"
			done := make(chan struct{})
			defer close(done)
			go func() {
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				deadline := time.NewTimer(10 * time.Second)
				defer deadline.Stop()
				for {
					if _, err := os.Stat(filepath.Join(bin, "request.json")); err == nil {
						cancel()
						return
					}
					select {
					case <-done:
						return
					case <-deadline.C:
						cancel()
						return
					case <-ticker.C:
					}
				}
			}()
		}
		stopped, err = transport.Start(run, job.ID)
		if mode == "cancel" {
			if !errors.Is(err, context.Canceled) {
				t.Fatal("canceled request lost cancellation", err)
			}
			if _, err := os.Stat(filepath.Join(bin, "request.json")); err != nil {
				t.Fatal("cancel test did not reach the native write", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if mode == "unknown" {
			expected = "stopped_unknown"
		}
		if mode == "no_authority" {
			expected = "stopped_before_http"
		}
	}
	if stopped.State != expected {
		t.Fatal("transport disposition", stopped, expected)
	}
	if stopped.HTTP != nil {
		if alive, err := process.GroupAlive(stopped.HTTP.Process); err != nil || alive || stopped.HTTPStop == nil {
			t.Fatal("receipt before physical stop", alive, err, stopped.HTTPStop)
		}
	}
	if stopped.Stop != nil && (stopped.Stop.Disposition == "outcome_unknown") != (expected == "stopped_unknown") {
		t.Fatal("missing transport outcome inferred unsent", stopped.Stop)
	}
	var leaseState string
	if err := k.DB.QueryRow("SELECT state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&leaseState); err != nil || (leaseState == "held") != (expected == "stopped_unknown") {
		t.Fatal("incorrect mutation ownership", leaseState, err)
	}
	seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
	if err != nil || (seat.State == "released") != (expected == "merged") {
		t.Fatal("incorrect PR lifetime", seat, err)
	}
	before, _ := os.ReadFile(filepath.Join(bin, "calls"))
	replayed, err := transport.Recover(ctx, job.ID)
	if err != nil || replayed.State != stopped.State {
		t.Fatal("recovery replay changed receipt", replayed, err)
	}
	if _, err := transport.Start(ctx, job.ID); !errors.Is(err, storage.ErrProductMergeHeld) {
		t.Fatal("terminal operation relaunched", err)
	}
	after, _ := os.ReadFile(filepath.Join(bin, "calls"))
	if string(after) != string(before) || strings.Count(string(after), "--method PUT") > 1 {
		t.Fatal("recovery resent a native request", string(before), string(after))
	}
}
