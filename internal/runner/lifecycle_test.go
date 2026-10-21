package runner

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestResolveUnstartedJobFencesDelayedExecution(t *testing.T) {
	s := spec(t)
	ctx := context.Background()
	specPath := filepath.Join(s.Root, s.JobID+".json")
	if err := WriteJSON(specPath, s); err != nil {
		t.Fatal(err)
	}
	if _, err := Launch(ctx, "/missing-runner-executable", specPath); err == nil {
		t.Fatal("start unexpectedly succeeded")
	}
	life, err := Inspect(ctx, s)
	if err != nil || life.State != "not_started" {
		t.Fatal(life, err)
	}
	r, err := Resolve(ctx, s, "not_started", "audit:process-table-no-runner", "confirmed launch failure")
	if err != nil || r.Status != "infrastructure_failure" || r.TestedSHA != "" || r.ExitCode != nil || r.ResolutionEvidence == "" || !r.Started.IsZero() || r.ResolutionState != "not_started" || r.ResolvedAt.IsZero() {
		t.Fatal(r, err)
	}
	if _, err = Execute(ctx, s); err == nil {
		t.Fatal("late runner executed a resolved job")
	}
	if _, err = Launch(ctx, "/missing-runner-executable", specPath); err == nil {
		t.Fatal("resolved job was relaunched")
	}
	s.JobID = "retry"
	if r, err = Execute(ctx, s); err != nil || r.Status != "passed" {
		t.Fatal("new job could not retry", r, err)
	}
}
func TestLiveRunnerRefusesResolution(t *testing.T) {
	s := spec(t)
	s.Profile.Argv = []string{"sh", "-c", "sleep 10; printf PASS"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := Execute(ctx, s); done <- err }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(s.Root, s.JobID+".lifecycle.json")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runner did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	life, err := Inspect(context.Background(), s)
	if err != nil || life.State != "running" {
		t.Fatal(life, err)
	}
	if _, err = Resolve(context.Background(), s, "not_started", "incorrect absence claim", "must refuse"); !errors.Is(err, ErrJobBusy) {
		t.Fatal("live job resolved", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop")
	}
}
func TestOrphanProfileMustStopBeforeResolution(t *testing.T) {
	s := spec(t)
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	os.Mkdir(filepath.Join(s.Root, s.JobID), 0700)
	if err := saveLifecycle(s, Lifecycle{State: "running", Profile: identity(cmd.Process.Pid)}); err != nil {
		t.Fatal(err)
	}
	life, err := Inspect(context.Background(), s)
	if err != nil || life.State != "running" {
		t.Fatal(life, err)
	}
	if _, err = Resolve(context.Background(), s, "stopped", "operator:stop", "must refuse live group"); err == nil {
		t.Fatal("live orphan was resolved")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	life, err = Inspect(context.Background(), s)
	if err != nil || life.State != "stopped" {
		t.Fatal(life, err)
	}
	if _, err = Resolve(context.Background(), s, "stopped", "operator:group-gone", "confirmed quiescence"); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidLaunchSpecificationCanStillBeResolved(t *testing.T) {
	s := spec(t)
	s.Profile.SuccessPattern = ""
	path := filepath.Join(s.Root, s.JobID+".json")
	if err := WriteJSON(path, s); err != nil {
		t.Fatal(err)
	}
	if _, err := Launch(context.Background(), "/missing-runner-executable", path); err == nil {
		t.Fatal("invalid launch accepted")
	}
	if _, err := Resolve(context.Background(), s, "not_started", "audit:no-process", "invalid execution spec never launched"); err != nil {
		t.Fatal("invalid execution profile prevented safe resolution", err)
	}
	s.Profile.SuccessPattern = "PASS"
	if _, err := Execute(context.Background(), s); err == nil {
		t.Fatal("repairing the profile bypassed the old job's resolution fence")
	}
}
