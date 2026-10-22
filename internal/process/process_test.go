package process

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestWaitStoppedRejectsMissingIdentityAndRetainsLiveProcess(t *testing.T) {
	for _, p := range []Identity{{}, {PID: os.Getpid()}, {PID: os.Getpid(), BootID: BootID()}} {
		if err := WaitStopped(context.Background(), p); !errors.Is(err, ErrUnconfirmed) {
			t.Fatal("missing identity proved absence", p, err)
		}
	}
	self := Identify(os.Getpid())
	if !self.Valid() {
		t.Fatal(self)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitStopped(ctx, self); !errors.Is(err, ErrUnconfirmed) {
		t.Fatal("cancellation implied stopping", err)
	}
	replaced := self
	replaced.StartTicks++
	if alive, err := GroupAlive(replaced); err != nil || alive {
		t.Fatal("different original identity still owned numeric PID", alive, err)
	}
	if err := WaitStopped(context.Background(), replaced); err != nil {
		t.Fatal("original process is gone", err)
	}
}
func TestProcessGroupKeepsOrphanChildrenUntilTheyStop(t *testing.T) {
	// The launcher exits while a same-group child keeps stdout open. Its numeric
	// root PID is reaped, but the live group must still fence capacity reuse.
	cmd := exec.Command("sh", "-c", "sleep 30 & printf ready")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := Identify(cmd.Process.Pid)
	t.Cleanup(func() { _ = syscall.Kill(-p.PID, syscall.SIGKILL); _ = cmd.Wait() })
	buf := make([]byte, 5)
	if _, err := pipe.Read(buf); err != nil || string(buf) != "ready" {
		t.Fatal(string(buf), err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	alive, err := GroupAlive(p)
	if err != nil || !alive {
		t.Fatal("orphan group considered stopped", alive, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := WaitStopped(ctx, p); !errors.Is(err, ErrUnconfirmed) {
		t.Fatal("live child released group", err)
	}
	if err := syscall.Kill(-p.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	stopped, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := WaitStopped(stopped, p); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveredGroupStopUsesOriginalIdentityAndPinnedMembers(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 30 & printf ready; wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := Identify(cmd.Process.Pid)
	t.Cleanup(func() { _ = syscall.Kill(-p.PID, syscall.SIGKILL); _ = cmd.Wait() })
	buf := make([]byte, 5)
	if _, err = out.Read(buf); err != nil {
		t.Fatal(err)
	}
	wrong := p
	wrong.StartTicks++
	if err = RequestGroupStop(wrong); err != nil {
		t.Fatal(err)
	}
	if !Alive(p) {
		t.Fatal("mismatched generation signaled live process")
	}
	if err = RequestGroupStop(Identity{}); !errors.Is(err, ErrUnconfirmed) {
		t.Fatal(err)
	}
	if err = RequestGroupStop(p); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = WaitStopped(ctx, p); err != nil {
		t.Fatal(err)
	}
}
