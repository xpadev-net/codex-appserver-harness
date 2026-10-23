package workspace

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

type stoppedCommandsKey struct{}

var ErrGitStopUnconfirmed = errors.New("Git owner process group stopping is unconfirmed")

// WithStoppedCommands selects the controller-owned preparation contract. Every
// command and config probe joins its process group before reporting an ordinary exit.
func WithStoppedCommands(ctx context.Context) context.Context {
	return context.WithValue(ctx, stoppedCommandsKey{}, true)
}
func stoppedCommands(ctx context.Context) bool {
	stopped, _ := ctx.Value(stoppedCommandsKey{}).(bool)
	return stopped
}
func gitOutput(ctx context.Context, args []string) ([]byte, error) {
	if !stoppedCommands(ctx) {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		return cmd.Output()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// This synchronous owner is the ONLY waiter. No context watcher may signal
	// a recycled numeric group after reaping. The leader remains waitable until
	// group stopping and its absence proof are complete.
	cmd := exec.Command("git", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 2 * time.Second
	var output bytes.Buffer
	cmd.Stdout = &output
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	identity := process.Identify(cmd.Process.Pid)
	leaderErr := process.WaitLeaderExit(ctx, identity)
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer stop()
	signalErr := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(signalErr, syscall.ESRCH) {
		signalErr = nil
	}
	_ = cmd.Process.Kill() // The unreaped owned leader also covers identity-read failure.
	stopErr := process.WaitStopped(cleanup, identity)
	waitErr := cmd.Wait()
	if stopErr != nil {
		return output.Bytes(), errors.Join(ErrGitStopUnconfirmed, leaderErr, signalErr, stopErr, waitErr)
	}
	return output.Bytes(), errors.Join(leaderErr, signalErr, waitErr)
}
