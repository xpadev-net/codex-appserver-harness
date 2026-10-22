// Package control owns bounded physical sessions and their controller runtime.
package control

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// ErrNotStarted is asserted only by a trusted factory that knows no process
// executed. Ordinary transport errors, cancellation and missing PID are unknown.
var ErrNotStarted = errors.New("managed factory did not start a process")
var ErrReadOnly = errors.New("recovery session permits only its own thread history read")
var ErrOwnerClosed = errors.New("managed process owner is closed")

type Process interface {
	codex.Stream
	Close() error
	GroupStopped() bool
	ProcessIdentity() process.Identity
	StartedAt() time.Time
}
type Launch struct {
	Binding contract.SessionBinding
	Process storage.ManagedProcess
}
type Factory interface {
	Open(context.Context, Launch) (Process, error)
}

type Command struct {
	Program             string
	Arguments           []string
	Environment         []string
	ContainmentVerified bool
}

// CommandFactory has no inherited environment or implicit shell. The trusted
// builder supplies the same approved launcher policy for parent and Task roles.
// Containment must cover descendants outside the transport's process group.
type CommandFactory struct {
	Build func(context.Context, Launch) (Command, error)
}

func (f CommandFactory) Open(ctx context.Context, launch Launch) (Process, error) {
	if f.Build == nil {
		return nil, errors.Join(ErrNotStarted, errors.New("runtime launch configuration is missing"))
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrNotStarted, err)
	}
	command, err := f.Build(ctx, launch)
	if err != nil {
		return nil, errors.Join(ErrNotStarted, err)
	}
	if !command.ContainmentVerified || !filepath.IsAbs(command.Program) || !filepath.IsAbs(launch.Binding.CWD) || command.Environment == nil {
		return nil, errors.Join(ErrNotStarted, errors.New("verified launcher, fixed cwd and explicit environment required"))
	}
	p, err := codex.StartCommandIn(ctx, command.Program, command.Arguments, command.Environment, launch.Binding.CWD)
	if err != nil {
		return nil, errors.Join(ErrNotStarted, err)
	}
	return p, nil
}
