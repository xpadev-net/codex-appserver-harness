package runner

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var ErrJobBusy = errors.New("runner lifecycle is locked")

type ProcessIdentity = process.Identity

type Lifecycle struct {
	State   string          `json:"state"`
	Process ProcessIdentity `json:"process"`
	Profile ProcessIdentity `json:"profile"`
}

func bootID() string                                   { return process.BootID() }
func processStat(pid int) (string, int, uint64, error) { return process.ReadStat(pid) }
func identity(pid int) ProcessIdentity                 { return process.Identify(pid) }
func processAlive(p ProcessIdentity) bool              { return process.Alive(p) }
func groupAlive(p ProcessIdentity) (bool, error)       { return process.GroupAlive(p) }
func jobLock(ctx context.Context, s Spec, wait bool) (*os.File, error) {
	if !identifier.MatchString(s.JobID) || !filepath.IsAbs(s.Root) {
		return nil, errors.New("invalid lifecycle identity")
	}
	if err := os.MkdirAll(s.Root, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(s.Root, s.JobID+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			f.Close()
			return nil, err
		}
		if !wait {
			f.Close()
			return nil, ErrJobBusy
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func readLifecycle(s Spec) (Lifecycle, error) {
	var life Lifecycle
	b, err := os.ReadFile(filepath.Join(s.Root, s.JobID+".lifecycle.json"))
	if os.IsNotExist(err) {
		return life, nil
	}
	if err != nil {
		return life, err
	}
	err = json.Unmarshal(b, &life)
	return life, err
}
func saveLifecycle(s Spec, life Lifecycle) error {
	return WriteJSON(filepath.Join(s.Root, s.JobID+".lifecycle.json"), life)
}
func inspectLocked(s Spec) (Lifecycle, error) {
	life, err := readLifecycle(s)
	if err != nil {
		return life, err
	}
	if processAlive(life.Process) {
		life.State = "running"
		return life, nil
	}
	if alive, err := groupAlive(life.Profile); err != nil {
		return life, err
	} else if alive {
		life.State = "running"
		return life, nil
	}
	if life.Profile.PID > 0 || life.State == "quiesced" {
		life.State = "stopped"
		return life, nil
	}
	if _, err := os.Stat(filepath.Join(s.Root, s.JobID)); os.IsNotExist(err) {
		life.State = "not_started"
	} else if err != nil {
		return life, err
	} else {
		life.State = "unknown"
	}
	return life, nil
}

// Inspect never uses the controller's Launched flag as absence evidence.
func Inspect(ctx context.Context, s Spec) (Lifecycle, error) {
	if r, err := LoadResult(s.Root, s.JobID); err == nil {
		state := "completed"
		if r.ResolutionEvidence != "" {
			state = "resolved"
		}
		return Lifecycle{State: state}, nil
	} else if !os.IsNotExist(err) {
		return Lifecycle{}, err
	}
	lock, err := jobLock(ctx, s, false)
	if errors.Is(err, ErrJobBusy) {
		return Lifecycle{State: "running"}, nil
	}
	if err != nil {
		return Lifecycle{}, err
	}
	defer lock.Close()
	// Completion can be published between the optimistic read and this lock.
	if r, err := LoadResult(s.Root, s.JobID); err == nil {
		state := "completed"
		if r.ResolutionEvidence != "" {
			state = "resolved"
		}
		return Lifecycle{State: state}, nil
	} else if !os.IsNotExist(err) {
		return Lifecycle{}, err
	}
	return inspectLocked(s)
}

// Resolve fences every future launch of this job ID. Unknown jobs require an
// operator's explicit external quiescence proof; known live processes refuse it.
// The synthetic infrastructure result is never execution or success evidence.
func Resolve(ctx context.Context, s Spec, confirmedState, evidence, reason string) (Result, error) {
	if (confirmedState != "not_started" && confirmedState != "stopped") || strings.TrimSpace(evidence) == "" || strings.TrimSpace(reason) == "" {
		return Result{}, errors.New("confirmed not_started/stopped state, evidence and reason are required")
	}
	lock, err := jobLock(ctx, s, false)
	if err != nil {
		return Result{}, err
	}
	defer lock.Close()
	if r, err := LoadResult(s.Root, s.JobID); err == nil {
		return r, nil
	} else if !os.IsNotExist(err) {
		return Result{}, err
	}
	life, err := inspectLocked(s)
	if err != nil {
		return Result{}, err
	}
	if life.State == "running" || (confirmedState == "not_started" && life.State != "not_started") {
		return Result{}, errors.New("job is live or its claimed absence is unverified")
	}
	now := time.Now().UTC()
	r := Result{JobID: s.JobID, Candidate: s.Candidate, ProfileID: s.ProfileID, ProfileRevision: s.Profile.Revision, Status: "infrastructure_failure", ResolutionState: confirmedState, ResolvedAt: now, Reason: "operator confirmed " + confirmedState + ": " + reason, ResolutionEvidence: evidence}
	if err = WriteJSON(filepath.Join(s.Root, s.JobID+".resolved.json"), r); err != nil {
		return Result{}, err
	}
	return r, nil
}
