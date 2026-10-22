// Package process inspects Linux process identities and process groups.
// Descendant containment remains the verified launcher's separate contract.
package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Identity struct {
	PID        int    `json:"pid"`
	BootID     string `json:"boot_id"`
	StartTicks uint64 `json:"start_ticks"`
}

func (p Identity) Valid() bool { return p.PID > 0 && p.BootID != "" && p.StartTicks > 0 }

var ErrUnconfirmed = errors.New("process group stopping is unconfirmed")

func readBootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", errors.New("empty process boot identity")
	}
	return id, nil
}
func BootID() string { id, _ := readBootID(); return id }
func ReadStat(pid int) (state string, group int, ticks uint64, err error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", 0, 0, err
	}
	pos := strings.LastIndex(string(b), ") ")
	if pos < 0 {
		return "", 0, 0, errors.New("invalid process stat")
	}
	fields := strings.Fields(string(b)[pos+2:])
	if len(fields) < 20 {
		return "", 0, 0, errors.New("short process stat")
	}
	group, err = strconv.Atoi(fields[2])
	if err != nil {
		return "", 0, 0, err
	}
	ticks, err = strconv.ParseUint(fields[19], 10, 64)
	return fields[0], group, ticks, err
}
func Identify(pid int) Identity {
	_, _, ticks, _ := ReadStat(pid)
	return Identity{PID: pid, BootID: BootID(), StartTicks: ticks}
}
func Alive(p Identity) bool {
	if p.PID <= 0 {
		return false
	}
	if p.BootID != "" {
		current, err := readBootID()
		if err != nil {
			return true
		}
		if p.BootID != current {
			return false
		}
	}
	state, _, ticks, err := ReadStat(p.PID)
	if err == nil {
		return state != "Z" && state != "X" && (p.StartTicks == 0 || ticks == p.StartTicks)
	}
	return !os.IsNotExist(err) && !errors.Is(err, syscall.ESRCH) // Unreadable identity is uncertainty, never absence.
}
func GroupAlive(p Identity) (bool, error) {
	if p.PID <= 0 {
		return false, nil
	}
	if p.BootID != "" {
		current, err := readBootID()
		if err != nil {
			return true, err
		}
		if p.BootID != current {
			return false, nil
		}
	}
	if state, _, ticks, err := ReadStat(p.PID); err == nil {
		if p.StartTicks != 0 && ticks != p.StartTicks {
			return false, nil
		}
		if state != "Z" && state != "X" {
			return true, nil
		}
	} else if !os.IsNotExist(err) && !errors.Is(err, syscall.ESRCH) {
		return true, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return true, err
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		state, group, _, err := ReadStat(pid)
		if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return true, err
		}
		if group == p.PID && state != "Z" && state != "X" {
			return true, nil
		}
	}
	return false, nil
}

// WaitStopped requires a complete original identity. A cancellation, read
// failure or missing identity never supplies absence evidence.
func WaitStopped(ctx context.Context, p Identity) error {
	if !p.Valid() {
		return ErrUnconfirmed
	}
	for {
		alive, err := GroupAlive(p)
		if err != nil {
			return errors.Join(ErrUnconfirmed, err)
		}
		if !alive {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ErrUnconfirmed, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// RequestGroupStop signals members pinned by Linux process handles. The
// original leader must remain identifiable while collecting the group snapshot;
// a missing leader with surviving children is uncertain and stays held. No
// numeric group signal is used, and missing pidfd support fails closed.
// This requests stopping only; callers still need GroupAlive/WaitStopped proof.
func RequestGroupStop(p Identity) error { return RequestGroupStopContext(context.Background(), p) }

// RequestGroupStopContext also bounds collection by the caller's stop budget.
func RequestGroupStopContext(ctx context.Context, p Identity) error {
	bounded, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	return requestGroupStop(bounded, p)
}
func requestGroupStop(ctx context.Context, p Identity) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(ErrUnconfirmed, err)
	}
	if !p.Valid() {
		return ErrUnconfirmed
	}
	alive, err := GroupAlive(p)
	if err != nil {
		return errors.Join(ErrUnconfirmed, err)
	}
	if !alive {
		return nil
	}
	targets := map[Identity]*os.Process{}
	defer func() {
		for _, target := range targets {
			_ = target.Release()
		}
	}()
	// Freeze the original leader before enumeration so it cannot fork between
	// the snapshot and its death. Then freeze and rescan descendant members until
	// the complete observed group is stopped. Numeric PIDs never receive signals.
	pinAndFreeze := func(pid int, ticks uint64) error {
		key := Identity{PID: pid, BootID: p.BootID, StartTicks: ticks}
		if target := targets[key]; target != nil {
			return target.Signal(syscall.SIGSTOP)
		}
		target, e := os.FindProcess(pid)
		if e != nil {
			return e
		}
		matched := false
		var inspectErr error
		handleErr := target.WithHandle(func(_ uintptr) {
			state, group, currentTicks, e := ReadStat(pid)
			if os.IsNotExist(e) || errors.Is(e, syscall.ESRCH) {
				return
			}
			if e != nil {
				inspectErr = e
				return
			}
			matched = group == p.PID && ticks == currentTicks && state != "Z" && state != "X"
			if matched {
				_, leaderGroup, leaderTicks, e := ReadStat(p.PID)
				if e != nil || leaderGroup != p.PID || leaderTicks != p.StartTicks || BootID() != p.BootID {
					inspectErr = errors.Join(ErrUnconfirmed, e)
					return
				}
				inspectErr = target.Signal(syscall.SIGSTOP)
			}
		})
		if errors.Is(handleErr, os.ErrProcessDone) {
			handleErr = nil
		}
		if errors.Is(inspectErr, os.ErrProcessDone) || errors.Is(inspectErr, syscall.ESRCH) {
			inspectErr = nil
			matched = false
		}
		if handleErr != nil || inspectErr != nil {
			_ = target.Release()
			return errors.Join(handleErr, inspectErr)
		}
		if matched {
			targets[key] = target
		} else {
			_ = target.Release()
		}
		return nil
	}
	if Identify(p.PID) != p {
		return ErrUnconfirmed
	}
	if err = pinAndFreeze(p.PID, p.StartTicks); err != nil {
		return errors.Join(ErrUnconfirmed, err)
	}
	if targets[p] == nil {
		return ErrUnconfirmed
	}
	previousFrozen := false
	for {
		if err = ctx.Err(); err != nil {
			return errors.Join(ErrUnconfirmed, err)
		}
		entries, e := os.ReadDir("/proc")
		if e != nil {
			return errors.Join(ErrUnconfirmed, e)
		}
		frozen := true
		for _, entry := range entries {
			pid, e := strconv.Atoi(entry.Name())
			if e != nil {
				continue
			}
			state, group, ticks, e := ReadStat(pid)
			if os.IsNotExist(e) || errors.Is(e, syscall.ESRCH) {
				continue
			}
			if e != nil {
				return errors.Join(ErrUnconfirmed, e)
			}
			if group != p.PID || state == "Z" || state == "X" {
				continue
			}
			key := Identity{PID: pid, BootID: p.BootID, StartTicks: ticks}
			threadsStopped, e := allThreadsStopped(pid)
			if e != nil {
				return errors.Join(ErrUnconfirmed, e)
			}
			// A vfork parent can be in D until its frozen child exits. Kill
			// pinned, fully stopped children first; the original leader stays
			// as the group anchor while blocked parents become stoppable.
			if pid != p.PID && targets[key] != nil && threadsStopped && (state == "T" || state == "t") {
				e = targets[key].Signal(syscall.SIGKILL)
				if e != nil && !errors.Is(e, os.ErrProcessDone) && !errors.Is(e, syscall.ESRCH) {
					return errors.Join(ErrUnconfirmed, e)
				}
				frozen = false
				continue
			}
			if targets[key] == nil || state != "T" && state != "t" || !threadsStopped {
				frozen = false
				if e = pinAndFreeze(pid, ticks); e != nil && !errors.Is(e, os.ErrProcessDone) && !errors.Is(e, syscall.ESRCH) {
					return errors.Join(ErrUnconfirmed, e)
				}
			}
		}
		// Holding this exact stopped leader ensures the PGID was never recycled
		// during collection; every target below is pinned to its original process.
		state, group, ticks, e := ReadStat(p.PID)
		if e != nil || group != p.PID || ticks != p.StartTicks || state == "Z" || state == "X" || BootID() != p.BootID {
			return errors.Join(ErrUnconfirmed, e)
		}
		if frozen && previousFrozen && (state == "T" || state == "t") {
			for _, target := range targets {
				if e = target.Signal(syscall.SIGKILL); e != nil && !errors.Is(e, os.ErrProcessDone) && !errors.Is(e, syscall.ESRCH) {
					return errors.Join(ErrUnconfirmed, e)
				}
			}
			return nil
		}
		previousFrozen = frozen
		select {
		case <-ctx.Done():
			return errors.Join(ErrUnconfirmed, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func allThreadsStopped(pid int) (bool, error) {
	threads, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	for _, thread := range threads {
		tid, err := strconv.Atoi(thread.Name())
		if err != nil {
			return false, err
		}
		state, _, _, err := ReadStat(tid)
		if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return false, err
		}
		if state != "T" && state != "t" && state != "Z" && state != "X" {
			return false, nil
		}
	}
	return true, nil
}

// StopGroup retries a bounded collection that was interrupted before all
// members froze, and returns only after strict group-absence proof or deadline.
func StopGroup(ctx context.Context, p Identity) error {
	if !p.Valid() {
		return ErrUnconfirmed
	}
	for {
		requestErr := RequestGroupStopContext(ctx, p)
		pulse, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		waitErr := WaitStopped(pulse, p)
		cancel()
		if waitErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Join(ErrUnconfirmed, requestErr, waitErr, ctx.Err())
		}
	}
}
