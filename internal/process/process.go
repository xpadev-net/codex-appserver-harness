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
func RequestGroupStop(p Identity) error {
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
	if Identify(p.PID) != p {
		return ErrUnconfirmed
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return errors.Join(ErrUnconfirmed, err)
	}
	targets := []*os.Process{}
	defer func() {
		for _, target := range targets {
			_ = target.Release()
		}
	}()
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
		target, e := os.FindProcess(pid)
		if e != nil {
			return errors.Join(ErrUnconfirmed, e)
		}
		matched := false
		var inspectErr error
		handleErr := target.WithHandle(func(_ uintptr) {
			currentState, currentGroup, currentTicks, e := ReadStat(pid)
			if os.IsNotExist(e) || errors.Is(e, syscall.ESRCH) {
				return
			}
			if e != nil {
				inspectErr = e
				return
			}
			matched = currentGroup == p.PID && currentTicks == ticks && currentState != "Z" && currentState != "X"
		})
		if errors.Is(handleErr, os.ErrProcessDone) {
			handleErr = nil
		}
		if handleErr != nil || inspectErr != nil {
			_ = target.Release()
			return errors.Join(ErrUnconfirmed, handleErr, inspectErr)
		}
		if matched {
			targets = append(targets, target)
		} else {
			_ = target.Release()
		}
	}
	// No signals precede this final check. If this exact leader is still here,
	// its PGID could not have been recycled during the pinned snapshot above.
	state, group, ticks, err := ReadStat(p.PID)
	if err != nil || group != p.PID || ticks != p.StartTicks || state == "Z" || state == "X" || BootID() != p.BootID {
		return errors.Join(ErrUnconfirmed, err)
	}
	for _, target := range targets {
		if err = target.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
			return errors.Join(ErrUnconfirmed, err)
		}
	}
	return nil
}
