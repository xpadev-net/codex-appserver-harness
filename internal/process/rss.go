package process

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// TreeRSS is a point-in-time sum of /proc RSS for the original process and its
// visible descendants, including children in other process groups. Shared pages
// are counted per process. Concurrent births, exits and reparenting make this a
// sampled observation, not an exact peak or unique physical-memory measurement.
// A missing/reused root or unreadable/changed selected process yields unknown.
func TreeRSS(ctx context.Context, root Identity) (int64, error) {
	if !root.Valid() {
		return 0, errors.New("RSS requires original process identity")
	}
	boot, err := readBootID()
	if err != nil || boot != root.BootID {
		return 0, errors.New("RSS process boot identity is unavailable")
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	stats := map[int]rssStat{}
	for _, entry := range entries {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		stat, err := readRSSStat(pid)
		if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return 0, err
		}
		stats[pid] = stat
	}
	first, ok := stats[root.PID]
	if !ok || first.ticks != root.StartTicks || first.state == "Z" || first.state == "X" {
		return 0, errors.New("RSS original process is unavailable")
	}
	children := map[int][]int{}
	for pid, stat := range stats {
		children[stat.parent] = append(children[stat.parent], pid)
	}
	queue := []int{root.PID}
	seen := map[int]bool{}
	var total int64
	for len(queue) > 0 {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		pid := queue[0]
		queue = queue[1:]
		if seen[pid] {
			return 0, errors.New("RSS ancestry cycle")
		}
		seen[pid] = true
		stat, err := readRSSStat(pid)
		old := stats[pid]
		if err != nil || stat.ticks != old.ticks || stat.parent != old.parent {
			return 0, errors.New("RSS selected process changed during observation")
		}
		if stat.bytes > math.MaxInt64-total {
			return 0, errors.New("RSS sum overflow")
		}
		total += stat.bytes
		queue = append(queue, children[pid]...)
	}
	last, err := readRSSStat(root.PID)
	if err != nil || last.ticks != root.StartTicks || last.state == "Z" || last.state == "X" {
		return 0, errors.New("RSS original process changed during observation")
	}
	return total, nil
}

type rssStat struct {
	state  string
	parent int
	ticks  uint64
	bytes  int64
}

func readRSSStat(pid int) (rssStat, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return rssStat{}, err
	}
	return parseRSSStat(string(b), int64(os.Getpagesize()))
}
func parseRSSStat(raw string, pageSize int64) (s rssStat, err error) {
	pos := strings.LastIndex(raw, ") ")
	if pos < 0 {
		return s, errors.New("invalid RSS process stat")
	}
	f := strings.Fields(raw[pos+2:])
	if len(f) < 22 || pageSize <= 0 {
		return s, errors.New("short RSS process stat or invalid page size")
	}
	s.state = f[0]
	if s.parent, err = strconv.Atoi(f[1]); err != nil || s.parent < 0 {
		return s, errors.New("invalid RSS parent")
	}
	if s.ticks, err = strconv.ParseUint(f[19], 10, 64); err != nil {
		return s, errors.New("invalid RSS process start identity")
	}
	pages, err := strconv.ParseInt(f[21], 10, 64)
	if err != nil || pages < 0 || pages > math.MaxInt64/pageSize {
		return s, errors.New("invalid RSS page count")
	}
	s.bytes = pages * pageSize
	return s, nil
}
