package worker

import (
	"encoding/json"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type runUsageTracker struct {
	baseline      *int64
	lastTotal     *int64
	previousTurns []string
	currentSeen   bool
	reset         bool
	observed      contract.RunUsageObservation
}

func newRunUsageTracker(s Session) *runUsageTracker {
	tracker := &runUsageTracker{observed: contract.RunUsageObservation{Version: 1, Source: "unavailable"}}
	if s.BaselineKnown {
		tracker.previousTurns = append([]string{}, s.BaselineTurnIDs...)
	}
	if s.UsageBaseline != nil && *s.UsageBaseline >= 0 {
		baseline := *s.UsageBaseline
		tracker.baseline = &baseline
	}
	return tracker
}

// Only a fresh thread's zero or a replay for a saved baseline turn supplies
// the pre-run total. last is one model response, not a whole tool-using turn.
func (t *runUsageTracker) observe(currentTurn, turn string, raw json.RawMessage) (contract.RunUsageObservation, bool) {
	var usage struct {
		Total struct {
			TotalTokens *int64 `json:"totalTokens"`
		} `json:"total"`
	}
	valid := json.Unmarshal(raw, &usage) == nil && usage.Total.TotalTokens != nil && *usage.Total.TotalTokens >= 0
	if turn != currentTurn || currentTurn == "" {
		if !t.currentSeen && valid && slices.Contains(t.previousTurns, turn) {
			baseline := *usage.Total.TotalTokens
			if t.baseline == nil || baseline > *t.baseline {
				t.baseline = &baseline
			}
		}
		return t.observed, false
	}
	t.currentSeen = true
	if !valid || t.baseline == nil || t.reset {
		return t.observed, true
	}
	total := *usage.Total.TotalTokens
	if total < *t.baseline || t.lastTotal != nil && total < *t.lastTotal {
		t.reset = true
		t.observed.Source = "counter_reset"
		return t.observed, true
	}
	t.lastTotal = &total
	t.observed.Source = "thread_total_delta"
	t.observed.HasObservation = true
	t.observed.TotalTokens = total - *t.baseline
	return t.observed, true
}
