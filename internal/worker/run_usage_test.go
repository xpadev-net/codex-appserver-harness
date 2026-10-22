package worker

import (
	"encoding/json"
	"fmt"
	"testing"
)

func usageFrame(total, last int64) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"total":{"totalTokens":%d},"last":{"totalTokens":%d}}`, total, last))
}
func TestRunUsageUsesThreadDeltaAcrossMultipleModelResponses(t *testing.T) {
	zero := int64(0)
	for _, continued := range []bool{false, true} {
		t.Run(fmt.Sprint(continued), func(t *testing.T) {
			session := Session{BaselineKnown: true, UsageBaseline: &zero}
			offset := int64(0)
			if continued {
				session.UsageBaseline = nil
				session.BaselineTurnIDs = []string{"old"}
				offset = 500
			}
			tracker := newRunUsageTracker(session)
			if continued {
				if _, current := tracker.observe("new", "old", usageFrame(offset, 150)); current {
					t.Fatal("baseline charged as current")
				}
			}
			for _, step := range []struct{ total, last, want int64 }{{30, 30, 30}, {70, 40, 70}, {70, 40, 70}, {80, 10, 80}} {
				observation, current := tracker.observe("new", "new", usageFrame(offset+step.total, step.last))
				if !current || !observation.HasObservation || observation.TotalTokens != step.want || observation.Source != "thread_total_delta" {
					t.Fatal(observation, current)
				}
			}
			if _, current := tracker.observe("new", "foreign", usageFrame(999999, 999999)); current {
				t.Fatal("foreign turn charged")
			}
		})
	}
}
func TestRunUsageMissingBaselineAndResetRemainExplicitlyUnknown(t *testing.T) {
	tracker := newRunUsageTracker(Session{BaselineKnown: true, BaselineTurnIDs: []string{"old"}})
	observation, _ := tracker.observe("new", "new", usageFrame(900, 400))
	if observation.HasObservation || observation.Source != "unavailable" {
		t.Fatal("thread lifetime/last response charged without baseline", observation)
	}
	tracker.observe("new", "old", usageFrame(500, 150))
	observation, _ = tracker.observe("new", "new", usageFrame(1000, 100))
	if observation.HasObservation {
		t.Fatal("late baseline authorized attribution", observation)
	}
	baseline := int64(500)
	tracker = newRunUsageTracker(Session{UsageBaseline: &baseline})
	tracker.observe("new", "new", usageFrame(570, 40))
	observation, _ = tracker.observe("new", "new", usageFrame(20, 20))
	if observation.TotalTokens != 70 || observation.Source != "counter_reset" || !observation.HasObservation {
		t.Fatal(observation)
	}
	observation, _ = tracker.observe("new", "new", usageFrame(10000, 30))
	if observation.TotalTokens != 70 {
		t.Fatal("reset counter resumed attribution", observation)
	}
}
