package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"math"
	"time"
)

func validateSnapshotDigest(snapshot contract.InputSnapshot) error {
	if snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return errors.New("input snapshot digest conflict")
	}
	return nil
}

// Wall-clock Wave expiry remains dynamic in the event loop, so an audited
// deadline extension does not leave an obsolete fixed context deadline behind.
// Every process operation is nevertheless bounded by the adopted turn limit.
func (r Runtime) executionContext(ctx context.Context, sessionID string, input []byte, run *contract.SessionRun) (context.Context, context.CancelFunc, error) {
	if r.Store == nil {
		return nil, nil, errors.New("runtime store is missing")
	}
	b, err := r.Store.SessionBinding(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}
	targets := []contract.Target{b.Target}
	if b.Kind == "orchestrator" {
		if run != nil {
			var budget storage.RunBudgetBinding
			if err = r.Store.Record(ctx, "run_budget", run.ID, &budget); err != nil {
				return nil, nil, err
			}
			if budget.SessionID != sessionID || budget.ThreadID != run.ThreadID {
				return nil, nil, errors.New("recovery budget source conflict")
			}
			targets = budget.Targets
		} else {
			var snapshot struct {
				SchemaVersion int               `json:"schema_version"`
				SessionID     string            `json:"session_id"`
				Targets       []contract.Target `json:"targets"`
			}
			if err = json.Unmarshal(input, &snapshot); err != nil {
				return nil, nil, err
			}
			if snapshot.SchemaVersion != 1 || snapshot.SessionID != sessionID {
				return nil, nil, errors.New("main input must name its logical session")
			}
			targets = snapshot.Targets
		}
	}
	if len(targets) == 0 {
		return nil, nil, errors.New("runtime needs scoped Wave targets")
	}
	seconds := int64(math.MaxInt64 / int64(time.Second))
	seen := map[string]bool{}
	for _, target := range targets {
		if !target.Valid() || seen[target.WaveID] {
			return nil, nil, errors.New("runtime target must be versioned and distinct")
		}
		seen[target.WaveID] = true
		w, err := r.Store.Wave(ctx, target.WaveID)
		if err != nil {
			return nil, nil, err
		}
		parent := b.ID
		if b.Kind == "task" {
			parent = b.ParentSessionID
		}
		if w.OrchestratorSessionID != parent {
			return nil, nil, errors.New("runtime target belongs to another parent")
		}
		limits, err := r.Store.WaveOperationLimits(ctx, target.WaveID)
		if err != nil {
			return nil, nil, err
		}
		seconds = min(seconds, limits.TurnSeconds)
	}
	bounded, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	return bounded, cancel, nil
}
