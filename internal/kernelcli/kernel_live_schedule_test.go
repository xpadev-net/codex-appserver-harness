package kernelcli

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type nativeInterval struct {
	SessionID   string
	Start, Stop time.Time
}
type nativeSchedule struct {
	Initial []domain.Task
	Runs    []nativeInterval
	MainID  string
}

// Read headers only; raw model output and private workspace paths are unnecessary.
func readNativeSchedule(ctx context.Context, k *storage.KernelStore, waveID string) (nativeSchedule, error) {
	s := nativeSchedule{}
	tasks, err := k.Tasks(ctx, waveID)
	if err != nil {
		return s, err
	}
	for _, task := range tasks {
		if task.PlanRevision == 1 && len(task.Spec.DependsOn) == 0 && task.Spec.Kind != "review" {
			s.Initial = append(s.Initial, task)
		}
		s.MainID = task.ParentSessionID
	}
	rows, err := k.DB.QueryContext(ctx, `SELECT r.work_item_id,json_extract(r.payload,'$.started'),
		json_extract(p.payload,'$.stopped_at'),json_extract(p.payload,'$.state')
		FROM records r LEFT JOIN records p ON p.kind='managed_process'
		AND json_extract(p.payload,'$.request.run_id')=r.id
		AND json_extract(p.payload,'$.request.mode')='execute'
		WHERE r.kind='agent_run' ORDER BY r.rowid`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var interval nativeInterval
		var start, stop, state string
		if err = rows.Scan(&interval.SessionID, &start, &stop, &state); err != nil {
			return s, err
		}
		if state != "stopped" {
			return s, errors.New("native execute owner not stopped")
		}
		if interval.Start, err = time.Parse(time.RFC3339Nano, start); err != nil {
			return s, err
		}
		if interval.Stop, err = time.Parse(time.RFC3339Nano, stop); err != nil {
			return s, err
		}
		s.Runs = append(s.Runs, interval)
	}
	return s, rows.Err()
}

func verifyNativeSchedule(s nativeSchedule) error {
	if len(s.Initial) != 2 || s.Initial[0].PhaseID != s.Initial[1].PhaseID || s.Initial[0].SessionID == s.Initial[1].SessionID {
		return errors.New("two independent initial Tasks in one Phase required")
	}
	initial := map[string]nativeInterval{}
	mains := []nativeInterval{}
	for _, run := range s.Runs {
		if run.Start.IsZero() || !run.Start.Before(run.Stop) {
			return errors.New("invalid execution interval")
		}
		if run.SessionID == s.MainID {
			mains = append(mains, run)
		}
		for _, task := range s.Initial {
			if run.SessionID == task.SessionID {
				if _, duplicate := initial[task.SessionID]; duplicate {
					return errors.New("unexpected initial Task retry")
				}
				initial[task.SessionID] = run
			}
		}
	}
	a, aOK := initial[s.Initial[0].SessionID]
	b, bOK := initial[s.Initial[1].SessionID]
	if !aOK || !bOK || !a.Start.Before(b.Stop) || !b.Start.Before(a.Stop) {
		return errors.New("initial Tasks did not overlap")
	}
	if len(mains) != 2 {
		return errors.New("only initial/final main model runs permitted")
	}
	for _, run := range s.Runs {
		if run.SessionID != s.MainID && (!mains[0].Start.Before(run.Start) || !run.Stop.Before(mains[1].Start)) {
			return errors.New("main model run interrupted Task execution pipeline")
		}
	}
	for _, run := range s.Runs {
		_, isInitial := initial[run.SessionID]
		if run.SessionID != s.MainID && !isInitial && !mains[0].Stop.Before(run.Start) {
			return errors.New("initial main continued into review pipeline")
		}
	}
	return nil
}
