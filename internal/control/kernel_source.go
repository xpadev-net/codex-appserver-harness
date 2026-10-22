package control

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
)

// KernelSource reconstructs work from committed DB facts. It never starts a
// model itself. Loop/Runtime retain process, capacity and physical stop ownership.
// Quality/template progression and explicit recovery controls are separate from
// this session dispatch adapter; no Task/main output can complete a Wave here.
type KernelSource struct {
	Store         *storage.KernelStore
	Report        func(error)
	mu            sync.Mutex
	planningAfter map[string]string
}
type sourceHold struct {
	SessionID     string `json:"session_id"`
	RunID         string `json:"run_id"`
	PreparationID string `json:"preparation_id"`
	Reason        string `json:"reason"`
}
type recoveryScan struct {
	RetryAfter time.Time `json:"retry_after"`
}

func NewKernelSource(k *storage.KernelStore) *KernelSource { return &KernelSource{Store: k} }
func (s *KernelSource) now() time.Time {
	if s.Store.Now != nil {
		return s.Store.Now().UTC()
	}
	return time.Now().UTC()
}
func (s *KernelSource) report(err error) {
	if err != nil && s.Report != nil {
		s.Report(err)
	}
}
func sourceID(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%x", prefix, b), nil
}
func sourceContract(kind string) (any, worker.OutputValidator, error) {
	if kind == "task" {
		return contract.TaskResultSchema(), worker.ValidateTaskOutput, nil
	}
	if kind != "orchestrator" {
		return nil, nil, errors.New("unknown source session kind")
	}
	return contract.MainResultSchema(), func(data []byte, b contract.SessionBinding) error {
		var r contract.MainResult
		if err := contract.DecodeStrict(data, &r); err != nil {
			return err
		}
		if err := r.Validate(); err != nil {
			return err
		}
		if b.Kind != "orchestrator" || r.SessionID != b.ID {
			return errors.New("main result session mismatch")
		}
		return nil
	}, nil
}
func (s *KernelSource) Ready(ctx context.Context, after string, limit int) (WorkPage, error) {
	out := WorkPage{Work: []SessionWork{}}
	if s.Store == nil || limit < 1 || limit > 128 {
		return out, errors.New("configured bounded Kernel source required")
	}
	publications, publicationErr := s.Store.ReconcileKernelPublications(ctx, 16)
	s.report(publicationErr)
	out.Recheck = len(publications.Completed) > 0
	page, err := s.Store.DispatchSessions(ctx, after, 128)
	if err != nil {
		return out, err
	}
	for i, sid := range page.SessionIDs {
		state, err := s.Store.SessionDispatchState(ctx, sid)
		if err != nil {
			s.report(err)
			continue
		}
		work, recheck, err := s.selectSession(ctx, state)
		if err != nil {
			s.report(err)
		}
		out.Recheck = out.Recheck || recheck
		if work != nil {
			out.Work = append(out.Work, *work)
		}
		if len(out.Work) == limit {
			if i+1 < len(page.SessionIDs) || page.NextAfter != "" {
				out.NextAfter = sid
			}
			return out, nil
		}
	}
	out.NextAfter = page.NextAfter
	return out, nil
}
func (s *KernelSource) selectSession(ctx context.Context, state storage.SessionDispatchState) (*SessionWork, bool, error) {
	b := state.Binding
	if state.Creating || !state.ProcessStopped {
		return nil, false, nil
	}
	schema, validate, err := sourceContract(b.Kind)
	if err != nil {
		return nil, false, err
	}
	if state.Run != nil {
		if !state.RunStopped {
			return nil, false, nil
		}
		run := *state.Run
		switch run.State {
		case "completed", "failed", "interrupted", "blocked":
			if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: run.ID}); err != nil {
				return nil, false, err
			}
			next, err := s.Store.SessionDispatchState(ctx, b.ID)
			return nil, err == nil && next.Run == nil, err
		case "starting", "running", "inProgress", "unknown":
			var scan recoveryScan
			err = s.Store.Record(ctx, "kernel_recovery_scan", run.ID, &scan)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, false, err
			}
			now := s.now()
			if !scan.RetryAfter.IsZero() && now.Before(scan.RetryAfter) {
				return nil, false, nil
			}
			if b.ThreadID == "" {
				return nil, false, s.hold(ctx, state, run.ID, "unknown run has no readable bound thread")
			}
			if err = s.Store.Put(ctx, "kernel_recovery_scan", run.ID, b.ID, recoveryScan{RetryAfter: now.Add(60 * time.Second)}); err != nil {
				return nil, false, err
			}
			return &SessionWork{Recovery: &RecoveryRequest{SessionID: b.ID, Run: run, Validate: validate}}, false, nil
		default:
			return nil, false, errors.New("unknown saved run state")
		}
	}
	if state.Wake != nil {
		reconciled, e := s.Store.ReconcileUnstartedWake(ctx, state.Wake.Intent.ID)
		if e == nil && reconciled.Intent.State == "superseded" {
			return nil, true, nil
		}
		if e != nil && !errors.Is(e, storage.ErrWakeHeld) {
			return nil, false, e
		}
	}
	if b.Closed || b.Kind == "task" && b.TaskState != contract.TaskReady && b.TaskState != contract.TaskWaiting {
		return nil, false, nil
	}
	var hold sourceHold
	if err = s.Store.Record(ctx, "kernel_dispatch_hold", b.ID, &hold); err == nil {
		if hold.SessionID != b.ID {
			return nil, false, errors.New("source hold owner conflict")
		}
		if hold.PreparationID == b.PreparationID {
			return nil, false, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	wake := state.Wake
	if wake != nil {
		current, err := s.Store.RunnableWake(ctx, wake.Intent.ID)
		if errors.Is(err, storage.ErrWakeHeld) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		wake = &current
	} else {
		notifications, err := s.Store.PendingWakeNotifications(ctx, b.ID, 16)
		if err != nil {
			return nil, false, err
		}
		if len(notifications) > 0 {
			key, err := sourceID("wake-source-")
			if err != nil {
				return nil, false, err
			}
			for len(notifications) > 0 {
				ids := []string{}
				for _, n := range notifications {
					ids = append(ids, n.ID)
				}
				reservation, err := s.Store.ReserveWake(ctx, storage.WakeRequest{SessionID: b.ID, Key: key, NotificationIDs: ids, IncludeSessionContext: true})
				if err == nil {
					wake = &reservation
					break
				}
				if errors.Is(err, storage.ErrWakeBusy) {
					return nil, true, nil
				}
				if errors.Is(err, storage.ErrWakeHeld) {
					return nil, false, nil
				}
				if !errors.Is(err, storage.ErrWakeInputLimit) && !errors.Is(err, storage.ErrWakeScopeLimit) {
					return nil, false, err
				}
				if len(notifications) == 1 {
					return nil, false, err
				}
				notifications = notifications[:len(notifications)/2]
			}
		}
	}
	if b.Kind == "task" {
		request := storage.ReadyRequest{Target: b.Target}
		if b.TaskState == contract.TaskWaiting {
			if wake == nil {
				return nil, false, nil
			}
			request.ResumeTaskIDs = []string{b.TaskID}
		}
		ready, err := s.Store.ReadyTasks(ctx, request)
		if err != nil {
			return nil, false, err
		}
		found := false
		for _, candidate := range ready.Tasks {
			found = found || candidate.Task.ID == b.TaskID
		}
		if !found {
			return nil, false, nil
		}
	}
	if wake != nil {
		return &SessionWork{Execute: &ExecuteRequest{SessionID: b.ID, RunID: wake.Intent.RunID, Snapshot: wake.Snapshot, OutputSchema: schema, Validate: validate}}, false, nil
	}
	targets := []contract.Target{b.Target}
	if b.Kind == "orchestrator" {
		s.mu.Lock()
		planningAfter := s.planningAfter[b.ID]
		s.mu.Unlock()
		page, err := s.Store.PlanningInputs(ctx, b.ID, planningAfter, contract.MaxMainWaves)
		if err != nil {
			return nil, false, err
		}
		s.mu.Lock()
		if s.planningAfter == nil {
			s.planningAfter = map[string]string{}
		}
		s.planningAfter[b.ID] = page.NextAfter
		s.mu.Unlock()
		if len(page.Targets) == 0 {
			return nil, page.NextAfter != "", nil
		}
		targets = page.Targets
	}
	snapshot, err := s.Store.BuildSessionInput(ctx, b.ID, targets)
	if err != nil {
		return nil, false, err
	}
	runID, err := sourceID("run-source-")
	if err != nil {
		return nil, false, err
	}
	return &SessionWork{Execute: &ExecuteRequest{SessionID: b.ID, RunID: runID, Snapshot: snapshot, OutputSchema: schema, Validate: validate}}, false, nil
}
func (s *KernelSource) hold(ctx context.Context, state storage.SessionDispatchState, runID, reason string) error {
	return s.Store.Put(ctx, "kernel_dispatch_hold", state.Binding.ID, state.Binding.ID, sourceHold{SessionID: state.Binding.ID, RunID: runID, PreparationID: state.Binding.PreparationID, Reason: reason})
}

// Complete trusts the saved run and its original stop proof, never an outcome
// supplied by a hint/callback. Held/invalid known results stay undecided in DB,
// without occupying a local flight or replaying a model on that session.
func (s *KernelSource) Complete(ctx context.Context, c Completion) error {
	if s.Store == nil {
		return errors.New("Kernel source store required")
	}
	var owner string
	var applied, stopped bool
	runErr := s.Store.DB.QueryRowContext(ctx, "SELECT session_id,applied,process_stopped FROM kernel_run_bindings WHERE run_id=?", c.RunID).Scan(&owner, &applied, &stopped)
	if runErr == nil {
		if owner != c.SessionID {
			return errors.New("completion belongs to another session")
		}
		if applied {
			if !stopped {
				return errors.New("applied run lacks physical stopping proof")
			}
			return nil
		}
	} else if !errors.Is(runErr, sql.ErrNoRows) {
		return runErr
	}
	state, err := s.Store.SessionDispatchState(ctx, c.SessionID)
	if err != nil {
		return err
	}
	if state.Run == nil {
		if runErr == nil {
			return errors.New("undecided completion is not the active saved run")
		}
		if c.Err == nil || errors.Is(c.Err, context.Canceled) || errors.Is(c.Err, context.DeadlineExceeded) || errors.Is(c.Err, storage.ErrManagedProcessHeld) || errors.Is(c.Err, storage.ErrExecutionCapacity) {
			return nil
		}
		return s.hold(ctx, state, c.RunID, "execution did not reserve a run; explicit dispatch reconciliation required")
	}
	if state.Run.ID != c.RunID {
		return errors.New("completion does not match active saved run")
	}
	if !state.ProcessStopped || !state.RunStopped {
		return errors.New("source completion awaits physical stop proof")
	}
	s.mu.Lock()
	delete(s.planningAfter, c.SessionID)
	s.mu.Unlock()
	switch state.Run.State {
	case "completed":
		if state.Binding.Kind == "task" {
			_, err = s.Store.ApplyTaskResult(ctx, c.SessionID, c.RunID)
		} else {
			_, err = s.Store.ApplyMainResult(ctx, c.SessionID, c.RunID)
		}
		if errors.Is(err, storage.ErrTaskResultReferences) || errors.Is(err, storage.ErrMainResultHeld) || errors.Is(err, storage.ErrMainResultInvalid) || errors.Is(err, storage.ErrInterruptionHeld) {
			if holdErr := s.hold(ctx, state, c.RunID, "saved output requires result reconciliation"); holdErr != nil {
				return holdErr
			}
			s.report(err)
			return nil
		}
		return err
	case "failed", "interrupted", "blocked":
		if state.Binding.Kind == "task" {
			return s.Store.ArchiveSessionRun(ctx, c.SessionID, c.RunID, "known stopped "+state.Run.State+" Task run requires recovery")
		}
		return s.hold(ctx, state, c.RunID, "known stopped main run requires explicit recovery")
	case "starting", "running", "inProgress", "unknown":
		return nil
	}
	return errors.New("unrecognized saved completion state")
}

var _ WorkSource = (*KernelSource)(nil)
