package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// SessionBinding is a controller-owned identity, not model-supplied state.
// Task bindings retain their thread until terminal; main rebinding is audited.
type SessionBinding = contract.SessionBinding
type InputSnapshot = contract.InputSnapshot

// Persistence must compare bindings inside each write transaction.
// ReserveSessionThread reserves preparation for both new and bound threads and
// saves a fresh PreparationID; the driver reloads it before any RPC. ReserveRun
// atomically records the snapshot/start intent and exclusive active run; it must
// not release a reservation for a process whose stopping is unconfirmed.
// Journal.Put for unknown/blocked outcomes must atomically retain any turn
// association observed by an early tool while preserving the failure outcome.
type SessionPersistence interface {
	Journal
	SessionBinding(context.Context, string) (SessionBinding, error)
	ReserveSessionThread(context.Context, SessionBinding) error
	BindSessionThread(context.Context, SessionBinding, string) error
	SessionThreadUnused(context.Context, SessionBinding) (bool, error)
	ReserveSessionRun(context.Context, SessionBinding, InputSnapshot, Run) error
	RebindMainThread(context.Context, SessionBinding, string, string, string) error
}
type PreparedSession struct {
	Binding         SessionBinding
	BaselineTurnIDs []string
	BaselineKnown   bool
	UsageBaseline   *int64
}
type SessionDriver struct {
	RPC   RPC
	Store SessionPersistence
}
type OutputValidator func([]byte, SessionBinding) error

func validSession(b SessionBinding) error {
	if b.ID == "" || !filepath.IsAbs(b.CWD) || b.Model == "" || b.Generation < 1 || b.Closed || (b.Sandbox != "read-only" && b.Sandbox != "workspace-write") {
		return errors.New("invalid/closed session binding")
	}
	switch b.Kind {
	case "orchestrator":
		if b.TaskID != "" || b.ParentSessionID != "" {
			return errors.New("invalid main session")
		}
	case "task":
		if b.TaskID == "" || b.PhaseID == "" || b.ParentSessionID == "" || !b.Target.Valid() {
			return errors.New("invalid Task binding")
		}
		switch b.TaskState {
		case "ready", "running", "waiting", "blocked":
		default:
			return errors.New("terminal Task sessions cannot resume")
		}
	default:
		return errors.New("unknown session kind")
	}
	return nil
}
func (d SessionDriver) Prepare(ctx context.Context, id string) (PreparedSession, error) {
	if d.Store == nil || d.RPC == nil {
		return PreparedSession{}, errors.New("session driver is unconfigured")
	}
	b, err := d.Store.SessionBinding(ctx, id)
	if err != nil {
		return PreparedSession{}, err
	}
	if err = validSession(b); err != nil {
		return PreparedSession{}, err
	}
	method := "thread/start"
	params := map[string]any{"cwd": b.CWD, "model": b.Model, "approvalPolicy": "never", "sandbox": b.Sandbox}
	// Reserve preparation before either start or resume. A prepared app-server
	// is live even before it has a turn, so replanning must await stopping proof.
	if err = d.Store.ReserveSessionThread(ctx, b); err != nil {
		return PreparedSession{}, err
	}
	prior := b
	b, err = d.Store.SessionBinding(ctx, id)
	if err != nil {
		return PreparedSession{}, err
	}
	comparable := b
	comparable.PreparationID = prior.PreparationID
	if b.PreparationID == "" || !reflect.DeepEqual(comparable, prior) {
		return PreparedSession{}, errors.New("session changed during preparation reservation")
	}
	if b.ThreadID != "" {
		method = "thread/resume"
		params["threadId"] = b.ThreadID
	} else {
		params["dynamicTools"] = contract.KernelTools(b.Kind)
		// A lost thread/start response must hold the creation intent rather
		// than silently allocate a second thread to this Task on retry.
	}
	var reply struct {
		Thread Thread `json:"thread"`
		Model  string `json:"model"`
	}
	if err = d.RPC.Call(ctx, method, params, &reply); err != nil {
		return PreparedSession{}, err
	}
	if reply.Thread.ID == "" || reply.Thread.CWD != b.CWD || reply.Model != b.Model || (b.ThreadID != "" && reply.Thread.ID != b.ThreadID) {
		return PreparedSession{}, errors.New("session thread/model/workspace mismatch")
	}
	baseline := reply.Thread
	if b.ThreadID != "" && baseline.Turns == nil {
		var history struct {
			Thread Thread `json:"thread"`
		}
		if err = d.RPC.Call(ctx, "thread/read", map[string]any{"threadId": b.ThreadID, "includeTurns": true}, &history); err != nil {
			return PreparedSession{}, err
		}
		if history.Thread.ID != b.ThreadID || history.Thread.CWD != b.CWD || history.Thread.Turns == nil {
			return PreparedSession{}, errors.New("session history unavailable; reconcile before dispatch")
		}
		baseline = history.Thread
	}
	if err = d.Store.BindSessionThread(ctx, b, reply.Thread.ID); err != nil {
		return PreparedSession{}, err
	}
	b.ThreadID = reply.Thread.ID
	prepared := PreparedSession{Binding: b, BaselineKnown: true, BaselineTurnIDs: []string{}}
	if len(baseline.Turns) == 0 {
		unused := method == "thread/start"
		if !unused {
			unused, err = d.Store.SessionThreadUnused(ctx, b)
			if err != nil {
				return PreparedSession{}, err
			}
		}
		if unused {
			zero := int64(0)
			prepared.UsageBaseline = &zero
		}
	}
	for _, turn := range baseline.Turns {
		prepared.BaselineTurnIDs = append(prepared.BaselineTurnIDs, turn.ID)
	}
	return prepared, nil
}

type sessionJournal struct {
	SessionPersistence
	binding  SessionBinding
	snapshot InputSnapshot
}

func (j sessionJournal) Put(ctx context.Context, kind, id, owner string, value any) error {
	if kind == "agent_run" {
		if run, ok := value.(Run); ok && run.State == "starting" {
			return j.ReserveSessionRun(ctx, j.binding, j.snapshot, run)
		}
	}
	return j.SessionPersistence.Put(ctx, kind, id, owner, value)
}
func (d SessionDriver) Execute(ctx context.Context, p PreparedSession, runID string, snapshot InputSnapshot, outputSchema any, validate OutputValidator) (Run, error) {
	if d.Store == nil || d.RPC == nil || !p.BaselineKnown {
		return Run{}, errors.New("configured driver and known history baseline required")
	}
	if validate == nil || outputSchema == nil || snapshot.ID == "" || snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return Run{}, errors.New("trusted output contract and digest-bound snapshot required")
	}
	if err := contract.ValidateSessionInput(snapshot.Input); err != nil {
		return Run{}, err
	}
	latest, err := d.Store.SessionBinding(ctx, p.Binding.ID)
	if err != nil {
		return Run{}, err
	}
	if err = validSession(latest); err != nil {
		return Run{}, err
	}
	if !reflect.DeepEqual(latest, p.Binding) || latest.ThreadID == "" {
		return Run{}, errors.New("session changed before dispatch")
	}
	session := Session{WorkItemID: latest.ID, Role: latest.Kind, ThreadID: latest.ThreadID, CWD: latest.CWD, Model: latest.Model, BaselineTurnIDs: p.BaselineTurnIDs, BaselineKnown: p.BaselineKnown, UsageBaseline: p.UsageBaseline}
	worker := Worker{RPC: d.RPC, Journal: sessionJournal{SessionPersistence: d.Store, binding: latest, snapshot: snapshot}}
	run, err := worker.Execute(ctx, session, runID, "snapshot:"+snapshot.ID, string(snapshot.Input), outputSchema)
	if err != nil {
		return run, err
	}
	if err = validate([]byte(run.Final), latest); err != nil {
		if saveErr := d.Store.Put(context.WithoutCancel(ctx), "session_output_error", run.ID, latest.ID, map[string]string{"reason": err.Error()}); saveErr != nil {
			return run, saveErr
		}
		return run, err
	}
	return run, nil
}
func (d SessionDriver) Reconcile(ctx context.Context, p PreparedSession, run Run, validate OutputValidator) (Run, error) {
	if d.Store == nil || d.RPC == nil {
		return run, errors.New("session driver is unconfigured")
	}
	if validate == nil || run.ThreadID != p.Binding.ThreadID {
		return run, errors.New("saved run binding/validator mismatch")
	}
	latest, err := d.Store.SessionBinding(ctx, p.Binding.ID)
	if err != nil {
		return run, err
	}
	readable := latest
	readable.Closed = false
	if readable.Kind == "task" {
		readable.TaskState = "running"
	}
	if err = validSession(readable); err != nil {
		return run, err
	}
	expected := p.Binding
	expected.Closed = latest.Closed
	expected.TaskState = latest.TaskState
	if !reflect.DeepEqual(latest, expected) {
		return run, errors.New("session changed before reconciliation")
	}
	session := Session{WorkItemID: latest.ID, Role: latest.Kind, ThreadID: latest.ThreadID, CWD: latest.CWD, Model: latest.Model}
	recovered, err := (Worker{RPC: d.RPC, Journal: d.Store}).Reconcile(ctx, session, run)
	if err != nil {
		return recovered, err
	}
	return recovered, validate([]byte(recovered.Final), latest)
}

// RebindMainThread must verify quiescence, no unknown/unapplied run and the saved
// snapshot before atomically writing the old/new binding and audit. Tasks never
// take this path; an unrecoverable Task requires a new Task/session instead.
func (d SessionDriver) RebindMain(ctx context.Context, id, newThread, snapshotID, reason string) error {
	if d.Store == nil {
		return errors.New("session driver is unconfigured")
	}
	b, err := d.Store.SessionBinding(ctx, id)
	if err != nil {
		return err
	}
	if err = validSession(b); err != nil {
		return err
	}
	if b.Kind != "orchestrator" || b.ThreadID == "" || newThread == "" || newThread == b.ThreadID || snapshotID == "" || reason == "" {
		return errors.New("only main can rebind with snapshot and audit reason")
	}
	return d.Store.RebindMainThread(ctx, b, newThread, snapshotID, reason)
}
func ValidateTaskOutput(data []byte, b SessionBinding) error {
	if b.Kind != "task" {
		return errors.New("Task output requires a Task binding")
	}
	var result contract.TaskResult
	if err := contract.DecodeStrict(data, &result); err != nil {
		return err
	}
	if err := result.Validate(); err != nil {
		return err
	}
	if result.TaskID != b.TaskID || result.PhaseID != b.PhaseID || result.Target != b.Target {
		return errors.New("result belongs to another Task/scope")
	}
	return nil
}
