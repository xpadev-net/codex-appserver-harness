package control

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
)

type owner struct {
	store                      *storage.KernelStore
	factory                    Factory
	id, sessionID, runID, mode string
	ctx                        context.Context
	cancel                     context.CancelFunc
	now                        func() time.Time
	mu                         sync.Mutex
	reservation                *storage.ManagedReservation
	reserved                   bool
	attempted                  bool
	factoryCalled              bool
	notStarted                 bool
	process                    Process
	rpc                        *codex.SessionRPC
	startErr                   error
	closed                     atomic.Bool
	closeOnce                  sync.Once
	closeErr                   error
	released                   bool
}

func newOwner(ctx context.Context, k *storage.KernelStore, f Factory, sessionID, runID, mode string, now func() time.Time) (*owner, error) {
	if k == nil || f == nil || sessionID == "" || runID == "" || (mode != "execute" && mode != "recovery") {
		return nil, errors.New("configured runtime session/run/mode required")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &owner{store: k, factory: f, id: fmt.Sprintf("owner-%x", nonce), sessionID: sessionID, runID: runID, mode: mode, ctx: ctx, cancel: cancel, now: now}, nil
}

// The driver invokes this override before making its first RPC. Constructing
// the owner and RPC adapter has no process or model side effect.
type persistence struct {
	*storage.KernelStore
	owner *owner
}

func (p persistence) ReserveSessionThread(ctx context.Context, b contract.SessionBinding) error {
	return p.owner.reserve(ctx, b)
}
func (o *owner) reserve(ctx context.Context, b contract.SessionBinding) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed.Load() || o.reserved {
		return ErrOwnerClosed
	}
	o.reserved = true
	r, err := o.store.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: o.id, RunID: o.runID, Mode: o.mode, Binding: b})
	if err != nil {
		return err
	}
	o.reservation = &r
	if !r.Created {
		return storage.ErrManagedProcessHeld
	}
	return nil
}
func (o *owner) driver() worker.SessionDriver {
	return worker.SessionDriver{Store: persistence{KernelStore: o.store, owner: o}, RPC: o}
}
func (o *owner) ensure(ctx context.Context) (*codex.SessionRPC, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed.Load() {
		return nil, ErrOwnerClosed
	}
	if o.startErr != nil {
		return nil, o.startErr
	}
	if o.rpc != nil {
		return o.rpc, nil
	}
	if o.attempted {
		return nil, o.startErr
	}
	o.attempted = true
	fail := func(err error) (*codex.SessionRPC, error) { o.startErr = err; return nil, err }
	if o.reservation == nil {
		return fail(storage.ErrManagedProcessHeld)
	}
	p, err := o.store.BeginManagedProcessLaunch(ctx, o.id)
	if err != nil {
		return fail(err)
	}
	o.factoryCalled = true
	transport, err := o.factory.Open(o.ctx, Launch{Binding: o.reservation.Binding, Process: p})
	o.process = transport
	if err != nil {
		o.notStarted = transport == nil && errors.Is(err, ErrNotStarted)
		if !o.notStarted && transport == nil {
			err = errors.Join(err, process.ErrUnconfirmed)
		}
		return fail(err)
	}
	if transport == nil {
		return fail(process.ErrUnconfirmed)
	}
	identity := transport.ProcessIdentity()
	if err = o.store.RecordManagedProcessStarted(ctx, o.id, identity); err != nil {
		return fail(err)
	}
	if !identity.Valid() {
		return fail(process.ErrUnconfirmed)
	}
	pid := int64(identity.PID)
	spawned := transport.StartedAt()
	if err = o.store.ObserveProcess(ctx, storage.ProcessObservation{ReservationID: p.PreparationID, SessionID: o.sessionID, PID: &pid, SpawnedAt: &spawned}); err != nil {
		return fail(err)
	}
	var handler codex.ToolHandler
	if o.mode == "execute" {
		handler, err = worker.KernelToolHandler(o.sessionID, o.runID, contract.KernelGateway{Backend: o.store})
		if err != nil {
			return fail(err)
		}
	}
	o.rpc = codex.NewSessionRPC(o.ctx, transport, handler)
	if err = o.rpc.Initialize(ctx); err != nil {
		// Keep the initialized adapter for shutdown, never retry initialize/start.
		o.startErr = err
		return nil, err
	}
	return o.rpc, nil
}
func (o *owner) Call(ctx context.Context, method string, params, result any) error {
	switch method {
	case "thread/start", "thread/resume", "thread/read", "turn/start", "turn/interrupt":
	default:
		return errors.New("runtime RPC method is not admitted")
	}
	if o.mode == "recovery" && method != "thread/read" {
		return ErrReadOnly
	}
	if o.closed.Load() {
		return ErrOwnerClosed
	}
	rpc, err := o.ensure(ctx)
	if err != nil {
		return err
	}
	if o.mode == "recovery" || method == "thread/read" || method == "turn/start" || method == "turn/interrupt" {
		raw, err := json.Marshal(params)
		if err != nil {
			return err
		}
		var target struct {
			ThreadID string `json:"threadId"`
		}
		if err = json.Unmarshal(raw, &target); err != nil {
			return err
		}
		b, err := o.store.SessionBinding(ctx, o.sessionID)
		if err != nil {
			return err
		}
		if target.ThreadID == "" || target.ThreadID != b.ThreadID || b.PreparationID != o.reservation.Binding.PreparationID {
			return ErrReadOnly
		}
	}
	if method == "thread/start" || method == "thread/resume" {
		if err = o.store.MarkManagedThreadRPC(ctx, o.id, method); err != nil {
			return err
		}
	}
	return rpc.Call(ctx, method, params, result)
}
func (o *owner) Next(ctx context.Context) (codex.Message, error) {
	if o.mode == "recovery" {
		return codex.Message{}, ErrReadOnly
	}
	o.mu.Lock()
	rpc := o.rpc
	err := o.startErr
	o.mu.Unlock()
	if err != nil {
		return codex.Message{}, err
	}
	if rpc == nil {
		return codex.Message{}, storage.ErrManagedProcessHeld
	}
	return rpc.Next(ctx)
}
func (o *owner) ready(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reservation == nil || o.process == nil {
		return storage.ErrManagedProcessHeld
	}
	at := o.now()
	return o.store.ObserveProcess(ctx, storage.ProcessObservation{ReservationID: o.reservation.Binding.PreparationID, SessionID: o.sessionID, ReadyAt: &at})
}
func (o *owner) Close() error {
	o.closeOnce.Do(func() {
		o.closed.Store(true)
		requested := o.now()
		o.cancel()
		o.mu.Lock()
		defer o.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(o.ctx), 10*time.Second)
		defer cancel()
		if o.reservation == nil {
			p, err := o.store.ManagedProcess(ctx, o.id)
			if errors.Is(err, sql.ErrNoRows) {
				return
			}
			if err != nil {
				o.closeErr = err
				return
			}
			b, err := o.store.SessionBinding(ctx, o.sessionID)
			if err != nil {
				o.closeErr = err
				return
			}
			if b.PreparationID != p.PreparationID {
				o.closeErr = storage.ErrManagedProcessHeld
				return
			}
			o.reservation = &storage.ManagedReservation{Binding: b, Process: p}
		}
		if o.rpc != nil {
			o.rpc.Close()
		}
		proof := storage.ManagedStopProof{OwnerID: o.id, Disposition: "not_started"}
		var metricErr error
		if o.process != nil {
			metricErr = o.store.ObserveProcess(ctx, storage.ProcessObservation{ReservationID: o.reservation.Binding.PreparationID, SessionID: o.sessionID, StopRequestedAt: &requested})
			_ = o.process.Close()
			if !o.process.GroupStopped() {
				o.closeErr = errors.Join(process.ErrUnconfirmed, metricErr)
				return
			}
			proof.Disposition, proof.Identity = "group_stopped", o.process.ProcessIdentity()
		} else if o.factoryCalled && !o.notStarted {
			o.closeErr = process.ErrUnconfirmed
			return
		}
		if err := o.store.ConfirmManagedProcessStopped(ctx, proof); err != nil {
			o.closeErr = errors.Join(err, metricErr)
			return
		}
		o.released = true
		if o.process != nil {
			stopped := o.now()
			metricErr = errors.Join(metricErr, o.store.ObserveProcess(ctx, storage.ProcessObservation{ReservationID: o.reservation.Binding.PreparationID, SessionID: o.sessionID, StoppedAt: &stopped}))
		}
		o.closeErr = metricErr
	})
	return o.closeErr
}

type Runtime struct {
	Store   *storage.KernelStore
	Factory Factory
	Now     func() time.Time
}
type ExecuteRequest struct {
	SessionID    string
	RunID        string
	Snapshot     contract.InputSnapshot
	OutputSchema any
	Validate     worker.OutputValidator
}
type Outcome struct {
	OwnerID       string
	PreparationID string
	Run           contract.SessionRun
	Stopped       bool
}

func (o *owner) outcome(run contract.SessionRun) Outcome {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := Outcome{OwnerID: o.id, Run: run, Stopped: o.released}
	if o.reservation != nil {
		result.PreparationID = o.reservation.Binding.PreparationID
	}
	return result
}
func (r Runtime) Execute(ctx context.Context, request ExecuteRequest) (out Outcome, err error) {
	if request.Validate == nil || request.OutputSchema == nil || request.Snapshot.ID == "" {
		return out, errors.New("trusted output contract and input snapshot required")
	}
	if err = contract.ValidateSessionInput(request.Snapshot.Input); err != nil {
		return out, err
	}
	// A malformed digest must fail before preparing an app-server.
	if err = validateSnapshotDigest(request.Snapshot); err != nil {
		return out, err
	}
	ctx, cancel, err := r.executionContext(ctx, request.SessionID, request.Snapshot.Input, nil)
	if err != nil {
		return out, err
	}
	defer cancel()
	o, err := newOwner(ctx, r.Store, r.Factory, request.SessionID, request.RunID, "execute", r.Now)
	if err != nil {
		return out, err
	}
	var run contract.SessionRun
	defer func() { err = errors.Join(err, o.Close()); out = o.outcome(run) }()
	prepared, err := o.driver().Prepare(o.ctx, request.SessionID)
	if err != nil {
		return out, err
	}
	if err = o.ready(o.ctx); err != nil {
		return out, err
	}
	run, err = o.driver().Execute(o.ctx, prepared, request.RunID, request.Snapshot, request.OutputSchema, request.Validate)
	return out, err
}
func (r Runtime) Reconcile(ctx context.Context, sessionID string, run contract.SessionRun, validate worker.OutputValidator) (out Outcome, err error) {
	if validate == nil {
		return out, errors.New("trusted output validator required")
	}
	ctx, cancel, err := r.executionContext(ctx, sessionID, nil, &run)
	if err != nil {
		return out, err
	}
	defer cancel()
	o, err := newOwner(ctx, r.Store, r.Factory, sessionID, run.ID, "recovery", r.Now)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, o.Close()); out = o.outcome(run) }()
	b, err := r.Store.SessionBinding(o.ctx, sessionID)
	if err != nil {
		return out, err
	}
	if err = o.reserve(o.ctx, b); err != nil {
		return out, err
	}
	prepared := worker.PreparedSession{Binding: o.reservation.Binding, BaselineKnown: run.BaselineKnown, BaselineTurnIDs: run.BaselineTurnIDs}
	run, err = o.driver().Reconcile(o.ctx, prepared, run, validate)
	if err == nil {
		err = o.ready(o.ctx)
	}
	return out, err
}
