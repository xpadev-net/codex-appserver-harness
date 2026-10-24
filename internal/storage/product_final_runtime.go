package storage

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type productFinalRuntimeBinding struct {
	sync.RWMutex
	generation   uint64
	epoch        string
	configDigest string
	gate         func(context.Context, contract.Target) error
}

// ProductFinalRuntimeAdmission identifies the live host gate that refreshed
// source/HEAD/capability facts for this exact normally stopped main result.
// It is persisted as audit data, never accepted as a model-provided authority.
type ProductFinalRuntimeAdmission struct {
	Epoch          string            `json:"epoch"`
	Generation     uint64            `json:"generation"`
	ConfigDigest   string            `json:"config_digest"`
	SessionID      string            `json:"session_id"`
	RunID          string            `json:"run_id"`
	OutputDigest   string            `json:"output_digest"`
	SnapshotID     string            `json:"snapshot_id"`
	SnapshotDigest string            `json:"snapshot_digest"`
	Targets        []contract.Target `json:"targets"`
}

func (k *KernelStore) BindProductFinalRuntime(configDigest string, gate func(context.Context, contract.Target) error) (func(), error) {
	if gate == nil || !validSourceDigest(configDigest) {
		return nil, errors.New("live host final gate and config digest required")
	}
	epoch, err := k.branchOwnerInstanceID()
	if err != nil {
		return nil, err
	}
	binding := &k.productFinalRuntime
	binding.Lock()
	defer binding.Unlock()
	if binding.gate != nil || binding.generation == ^uint64(0) {
		return nil, ErrResourceBusy
	}
	binding.generation++
	generation := binding.generation
	binding.epoch, binding.configDigest, binding.gate = epoch, configDigest, gate
	return func() {
		binding.Lock()
		defer binding.Unlock()
		if binding.generation == generation {
			binding.gate = nil
			binding.configDigest = ""
		}
	}, nil
}

// proposedProductCompletions reads only the actual active run and its budget.
// The ordinary application transaction authenticates the full snapshot again.
func (k *KernelStore) proposedProductCompletions(ctx context.Context, sessionID, runID string) (*ProductFinalRuntimeAdmission, error) {
	state, err := k.SessionDispatchState(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	b, run := state.Binding, state.Run
	if b.Kind != "orchestrator" || b.Closed || run == nil || run.ID != runID || run.State != "completed" || !state.ProcessStopped || !state.RunStopped {
		return nil, nil
	}
	var result contract.MainResult
	if contract.DecodeStrict([]byte(run.Final), &result) != nil || result.Validate() != nil || result.SessionID != b.ID {
		return nil, nil
	}
	targets := []contract.Target{}
	for _, decision := range result.Decisions {
		if decision.Action == "accept" && decision.ProductAcceptance != nil {
			w, err := k.Wave(ctx, decision.Target.WaveID)
			if err != nil {
				return nil, err
			}
			if w.OrchestratorSessionID != b.ID {
				return nil, ErrMainResultInvalid
			}
			if w.Target() != decision.Target || w.PendingDirectionID != "" || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted {
				return nil, nil
			}
			if w.Policy.Stage != "P" || !w.Policy.Product {
				return nil, nil
			}
			targets = append(targets, decision.Target)
		}
	}
	if len(targets) == 0 {
		return nil, nil
	}
	var budget RunBudgetBinding
	if err := k.Record(ctx, "run_budget", runID, &budget); err != nil {
		return nil, err
	}
	if budget.RunID != runID || budget.SessionID != b.ID || budget.ThreadID != run.ThreadID || budget.SnapshotID == "" || !validSourceDigest(budget.SnapshotDigest) {
		return nil, ErrMainResultInvalid
	}
	for _, target := range targets {
		if !slices.Contains(budget.Targets, target) {
			return nil, ErrMainResultInvalid
		}
	}
	return &ProductFinalRuntimeAdmission{SessionID: b.ID, RunID: runID, OutputDigest: bytesDigest([]byte(run.Final)), SnapshotID: budget.SnapshotID, SnapshotDigest: budget.SnapshotDigest, Targets: targets}, nil
}
func (k *KernelStore) ApplyMainResult(ctx context.Context, sessionID, runID string) (MainResultApplication, error) {
	empty := MainResultApplication{SessionID: sessionID, RunID: runID}
	admission, err := k.proposedProductCompletions(ctx, sessionID, runID)
	if err != nil {
		return empty, err
	}
	if admission == nil {
		return k.applyMainResult(ctx, sessionID, runID, nil)
	}
	binding := &k.productFinalRuntime
	binding.RLock()
	gate, epoch, generation, digest := binding.gate, binding.epoch, binding.generation, binding.configDigest
	binding.RUnlock()
	if gate == nil {
		return empty, ErrMainResultHeld
	}
	for _, target := range admission.Targets {
		if err := gate(ctx, target); err != nil {
			return empty, errors.Join(ErrMainResultHeld, err)
		}
	}
	// External observations occur outside a DB transaction. Keep this live
	// binding stable through the ordinary atomic result application afterwards.
	binding.RLock()
	defer binding.RUnlock()
	if binding.gate == nil || binding.epoch != epoch || binding.generation != generation || binding.configDigest != digest {
		return empty, ErrMainResultHeld
	}
	admission.Epoch, admission.Generation, admission.ConfigDigest = epoch, generation, digest
	return k.applyMainResult(ctx, sessionID, runID, admission)
}
