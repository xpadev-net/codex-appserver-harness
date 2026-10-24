package storage

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestPolicyDirectionPreservesAuthorityBudgetsAndHistoricalJSON(t *testing.T) {
	ctx := context.Background()
	k, w, oldTasks := readyFixture(t)
	before, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := directionRequest(w)
	legacy, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var oldShape struct {
		Key         string
		Target      contract.Target
		Instruction string
	}
	if err := json.Unmarshal(legacy, &oldShape); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(legacy, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["next_policy_revision"]; present {
		t.Fatal("old direction JSON changed", string(legacy))
	}
	for _, next := range []int{-1, w.Policy.Revision, w.Policy.Revision + 2} {
		wrong := request
		wrong.NextPolicyRevision = next
		if _, err := k.RequestWaveDirection(ctx, wrong); err == nil {
			t.Fatal("non-successor policy queued", next)
		}
	}
	unchanged, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(unchanged, before) {
		t.Fatal("invalid policy changed Wave", unchanged, err)
	}
	request.NextPolicyRevision = w.Policy.Revision + 1
	queued, err := k.RequestWaveDirection(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := k.ApplyWaveDirection(ctx, queued.ID)
	if err != nil || applied.AppliedTarget.PolicyRevision != request.NextPolicyRevision || applied.AppliedTarget.ScopeRevision != w.ScopeRevision+1 {
		t.Fatal(applied, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectedPolicy := before.Policy
	expectedPolicy.Revision++
	if current.Policy != expectedPolicy || current.Deadline != before.Deadline || current.TasksCreated != before.TasksCreated || current.PhasesCreated != before.PhasesCreated || current.Replans != before.Replans || current.MainTurns != before.MainTurns || current.FixRounds != before.FixRounds || current.OriginalRequest != before.OriginalRequest {
		t.Fatal("policy epoch reset authority or usage", current, before)
	}
	for _, old := range oldTasks {
		saved, err := k.Task(ctx, old.ID)
		if err != nil || saved.PolicyRevision != before.Policy.Revision || saved.State != contract.TaskCanceled {
			t.Fatal("old Task relabeled", saved, err)
		}
	}
	replay, err := k.RequestWaveDirection(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, applied) {
		t.Fatal("operator replay changed policy", replay, err)
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "old-policy-plan", storagePlanFixture()); err == nil {
		t.Fatal("old policy plan accepted")
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	reopened, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(reopened, current) {
		t.Fatal("policy direction lost after restart", reopened, err)
	}
}

func TestPolicyDirectionWaitsForPhysicalStopAndOldResultApplication(t *testing.T) {
	ctx := context.Background()
	k, b, run, _ := recordedTaskResultFixture(t, nil)
	w, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	request := directionRequest(w)
	request.NextPolicyRevision = w.Policy.Revision + 1
	queued, err := k.RequestWaveDirection(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, queued.ID); !errors.Is(err, ErrDirectionHeld) {
		t.Fatal("policy changed before physical stop", err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, queued.ID); !errors.Is(err, ErrDirectionHeld) {
		t.Fatal("policy discarded old result", err)
	}
	appliedResult, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || appliedResult.State != contract.TaskCompleted {
		t.Fatal(appliedResult, err)
	}
	if _, err := k.ApplyWaveDirection(ctx, queued.ID); err != nil {
		t.Fatal(err)
	}
	var result contract.TaskResult
	if err := k.Record(ctx, "task_result", appliedResult.ResultID, &result); err != nil || result.Target != b.Target {
		t.Fatal("policy relabeled old result", result, err)
	}
}

func TestPolicyDirectionRefusesPolicyCounterOverflow(t *testing.T) {
	_, w, _ := readyFixture(t)
	// Pure arithmetic boundary: no future revision is persisted or authorized.
	w.Policy.Revision = math.MaxInt
	request := directionRequest(w)
	request.NextPolicyRevision = math.MaxInt
	if _, err := directionPolicy(w, request); err == nil {
		t.Fatal("policy counter overflow permitted")
	}
}
