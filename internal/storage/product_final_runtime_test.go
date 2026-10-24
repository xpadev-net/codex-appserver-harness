package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductFinalRuntimeRefreshesActualParentAndPreservesRevocation(t *testing.T) {
	for _, mode := range []string{"allow", "held", "revoke", "pause", "reopen"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, _ := completeFinalEvidenceFixture(t, "passed")
			finishProductCloudForParent(t, k, plan)
			calls := 0
			blocked := errors.New("fixture live capability is unknown")
			var revoke func()
			gate := func(ctx context.Context, target contract.Target) error {
				calls++
				if target != plan.Value.Target {
					t.Fatal("gate refreshed a caller-selected scope", target)
				}
				switch mode {
				case "held":
					return blocked
				case "revoke":
					revoke()
				case "pause":
					_, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-at-final-gate", Target: target, Action: "pause", Reason: "fixture source changed"})
					return err
				}
				return nil
			}
			var err error
			revoke, err = k.BindProductFinalRuntime(strings.Repeat("b", 64), gate)
			if err != nil {
				t.Fatal(err)
			}
			defer revoke()
			b, run, snapshot := productParentRun(t, k, plan.Value.Target)
			result := productAcceptanceResult(t, k, b, plan.Value.Target)
			run = saveMainCompletion(t, k, b, run, result, true)
			if mode == "reopen" {
				root := k.Root
				k.Close()
				k, err = OpenExistingKernel(root + "/state.db")
				if err != nil {
					t.Fatal(err)
				}
				defer k.Close()
			}
			applied, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			if mode == "allow" {
				if err != nil || calls != 1 || len(applied.ProductCompletions) != 1 {
					t.Fatal(applied, calls, err)
				}
				a := applied.ProductCompletions[0].Proof.FinalRuntime
				if a == nil || a.Epoch == "" || a.Generation == 0 || a.ConfigDigest != strings.Repeat("b", 64) || a.SessionID != b.ID || a.RunID != run.ID || a.SnapshotID != snapshot.ID || a.SnapshotDigest != snapshot.Digest || a.OutputDigest != bytesDigest([]byte(run.Final)) {
					t.Fatal("final admission lost actual runtime/run/input", a)
				}
				revoke()
				replay, replayErr := k.ApplyMainResult(ctx, b.ID, run.ID)
				if replayErr != nil || !reflect.DeepEqual(applied, replay) || calls != 1 {
					t.Fatal("historical replay refreshed or completed again", replay, replayErr)
				}
			} else {
				if err == nil {
					t.Fatal("unbound/stale/held runtime completed P", mode, applied)
				}
				w, readErr := k.Wave(ctx, plan.Value.Target.WaveID)
				if readErr != nil || w.State == domain.WaveCompleted {
					t.Fatal("held gate mutated completion", w, readErr)
				}
				if mode == "held" && !errors.Is(err, blocked) {
					t.Fatal("lost external gate failure", err)
				}
				if mode == "reopen" && calls != 0 {
					t.Fatal("reopen inherited a live callback")
				}
			}
		})
	}
}
