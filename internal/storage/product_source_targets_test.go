package storage

import (
	"context"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductSourceObservationTargetsAreBoundedScopedAndExcludeTerminal(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Policy.Stage, request.Policy.Product = "P", true
	create := func(source, sourceID string) domain.Wave {
		t.Helper()
		r := request
		r.Source = source
		r.SourceID = sourceID
		w, err := k.CreateProductWave(ctx, ProductWaveRequest{Wave: r, WorkItems: []ProductWorkItemDefinition{{ID: sourceID, Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	canceled := create("github", "r17-i46")
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-page", Target: canceled.Target(), Action: "cancel", Reason: "terminal sources are not polled"}); err != nil {
		t.Fatal(err)
	}
	completed := create("github", "r17-i47")
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	completed.State = domain.WaveCompleted
	if err := saveWave(ctx, tx, completed); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	a := create("github", "r17-i42")
	b := create("github", "r17-i43")
	create("github", "r18-i44")
	create("direct", "r17-i45")
	if a.ID > b.ID {
		a, b = b, a
	}
	first, err := k.ProductSourceObservationTargets(ctx, 17, "", 1)
	if err != nil || len(first) != 1 || first[0].ID != a.ID {
		t.Fatal(first, err)
	}
	second, err := k.ProductSourceObservationTargets(ctx, 17, a.ID, 1)
	if err != nil || len(second) != 1 || second[0].ID != b.ID {
		t.Fatal(second, err)
	}
	last, err := k.ProductSourceObservationTargets(ctx, 17, b.ID, 32)
	if err != nil || len(last) != 0 {
		t.Fatal(last, err)
	}
	for _, limit := range []int{0, 33} {
		if _, err := k.ProductSourceObservationTargets(ctx, 17, "", limit); err == nil {
			t.Fatal("unbounded page accepted", limit)
		}
	}
	if _, err := k.ProductSourceObservationTargets(ctx, 0, "", 1); err == nil {
		t.Fatal("missing repository accepted")
	}
}
