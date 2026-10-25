package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/control"
)

func TestProductSplitDispatchConfigIsSeparateExplicitAndBounded(t *testing.T) {
	k, _, err := openKernel(context.Background(), cliRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	old := publicationConfigFixture(t)
	raw, err := json.Marshal(old.Publication)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "\"split\"") {
		t.Fatal("old publication gained split opt-in")
	}
	allowed, err := old.Publication.splitRepositories(old.Repositories)
	if err != nil || allowed != nil {
		t.Fatal("split enabled by old publication config", allowed, err)
	}
	for _, mode := range []string{"zero_workers", "many_workers", "empty", "unknown", "zero_id", "duplicate", "too_many"} {
		t.Run(mode, func(t *testing.T) {
			c := publicationConfigFixture(t)
			c.Publication.Split = &ProductSplitDispatchConfig{Workers: 1, RepositoryIDs: []int64{17}}
			switch mode {
			case "zero_workers":
				c.Publication.Split.Workers = 0
			case "many_workers":
				c.Publication.Split.Workers = 9
			case "empty":
				c.Publication.Split.RepositoryIDs = nil
			case "unknown":
				c.Publication.Split.RepositoryIDs = []int64{18}
			case "zero_id":
				c.Publication.Split.RepositoryIDs = []int64{0}
			case "duplicate":
				c.Publication.Split.RepositoryIDs = []int64{17, 17}
			case "too_many":
				c.Publication.Split.RepositoryIDs = make([]int64, 129)
			}
			if _, _, err := loadProductExecutor(operatorFile(t, c), k, control.IsolatedConfig{}); err == nil {
				t.Fatal("invalid split config accepted", mode)
			}
		})
	}
	configured := publicationConfigFixture(t)
	configured.Publication.Split = &ProductSplitDispatchConfig{Workers: 1, RepositoryIDs: []int64{17}}
	worker, hint, err := loadProductExecutor(operatorFile(t, configured), k, control.IsolatedConfig{})
	if err != nil || worker == nil || hint == nil {
		t.Fatal("explicit split pipeline not configured", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := worker(ctx, k, func() {}, func(error) {}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal("configured worker did not join", err)
	}
}
