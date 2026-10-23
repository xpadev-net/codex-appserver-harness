package kernelcli

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type IssueSourceObserverConfig struct {
	SchemaVersion int                       `json:"schema_version"`
	SourcePolicy  product.IssueSourcePolicy `json:"source_policy"`
	Attempts      int                       `json:"attempts"`
	EverySeconds  int                       `json:"every_seconds"`
}

func loadIssueSourceObserver(path string, k *storage.KernelStore) (daemonWorker, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := readFile(path, 64<<10, true)
	if err != nil {
		return nil, err
	}
	var c IssueSourceObserverConfig
	if err := contract.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	if c.SchemaVersion != 1 || c.EverySeconds < 1 || c.EverySeconds > 3600 {
		return nil, errors.New("Issue source observer schema 1 and every_seconds 1..3600 required")
	}
	observer := func(store *storage.KernelStore, wake func(), report func(error)) product.IssueSourceObserver {
		return product.IssueSourceObserver{Store: store, API: github.Client{Repository: c.SourcePolicy.Repository, RepositoryID: c.SourcePolicy.RepositoryID}, Policy: c.SourcePolicy, Attempts: c.Attempts, Every: time.Duration(c.EverySeconds) * time.Second, OnMutation: wake, Report: report}
	}
	if err := observer(k, nil, nil).Validate(); err != nil {
		return nil, err
	}
	return func(ctx context.Context, store *storage.KernelStore, wake func(), report func(error)) error {
		return observer(store, wake, report).Run(ctx)
	}, nil
}
