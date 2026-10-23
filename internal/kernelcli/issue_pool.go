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

type IssuePoolConfig struct {
	SchemaVersion  int                       `json:"schema_version"`
	SourcePolicy   product.IssueSourcePolicy `json:"source_policy"`
	PriorityLabels []string                  `json:"priority_labels"`
	Attempts       int                       `json:"attempts"`
	EverySeconds   int                       `json:"every_seconds"`
}

func loadIssuePool(path string, k *storage.KernelStore, main Config) (daemonWorker, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := readFile(path, 64<<10, true)
	if err != nil {
		return nil, err
	}
	var c IssuePoolConfig
	if err := contract.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	if c.SchemaVersion != 1 || c.EverySeconds < 1 || c.EverySeconds > 3600 {
		return nil, errors.New("Issue pool schema 1 and every_seconds 1..3600 required")
	}
	pool := func(store *storage.KernelStore, wake func(), report func(error)) product.IssuePool {
		return product.IssuePool{
			Admitter:       product.IssueAdmitter{Store: store, API: github.Client{Repository: c.SourcePolicy.Repository, RepositoryID: c.SourcePolicy.RepositoryID}, Policy: c.SourcePolicy, SessionID: main.SessionID, Limits: main.Limits},
			PriorityLabels: c.PriorityLabels, Attempts: c.Attempts, Every: time.Duration(c.EverySeconds) * time.Second, OnMutation: wake, Report: report,
		}
	}
	if err := pool(k, nil, nil).Validate(); err != nil {
		return nil, err
	}
	return func(ctx context.Context, store *storage.KernelStore, wake func(), report func(error)) error {
		return pool(store, wake, report).Run(ctx)
	}, nil
}
