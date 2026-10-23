package kernelcli

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// PRObserverConfig is an explicit host opt-in. It grants no model tool or
// mutation capability and uses the host's gh read credentials.
type PRObserverConfig struct {
	SchemaVersion int `json:"schema_version"`
	Workers       int `json:"workers"`
	EverySeconds  int `json:"every_seconds"`
}

func loadPRObserver(path string) (daemonWorker, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := readFile(path, 4096, true)
	if err != nil {
		return nil, err
	}
	var c PRObserverConfig
	if err = contract.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	if c.SchemaVersion != 1 || c.Workers < 1 || c.Workers > 8 || c.EverySeconds < 1 || c.EverySeconds > 3600 {
		return nil, errors.New("PR observer schema 1, workers 1..8 and every_seconds 1..3600 required")
	}
	return func(ctx context.Context, k *storage.KernelStore, wake func(), report func(error)) error {
		return (product.Observer{Store: k, API: product.GitHubResourceAPI, Workers: c.Workers, Every: time.Duration(c.EverySeconds) * time.Second, OnMutation: wake, Report: report}).Run(ctx)
	}, nil
}
