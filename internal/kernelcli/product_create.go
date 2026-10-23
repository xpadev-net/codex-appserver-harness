package kernelcli

import (
	"context"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// ProductDirectRequest is an operator-authored private source file. Stage and
// execution limits come from the command and saved controller configuration.
type ProductDirectRequest struct {
	SchemaVersion int                                 `json:"schema_version"`
	RequestID     string                              `json:"request_id"`
	Request       string                              `json:"request"`
	Criteria      []domain.WaveCriterion              `json:"criteria"`
	WorkItems     []storage.ProductWorkItemDefinition `json:"work_items"`
}

func createProduct(ctx context.Context, k *storage.KernelStore, c Config, file string) (domain.Wave, error) {
	if file == "" {
		return domain.Wave{}, errors.New("--file required")
	}
	raw, err := readFile(file, 12<<20, true)
	if err != nil {
		return domain.Wave{}, err
	}
	var input ProductDirectRequest
	if err := contract.DecodeStrictBounded(raw, &input, 12<<20); err != nil {
		return domain.Wave{}, err
	}
	if input.SchemaVersion != 1 {
		return domain.Wave{}, errors.New("version 1 product direct request required")
	}
	request := storage.ProductWaveRequest{
		Wave:      storage.WaveRequest{OrchestratorSessionID: c.SessionID, Source: "direct", SourceID: input.RequestID, OriginalRequest: input.Request, Criteria: input.Criteria, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: c.Limits}},
		WorkItems: input.WorkItems,
	}
	var wave domain.Wave
	err = retryMutation(ctx, func(attempt context.Context) error {
		var createErr error
		wave, createErr = k.CreateProductWave(attempt, request)
		return createErr
	})
	return wave, err
}
