package kernelcli

import (
	"context"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type IssueImportRequest struct {
	SchemaVersion int                       `json:"schema_version"`
	IssueNumber   int                       `json:"issue_number"`
	SourcePolicy  product.IssueSourcePolicy `json:"source_policy"`
}

func importIssue(ctx context.Context, k *storage.KernelStore, c Config, file string) (domain.Wave, error) {
	if file == "" {
		return domain.Wave{}, errors.New("--file required")
	}
	raw, err := readFile(file, 64<<10, true)
	if err != nil {
		return domain.Wave{}, err
	}
	var input IssueImportRequest
	if err := contract.DecodeStrict(raw, &input); err != nil {
		return domain.Wave{}, err
	}
	if input.SchemaVersion != 1 || input.IssueNumber < 1 {
		return domain.Wave{}, errors.New("version 1 Issue import and positive issue_number required")
	}
	admitter := product.IssueAdmitter{Store: k, API: github.Client{Repository: input.SourcePolicy.Repository, RepositoryID: input.SourcePolicy.RepositoryID}, Policy: input.SourcePolicy, SessionID: c.SessionID, Limits: c.Limits}
	var wave domain.Wave
	err = retryMutation(ctx, func(attempt context.Context) error {
		var admitErr error
		wave, admitErr = admitter.Admit(attempt, input.IssueNumber)
		return admitErr
	})
	return wave, err
}
