package kernelcli

import (
	"context"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type ProductWorkspaceFile struct {
	SchemaVersion int                             `json:"schema_version"`
	Request       storage.ProductWorkspaceRequest `json:"request"`
}

func prepareProductWorkspace(ctx context.Context, k *storage.KernelStore, path string) (storage.ProductWorkspace, error) {
	raw, err := readFile(path, 64<<10, true)
	if err != nil {
		return storage.ProductWorkspace{}, err
	}
	var file ProductWorkspaceFile
	if err := contract.DecodeStrict(raw, &file); err != nil {
		return storage.ProductWorkspace{}, err
	}
	if file.SchemaVersion != 1 {
		return storage.ProductWorkspace{}, errors.New("product workspace schema 1 required")
	}
	return (product.WorkspacePreparer{Store: k}).Prepare(ctx, file.Request)
}
