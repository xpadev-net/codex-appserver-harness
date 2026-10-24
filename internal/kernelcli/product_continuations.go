package kernelcli

import (
	"context"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Only the operator CLI calls these trusted metadata ports. A private file
// supplies explicit versions/evidence, never a model's source or policy grant.
func applyProductSourceFile(ctx context.Context, k *storage.KernelStore, command, file string) (any, error) {
	if file == "" {
		return nil, errors.New("--file with explicit target and source evidence required")
	}
	raw, err := readFile(file, 1<<20, true)
	if err != nil {
		return nil, err
	}
	var result any
	var apply func(context.Context) error
	switch command {
	case "continue-product":
		var request storage.ProductContinuationRequest
		if err := contract.DecodeStrictBounded(raw, &request, 1<<20); err != nil {
			return nil, err
		}
		apply = func(ctx context.Context) error {
			var err error
			result, err = k.RegisterProductContinuation(ctx, request)
			return err
		}
	case "adopt-product-source":
		var request storage.ProductSourceRequestAdoptionRequest
		if err := contract.DecodeStrictBounded(raw, &request, 1<<20); err != nil {
			return nil, err
		}
		apply = func(ctx context.Context) error {
			var err error
			result, err = k.AdoptProductSourceRequest(ctx, request)
			return err
		}
	default:
		return nil, errors.New("unsupported product source command")
	}
	if err := retryMutation(ctx, apply); err != nil {
		return nil, err
	}
	return result, nil
}
