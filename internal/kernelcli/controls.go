package kernelcli

import (
	"context"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func applyControlFile(ctx context.Context, k *storage.KernelStore, command, file string) (any, error) {
	if file == "" {
		return nil, errors.New("--file with explicit target versions, key and reason required")
	}
	raw, err := readFile(file, 1<<20, false)
	if err != nil {
		return nil, err
	}
	var result any
	var apply func(context.Context) error
	switch command {
	case "pause", "resume", "cancel":
		var request storage.OperatorControl
		if err = contract.DecodeStrict(raw, &request); err != nil {
			return nil, err
		}
		if request.Action != command {
			return nil, errors.New("operator file action must match the command")
		}
		apply = func(ctx context.Context) error {
			var err error
			result, err = k.ApplyOperatorControl(ctx, request)
			return err
		}
	case "steer":
		var request storage.WaveDirectionRequest
		if err = contract.DecodeStrict(raw, &request); err != nil {
			return nil, err
		}
		apply = func(ctx context.Context) error {
			var err error
			result, err = k.RequestWaveDirection(ctx, request)
			return err
		}
	case "interrupt":
		var request storage.SessionInterruptRequest
		if err = contract.DecodeStrict(raw, &request); err != nil {
			return nil, err
		}
		apply = func(ctx context.Context) error {
			var err error
			result, err = k.RequestSessionInterrupt(ctx, request)
			return err
		}
	default:
		return nil, errors.New("unknown operator command")
	}
	if err = retryMutation(ctx, apply); err != nil {
		return nil, err
	}
	return result, nil
}
