package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func watchEvents(ctx context.Context, k *storage.KernelStore, root string, after int64, limit int, once bool, out io.Writer) error {
	if after < 0 || limit < 1 || limit > contract.MaxStatePage {
		return errors.New("watch cursor must be nonnegative and page size must be 1..32")
	}
	encoder := json.NewEncoder(out)
	if once {
		page, err := k.StateEvents(ctx, after, limit)
		if err != nil {
			return err
		}
		return encoder.Encode(page)
	}
	first := true
	last := after
	err := ipc.Stream(ctx, filepath.Join(root, ipc.SocketName), after, limit, func(page contract.StateEventPage) error {
		// Heartbeats retain the same cursor. Publish progress only after a complete
		// successful output write, so reconnecting never skips an undelivered page.
		if !first && len(page.Events) == 0 && page.Cursor == last {
			return nil
		}
		if err := encoder.Encode(page); err != nil {
			return err
		}
		first = false
		last = page.Cursor
		return nil
	})
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
