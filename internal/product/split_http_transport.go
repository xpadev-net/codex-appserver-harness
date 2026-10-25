package product

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type SplitHTTPTransport struct {
	Store        *storage.KernelStore
	Executable   string
	SourcePolicy *IssueSourcePolicy
}

func (t SplitHTTPTransport) Start(ctx context.Context, id string) (storage.ProductSplitCreate, error) {
	var empty storage.ProductSplitCreate
	if t.Store == nil || !filepath.IsAbs(t.Executable) {
		return empty, errors.New("split journal and absolute trusted executable required")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	spec, err := WriteSplitHTTPSpec(ctx, t.Store, id, t.SourcePolicy)
	if err != nil {
		return empty, err
	}
	cmd := exec.Command(t.Executable, "_split-http", spec.Path())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := ctx.Err(); err != nil {
		return t.cleanupUnstarted(id, err)
	}
	if err := cmd.Start(); err != nil {
		return t.cleanupUnstarted(id, err)
	}
	identity := process.Identify(cmd.Process.Pid)
	if !identity.Valid() {
		return empty, process.ErrUnconfirmed
	}
	wait, cancel := context.WithTimeout(ctx, 35*time.Second)
	waitErr := t.waitResult(wait, id, identity)
	cancel()
	cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := process.StopGroup(cleanup, identity); err != nil {
		return empty, errors.Join(waitErr, err)
	}
	_ = cmd.Wait()
	job, err := t.Recover(cleanup, id)
	return job, errors.Join(waitErr, err)
}
func (t SplitHTTPTransport) cleanupUnstarted(id string, cause error) (storage.ProductSplitCreate, error) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	job, err := t.Recover(cleanup, id)
	return job, errors.Join(cause, err)
}
func (t SplitHTTPTransport) waitResult(ctx context.Context, id string, identity process.Identity) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := t.Store.ReadProductSplitCreate(ctx, id)
		if err != nil {
			return err
		}
		if job.HTTP != nil {
			if job.HTTP.Process != identity {
				return storage.ErrProductSplitHeld
			}
			if _, err := ReadSplitHTTPResult(ctx, t.Store, id); err == nil {
				return nil
			}
		}
		alive, err := process.GroupAlive(identity)
		if err != nil {
			return err
		}
		if !alive {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Recover performs no native request and launches nothing. It seals an
// unactivated delayed wrapper, or stops the saved group before recording only
// quiescence. Missing/invalid result files never grant a POST retry.
func (t SplitHTTPTransport) Recover(ctx context.Context, id string) (storage.ProductSplitCreate, error) {
	var empty storage.ProductSplitCreate
	if t.Store == nil {
		return empty, errors.New("split journal required")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	job, err := t.Store.ReadProductSplitCreate(ctx, id)
	if err != nil {
		return empty, err
	}
	if job.Stop != nil || job.Abort != nil {
		return job, nil
	}
	if job.HTTP == nil {
		sealed, err := t.Store.AbortUnactivatedProductSplitCreate(ctx, id, job.Value.OwnerID, job.Digest, "split-http-abort-"+job.Digest)
		if err == nil {
			return sealed, nil
		}
		if !errors.Is(err, storage.ErrProductSplitHeld) {
			return empty, err
		}
		// Activation may have won the atomic race. Authenticate and join that
		// physical owner instead; the failed abort is not no-send evidence.
		job, err = t.Store.ReadProductSplitCreate(ctx, id)
		if err != nil || job.HTTP == nil {
			return empty, errors.Join(storage.ErrProductSplitHeld, err)
		}
	}
	if err := process.StopGroup(ctx, job.HTTP.Process); err != nil {
		return empty, err
	}
	return t.Store.FinishProductSplitCreateHTTP(ctx, id, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, "split-http-stop-"+job.HTTP.SpecDigest)
}
