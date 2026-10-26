package product

import (
	"context"
	"encoding/json"
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

// The bounded split proof may contain the complete depth-three creation tree.
// Journal reads and the wrapper result wait must allow that metadata to decode;
// these limits do not extend source freshness or physical StopGroup deadlines.
const (
	splitHTTPExecutionTimeout = 2 * time.Minute
	splitHTTPResultWait       = splitHTTPExecutionTimeout + 10*time.Second
	splitHTTPRecoveryTimeout  = 2 * time.Minute
)

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
	_, specDigest, err := ReadSplitHTTPSpec(spec.Path())
	if err != nil {
		return t.cleanupUnstarted(id, err)
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
	wait, cancel := context.WithTimeout(ctx, splitHTTPResultWait)
	waitErr := t.waitResult(wait, spec, specDigest, identity)
	cancel()
	cleanup, stop := context.WithTimeout(context.Background(), splitHTTPRecoveryTimeout)
	defer stop()
	physicalStop, cancelStop := context.WithTimeout(cleanup, 5*time.Second)
	stopErr := process.StopGroup(physicalStop, identity)
	cancelStop()
	if stopErr != nil {
		return empty, errors.Join(waitErr, stopErr)
	}
	_ = cmd.Wait()
	job, err := t.Recover(cleanup, id)
	return job, errors.Join(waitErr, err)
}
func (t SplitHTTPTransport) cleanupUnstarted(id string, cause error) (storage.ProductSplitCreate, error) {
	cleanup, cancel := context.WithTimeout(context.Background(), splitHTTPRecoveryTimeout)
	defer cancel()
	job, err := t.Recover(cleanup, id)
	return job, errors.Join(cause, err)
}
func (t SplitHTTPTransport) waitResult(ctx context.Context, spec SplitHTTPSpec, specDigest string, identity process.Identity) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		// The result file can only request an early stop. It is not a journal,
		// Process, or Issue proof; Recover and the GET reconciler still perform
		// fresh canonical reads before recording any outcome.
		if splitHTTPResultHint(spec, specDigest, identity) {
			return nil
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

func splitHTTPResultHint(spec SplitHTTPSpec, specDigest string, identity process.Identity) bool {
	raw, err := readMergeHTTPFile(filepath.Join(spec.directory(), "result.json"))
	if err != nil {
		return false
	}
	var result SplitHTTPResult
	if json.Unmarshal(raw, &result) != nil || result.Version != 1 || result.CreationID != spec.CreationID || result.CreationDigest != spec.CreationDigest || result.SpecDigest != specDigest || result.Process != identity || result.StartedAt.IsZero() || result.FinishedAt.Before(result.StartedAt) {
		return false
	}
	switch result.Disposition {
	case "client_not_started", "outcome_unknown":
		return result.Response == nil
	case "response_received":
		return result.Response != nil && result.Response.ID > 0 && result.Response.Number > 0
	default:
		return false
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
	ctx, cancel := context.WithTimeout(ctx, splitHTTPRecoveryTimeout)
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
	physicalStop, cancelStop := context.WithTimeout(ctx, 5*time.Second)
	stopErr := process.StopGroup(physicalStop, job.HTTP.Process)
	cancelStop()
	if stopErr != nil {
		return empty, stopErr
	}
	return t.Store.FinishProductSplitCreateHTTP(ctx, id, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, "split-http-stop-"+job.HTTP.SpecDigest)
}
