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

// MergeHTTPTransport supervises only already fenced registered HTTP intents.
// Preparation and the parent gate remain the controller's responsibility. The
// executable is trusted host configuration, never a model-selected command.
type MergeHTTPTransport struct {
	Store      *storage.KernelStore
	Executable string
}

// Start consumes a new immutable spec exactly once and waits for the trusted
// wrapper's outcome. It always stops the owned group before reaping its leader.
// Cancellation uses a separate bounded cleanup budget; it is not stop evidence.
func (t MergeHTTPTransport) Start(ctx context.Context, id string) (storage.ProductMerge, error) {
	var job storage.ProductMerge
	if t.Store == nil || !filepath.IsAbs(t.Executable) {
		return job, errors.New("merge store and absolute trusted executable required")
	}
	if err := ctx.Err(); err != nil {
		return job, err
	}
	spec, err := WriteMergeHTTPSpec(ctx, t.Store, id)
	if err != nil {
		// An existing/partial spec is never a new launch grant. Recovery owns it.
		return job, err
	}
	cmd := exec.Command(t.Executable, "_merge-http", spec.Path())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := ctx.Err(); err != nil {
		return t.cleanupUnstarted(id, err)
	}
	if err := cmd.Start(); err != nil {
		return t.cleanupUnstarted(id, err)
	}
	identity := process.Identify(cmd.Process.Pid)
	if !identity.Valid() {
		// Never signal a numeric/recycled PID or infer native absence here.
		return job, process.ErrUnconfirmed
	}
	waitCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
	err = t.waitResult(waitCtx, id, identity)
	cancel()
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCleanup()
	if stopErr := process.StopGroup(cleanup, identity); stopErr != nil {
		// Do not reap a still-needed anchor or release its journal ownership.
		return job, errors.Join(err, stopErr)
	}
	_ = cmd.Wait() // SIGKILL is expected; physical stopping was checked above.
	job, finishErr := t.Recover(cleanup, id)
	return job, errors.Join(err, finishErr)
}

func (t MergeHTTPTransport) cleanupUnstarted(id string, cause error) (storage.ProductMerge, error) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	job, err := t.Recover(cleanup, id)
	return job, errors.Join(cause, err)
}

func (t MergeHTTPTransport) waitResult(ctx context.Context, id string, identity process.Identity) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := t.Store.ReadProductMerge(ctx, id)
		if err != nil {
			return err
		}
		if job.HTTP != nil {
			if job.HTTP.Process != identity {
				return storage.ErrProductMergeHeld
			}
			if _, _, err := ReadMergeHTTPResult(ctx, t.Store, id); err == nil {
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

// Recover never launches or resends. It atomically seals an unactivated intent,
// or stops the saved group and authenticates its result. Missing/invalid private
// files become outcome_unknown even when no native write record is present.
// A saved terminal receipt is returned without touching a possibly reused PID.
func (t MergeHTTPTransport) Recover(ctx context.Context, id string) (storage.ProductMerge, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var job storage.ProductMerge
	if t.Store == nil {
		return job, errors.New("merge store required")
	}
	job, err := t.Store.ReadProductMerge(ctx, id)
	if err != nil {
		return job, err
	}
	if job.Value.HTTPProtocol != "registered_http_v1" {
		return job, storage.ErrProductMergeHeld
	}
	if job.Stop != nil || job.HTTPAbort != nil || job.Cancellation != nil {
		return job, nil
	}
	if job.State != "unknown" && job.State != "launching" {
		return job, storage.ErrProductMergeHeld
	}
	if job.HTTP == nil {
		sealed, err := t.Store.AbortUnactivatedProductMergeHTTP(ctx, id, job.Value.OwnerID, job.Digest, "merge-http-abort-"+job.Digest, "controller sealed unactivated HTTP admission")
		if err == nil {
			return sealed, nil
		}
		if !errors.Is(err, storage.ErrProductMergeHeld) {
			return job, err
		}
		// Activation may have won the transaction race. Authenticate it again;
		// the failed abort never authorizes release or a replacement launch.
		job, err = t.Store.ReadProductMerge(ctx, id)
		if err != nil || job.HTTP == nil {
			return job, errors.Join(storage.ErrProductMergeHeld, err)
		}
	}
	if err := process.StopGroup(ctx, job.HTTP.Process); err != nil {
		return job, err
	}
	// Read only after stopping: a newly created/partially written result must not
	// race its writer or be turned into positive no-request evidence.
	result, digest, readErr := ReadMergeHTTPResult(ctx, t.Store, id)
	proof := storage.ProductMergeStop{MergeID: id, StartedAt: job.HTTP.ActivatedAt}
	if readErr == nil {
		proof.ProofID = "merge-http-result-" + digest
		proof.Reason = "authenticated trusted HTTP result after group stop"
		proof.FinishedAt, proof.Disposition, proof.Response = result.FinishedAt, result.Disposition, result.Response
	} else {
		proof.ProofID = "merge-http-unknown-" + job.HTTP.SpecDigest
		proof.Reason = "HTTP group stopped without an authenticated transport result"
		proof.FinishedAt = time.Now().UTC()
		if t.Store.Now != nil {
			proof.FinishedAt = t.Store.Now().UTC()
		}
		proof.Disposition = "outcome_unknown"
	}
	return t.Store.FinishProductMergeHTTP(ctx, id, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof)
}
