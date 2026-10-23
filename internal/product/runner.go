package product

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Runner owns detached trusted wrappers, not model turns. A wrapper writes only
// private files; its completion hint never authorizes a DB update by itself.
type Runner struct {
	Store      *storage.KernelStore
	Executable string
	SocketPath string
}

func (r Runner) Start(ctx context.Context, planID, profileID string, runtime storage.ProductRunnerRuntime) (storage.ProductRunnerJob, error) {
	if r.Store == nil || !filepath.IsAbs(r.Executable) {
		return storage.ProductRunnerJob{}, errors.New("configured store and absolute trusted runner executable required")
	}
	job, err := r.Store.ReserveProductRunnerJob(ctx, planID, profileID, runtime)
	if err != nil {
		return job, err
	}
	if job.State == "completed" {
		return job, nil
	}
	claim, err := r.Store.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
	if err != nil {
		return job, err
	}
	if !claim.Start {
		return claim.Job, storage.ErrProductRunnerHeld
	}
	job = claim.Job
	// After claiming, every failure is retained for reconciliation. An error
	// from LaunchWithWake can occur after process Start and is not absence proof.
	if err := sameUserDirectory(r.Store.Root, true); err != nil {
		return job, err
	}
	s := job.Value.Spec
	if err := sameUserDirectory(s.Repository, false); err != nil {
		return job, err
	}
	if err := os.MkdirAll(s.Root, 0700); err != nil {
		return job, err
	}
	if err := sameUserDirectory(s.Root, true); err != nil {
		return job, err
	}
	path := filepath.Join(s.Root, job.ID+".launch.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return job, err
	}
	encodeErr := json.NewEncoder(f).Encode(s)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(encodeErr, syncErr, closeErr); err != nil {
		return job, err
	}
	_, err = runner.LaunchWithWake(ctx, r.Executable, path, r.SocketPath)
	return job, err
}

func (r Runner) Reconcile(ctx context.Context, id string) (storage.ProductRunnerJob, error) {
	if r.Store == nil {
		return storage.ProductRunnerJob{}, errors.New("configured product runner store required")
	}
	job, err := r.Store.ReadProductRunnerJob(ctx, id)
	if err != nil || job.State == "completed" {
		return job, err
	}
	if job.State != "launching" {
		return job, storage.ErrProductRunnerHeld
	}
	if err := sameUserDirectory(job.Value.Spec.Root, true); err != nil {
		return job, err
	}
	if err := sameUserDirectory(filepath.Join(job.Value.Spec.Root, job.ID), true); err != nil {
		return job, err
	}
	completion, err := runner.ReadStoppedResult(ctx, job.Value.Spec)
	if err != nil {
		return job, err
	}
	return r.Store.FinishProductRunnerJob(ctx, job.ID, job.Digest, completion)
}

// RequestStop asks the independent wrapper to cancel its own job. The marker
// and a successful return are not physical stopping proof and release nothing.
func (r Runner) RequestStop(ctx context.Context, id string) error {
	if r.Store == nil {
		return errors.New("configured product runner store required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	job, err := r.Store.ReadProductRunnerJob(ctx, id)
	if err != nil || job.State == "completed" {
		return err
	}
	if job.State != "launching" {
		return storage.ErrProductRunnerHeld
	}
	if err := sameUserDirectory(job.Value.Spec.Root, true); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(job.Value.Spec.Root, id+".cancel"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
