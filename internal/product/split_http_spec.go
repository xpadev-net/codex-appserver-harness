package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type SplitHTTPSpec struct {
	Version        int                `json:"version"`
	StoreRoot      string             `json:"store_root"`
	CreationID     string             `json:"creation_id"`
	OwnerID        string             `json:"owner_id"`
	CreationDigest string             `json:"creation_digest"`
	SourcePolicy   *IssueSourcePolicy `json:"source_policy,omitempty"`
}

func (s SplitHTTPSpec) directory() string {
	return filepath.Join(s.StoreRoot, "split-http", s.CreationID)
}
func (s SplitHTTPSpec) Path() string { return filepath.Join(s.directory(), "spec.json") }
func SplitHTTPProtectedPaths(root string) []string {
	return []string{filepath.Join(root, "split-http")}
}
func (s SplitHTTPSpec) validate() error {
	if s.Version != 1 || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,191}$`).MatchString(s.CreationID) || strings.TrimSpace(s.OwnerID) == "" || len(s.OwnerID) > 256 || strings.ContainsAny(s.OwnerID, "\x00\r\n") || !mergeHTTPFixedDigest(s.CreationDigest) {
		return errors.New("bounded split HTTP spec required")
	}
	if s.SourcePolicy != nil {
		if err := s.SourcePolicy.Validate(); err != nil {
			return err
		}
	}
	for _, path := range []string{s.StoreRoot, filepath.Join(s.StoreRoot, "split-http"), s.directory()} {
		if err := mergeHTTPDirectory(path); err != nil {
			return err
		}
	}
	return nil
}
func validateSplitHTTPPolicy(ctx context.Context, k *storage.KernelStore, job storage.ProductSplitCreate, policy *IssueSourcePolicy) error {
	w, err := k.Wave(ctx, job.Value.Request.Target.WaveID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if k.Now != nil {
		now = k.Now().UTC()
	}
	if w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !now.Before(w.Deadline) {
		return storage.ErrProductSplitHeld
	}
	if w.Target() != job.Value.Request.Target {
		return storage.ErrProductSplitHeld
	}
	if w.Source == "direct" {
		if policy != nil {
			return storage.ErrProductSplitHeld
		}
		return nil
	}
	child := job.Value.Child.Value
	if w.Source != "github" || policy == nil || policy.Validate() != nil || policy.Repository != child.Repository || policy.RepositoryID != child.RepositoryID {
		return storage.ErrProductSplitHeld
	}
	pinned, err := k.ReadProductSourcePolicy(ctx, w.Target())
	if err != nil {
		return err
	}
	if pinned != policy.Digest() {
		return storage.ErrProductSplitHeld
	}
	return nil
}

// A private immutable spec selects only the registered intent and host-pinned
// source policy, never a model command/URL/credential. Existing files are not
// overwritten and never authorize a second launch.
func WriteSplitHTTPSpec(ctx context.Context, k *storage.KernelStore, id string, policy *IssueSourcePolicy) (SplitHTTPSpec, error) {
	var empty SplitHTTPSpec
	if k == nil {
		return empty, errors.New("split creation journal required")
	}
	job, err := k.ReadProductSplitCreate(ctx, id)
	if err != nil {
		return empty, err
	}
	if job.State != "prepared" || job.HTTP != nil {
		return empty, storage.ErrProductSplitHeld
	}
	if err := validateSplitHTTPPolicy(ctx, k, job, policy); err != nil {
		return empty, err
	}
	if err := mergeHTTPDirectory(k.Root); err != nil {
		return empty, err
	}
	if policy != nil {
		copy := *policy
		policy = &copy
	}
	spec := SplitHTTPSpec{1, k.Root, job.ID, job.Value.OwnerID, job.Digest, policy}
	for _, path := range []string{filepath.Join(k.Root, "split-http"), spec.directory()} {
		if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
			return empty, err
		}
		if err := mergeHTTPDirectory(path); err != nil {
			return empty, err
		}
	}
	if err := spec.validate(); err != nil {
		return empty, err
	}
	if err := writeMergeHTTPFile(spec.Path(), spec); err != nil {
		return empty, err
	}
	return spec, nil
}
func ReadSplitHTTPSpec(path string) (SplitHTTPSpec, string, error) {
	var spec SplitHTTPSpec
	raw, err := readMergeHTTPFile(path)
	if err != nil {
		return spec, "", err
	}
	if err := contract.DecodeStrict(raw, &spec); err != nil {
		return spec, "", err
	}
	if err := spec.validate(); err != nil {
		return spec, "", err
	}
	if path != spec.Path() {
		return spec, "", errors.New("split HTTP spec path differs from registered private location")
	}
	return spec, mergeHTTPDigest(raw), nil
}
