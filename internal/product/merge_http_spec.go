package product

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

const mergeHTTPBytes = 1 << 20

type MergeHTTPSpec struct {
	Version     int    `json:"version"`
	StoreRoot   string `json:"store_root"`
	MergeID     string `json:"merge_id"`
	OwnerID     string `json:"owner_id"`
	MergeDigest string `json:"merge_digest"`
	GateDigest  string `json:"gate_digest"`
}

func (s MergeHTTPSpec) directory() string { return filepath.Join(s.StoreRoot, "merge-http", s.MergeID) }
func (s MergeHTTPSpec) Path() string      { return filepath.Join(s.directory(), "spec.json") }
func mergeHTTPDigest(raw []byte) string   { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func mergeHTTPFixedDigest(s string) bool {
	return len(s) == 64 && strings.Trim(s, "0123456789abcdef") == ""
}
func mergeHTTPDirectory(path string) error {
	actual, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || actual != path {
		return errors.New("canonical private merge HTTP directory required")
	}
	return sameUserDirectory(path, true)
}
func (s MergeHTTPSpec) validate() error {
	if s.Version != 1 || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,191}$`).MatchString(s.MergeID) || strings.TrimSpace(s.OwnerID) == "" || len(s.OwnerID) > 256 || strings.ContainsAny(s.OwnerID, "\x00\r\n") || !mergeHTTPFixedDigest(s.MergeDigest) || !mergeHTTPFixedDigest(s.GateDigest) {
		return errors.New("bounded versioned merge HTTP spec required")
	}
	for _, path := range []string{s.StoreRoot, filepath.Join(s.StoreRoot, "merge-http"), s.directory()} {
		if err := mergeHTTPDirectory(path); err != nil {
			return err
		}
	}
	return nil
}
func readMergeHTTPFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || info.Mode().Perm()&0077 != 0 || info.Size() > mergeHTTPBytes {
		return nil, errors.New("bounded private single-link merge HTTP file required")
	}
	raw, err := io.ReadAll(io.LimitReader(f, mergeHTTPBytes+1))
	if len(raw) > mergeHTTPBytes {
		return nil, errors.New("merge HTTP file exceeds limit")
	}
	return raw, err
}
func writeMergeHTTPFile(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if len(raw) > mergeHTTPBytes {
		return errors.New("merge HTTP record exceeds limit")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(raw)
	syncErr, closeErr := f.Sync(), f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// WriteMergeHTTPSpec derives the complete transport identity from the journal.
// It contains no model-selected command, URL, credentials or approval claim.
func WriteMergeHTTPSpec(ctx context.Context, k *storage.KernelStore, id string) (MergeHTTPSpec, error) {
	var empty MergeHTTPSpec
	if k == nil {
		return empty, errors.New("merge HTTP journal required")
	}
	job, err := k.ReadProductMerge(ctx, id)
	if err != nil {
		return empty, err
	}
	if job.Value.HTTPProtocol != "registered_http_v1" || job.State != "unknown" || job.HTTP != nil || job.HTTPAbort != nil {
		return empty, storage.ErrProductMergeHeld
	}
	if err := mergeHTTPDirectory(k.Root); err != nil {
		return empty, err
	}
	spec := MergeHTTPSpec{Version: 1, StoreRoot: k.Root, MergeID: job.ID, OwnerID: job.Value.OwnerID, MergeDigest: job.Digest, GateDigest: job.GateDigest}
	for _, path := range []string{filepath.Join(k.Root, "merge-http"), spec.directory()} {
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
func ReadMergeHTTPSpec(path string) (MergeHTTPSpec, string, error) {
	var spec MergeHTTPSpec
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
		return spec, "", errors.New("merge HTTP spec path differs from journal-derived location")
	}
	return spec, mergeHTTPDigest(raw), nil
}
