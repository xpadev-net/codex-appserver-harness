package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrCompletionUnconfirmed = errors.New("runner completion or process stopping is unconfirmed")

// Completion is measured by the trusted reconciler after process quiescence.
// Digests bind the exact private result file and execution log, not model prose.
type Completion struct {
	Result       Result `json:"result"`
	ResultDigest string `json:"result_digest"`
	LogDigest    string `json:"log_digest"`
	LogBytes     int64  `json:"log_bytes"`
}

func digestBytes(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }
func evidenceDigest(s string) bool {
	return regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(s)
}

// Validate checks source identity and status consistency. Only ReadStoppedResult
// supplies physical quiescence and the actual file/log observations.
func (c Completion) Validate(s Spec) error {
	r := c.Result
	if s.Validate() != nil || r.JobID != s.JobID || r.Candidate != s.Candidate || r.ProfileID != s.ProfileID || r.ProfileRevision != s.Profile.Revision || r.Started.IsZero() || r.Finished.IsZero() || r.Finished.Before(r.Started) || r.Environment == "" || r.LogPath != filepath.Join(s.Root, s.JobID, "output.log") || !evidenceDigest(c.ResultDigest) || c.LogBytes < 0 || c.LogBytes > 8<<20 || c.LogDigest != "" && !evidenceDigest(c.LogDigest) || c.LogBytes > 0 && c.LogDigest == "" || r.ResolutionEvidence != "" || r.ResolutionState != "" || !r.ResolvedAt.IsZero() {
		return ErrCompletionUnconfirmed
	}
	if r.TestedSHA != "" && r.TestedSHA != s.Candidate.SHA {
		return ErrCompletionUnconfirmed
	}
	switch r.Status {
	case "passed":
		if r.TestedSHA != s.Candidate.SHA || r.ExitCode == nil || *r.ExitCode != 0 || r.LogTruncated || c.LogBytes == 0 || c.LogDigest == "" || r.Reason != "" {
			return ErrCompletionUnconfirmed
		}
	case "failed", "timeout":
		if r.TestedSHA != s.Candidate.SHA || r.ExitCode == nil || c.LogDigest == "" {
			return ErrCompletionUnconfirmed
		}
	case "canceled", "infrastructure_failure":
	default:
		return ErrCompletionUnconfirmed
	}
	return nil
}

func readEvidenceFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, ErrCompletionUnconfirmed
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(b)) > limit {
		return nil, ErrCompletionUnconfirmed
	}
	return b, err
}

// ReadStoppedResult takes the lifecycle lock and verifies both wrapper and
// profile process groups. Even result.json alone is insufficient for release.
// Unknown and synthetic operator resolutions are never execution evidence.
func ReadStoppedResult(ctx context.Context, s Spec) (Completion, error) {
	var completion Completion
	if err := s.Validate(); err != nil {
		return completion, err
	}
	lock, err := jobLock(ctx, s, false)
	if err != nil {
		return completion, err
	}
	defer lock.Close()
	life, err := readLifecycle(s)
	if err != nil {
		return completion, err
	}
	if !life.Process.Valid() || life.Profile.PID > 0 && !life.Profile.Valid() {
		return completion, ErrCompletionUnconfirmed
	}
	for _, p := range []ProcessIdentity{life.Process, life.Profile} {
		if p.PID == 0 {
			continue
		}
		alive, err := groupAlive(p)
		if err != nil {
			return completion, err
		}
		if alive {
			return completion, ErrCompletionUnconfirmed
		}
	}
	dir := filepath.Join(s.Root, s.JobID)
	specBytes, err := readEvidenceFile(filepath.Join(dir, "spec.json"), 1<<20)
	if err != nil {
		return completion, err
	}
	// Execute writes this exact private snapshot with WriteJSON. Comparing the
	// complete encoding also supports legacy nil slices and rejects extra or
	// duplicate fields rather than decoding away changes to the saved spec.
	expectedSpec, err := json.MarshalIndent(s, "", "  ")
	if err != nil || !bytes.Equal(specBytes, expectedSpec) {
		return completion, fmt.Errorf("%w: saved runner spec differs", ErrCompletionUnconfirmed)
	}
	raw, err := readEvidenceFile(filepath.Join(dir, "result.json"), 64<<10)
	if err != nil {
		return completion, err
	}
	if err := contract.DecodeStrict(raw, &completion.Result); err != nil {
		return completion, err
	}
	if status := completion.Result.Status; (status == "passed" || status == "failed" || status == "timeout") && !life.Profile.Valid() {
		return completion, ErrCompletionUnconfirmed
	}
	completion.ResultDigest = digestBytes(raw)
	log, err := readEvidenceFile(filepath.Join(dir, "output.log"), 8<<20)
	if err != nil && !os.IsNotExist(err) {
		return completion, err
	}
	if err == nil {
		completion.LogDigest, completion.LogBytes = digestBytes(log), int64(len(log))
	}
	if err := completion.Validate(s); err != nil {
		return completion, err
	}
	if completion.Result.Status == "passed" && !regexp.MustCompile(s.Profile.SuccessPattern).Match(log) {
		return completion, ErrCompletionUnconfirmed
	}
	return completion, nil
}
