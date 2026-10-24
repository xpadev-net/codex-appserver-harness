package product

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// MergeHTTPResult records raw transport outcome only. Its presence does not
// prove stopping of the wrapper/group and never releases a lease by itself.
type MergeHTTPResult struct {
	Version     int                           `json:"version"`
	MergeID     string                        `json:"merge_id"`
	MergeDigest string                        `json:"merge_digest"`
	SpecDigest  string                        `json:"spec_digest"`
	Process     process.Identity              `json:"process"`
	StartedAt   time.Time                     `json:"started_at"`
	FinishedAt  time.Time                     `json:"finished_at"`
	Disposition string                        `json:"disposition"`
	Response    *storage.ProductMergeResponse `json:"response,omitempty"`
	Error       string                        `json:"error,omitempty"`
}
type mergeHTTPOutput struct{ buffer bytes.Buffer }

func (b *mergeHTTPOutput) Write(p []byte) (int, error) {
	if len(p) > mergeHTTPBytes-b.buffer.Len() {
		return 0, errors.New("native merge output exceeds limit")
	}
	return b.buffer.Write(p)
}

func (b *mergeHTTPOutput) Bytes() []byte { return b.buffer.Bytes() }

// ExecuteMergeHTTP runs only in the trusted wrapper's own process group. gh
// inherits that SAME group so a restarted controller can stop/check every
// descendant through the registered wrapper identity. No model process runs.
func ExecuteMergeHTTP(ctx context.Context, path string) (MergeHTTPResult, error) {
	var empty MergeHTTPResult
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	spec, specDigest, err := ReadMergeHTTPSpec(path)
	if err != nil {
		return empty, err
	}
	k, err := storage.OpenExistingKernel(filepath.Join(spec.StoreRoot, "state.db"))
	if err != nil {
		return empty, err
	}
	defer k.Close()
	job, err := k.ReadProductMerge(ctx, spec.MergeID)
	if err != nil {
		return empty, err
	}
	if job.Digest != spec.MergeDigest || job.GateDigest != spec.GateDigest || job.Value.OwnerID != spec.OwnerID {
		return empty, storage.ErrProductMergeHeld
	}
	claim, err := k.ActivateProductMergeHTTP(ctx, job.ID, spec.OwnerID, job.Digest, specDigest)
	if err != nil {
		return empty, err
	}
	if !claim.Start {
		return empty, storage.ErrProductMergeHeld
	}
	job = claim.Merge
	result := MergeHTTPResult{Version: 1, MergeID: job.ID, MergeDigest: job.Digest, SpecDigest: specDigest, Process: job.HTTP.Process, StartedAt: job.HTTP.ActivatedAt, Disposition: "client_not_started"}
	finish := func(reason string) (MergeHTTPResult, error) {
		result.Error = reason
		result.FinishedAt = time.Now().UTC()
		if err := writeMergeHTTPFile(filepath.Join(spec.directory(), "result.json"), result); err != nil {
			return result, err
		}
		return result, nil
	}
	p := job.Value.Product.Value
	client := github.Client{Repository: p.Repository, RepositoryID: p.WorkItem.RepositoryID}
	raw, err := client.ResourceGET(github.WithResourceReadBudget(ctx, 4<<20), "")
	if err != nil {
		return finish("repository metadata read failed")
	}
	var repo Repository
	if json.Unmarshal(raw, &repo) != nil || repo.ID != p.WorkItem.RepositoryID || repo.Name != p.Repository || repo.DefaultBranch != p.BaseBranch {
		return finish("repository identity or default branch changed")
	}
	if ctx.Err() != nil {
		return finish("transport canceled before native request")
	}
	body, err := json.Marshal(struct {
		SHA    string `json:"sha"`
		Method string `json:"merge_method"`
	}{p.Candidate.SHA, job.Value.Method})
	if err != nil {
		return finish("native request encoding failed")
	}
	fresh, err := requestMergeHTTPAuthority(ctx, k, job, specDigest)
	if err != nil {
		return finish("fresh controller merge authorization unavailable")
	}
	if _, err := k.MarkProductMergeHTTPMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, specDigest, fresh); err != nil {
		return finish("native merge write fence held")
	}

	result.Disposition = "outcome_unknown"
	cmd := exec.CommandContext(ctx, "gh", "api", "--method", "PUT", fmt.Sprintf("repos/%s/pulls/%d/merge", p.Repository, p.PRNumber), "--input", "-")
	// No Setpgid here: all native descendants must remain in our registered group.
	cmd.Stdin = bytes.NewReader(body)
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	var output mergeHTTPOutput
	cmd.Stdout = &output
	runErr := cmd.Run()
	var response struct {
		Merged *bool  `json:"merged"`
		SHA    string `json:"sha"`
	}
	if json.Unmarshal(output.Bytes(), &response) == nil && response.Merged != nil && ((*response.Merged && len(response.SHA) == 40 && strings.Trim(response.SHA, "0123456789abcdef") == "") || (!*response.Merged && response.SHA == "")) {
		result.Disposition = "response_received"
		result.Response = &storage.ProductMergeResponse{RepositoryID: p.WorkItem.RepositoryID, PRNumber: p.PRNumber, ExpectedHeadSHA: p.Candidate.SHA, Method: job.Value.Method, Merged: *response.Merged, MergeSHA: response.SHA}
		return finish("")
	}
	if runErr != nil {
		return finish("native merge response unknown after command failure")
	}
	return finish("native merge response missing or invalid")
}

// ReadMergeHTTPResult authenticates private transport data, not physical stop.
// The caller must separately use FinishProductMergeHTTP, which checks the group.
func ReadMergeHTTPResult(ctx context.Context, k *storage.KernelStore, id string) (MergeHTTPResult, string, error) {
	var result MergeHTTPResult
	if k == nil {
		return result, "", errors.New("merge HTTP journal required")
	}
	job, err := k.ReadProductMerge(ctx, id)
	if err != nil {
		return result, "", err
	}
	if job.HTTP == nil {
		return result, "", storage.ErrProductMergeHeld
	}
	path := filepath.Join(k.Root, "merge-http", id, "spec.json")
	spec, digest, err := ReadMergeHTTPSpec(path)
	if err != nil {
		return result, "", err
	}
	if digest != job.HTTP.SpecDigest || spec.MergeID != id || spec.MergeDigest != job.Digest || spec.GateDigest != job.GateDigest || spec.OwnerID != job.Value.OwnerID {
		return result, "", storage.ErrTaskEvidence
	}
	raw, err := readMergeHTTPFile(filepath.Join(spec.directory(), "result.json"))
	if err != nil {
		return result, "", err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, "", err
	}
	now := time.Now().UTC()
	if k.Now != nil {
		now = k.Now().UTC()
	}
	if result.Version != 1 || result.MergeID != id || result.MergeDigest != job.Digest || result.SpecDigest != digest || result.Process != job.HTTP.Process || !result.StartedAt.Equal(job.HTTP.ActivatedAt) || result.FinishedAt.Before(result.StartedAt) || result.FinishedAt.After(now) || len(result.Error) > 8192 {
		return result, "", storage.ErrTaskEvidence
	}
	switch result.Disposition {
	case "client_not_started":
		if result.Response != nil || job.HTTPWrite != nil {
			return result, "", storage.ErrTaskEvidence
		}
	case "outcome_unknown":
		if result.Response != nil || job.HTTPWrite == nil {
			return result, "", storage.ErrTaskEvidence
		}
	case "response_received":
		if job.HTTPWrite == nil {
			return result, "", storage.ErrTaskEvidence
		}
		r := result.Response
		p := job.Value.Product.Value
		if r == nil || r.RepositoryID != p.WorkItem.RepositoryID || r.PRNumber != p.PRNumber || r.ExpectedHeadSHA != p.Candidate.SHA || r.Method != job.Value.Method || r.Merged && (len(r.MergeSHA) != 40 || strings.Trim(r.MergeSHA, "0123456789abcdef") != "") || !r.Merged && r.MergeSHA != "" {
			return result, "", storage.ErrTaskEvidence
		}
	default:
		return result, "", storage.ErrTaskEvidence
	}
	return result, mergeHTTPDigest(raw), nil
}

// ServeMergeHTTP retains the registered group leader for its controller's
// stop-before-reap protocol. Result persistence is not physical completion.
// SIGKILL via the pinned-group supervisor ends this internal holder; ordinary
// cancellation still bounds/cancels native work inside ExecuteMergeHTTP.
func ServeMergeHTTP(ctx context.Context, path string) error {
	result, err := ExecuteMergeHTTP(ctx, path)
	if result.Process.Valid() {
		for {
			time.Sleep(time.Hour)
		}
	}
	return err
}
