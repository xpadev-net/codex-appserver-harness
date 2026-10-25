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
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type SplitHTTPResponse struct {
	ID     int64 `json:"id"`
	Number int   `json:"number"`
}
type SplitHTTPResult struct {
	Version        int                `json:"version"`
	CreationID     string             `json:"creation_id"`
	CreationDigest string             `json:"creation_digest"`
	SpecDigest     string             `json:"spec_digest"`
	Process        process.Identity   `json:"process"`
	StartedAt      time.Time          `json:"started_at"`
	FinishedAt     time.Time          `json:"finished_at"`
	Disposition    string             `json:"disposition"`
	Response       *SplitHTTPResponse `json:"response,omitempty"`
	Error          string             `json:"error,omitempty"`
}
type splitHTTPOutput struct{ buffer bytes.Buffer }

func (b *splitHTTPOutput) Write(p []byte) (int, error) {
	if len(p) > mergeHTTPBytes-b.buffer.Len() {
		return 0, errors.New("native split output exceeds limit")
	}
	return b.buffer.Write(p)
}

func (b *splitHTTPOutput) Bytes() []byte { return b.buffer.Bytes() }

func refreshSplitHTTPSource(ctx context.Context, k *storage.KernelStore, job storage.ProductSplitCreate, policy *IssueSourcePolicy, api ResourceAPI) (string, string, error) {
	if err := validateSplitHTTPPolicy(ctx, k, job, policy); err != nil {
		return "", "", err
	}
	w, err := k.Wave(ctx, job.Value.Request.Target.WaveID)
	if err != nil {
		return "", "", err
	}
	if w.Source == "direct" {
		return "", "", nil
	}
	observer := IssueSourceObserver{Store: k, API: api, Policy: *policy, Attempts: 1}
	if err := observer.Validate(); err != nil {
		return "", "", err
	}
	read := github.WithResourceReadBudget(ctx, 16<<20)
	update, acknowledged, err := observer.observe(ctx, read, &budgetResourceAPI{ResourceAPI: api}, w, 5*time.Second)
	if err != nil {
		return "", "", err
	}
	if !acknowledged || update.Hold != nil || update.Snapshot.Value.Status != "ready" {
		return "", "", storage.ErrProductSplitHeld
	}
	return update.Snapshot.ID, update.Snapshot.Digest, nil
}

// The wrapper performs bounded read-only identity/source GETs before the
// one-use activation. Native gh inherits this group's identity; it does not
// create a separate group. No automatic POST retry is possible.
func ExecuteSplitHTTP(ctx context.Context, path string) (SplitHTTPResult, error) {
	var empty SplitHTTPResult
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	spec, specDigest, err := ReadSplitHTTPSpec(path)
	if err != nil {
		return empty, err
	}
	k, err := storage.OpenExistingKernel(filepath.Join(spec.StoreRoot, "state.db"))
	if err != nil {
		return empty, err
	}
	defer k.Close()
	job, err := k.ReadProductSplitCreate(ctx, spec.CreationID)
	if err != nil {
		return empty, err
	}
	if job.Value.OwnerID != spec.OwnerID || job.Digest != spec.CreationDigest || job.State != "prepared" {
		return empty, storage.ErrProductSplitHeld
	}
	child := job.Value.Child.Value
	client := github.Client{Repository: child.Repository, RepositoryID: child.RepositoryID}
	raw, err := client.ResourceGET(github.WithResourceReadBudget(ctx, 4<<20), "")
	if err != nil {
		return empty, err
	}
	var repository Repository
	if json.Unmarshal(raw, &repository) != nil || repository.ID != child.RepositoryID || repository.Name != child.Repository {
		return empty, storage.ErrProductSplitHeld
	}
	body, err := json.Marshal(struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}{child.Child.Title, child.Body})
	if err != nil {
		return empty, err
	}
	sourceID, sourceDigest, err := refreshSplitHTTPSource(ctx, k, job, spec.SourcePolicy, liveAPI{client})
	if err != nil {
		return empty, err
	}
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, spec.OwnerID, job.Digest, sourceID, sourceDigest); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	claim, err := k.ActivateProductSplitCreateHTTP(ctx, job.ID, spec.OwnerID, job.Digest, specDigest)
	if err != nil {
		return empty, err
	}
	if !claim.Start {
		return empty, storage.ErrProductSplitHeld
	}
	job = claim.Creation
	result := SplitHTTPResult{Version: 1, CreationID: job.ID, CreationDigest: job.Digest, SpecDigest: specDigest, Process: job.HTTP.Process, StartedAt: job.HTTP.ActivatedAt, Disposition: "client_not_started"}
	finish := func(reason string) (SplitHTTPResult, error) {
		result.Error = reason
		result.FinishedAt = time.Now().UTC()
		if err := writeMergeHTTPFile(filepath.Join(spec.directory(), "result.json"), result); err != nil {
			return result, err
		}
		return result, nil
	}
	if ctx.Err() != nil {
		return finish("transport canceled before native request")
	}
	result.Disposition = "outcome_unknown"
	cmd := exec.CommandContext(ctx, "gh", "api", "--method", "POST", fmt.Sprintf("repos/%s/issues", child.Repository), "--input", "-")
	cmd.Stdin = bytes.NewReader(body)
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	var output splitHTTPOutput
	cmd.Stdout = &output
	runErr := cmd.Run()
	var response SplitHTTPResponse
	if runErr == nil && json.Unmarshal(output.Bytes(), &response) == nil && response.ID > 0 && response.Number > 0 {
		result.Disposition = "response_received"
		result.Response = &response
		return finish("")
	}
	return finish("native Issue creation outcome unknown")
}

// A private result is a response hint and audit data, never a stop proof or an
// authoritative Issue binding. Reconciliation must GET after physical stop.
func ReadSplitHTTPResult(ctx context.Context, k *storage.KernelStore, id string) (SplitHTTPResult, error) {
	var result SplitHTTPResult
	if k == nil {
		return result, errors.New("split creation journal required")
	}
	job, err := k.ReadProductSplitCreate(ctx, id)
	if err != nil {
		return result, err
	}
	if job.HTTP == nil {
		return result, storage.ErrProductSplitHeld
	}
	spec, digest, err := ReadSplitHTTPSpec(filepath.Join(k.Root, "split-http", id, "spec.json"))
	if err != nil {
		return result, err
	}
	if digest != job.HTTP.SpecDigest || spec.CreationID != id || spec.CreationDigest != job.Digest || spec.OwnerID != job.Value.OwnerID {
		return result, storage.ErrTaskEvidence
	}
	raw, err := readMergeHTTPFile(filepath.Join(spec.directory(), "result.json"))
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	now := time.Now().UTC()
	if k.Now != nil {
		now = k.Now().UTC()
	}
	if result.Version != 1 || result.CreationID != id || result.CreationDigest != job.Digest || result.SpecDigest != digest || result.Process != job.HTTP.Process || !result.StartedAt.Equal(job.HTTP.ActivatedAt) || result.FinishedAt.Before(result.StartedAt) || result.FinishedAt.After(now) || len(result.Error) > 8192 {
		return result, storage.ErrTaskEvidence
	}
	switch result.Disposition {
	case "client_not_started", "outcome_unknown":
		if result.Response != nil {
			return result, storage.ErrTaskEvidence
		}
	case "response_received":
		if result.Response == nil || result.Response.ID < 1 || result.Response.Number < 1 {
			return result, storage.ErrTaskEvidence
		}
	default:
		return result, storage.ErrTaskEvidence
	}
	return result, nil
}

func ServeSplitHTTP(ctx context.Context, path string) error {
	result, err := ExecuteSplitHTTP(ctx, path)
	// Retain the registered group leader until its supervisor stops the entire
	// group before reaping it. Persisted output alone is not physical completion.
	if result.Process.Valid() {
		for {
			time.Sleep(time.Hour)
		}
	}
	return err
}
