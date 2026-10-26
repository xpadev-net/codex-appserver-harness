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
	"sync"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
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
		return "", "", fmt.Errorf("split source policy: %w", err)
	}
	w, err := k.Wave(ctx, job.Value.Request.Target.WaveID)
	if err != nil {
		return "", "", err
	}
	if job.Value.Ancestry != nil {
		ids := []string{job.Value.Request.ParentWorkItemID}
		if job.Value.Child.Value.Depth > 1 {
			ids, err = k.ReadProductSplitSourceLineage(ctx, job.Value.Request.Target, job.Value.Request.ParentWorkItemID)
			if err != nil {
				return "", "", err
			}
		}
		observer := SplitChildSourceObserver{Store: k, API: api, Policy: *policy, Attempts: 1}
		updates, acknowledged, err := observer.ObserveMany(ctx, job.Value.Request.Target, ids)
		if err != nil {
			return "", "", fmt.Errorf("split child source read: %w", err)
		}
		if !acknowledged || len(updates) != len(ids) {
			return "", "", storage.ErrProductSplitHeld
		}
		now := time.Now().UTC()
		if k.Now != nil {
			now = k.Now().UTC()
		}
		for _, update := range updates {
			if update.Snapshot.Value.Status != "ready" {
				return "", "", storage.ErrProductSplitHeld
			}
			if update.Snapshot.CheckedAt.IsZero() || now.Before(update.Snapshot.CheckedAt) || now.Sub(update.Snapshot.CheckedAt) > storage.ProductSplitActivationSourceMaxAge {
				return "", "", fmt.Errorf("split child source freshness: %w", storage.ErrProductSplitHeld)
			}
		}
	}
	sourceID, sourceDigest := "", ""
	if w.Source != "direct" {
		// ReadProductSplitSourceLineage above contains only registered split
		// child Issues. It cannot represent the Wave's original Issue source, so
		// re-fetch that root Issue after the child lineage reads and bind this
		// exact observation into the activation transaction below. Direct Waves
		// have no external root Issue to revalidate.
		observer := IssueSourceObserver{Store: k, API: api, Policy: *policy, Attempts: 1}
		if err := observer.Validate(); err != nil {
			return "", "", err
		}
		// Keep the Wave Issue's existing immediate-GET/gate contract: unlike
		// child sources its semantic snapshot has no replaceable CheckedAt.
		budget := github.WithResourceReadBudget(ctx, 16<<20)
		update, acknowledged, err := observer.observe(ctx, budget, &budgetResourceAPI{ResourceAPI: api}, w, 5*time.Second)
		if err != nil {
			return "", "", err
		}
		if !acknowledged || update.Hold != nil || update.Snapshot.Value.Status != "ready" {
			return "", "", storage.ErrProductSplitHeld
		}
		sourceID, sourceDigest = update.Snapshot.ID, update.Snapshot.Digest
	}
	return sourceID, sourceDigest, nil
}

// Join every observer, including source saves, before activation or returning
// an error. A failed/canceled read never leaves detached freshness updates.
func runSplitSourceReads(ctx context.Context, reads []func(context.Context) error) error {
	if len(reads) > 128 {
		return storage.ErrProductSplitHeld
	}
	if len(reads) == 1 {
		return reads[0](ctx)
	}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan func(context.Context) error, len(reads))
	for _, read := range reads {
		jobs <- read
	}
	close(jobs)
	results := make(chan error, len(reads))
	var joined sync.WaitGroup
	for i := 0; i < min(4, len(reads)); i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for read := range jobs {
				if work.Err() != nil {
					return
				}
				err := read(work)
				results <- err
				if err != nil {
					cancel()
					return
				}
			}
		}()
	}
	joined.Wait()
	close(results)
	count := 0
	var failed error
	for err := range results {
		count++
		failed = errors.Join(failed, err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if failed != nil {
		return failed
	}
	if count != len(reads) {
		return storage.ErrProductSplitHeld
	}
	return nil
}

// The wrapper performs bounded read-only identity/source GETs before the
// one-use activation. Native gh inherits this group's identity; it does not
// create a separate group. No automatic POST retry is possible.
func ExecuteSplitHTTP(ctx context.Context, path string) (SplitHTTPResult, error) {
	var empty SplitHTTPResult
	ctx, cancel := context.WithTimeout(ctx, splitHTTPExecutionTimeout)
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
	api := liveAPI{client}
	var claim storage.ProductSplitCreateClaim
	if job.Value.Ancestry != nil {
		w, waveErr := k.Wave(ctx, job.Value.Request.Target.WaveID)
		if waveErr != nil {
			return empty, waveErr
		}
		if w.Source == "direct" {
			if err := validateSplitHTTPPolicy(ctx, k, job, spec.SourcePolicy); err != nil {
				return empty, fmt.Errorf("split source policy: %w", err)
			}
			observer := SplitChildSourceObserver{Store: k, API: GitHubResourceAPI(storage.ProductTarget{Repository: job.Value.Child.Value.Repository, WorkItem: domain.WorkItem{RepositoryID: job.Value.Child.Value.RepositoryID}}), Policy: *spec.SourcePolicy, Attempts: 1}
			claim, err = k.ObserveAndActivateProductSplitCreateHTTPWithSources(ctx, job.ID, spec.OwnerID, job.Digest, specDigest, func(read context.Context, states []storage.ProductSplitChildSourceState) ([]storage.ProductSplitChildSourceObservation, error) {
				observations, acknowledged, err := observer.ObserveManyWithStates(read, job.Value.Request.Target, states)
				if err != nil {
					return nil, err
				}
				if !acknowledged || len(observations) != len(states) {
					return nil, storage.ErrProductSplitHeld
				}
				return observations, nil
			})
		} else {
			sourceID, sourceDigest, err := refreshSplitHTTPSource(ctx, k, job, spec.SourcePolicy, api)
			if err != nil {
				return empty, err
			}
			claim, err = k.ObserveAndActivateProductSplitCreateHTTP(ctx, job.ID, spec.OwnerID, job.Digest, specDigest, sourceID, sourceDigest)
		}
	} else {
		sourceID, sourceDigest, err := refreshSplitHTTPSource(ctx, k, job, spec.SourcePolicy, api)
		if err != nil {
			return empty, err
		}
		claim, err = k.ObserveAndActivateProductSplitCreateHTTP(ctx, job.ID, spec.OwnerID, job.Digest, specDigest, sourceID, sourceDigest)
	}
	if err != nil {
		return empty, fmt.Errorf("split creation activation: %w", err)
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
	return readSplitHTTPResultForCreation(k, job)
}

// The supplied job must come from the current canonical journal read. A result
// remains a hint; physical stopping and GET reconciliation use fresh authority.
func readSplitHTTPResultForCreation(k *storage.KernelStore, job storage.ProductSplitCreate) (SplitHTTPResult, error) {
	var result SplitHTTPResult
	id := job.ID
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
