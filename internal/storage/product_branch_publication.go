package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

var ErrProductBranchHeld = errors.New("candidate branch publication requires reconciliation")

// Remote metadata comes from the trusted repository API, never a model field,
// Issue URL, origin alias or unverified Git configuration.
type ProductBranchRemote struct {
	RepositoryID  int64  `json:"repository_id"`
	Repository    string `json:"repository"`
	DefaultBranch string `json:"default_branch"`
	URL           string `json:"url"`
}

type ProductBranchValue struct {
	OperationID     string                           `json:"operation_id"`
	OwnerID         string                           `json:"owner_id"`
	Product         ProductTargetSnapshot            `json:"product"`
	ReadinessID     string                           `json:"readiness_id"`
	ReadinessDigest string                           `json:"readiness_digest"`
	Plan            ProductVerificationPlanReference `json:"plan"`
	Source          workspace.FrozenCheckout         `json:"source"`
	Remote          ProductBranchRemote              `json:"remote"`
	Branch          string                           `json:"branch"`
	SessionID       string                           `json:"session_id"`
}

type ProductBranchStop struct {
	PublicationID string                      `json:"publication_id"`
	ProofID       string                      `json:"proof_id"`
	Reason        string                      `json:"reason"`
	StartedAt     time.Time                   `json:"started_at"`
	FinishedAt    time.Time                   `json:"finished_at"`
	Observation   workspace.BranchPublication `json:"observation"`
}

type ProductBranchRemoteProof struct {
	PublicationID string    `json:"publication_id"`
	ProofID       string    `json:"proof_id"`
	StopProofID   string    `json:"stop_proof_id"`
	RemoteSHA     string    `json:"remote_sha"`
	ReadAt        time.Time `json:"read_at"`
}

type ProductBranchCancellation struct {
	ProofID string    `json:"proof_id"`
	Reason  string    `json:"reason"`
	At      time.Time `json:"at"`
}

type ProductBranchPublication struct {
	ID              string                     `json:"id"`
	Value           ProductBranchValue         `json:"value"`
	Digest          string                     `json:"digest"`
	State           string                     `json:"state"` // prepared, launching, stopped, published, canceled
	CreatedAt       time.Time                  `json:"created_at"`
	ClaimedAt       time.Time                  `json:"claimed_at"`
	OwnerProcess    process.Identity           `json:"owner_process"`
	OwnerInstanceID string                     `json:"owner_instance_id"`
	MayHaveSentAt   time.Time                  `json:"may_have_sent_at"`
	StoppedAt       time.Time                  `json:"stopped_at"`
	Stop            *ProductBranchStop         `json:"stop,omitempty"`
	Published       *ProductBranchRemoteProof  `json:"published,omitempty"`
	Cancellation    *ProductBranchCancellation `json:"cancellation,omitempty"`
}

type ProductBranchClaim struct {
	Publication ProductBranchPublication `json:"publication"`
	Start       bool                     `json:"start"`
}

type productBranchRecord struct {
	Publication ProductBranchPublication `json:"publication"`
	Digest      string                   `json:"digest"`
}

type productBranchPhysicalClaim struct {
	PublicationID   string           `json:"publication_id"`
	OwnerProcess    process.Identity `json:"owner_process"`
	OwnerInstanceID string           `json:"owner_instance_id"`
	ClaimedAt       time.Time        `json:"claimed_at"`
}

type branchOwnerEpoch struct {
	once sync.Once
	id   string
	err  error
}

func (k *KernelStore) branchOwnerInstanceID() (string, error) {
	k.branchOwner.once.Do(func() {
		var nonce [16]byte
		_, k.branchOwner.err = rand.Read(nonce[:])
		if k.branchOwner.err == nil {
			k.branchOwner.id = hex.EncodeToString(nonce[:])
		}
	})
	return k.branchOwner.id, k.branchOwner.err
}

func productBranchID(operationID string) string {
	return structureID("product-branch-", operationID, "v1")
}

func saveProductBranchTx(ctx context.Context, tx *sql.Tx, job ProductBranchPublication) error {
	digest, err := contentDigest(job)
	if err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "product_branch_publication", job.ID, job.Value.Product.Value.Target.WaveID, productBranchRecord{Publication: job, Digest: digest})
}

func loadProductBranch(ctx context.Context, q queryer, id string) (job ProductBranchPublication, err error) {
	var record productBranchRecord
	if err := taskEvidenceRecord(ctx, q, "product_branch_publication", id, &record); err != nil {
		return job, err
	}
	defer func() {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrTaskEvidence
		}
	}()
	job = record.Publication
	digest, err := contentDigest(job)
	if err != nil {
		return job, err
	}
	valueDigest, err := contentDigest(job.Value)
	if err != nil {
		return job, err
	}
	if record.Digest != digest || job.Digest != valueDigest || job.ID != id || id != productBranchID(job.Value.OperationID) || job.CreatedAt.IsZero() {
		return job, ErrTaskEvidence
	}
	var history ProductBranchValue
	if err := taskEvidenceRecord(ctx, q, "product_branch_history", id, &history); err != nil {
		return job, err
	}
	if !reflect.DeepEqual(history, job.Value) {
		return job, ErrTaskEvidence
	}
	plan, err := loadProductVerificationPlan(ctx, q, job.Value.Plan.ID)
	if err != nil {
		return job, err
	}
	seal, err := sealedProductEvidence(ctx, q, plan.Value.TaskID, plan.Value.RunID)
	if err != nil {
		return job, err
	}
	if plan.Digest != job.Value.Plan.Digest || plan.Value.Candidate != job.Value.Product.Value.Candidate || *seal.Proof.Frozen != job.Value.Source || job.Value.Source.SHA != plan.Value.Candidate.SHA || job.Value.Source.CWD != job.Value.Product.Value.WorkItem.Worktree || job.Value.Branch != job.Value.Product.Value.WorkItem.Branch {
		return job, ErrTaskEvidence
	}
	r, err := loadOperationReservationTx(ctx, q, id)
	if err != nil {
		return job, err
	}
	if r.Kind != "git" || r.Request.Target != job.Value.Product.Value.Target || r.SessionID != job.Value.SessionID || r.Request.Repository != job.Value.Source.Repository || r.Request.Worktree != job.Value.Source.CWD {
		return job, ErrTaskEvidence
	}
	if job.State == "prepared" || job.State == "canceled" {
		var used bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_branch_claim','product_branch_send_claim','product_branch_stop','product_branch_remote'))", id).Scan(&used); err != nil {
			return job, err
		}
		if used || job.OwnerProcess != (process.Identity{}) || job.OwnerInstanceID != "" || !job.MayHaveSentAt.IsZero() {
			return job, ErrTaskEvidence
		}
		if job.State == "prepared" {
			var canceled bool
			if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_branch_cancel' AND id=?)", id).Scan(&canceled); err != nil {
				return job, err
			}
			if canceled {
				return job, ErrTaskEvidence
			}
		}
	} else {
		var claimed productBranchPhysicalClaim
		if err := taskEvidenceRecord(ctx, q, "product_branch_claim", id, &claimed); err != nil {
			return job, err
		}
		if !job.OwnerProcess.Valid() || job.OwnerInstanceID == "" || claimed.PublicationID != id || claimed.OwnerProcess != job.OwnerProcess || claimed.OwnerInstanceID != job.OwnerInstanceID || !claimed.ClaimedAt.Equal(job.ClaimedAt) || job.ClaimedAt.Before(job.CreatedAt) {
			return job, ErrTaskEvidence
		}
		var sent time.Time
		err := taskEvidenceRecord(ctx, q, "product_branch_send_claim", id, &sent)
		if job.MayHaveSentAt.IsZero() {
			if !errors.Is(err, sql.ErrNoRows) {
				if err != nil {
					return job, err
				}
				return job, ErrTaskEvidence
			}
		} else if err != nil {
			return job, err
		} else if !sent.Equal(job.MayHaveSentAt) || sent.Before(job.ClaimedAt) {
			return job, ErrTaskEvidence
		}
	}
	switch job.State {
	case "prepared", "launching":
		if (job.State == "prepared") != job.ClaimedAt.IsZero() || job.Stop != nil || job.Published != nil || !job.StoppedAt.IsZero() || r.State != "held" {
			return job, ErrTaskEvidence
		}
	case "stopped", "published":
		if job.Stop == nil || job.StoppedAt.IsZero() || r.State != "released" || r.Proof == nil || *r.Proof != productBranchReleaseProof(job) {
			return job, ErrTaskEvidence
		}
		var stop ProductBranchStop
		if err := taskEvidenceRecord(ctx, q, "product_branch_stop", id, &stop); err != nil {
			return job, err
		}
		if !reflect.DeepEqual(stop, *job.Stop) || validateProductBranchStop(job, stop) != nil {
			return job, ErrTaskEvidence
		}
		if (job.State == "published") != (job.Published != nil) {
			return job, ErrTaskEvidence
		}
		if job.Published != nil {
			var observed ProductBranchRemoteProof
			if err := taskEvidenceRecord(ctx, q, "product_branch_remote", id, &observed); err != nil {
				return job, err
			}
			if observed != *job.Published || observed.PublicationID != job.ID || observed.ProofID == "" || observed.StopProofID != stop.ProofID || observed.RemoteSHA != job.Value.Source.SHA || observed.ReadAt.IsZero() || observed.ReadAt.Before(job.CreatedAt) {
				return job, ErrTaskEvidence
			}
		}
	case "canceled":
		if !job.ClaimedAt.IsZero() || job.Stop != nil || job.Published != nil || !job.StoppedAt.IsZero() || job.Cancellation == nil || r.State != "released" || r.Proof == nil || *r.Proof != productBranchCancellationProof(job) {
			return job, ErrTaskEvidence
		}
		var canceled ProductBranchCancellation
		if err := taskEvidenceRecord(ctx, q, "product_branch_cancel", id, &canceled); err != nil {
			return job, err
		}
		if canceled != *job.Cancellation || canceled.ProofID != job.ID+":not-started" || canceled.At.Before(job.CreatedAt) || strings.TrimSpace(canceled.Reason) == "" {
			return job, ErrTaskEvidence
		}
	default:
		return job, ErrTaskEvidence
	}
	if job.State != "canceled" && job.Cancellation != nil {
		return job, ErrTaskEvidence
	}
	return job, nil
}

func (k *KernelStore) ProductBranchPublication(ctx context.Context, operationID string) (ProductBranchPublication, error) {
	return loadProductBranch(ctx, k.DB, productBranchID(operationID))
}

func (k *KernelStore) branchDispatchTx(ctx context.Context, tx *sql.Tx, operationID, ownerID string) (PRDispatchClaim, ProductPublicationReadiness, error) {
	var claim PRDispatchClaim
	var ready ProductPublicationReadiness
	intent, state, err := prIntentTx(ctx, tx, operationID)
	if err != nil {
		return claim, ready, err
	}
	if state != "accepted" || intent.Source.Name != "pr_publish" {
		return claim, ready, ErrProductBranchHeld
	}
	if err := taskEvidenceRecord(ctx, tx, "product_pr_dispatch", operationID, &claim); err != nil {
		return claim, ready, err
	}
	if claim.State != "prepared" || claim.OwnerID != ownerID {
		return claim, ready, ErrProductBranchHeld
	}
	if _, _, err := k.preflightPRTx(ctx, tx, intent); err != nil {
		return claim, ready, err
	}
	if err := validatePRClaimTx(ctx, tx, intent, claim); err != nil {
		return claim, ready, err
	}
	p := claim.Product.Value
	ready, err = loadProductPublicationReadiness(ctx, tx, p.Target, p.WorkItem.ID)
	if err != nil {
		return claim, ready, err
	}
	quality, err := k.productQualityTx(ctx, tx, p.Target, p.WorkItem.ID)
	if err != nil {
		return claim, ready, err
	}
	if !reflect.DeepEqual(ready.Value.Product, claim.Product) || !reflect.DeepEqual(ready.Value.Quality, quality) {
		return claim, ready, ErrProductBranchHeld
	}
	return claim, ready, nil
}

// ReserveProductBranchPublication derives the checkout from canonical machine
// evidence and a stopped seal, then binds Git capacity and a one-use intent to
// the existing PR mutation claim. It executes no command or HTTP request.
func (k *KernelStore) ReserveProductBranchPublication(ctx context.Context, operationID, ownerID string, remote ProductBranchRemote) (ProductBranchPublication, error) {
	var job ProductBranchPublication
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	claim, ready, err := k.branchDispatchTx(ctx, tx, operationID, ownerID)
	if err != nil {
		return job, err
	}
	p := claim.Product.Value
	u, err := url.Parse(remote.URL)
	if err != nil || len(remote.URL) > 8192 || strings.ContainsAny(remote.URL, "\x00\r\n\t ") || remote.RepositoryID != p.WorkItem.RepositoryID || remote.Repository != p.Repository || remote.DefaultBranch != p.BaseBranch || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.TrimSuffix(u.Path, ".git") != "/"+p.Repository {
		return job, ErrProductBranchHeld
	}
	ids := make([]string, 0, len(ready.Value.Quality.Value.Verifications))
	for id := range ready.Value.Quality.Value.Verifications {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var value ProductBranchValue
	for _, id := range ids {
		plan, err := loadProductVerificationPlan(ctx, tx, id)
		if err != nil {
			return job, err
		}
		seal, err := sealedProductEvidence(ctx, tx, plan.Value.TaskID, plan.Value.RunID)
		if err != nil {
			return job, err
		}
		if plan.Value.Candidate == p.Candidate && seal.Proof.Frozen.CWD == p.WorkItem.Worktree {
			value = ProductBranchValue{OperationID: operationID, OwnerID: ownerID, Product: claim.Product, ReadinessID: ready.ID, ReadinessDigest: ready.Digest, Plan: ProductVerificationPlanReference{ID: plan.ID, Digest: plan.Digest}, Source: *seal.Proof.Frozen, Remote: remote, Branch: p.WorkItem.Branch}
			break
		}
	}
	if value.OperationID == "" {
		return job, ErrProductBranchHeld
	}
	w, err := loadWave(ctx, tx, p.Target.WaveID)
	if err != nil {
		return job, err
	}
	value.SessionID = w.OrchestratorSessionID
	id := productBranchID(operationID)
	job, err = loadProductBranch(ctx, tx, id)
	if err == nil {
		if !reflect.DeepEqual(value, job.Value) {
			return job, ErrProductBranchHeld
		}
		return job, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return job, err
	}
	var admitted bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_branch_history','product_branch_claim','product_branch_send_claim','product_branch_stop','product_branch_remote','product_branch_cancel')) OR EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE run_id=?)", id, id).Scan(&admitted); err != nil {
		return job, err
	}
	if admitted {
		return job, ErrTaskEvidence
	}
	r, err := k.reserveOperationCapacityTx(ctx, tx, OperationCapacityRequest{ID: id, Kind: "git", SessionID: value.SessionID, Target: p.Target, Repository: value.Source.Repository, Worktree: value.Source.CWD})
	if err != nil {
		return job, err
	}
	if r.State != "held" {
		return job, ErrProductBranchHeld
	}
	digest, err := contentDigest(value)
	if err != nil {
		return job, err
	}
	job = ProductBranchPublication{ID: id, Value: value, Digest: digest, State: "prepared", CreatedAt: k.clock()}
	if err := kernelRecordTx(ctx, tx, "product_branch_history", id, p.Target.WaveID, value); err != nil {
		return job, err
	}
	if err := saveProductBranchTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}

func (k *KernelStore) ClaimProductBranchPublication(ctx context.Context, operationID, ownerID, digest string) (ProductBranchClaim, error) {
	var claim ProductBranchClaim
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback()
	job, err := loadProductBranch(ctx, tx, productBranchID(operationID))
	if err != nil {
		return claim, err
	}
	claim.Publication = job
	if job.Value.OwnerID != ownerID || job.Digest != digest {
		return claim, ErrProductBranchHeld
	}
	if job.State != "prepared" {
		return claim, tx.Commit()
	}
	pr, ready, err := k.branchDispatchTx(ctx, tx, operationID, ownerID)
	if err != nil {
		return claim, err
	}
	if !reflect.DeepEqual(pr.Product, job.Value.Product) || ready.ID != job.Value.ReadinessID || ready.Digest != job.Value.ReadinessDigest {
		return claim, ErrProductBranchHeld
	}
	if err := k.branchGitOwnerTx(ctx, tx, job); err != nil {
		return claim, err
	}
	job.State, job.ClaimedAt = "launching", k.clock()
	job.OwnerProcess = process.Identify(os.Getpid())
	job.OwnerInstanceID, err = k.branchOwnerInstanceID()
	if err != nil {
		return claim, err
	}
	if !job.OwnerProcess.Valid() {
		return claim, ErrProductBranchHeld
	}
	if err := kernelRecordTx(ctx, tx, "product_branch_claim", job.ID, job.Value.Product.Value.Target.WaveID, productBranchPhysicalClaim{PublicationID: job.ID, OwnerProcess: job.OwnerProcess, OwnerInstanceID: job.OwnerInstanceID, ClaimedAt: job.ClaimedAt}); err != nil {
		return claim, err
	}
	if err := saveProductBranchTx(ctx, tx, job); err != nil {
		return claim, err
	}
	claim.Publication = job
	if err := tx.Commit(); err != nil {
		return claim, err
	}
	claim.Start = true
	return claim, nil
}

// MarkProductBranchMayHaveSent is the separate one-use remote-write fence,
// after Claim permitted the Git owner to start its read-only preflight.
func (k *KernelStore) MarkProductBranchMayHaveSent(ctx context.Context, operationID, ownerID, digest string) (ProductBranchPublication, error) {
	var job ProductBranchPublication
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	job, err = loadProductBranch(ctx, tx, productBranchID(operationID))
	if err != nil {
		return job, err
	}
	instanceID, err := k.branchOwnerInstanceID()
	if err != nil {
		return job, err
	}
	if job.Value.OwnerID != ownerID || job.Digest != digest || job.State != "launching" || !job.MayHaveSentAt.IsZero() || job.OwnerProcess != process.Identify(os.Getpid()) || job.OwnerInstanceID != instanceID {
		return job, ErrProductBranchHeld
	}
	pr, ready, err := k.branchDispatchTx(ctx, tx, operationID, ownerID)
	if err != nil {
		return job, err
	}
	if !reflect.DeepEqual(pr.Product, job.Value.Product) || ready.ID != job.Value.ReadinessID || ready.Digest != job.Value.ReadinessDigest {
		return job, ErrProductBranchHeld
	}
	if err := k.branchGitOwnerTx(ctx, tx, job); err != nil {
		return job, err
	}
	job.MayHaveSentAt = k.clock()
	if err := kernelRecordTx(ctx, tx, "product_branch_send_claim", job.ID, job.Value.Product.Value.Target.WaveID, job.MayHaveSentAt); err != nil {
		return job, err
	}
	if err := saveProductBranchTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}

func (k *KernelStore) branchGitOwnerTx(ctx context.Context, tx *sql.Tx, job ProductBranchPublication) error {
	r, err := loadOperationReservationTx(ctx, tx, job.ID)
	if err != nil {
		return err
	}
	if r.State != "held" {
		return ErrProductBranchHeld
	}
	if err := operationCapacityOwnerTx(ctx, tx, r.Request, k.clock()); err != nil {
		return err
	}
	for _, lease := range r.Resources {
		var owner, state string
		var generation int64
		if err := tx.QueryRowContext(ctx, "SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", lease.Resource).Scan(&owner, &state, &generation); err != nil {
			return err
		}
		if owner != job.ID || state != "held" || generation != lease.Generation {
			return ErrProductBranchHeld
		}
	}
	return nil
}
