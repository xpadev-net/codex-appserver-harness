package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

var ErrProductFinalFetchHeld = errors.New("fixed final commit fetch requires owner reconciliation")

type ProductFinalFetchRequest struct {
	PlanID       string                  `json:"plan_id"`
	RepositoryID int64                   `json:"repository_id"`
	Source       workspace.RepositoryPin `json:"source"`
	Remote       string                  `json:"remote"`
}
type ProductFinalFetchValue struct {
	Request         ProductFinalFetchRequest  `json:"request"`
	PlanDigest      string                    `json:"plan_digest"`
	Target          contract.Target           `json:"target"`
	ParentSessionID string                    `json:"parent_session_id"`
	SHA             string                    `json:"sha"`
	Selection       ProductFinalPlanSelection `json:"selection"`
}
type ProductFinalFetchClaim struct {
	Selection ProductFinalPlanSelection `json:"selection"`
	ClaimedAt time.Time                 `json:"claimed_at"`
}
type ProductFinalFetchProof struct {
	ProofID     string                `json:"proof_id"`
	Observation workspace.CommitFetch `json:"observation"`
	Outcome     string                `json:"outcome"`
	Reason      string                `json:"reason"`
}
type ProductFinalFetch struct {
	ID            string                  `json:"id"`
	Value         ProductFinalFetchValue  `json:"value"`
	Digest        string                  `json:"digest"`
	State         string                  `json:"state"`
	CreatedAt     time.Time               `json:"created_at"`
	Claim         *ProductFinalFetchClaim `json:"claim,omitempty"`
	MayHaveSentAt time.Time               `json:"may_have_sent_at"`
	FinishedAt    time.Time               `json:"finished_at"`
	Proof         *ProductFinalFetchProof `json:"proof,omitempty"`
}

func finalFetchID(plan string, repo int64) string {
	return structureID("product-final-fetch-", plan, fmt.Sprint(repo))
}
func finalFetchPaths(source workspace.RepositoryPin) bool {
	for _, p := range []string{source.Repository, source.GitDir, source.CommonDir} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return false
		}
	}
	return true
}
func fetchGitReservation(ctx context.Context, q queryer, f ProductFinalFetch) (OperationReservation, error) {
	r, err := loadOperationReservationTx(ctx, q, f.ID)
	if err != nil {
		return r, err
	}
	if r.Kind != "git" || r.SessionID != f.Value.ParentSessionID || r.Request.Target != f.Value.Target || r.Request.Repository != f.Value.Request.Source.Repository || r.Request.Worktree != "" {
		return r, ErrTaskEvidence
	}
	if !slices.ContainsFunc(r.Resources, func(l ResourceLease) bool { return l.Resource == "repository_git:"+f.Value.Request.Source.CommonDir }) {
		return r, ErrTaskEvidence
	}
	return r, nil
}
func (k *KernelStore) finalFetchTx(ctx context.Context, q queryer, id string) (result ProductFinalFetch, retErr error) {
	var f, original ProductFinalFetch
	err := taskEvidenceRecord(ctx, q, "product_final_fetch", id, &f)
	if err == sql.ErrNoRows {
		var admitted bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_final_fetch_history','product_final_fetch_claim','product_final_fetch_send','product_final_fetch_terminal')) OR EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE run_id=?)", id, id).Scan(&admitted); err != nil {
			return f, err
		}
		if admitted {
			return f, ErrTaskEvidence
		}
		return f, sql.ErrNoRows
	}
	if err != nil {
		return f, err
	}
	// After the head was found, a missing dependent row is lost evidence.
	// It must never be returned as the "never admitted" sentinel to Reserve.
	defer func() {
		if errors.Is(retErr, sql.ErrNoRows) {
			retErr = errors.Join(ErrTaskEvidence, retErr)
		}
	}()
	digest, err := contentDigest(f.Value)
	if err != nil {
		return f, err
	}
	if f.ID != id || id != finalFetchID(f.Value.Request.PlanID, f.Value.Request.RepositoryID) || f.Digest != digest || f.CreatedAt.IsZero() || f.CreatedAt.After(k.clock()) || !fixedSHA(f.Value.SHA) || !finalFetchPaths(f.Value.Request.Source) {
		return f, ErrTaskEvidence
	}
	if err := taskEvidenceRecord(ctx, q, "product_final_fetch_history", id, &original); err != nil {
		return f, err
	}
	if original.ID != f.ID || original.Digest != f.Digest || !reflect.DeepEqual(original.Value, f.Value) || !original.CreatedAt.Equal(f.CreatedAt) || original.State != "prepared" || original.Claim != nil || original.Proof != nil || !original.MayHaveSentAt.IsZero() || !original.FinishedAt.IsZero() {
		return f, ErrTaskEvidence
	}
	plan, err := k.readProductFinalPlanTx(ctx, q, f.Value.Request.PlanID)
	if err != nil {
		return f, err
	}
	repo, err := finalRunnerRepository(plan, f.Value.Request.RepositoryID)
	if err != nil {
		return f, err
	}
	policy, err := loadProductVerificationPolicy(ctx, q, plan.Value.Target.WaveID, plan.Value.Target.PolicyRevision, repo.RepositoryID)
	if err != nil {
		return f, err
	}
	w, err := loadWave(ctx, q, f.Value.Target.WaveID)
	if err != nil {
		return f, err
	}
	if f.Value.PlanDigest != plan.Digest || f.Value.Target != plan.Value.Target || f.Value.SHA != repo.Candidate.SHA || f.Value.ParentSessionID != w.OrchestratorSessionID || f.Value.Request.Source.Repository != policy.Value.Repository || f.CreatedAt.Before(f.Value.Selection.ObservedAt) || f.CreatedAt.Sub(f.Value.Selection.ObservedAt) > 30*time.Second {
		return f, ErrTaskEvidence
	}
	if err := validateFinalSelection(ctx, q, plan, f.Value.Selection); err != nil {
		return f, err
	}
	r, err := fetchGitReservation(ctx, q, f)
	if err != nil {
		return f, err
	}
	if f.Claim != nil {
		var claim ProductFinalFetchClaim
		if err := taskEvidenceRecord(ctx, q, "product_final_fetch_claim", id, &claim); err != nil {
			return f, err
		}
		if !reflect.DeepEqual(claim, *f.Claim) || claim.ClaimedAt.Before(f.CreatedAt) || claim.ClaimedAt.After(k.clock()) || claim.ClaimedAt.Sub(claim.Selection.ObservedAt) > 30*time.Second || claim.Selection.ObservedAt.After(claim.ClaimedAt) {
			return f, ErrTaskEvidence
		}
		if err := validateFinalSelection(ctx, q, plan, claim.Selection); err != nil {
			return f, err
		}
		for _, head := range claim.Selection.Heads {
			if claim.ClaimedAt.Sub(head.ObservedAt) > 30*time.Second || head.ObservedAt.After(claim.ClaimedAt) {
				return f, ErrTaskEvidence
			}
		}
	} else {
		var claimExists bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_final_fetch_claim' AND id=?)", id).Scan(&claimExists); err != nil {
			return f, err
		}
		if claimExists {
			return f, ErrTaskEvidence
		}
	}
	var sent time.Time
	sentErr := taskEvidenceRecord(ctx, q, "product_final_fetch_send", id, &sent)
	if f.MayHaveSentAt.IsZero() {
		if sentErr == nil {
			return f, ErrTaskEvidence
		}
		if sentErr != sql.ErrNoRows {
			return f, sentErr
		}
	} else {
		if sentErr != nil {
			return f, sentErr
		}
		if f.Claim == nil || !sent.Equal(f.MayHaveSentAt) || sent.Before(f.Claim.ClaimedAt) || sent.After(k.clock()) {
			return f, ErrTaskEvidence
		}
	}
	if f.Proof == nil {
		var terminalExists bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_final_fetch_terminal' AND id=?)", id).Scan(&terminalExists); err != nil {
			return f, err
		}
		if terminalExists {
			return f, ErrTaskEvidence
		}
	}

	switch f.State {
	case "prepared":
		if f.Claim != nil || f.Proof != nil || !f.FinishedAt.IsZero() || r.State != "held" {
			return f, ErrTaskEvidence
		}
	case "launching":
		if f.Claim == nil || f.Proof != nil || !f.FinishedAt.IsZero() || r.State != "held" {
			return f, ErrTaskEvidence
		}
	case "available", "failed":
		if f.Claim == nil || f.Proof == nil || f.FinishedAt.Before(f.Claim.ClaimedAt) || f.FinishedAt.After(k.clock()) || r.State != "released" {
			return f, ErrTaskEvidence
		}
		var terminal ProductFinalFetch
		if err := taskEvidenceRecord(ctx, q, "product_final_fetch_terminal", id, &terminal); err != nil {
			return f, err
		}
		if !reflect.DeepEqual(terminal, f) || f.State != f.Proof.Outcome || validateFinalFetchProof(f, *f.Proof) != nil || r.Proof == nil || *r.Proof != (OperationReleaseProof{OperationID: id, ProofID: f.Proof.ProofID, Disposition: "process_stopped", Reason: f.Proof.Reason}) {
			return f, ErrTaskEvidence
		}
	default:
		return f, ErrTaskEvidence
	}
	return f, nil
}
func (k *KernelStore) ReadProductFinalFetch(ctx context.Context, id string) (ProductFinalFetch, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductFinalFetch{}, err
	}
	defer tx.Rollback()
	return k.finalFetchTx(ctx, tx, id)
}
func (k *KernelStore) ReserveProductFinalFetch(ctx context.Context, request ProductFinalFetchRequest) (ProductFinalFetch, error) {
	var f ProductFinalFetch
	if !finalFetchPaths(request.Source) {
		return f, ErrProductFinalFetchHeld
	}
	if err := workspace.ValidateCommitFetchRemote(request.Remote); err != nil {
		return f, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return f, err
	}
	defer tx.Rollback()
	plan, err := k.readProductFinalPlanTx(ctx, tx, request.PlanID)
	if err != nil {
		return f, err
	}
	repo, err := finalRunnerRepository(plan, request.RepositoryID)
	if err != nil {
		return f, err
	}
	policy, err := loadProductVerificationPolicy(ctx, tx, plan.Value.Target.WaveID, plan.Value.Target.PolicyRevision, repo.RepositoryID)
	if err != nil {
		return f, err
	}
	if request.Source.Repository != policy.Value.Repository {
		return f, ErrProductFinalFetchHeld
	}
	id := finalFetchID(plan.ID, repo.RepositoryID)
	if old, err := k.finalFetchTx(ctx, tx, id); err == nil {
		if !reflect.DeepEqual(old.Value.Request, request) {
			return f, ErrProductFinalFetchHeld
		}
		return old, tx.Commit()
	} else if err != sql.ErrNoRows {
		return f, err
	}
	selection, w, err := k.finalRunnerPreflightTx(ctx, tx, plan)
	if err != nil {
		return f, err
	}
	f = ProductFinalFetch{ID: id, Value: ProductFinalFetchValue{Request: request, PlanDigest: plan.Digest, Target: plan.Value.Target, ParentSessionID: w.OrchestratorSessionID, SHA: repo.Candidate.SHA, Selection: selection}, State: "prepared", CreatedAt: k.clock()}
	f.Digest, err = contentDigest(f.Value)
	if err != nil {
		return f, err
	}
	if _, err := k.reserveOperationCapacityTx(ctx, tx, OperationCapacityRequest{ID: id, Kind: "git", SessionID: w.OrchestratorSessionID, Target: w.Target(), Repository: request.Source.Repository}); err != nil {
		return f, err
	}
	if _, err := fetchGitReservation(ctx, tx, f); err != nil {
		return f, err
	}
	for _, kind := range []string{"product_final_fetch", "product_final_fetch_history"} {
		if err := kernelRecordTx(ctx, tx, kind, id, w.ID, f); err != nil {
			return f, err
		}
	}
	return f, tx.Commit()
}
func (k *KernelStore) finalFetchPreflightTx(ctx context.Context, tx *sql.Tx, f ProductFinalFetch) (ProductFinalPlanSelection, error) {
	plan, err := k.readProductFinalPlanTx(ctx, tx, f.Value.Request.PlanID)
	if err != nil {
		return ProductFinalPlanSelection{}, err
	}
	selection, _, err := k.finalRunnerPreflightTx(ctx, tx, plan)
	if err != nil {
		return selection, err
	}
	r, err := fetchGitReservation(ctx, tx, f)
	if err != nil {
		return selection, err
	}
	if r.State != "held" {
		return selection, ErrProductFinalFetchHeld
	}
	for _, lease := range r.Resources {
		var owner, state string
		var generation int64
		if err := tx.QueryRowContext(ctx, "SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", lease.Resource).Scan(&owner, &state, &generation); err != nil {
			return selection, err
		}
		if owner != f.ID || state != "held" || generation != lease.Generation {
			return selection, ErrProductFinalFetchHeld
		}
	}
	return selection, nil
}
func (k *KernelStore) ClaimProductFinalFetch(ctx context.Context, id, digest string) (ProductFinalFetch, bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductFinalFetch{}, false, err
	}
	defer tx.Rollback()
	f, err := k.finalFetchTx(ctx, tx, id)
	if err != nil {
		return f, false, err
	}
	if f.Digest != digest || f.State != "prepared" {
		return f, false, ErrProductFinalFetchHeld
	}
	selection, err := k.finalFetchPreflightTx(ctx, tx, f)
	if err != nil {
		return f, false, err
	}
	f.State = "launching"
	f.Claim = &ProductFinalFetchClaim{Selection: selection, ClaimedAt: k.clock()}
	if err := kernelRecordTx(ctx, tx, "product_final_fetch_claim", id, f.Value.Target.WaveID, *f.Claim); err != nil {
		return f, false, err
	}
	if err := kernelRecordTx(ctx, tx, "product_final_fetch", id, f.Value.Target.WaveID, f); err != nil {
		return f, false, err
	}
	if err := tx.Commit(); err != nil {
		return f, false, err
	}
	return f, true, nil
}
func (k *KernelStore) AuthorizeProductFinalFetch(ctx context.Context, id, digest string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	f, err := k.finalFetchTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if f.Digest != digest || f.State != "launching" || !f.MayHaveSentAt.IsZero() {
		return ErrProductFinalFetchHeld
	}
	if _, err := k.finalFetchPreflightTx(ctx, tx, f); err != nil {
		return err
	}
	f.MayHaveSentAt = k.clock()
	if err := kernelRecordTx(ctx, tx, "product_final_fetch_send", id, f.Value.Target.WaveID, f.MayHaveSentAt); err != nil {
		return err
	}
	if err := kernelRecordTx(ctx, tx, "product_final_fetch", id, f.Value.Target.WaveID, f); err != nil {
		return err
	}
	return tx.Commit()
}
func validateFinalFetchProof(f ProductFinalFetch, p ProductFinalFetchProof) error {
	o := p.Observation
	r := f.Value.Request
	if p.ProofID != f.ID+":stopped" || p.Reason == "" || len(p.Reason) > contract.MaxInline || o.Source != r.Source || o.Remote != r.Remote || o.SHA != f.Value.SHA || o.Attempted && f.MayHaveSentAt.IsZero() {
		return ErrTaskEvidence
	}
	if p.Outcome == "available" {
		if !o.Available {
			return ErrTaskEvidence
		}
	} else if p.Outcome != "failed" || o.Available {
		return ErrTaskEvidence
	}
	return nil
}
func (k *KernelStore) FinishProductFinalFetch(ctx context.Context, id, digest string, proof ProductFinalFetchProof) (ProductFinalFetch, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductFinalFetch{}, err
	}
	defer tx.Rollback()
	f, err := k.finalFetchTx(ctx, tx, id)
	if err != nil {
		return f, err
	}
	if f.Digest != digest || validateFinalFetchProof(f, proof) != nil {
		return f, ErrTaskEvidence
	}
	if f.Proof != nil {
		if !reflect.DeepEqual(*f.Proof, proof) {
			return f, ErrTaskEvidence
		}
		return f, tx.Commit()
	}
	if f.State != "launching" {
		return f, ErrProductFinalFetchHeld
	}
	f.State, f.Proof, f.FinishedAt = proof.Outcome, &proof, k.clock()
	if err := k.releaseOperationCapacityTx(ctx, tx, OperationReleaseProof{OperationID: id, ProofID: proof.ProofID, Disposition: "process_stopped", Reason: proof.Reason}); err != nil {
		return f, err
	}
	for _, kind := range []string{"product_final_fetch_terminal", "product_final_fetch"} {
		if err := kernelRecordTx(ctx, tx, kind, id, f.Value.Target.WaveID, f); err != nil {
			return f, err
		}
	}
	return f, tx.Commit()
}
