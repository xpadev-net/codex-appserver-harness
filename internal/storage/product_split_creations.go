package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

type ProductSplitCreateValue struct {
	Request          ProductSplitChildPlanRequest `json:"request"`
	ParentHeadID     string                       `json:"parent_head_id"`
	AllocationID     string                       `json:"allocation_id"`
	AllocationDigest string                       `json:"allocation_digest"`
	Child            ProductSplitChildIntent      `json:"child"`
	OwnerID          string                       `json:"owner_id"`
	CreatedAt        time.Time                    `json:"created_at"`
}
type ProductSplitCreateOwner struct {
	GateDigest  string           `json:"gate_digest"`
	Process     process.Identity `json:"process"`
	SpecDigest  string           `json:"spec_digest"`
	ActivatedAt time.Time        `json:"activated_at"`
}
type ProductSplitCreateStop struct {
	ProofID    string    `json:"proof_id"`
	VerifiedAt time.Time `json:"verified_at"`
}

// Observations are supplied only by a trusted host GET adapter, never a model.
// Exact title/body matching keeps edits or ambiguous markers held.
type ProductSplitCreatedIssue struct {
	RepositoryID  int64     `json:"repository_id"`
	ID            int64     `json:"id"`
	Number        int       `json:"number"`
	Title         string    `json:"title"`
	Body          string    `json:"body"`
	State         string    `json:"state"`
	IsPullRequest bool      `json:"is_pull_request"`
	ObservedAt    time.Time `json:"observed_at"`
}
type splitCreatedResource struct {
	CreationID   string `json:"creation_id"`
	ProposalID   string `json:"proposal_id"`
	ParentHeadID string `json:"parent_head_id"`
	ChildID      string `json:"child_id"`
	IssueID      int64  `json:"issue_id"`
}
type ProductSplitCreate struct {
	ID             string                    `json:"id"`
	Value          ProductSplitCreateValue   `json:"value"`
	Digest         string                    `json:"digest"`
	Sequence       int                       `json:"sequence"`
	PreviousDigest string                    `json:"previous_digest"`
	State          string                    `json:"state"`
	Lease          ResourceLease             `json:"lease"`
	HTTP           *ProductSplitCreateOwner  `json:"http,omitempty"`
	Stop           *ProductSplitCreateStop   `json:"stop,omitempty"`
	Abort          *ProductSplitCreateStop   `json:"abort,omitempty"`
	Issue          *ProductSplitCreatedIssue `json:"issue,omitempty"`
}
type splitCreateRecord struct {
	Value  ProductSplitCreate `json:"value"`
	Digest string             `json:"digest"`
}
type splitCreateAdmission struct {
	ProposalID   string `json:"proposal_id"`
	ParentHeadID string `json:"parent_head_id"`
	ChildID      string `json:"child_id"`
	Digest       string `json:"digest"`
}
type ProductSplitCreateClaim struct {
	Creation ProductSplitCreate `json:"creation"`
	Start    bool               `json:"start"`
}

func splitCreateID(childID string) string {
	return structureID("split-create-", childID, "registered-http-v1")
}
func splitCreateEventID(id string, seq int) string {
	return structureID("split-create-event-", id, strconv.Itoa(seq))
}

func saveSplitCreateTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate, previous *ProductSplitCreate) (ProductSplitCreate, error) {
	if previous != nil {
		digest, err := contentDigest(*previous)
		if err != nil {
			return job, err
		}
		job.Sequence = previous.Sequence + 1
		job.PreviousDigest = digest
	}
	digest, err := contentDigest(job)
	if err != nil {
		return job, err
	}
	record := splitCreateRecord{job, digest}
	if err := kernelRecordTx(ctx, tx, "product_split_create", job.ID, job.Value.Request.Target.WaveID, record); err != nil {
		return job, err
	}
	if err := kernelRecordTx(ctx, tx, "product_split_create_event", splitCreateEventID(job.ID, job.Sequence), job.ID, record); err != nil {
		return job, err
	}
	return job, nil
}

func loadSplitCreate(ctx context.Context, tx *sql.Tx, id string) (ProductSplitCreate, error) {
	var record splitCreateRecord
	var job ProductSplitCreate
	err := taskEvidenceRecord(ctx, tx, "product_split_create", id, &record)
	if errors.Is(err, sql.ErrNoRows) {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_create_history','product_split_create_admission','product_split_create_gate') AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_create_event','product_split_create_gate_history') AND work_item_id=?) OR EXISTS(SELECT 1 FROM kernel_resource_leases WHERE run_id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_created_resource' AND json_extract(payload,'$.creation_id')=?)", id, id, id, id).Scan(&known); err != nil {
			return job, err
		}
		if known {
			return job, ErrProductSplitHeld
		}
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return job, err
		}
		return job, splitEvidenceErr(err)
	}
	job = record.Value
	digest, err := contentDigest(job)
	if err != nil {
		return job, err
	}
	valueDigest, err := contentDigest(job.Value)
	if err != nil {
		return job, err
	}
	if record.Digest != digest || job.Digest != valueDigest || job.ID != id || id != splitCreateID(job.Value.Child.ID) || job.Value.CreatedAt.IsZero() || job.Sequence < 0 || job.Sequence > 3 {
		return job, ErrProductSplitHeld
	}
	var original ProductSplitCreateValue
	var admission splitCreateAdmission
	if err := taskEvidenceRecord(ctx, tx, "product_split_create_history", id, &original); err != nil {
		return job, splitEvidenceErr(err)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_create_admission", id, &admission); err != nil {
		return job, splitEvidenceErr(err)
	}
	if !reflect.DeepEqual(original, job.Value) || admission != (splitCreateAdmission{job.Value.Request.ProposalID, job.Value.ParentHeadID, job.Value.Child.ID, job.Digest}) {
		return job, ErrProductSplitHeld
	}
	// The complete bounded chain, including its current tail, is required.
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='product_split_create_event' AND work_item_id=?", id).Scan(&count); err != nil {
		return job, err
	}
	if count != job.Sequence+1 {
		return job, ErrProductSplitHeld
	}
	previous := ""
	var previousEvent ProductSplitCreate
	for seq := 0; seq <= job.Sequence; seq++ {
		var event splitCreateRecord
		if err := taskEvidenceRecord(ctx, tx, "product_split_create_event", splitCreateEventID(id, seq), &event); err != nil {
			return job, splitEvidenceErr(err)
		}
		checksum, err := contentDigest(event.Value)
		if err != nil {
			return job, err
		}
		if checksum != event.Digest || event.Value.ID != id || event.Value.Sequence != seq || event.Value.PreviousDigest != previous || !reflect.DeepEqual(event.Value.Value, job.Value) {
			return job, ErrProductSplitHeld
		}
		if seq == job.Sequence && !reflect.DeepEqual(event, record) {
			return job, ErrProductSplitHeld
		}
		if err := validateSplitCreateState(event.Value); err != nil {
			return job, err
		}
		if event.Value.Lease != job.Lease {
			return job, ErrProductSplitHeld
		}
		if seq > 1 && (!reflect.DeepEqual(event.Value.HTTP, previousEvent.HTTP) || previousEvent.State != "unknown" && previousEvent.State != "stopped") {
			return job, ErrProductSplitHeld
		}
		if seq == 3 && !reflect.DeepEqual(event.Value.Stop, previousEvent.Stop) {
			return job, ErrProductSplitHeld
		}
		previous = checksum
		previousEvent = event.Value
	}
	allocation, err := loadProductSplitAllocation(ctx, tx, job.Value.Request)
	if err != nil {
		return job, splitEvidenceErr(err)
	}
	if allocation.ID != job.Value.AllocationID || allocation.Digest != job.Value.AllocationDigest || allocation.Value.ParentHeadID != job.Value.ParentHeadID {
		return job, ErrProductSplitHeld
	}
	plan, err := loadProductSplitChildPlan(ctx, tx, job.Value.Request)
	if err != nil {
		return job, splitEvidenceErr(err)
	}
	found := false
	for _, child := range plan.Children {
		found = found || reflect.DeepEqual(child, job.Value.Child)
	}
	if !found {
		return job, ErrProductSplitHeld
	}
	if err := validateSplitCreateState(job); err != nil {
		return job, err
	}
	if job.Lease.Resource != fmt.Sprintf("product-split-mutation:%d:%s", job.Value.Child.Value.RepositoryID, job.Value.Child.ID) || job.Lease.Generation < 1 {
		return job, ErrProductSplitHeld
	}
	if job.HTTP != nil {
		gate, err := loadSplitCreateGate(ctx, tx, job)
		if err != nil {
			return job, err
		}
		if gate.Digest != job.HTTP.GateDigest || job.HTTP.ActivatedAt.Before(gate.Value.ObservedAt) || job.HTTP.ActivatedAt.Sub(gate.Value.ObservedAt) > 5*time.Second {
			return job, ErrProductSplitHeld
		}
	}
	if job.Issue != nil {
		observation := *job.Issue
		child := job.Value.Child.Value
		if observation.RepositoryID != child.RepositoryID || observation.ID < 1 || observation.Number < 1 || observation.Title != child.Child.Title || observation.Body != child.Body || observation.State != "open" || observation.IsPullRequest || observation.ObservedAt.Before(job.Stop.VerifiedAt) {
			return job, ErrProductSplitHeld
		}
		var resource splitCreatedResource
		key := fmt.Sprintf("github:%d:issue:%d", observation.RepositoryID, observation.Number)
		if err := taskEvidenceRecord(ctx, tx, "product_split_created_resource", key, &resource); err != nil {
			return job, splitEvidenceErr(err)
		}
		if resource != (splitCreatedResource{job.ID, job.Value.Request.ProposalID, job.Value.ParentHeadID, job.Value.Child.ID, observation.ID}) {
			return job, ErrProductSplitHeld
		}
	}
	var leaseOwner, leaseState string
	var generation int64
	if err := tx.QueryRowContext(ctx, "SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", job.Lease.Resource).Scan(&leaseOwner, &leaseState, &generation); err != nil {
		return job, splitEvidenceErr(err)
	}
	if job.State == "prepared" || job.State == "unknown" {
		if leaseOwner != id || leaseState != "held" || generation != job.Lease.Generation {
			return job, ErrProductSplitHeld
		}
	} else if generation < job.Lease.Generation || generation == job.Lease.Generation && (leaseOwner != id || leaseState != "released") {
		return job, ErrProductSplitHeld
	}
	return job, nil
}

func validateSplitCreateState(job ProductSplitCreate) error {
	switch job.State {
	case "prepared":
		if job.Sequence != 0 || job.HTTP != nil || job.Stop != nil || job.Abort != nil || job.Issue != nil {
			return ErrProductSplitHeld
		}
	case "unknown":
		if job.Sequence != 1 || job.HTTP == nil || job.Stop != nil || job.Abort != nil || job.Issue != nil {
			return ErrProductSplitHeld
		}
	case "stopped":
		if job.Sequence != 2 || job.HTTP == nil || job.Stop == nil || job.Abort != nil || job.Issue != nil {
			return ErrProductSplitHeld
		}
	case "observed":
		if job.Sequence != 3 || job.HTTP == nil || job.Stop == nil || job.Abort != nil || job.Issue == nil {
			return ErrProductSplitHeld
		}
	case "aborted":
		if job.Sequence != 1 || job.HTTP != nil || job.Stop != nil || job.Abort == nil || job.Issue != nil {
			return ErrProductSplitHeld
		}
	default:
		return ErrProductSplitHeld
	}
	if job.HTTP != nil && (!job.HTTP.Process.Valid() || !validSourceDigest(job.HTTP.SpecDigest) || !validSourceDigest(job.HTTP.GateDigest) || job.HTTP.ActivatedAt.Before(job.Value.CreatedAt)) {
		return ErrProductSplitHeld
	}
	if job.Stop != nil && (!validPRProofID(job.Stop.ProofID) || job.Stop.VerifiedAt.Before(job.HTTP.ActivatedAt)) {
		return ErrProductSplitHeld
	}
	if job.Abort != nil && (!validPRProofID(job.Abort.ProofID) || job.Abort.VerifiedAt.Before(job.Value.CreatedAt)) {
		return ErrProductSplitHeld
	}
	return nil
}

func (k *KernelStore) splitCreatePreflightTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) error {
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return err
	}
	if w.Target() != r.Target || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return ErrProductSplitHeld
	}
	proposal, err := k.readProductSplitProposalTx(ctx, tx, r.Target, r.ParentWorkItemID)
	if err != nil {
		return err
	}
	if proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest {
		return ErrProductSplitHeld
	}
	if w.Source == "direct" {
		return nil
	}
	if w.Source != "github" {
		return ErrProductSplitHeld
	}
	observation, err := loadProductSourceSnapshot(ctx, tx, w.ID)
	if err != nil {
		return splitEvidenceErr(err)
	}
	var history ProductSourceSnapshot
	if err := taskEvidenceRecord(ctx, tx, "product_source_observation_history", observation.ID, &history); err != nil {
		return splitEvidenceErr(err)
	}
	now := k.clock()
	policy, err := productSourcePolicyTx(ctx, tx, w)
	if err != nil {
		return splitEvidenceErr(err)
	}
	if !reflect.DeepEqual(observation, history) || observation.Value.ScopeRevision != w.ScopeRevision || observation.Value.PolicyRevision != w.Policy.Revision || observation.Value.SourceID != w.SourceID || observation.Value.Status != "ready" || observation.Value.Facts.PolicyDigest != policy || observation.ObservedAt.After(now) {
		return ErrProductSplitHeld
	}
	status, err := k.sourceStatusTx(ctx, tx, w, observation.Value.Facts)
	if err != nil {
		return splitEvidenceErr(err)
	}
	if status != "ready" {
		return ErrProductSplitHeld
	}
	return nil
}

func (k *KernelStore) PrepareProductSplitCreate(ctx context.Context, r ProductSplitChildPlanRequest, childID, ownerID string) (ProductSplitCreate, error) {
	var job ProductSplitCreate
	if childID == "" || strings.TrimSpace(ownerID) == "" || len(ownerID) > 256 || strings.ContainsAny(ownerID, "\x00\r\n") {
		return job, ErrProductSplitHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	prior, err := loadSplitCreate(ctx, tx, splitCreateID(childID))
	if err == nil {
		if prior.Value.Request != r || prior.Value.OwnerID != ownerID {
			return job, ErrProductSplitHeld
		}
		return prior, nil
	}
	if err != sql.ErrNoRows {
		return job, err
	}
	if err := k.splitCreatePreflightTx(ctx, tx, r); err != nil {
		return job, err
	}
	allocation, err := loadProductSplitAllocation(ctx, tx, r)
	if err != nil {
		return job, splitEvidenceErr(err)
	}
	plan, err := loadProductSplitChildPlan(ctx, tx, r)
	if err != nil {
		return job, splitEvidenceErr(err)
	}
	var child ProductSplitChildIntent
	for _, candidate := range plan.Children {
		if candidate.ID == childID {
			child = candidate
		}
	}
	if child.ID == "" {
		return job, ErrProductSplitHeld
	}
	job = ProductSplitCreate{ID: splitCreateID(childID), Value: ProductSplitCreateValue{r, plan.Set.Value.ParentHeadID, allocation.ID, allocation.Digest, child, ownerID, k.clock()}, State: "prepared"}
	job.Digest, err = contentDigest(job.Value)
	if err != nil {
		return job, err
	}
	job.Lease, err = claimResourceTx(ctx, tx, fmt.Sprintf("product-split-mutation:%d:%s", child.Value.RepositoryID, child.ID), job.ID, r.Target.WaveID)
	if err != nil {
		return job, err
	}
	if err := kernelRecordTx(ctx, tx, "product_split_create_history", job.ID, r.ProposalID, job.Value); err != nil {
		return job, err
	}
	if err := kernelRecordTx(ctx, tx, "product_split_create_admission", job.ID, r.Target.WaveID, splitCreateAdmission{r.ProposalID, job.Value.ParentHeadID, childID, job.Digest}); err != nil {
		return job, err
	}
	job, err = saveSplitCreateTx(ctx, tx, job, nil)
	if err != nil {
		return job, err
	}
	return job, tx.Commit()
}

// Only the actual registered group leader can win this one-use send admission.
// A delayed/replayed wrapper must not send when Start is false.
func (k *KernelStore) ActivateProductSplitCreateHTTP(ctx context.Context, id, owner, digest, specDigest string) (ProductSplitCreateClaim, error) {
	var claim ProductSplitCreateClaim
	physical := process.Identify(os.Getpid())
	if !validSourceDigest(specDigest) || !physical.Valid() || !process.Alive(physical) {
		return claim, ErrProductSplitHeld
	}
	_, group, _, err := process.ReadStat(physical.PID)
	if err != nil || group != physical.PID {
		return claim, ErrProductSplitHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback()
	job, err := loadSplitCreate(ctx, tx, id)
	if err != nil {
		return claim, err
	}
	claim.Creation = job
	if job.Value.OwnerID != owner || job.Digest != digest {
		return claim, ErrProductSplitHeld
	}
	if job.State == "unknown" && job.HTTP != nil && job.HTTP.Process == physical && job.HTTP.SpecDigest == specDigest {
		return claim, nil
	}
	if job.State != "prepared" {
		return claim, ErrProductSplitHeld
	}
	if err := k.splitCreatePreflightTx(ctx, tx, job.Value.Request); err != nil {
		return claim, err
	}
	gateDigest, err := k.splitCreateSendGateTx(ctx, tx, job)
	if err != nil {
		return claim, err
	}
	previous := job
	job.HTTP = &ProductSplitCreateOwner{GateDigest: gateDigest, Process: physical, SpecDigest: specDigest, ActivatedAt: k.clock()}
	job.State = "unknown"
	job, err = saveSplitCreateTx(ctx, tx, job, &previous)
	if err != nil {
		return claim, err
	}
	if err := tx.Commit(); err != nil {
		return claim, err
	}
	return ProductSplitCreateClaim{Creation: job, Start: true}, nil
}

func releaseSplitCreateLeaseTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) error {
	result, err := tx.ExecContext(ctx, "UPDATE kernel_resource_leases SET state='released' WHERE resource=? AND run_id=? AND generation=? AND state='held'", job.Lease.Resource, job.ID, job.Lease.Generation)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrProductSplitHeld
	}
	return nil
}
func (k *KernelStore) AbortUnactivatedProductSplitCreate(ctx context.Context, id, owner, digest, proofID string) (ProductSplitCreate, error) {
	var empty ProductSplitCreate
	if !validPRProofID(proofID) {
		return empty, ErrProductSplitHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err := loadSplitCreate(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	if job.Value.OwnerID != owner || job.Digest != digest {
		return empty, ErrProductSplitHeld
	}
	if job.State == "aborted" && job.Abort.ProofID == proofID {
		return job, nil
	}
	if job.State != "prepared" {
		return empty, ErrProductSplitHeld
	}
	previous := job
	job.State = "aborted"
	job.Abort = &ProductSplitCreateStop{proofID, k.clock()}
	if err := releaseSplitCreateLeaseTx(ctx, tx, job); err != nil {
		return empty, err
	}
	job, err = saveSplitCreateTx(ctx, tx, job, &previous)
	if err != nil {
		return empty, err
	}
	return job, tx.Commit()
}

// Stopping proves only quiescence. An unknown HTTP outcome is never re-sent,
// even if a later GET temporarily finds no matching Issue.
func (k *KernelStore) FinishProductSplitCreateHTTP(ctx context.Context, id, owner, digest, specDigest, proofID string) (ProductSplitCreate, error) {
	var empty ProductSplitCreate
	if !validPRProofID(proofID) {
		return empty, ErrProductSplitHeld
	}
	job, err := k.ReadProductSplitCreate(ctx, id)
	if err != nil {
		return empty, err
	}
	if job.Value.OwnerID != owner || job.Digest != digest || job.HTTP == nil || job.HTTP.SpecDigest != specDigest {
		return empty, ErrProductSplitHeld
	}
	if job.Stop == nil {
		work, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := process.WaitStopped(work, job.HTTP.Process); err != nil {
			return empty, err
		}
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err = loadSplitCreate(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	if job.Stop != nil {
		if job.Stop.ProofID != proofID {
			return empty, ErrProductSplitHeld
		}
		return job, nil
	}
	if job.State != "unknown" {
		return empty, ErrProductSplitHeld
	}
	previous := job
	job.State = "stopped"
	job.Stop = &ProductSplitCreateStop{proofID, k.clock()}
	if err := releaseSplitCreateLeaseTx(ctx, tx, job); err != nil {
		return empty, err
	}
	job, err = saveSplitCreateTx(ctx, tx, job, &previous)
	if err != nil {
		return empty, err
	}
	return job, tx.Commit()
}

func (k *KernelStore) RecordProductSplitCreatedIssue(ctx context.Context, id string, observation ProductSplitCreatedIssue) (ProductSplitCreate, error) {
	var empty ProductSplitCreate
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err := loadSplitCreate(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	if job.State == "observed" {
		if !reflect.DeepEqual(*job.Issue, observation) {
			return empty, ErrProductSplitHeld
		}
		return job, nil
	}
	now := k.clock()
	child := job.Value.Child.Value
	if job.State != "stopped" || observation.RepositoryID != child.RepositoryID || observation.ID < 1 || observation.Number < 1 || observation.Title != child.Child.Title || observation.Body != child.Body || observation.State != "open" || observation.IsPullRequest || observation.ObservedAt.Before(job.Stop.VerifiedAt) || observation.ObservedAt.After(now) || now.Sub(observation.ObservedAt) > 30*time.Second {
		return empty, ErrProductSplitHeld
	}
	key := fmt.Sprintf("github:%d:issue:%d", observation.RepositoryID, observation.Number)
	resource := splitCreatedResource{job.ID, job.Value.Request.ProposalID, job.Value.ParentHeadID, job.Value.Child.ID, observation.ID}
	var existing splitCreatedResource
	err = taskEvidenceRecord(ctx, tx, "product_split_created_resource", key, &existing)
	if err == nil && existing != resource {
		return empty, ErrProductSplitHeld
	}
	if err != nil && err != sql.ErrNoRows {
		return empty, splitEvidenceErr(err)
	}
	if err := kernelRecordTx(ctx, tx, "product_split_created_resource", key, job.Value.Request.Target.WaveID, resource); err != nil {
		return empty, err
	}
	previous := job
	job.State = "observed"
	job.Issue = &observation
	job, err = saveSplitCreateTx(ctx, tx, job, &previous)
	if err != nil {
		return empty, err
	}
	return job, tx.Commit()
}
func (k *KernelStore) ReadProductSplitCreate(ctx context.Context, id string) (ProductSplitCreate, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitCreate{}, err
	}
	defer tx.Rollback()
	return loadSplitCreate(ctx, tx, id)
}

// A downstream physical intent is independent evidence that a parent/plan/
// allocation once existed. Missing indexes cannot replace it with a new key.
func splitCreationWitnessTx(ctx context.Context, tx *sql.Tx, waveID, parentHead string) (bool, error) {
	var known bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_create_history','product_split_create_admission','product_split_created_resource') AND json_extract(payload,'$.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_create','product_split_create_event') AND json_extract(payload,'$.value.value.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_create_gate','product_split_create_gate_history') AND json_extract(payload,'$.value.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM kernel_resource_leases l WHERE l.wave_id=? AND l.resource LIKE 'product-split-mutation:%' AND NOT EXISTS(SELECT 1 FROM records r WHERE r.kind='product_split_create' AND r.id=l.run_id))`, parentHead, parentHead, parentHead, waveID).Scan(&known)
	return known, err
}
