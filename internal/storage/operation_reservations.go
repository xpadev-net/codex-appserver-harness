package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// These are trusted dispatch inputs, after business authorization and outbox
// admission. Capacity admission executes no command or external request.
type OperationCapacityRequest struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"` // runner, pr, cloud_review, git
	SessionID   string          `json:"session_id"`
	Target      contract.Target `json:"target"`
	Worktree    string          `json:"worktree"`
	Repository  string          `json:"repository"`
	ResourceKey string          `json:"resource_key"`
}
type OperationReleaseProof struct {
	OperationID string `json:"operation_id"`
	ProofID     string `json:"proof_id"`
	Disposition string `json:"disposition"`
	Reason      string `json:"reason"`
}
type OperationReservation struct {
	Kind           string                   `json:"kind"`
	ID             string                   `json:"id"`
	SessionID      string                   `json:"session_id"`
	WaveID         string                   `json:"wave_id"`
	State          string                   `json:"state"`
	Request        OperationCapacityRequest `json:"request"`
	RequestDigest  string                   `json:"request_digest"`
	PolicyRevision int                      `json:"policy_revision"`
	Resources      []ResourceLease          `json:"resources"`
	CreatedAt      time.Time                `json:"created_at"`
	ReleasedAt     time.Time                `json:"released_at"`
	Proof          *OperationReleaseProof   `json:"proof,omitempty"`
}

func validOperationCapacityKind(kind string) bool {
	return kind == "runner" || kind == "pr" || kind == "cloud_review" || kind == "git"
}
func validateOperationCapacityRequest(r OperationCapacityRequest) error {
	for _, value := range []string{r.ID, r.SessionID, r.Worktree, r.Repository, r.ResourceKey} {
		if !utf8.ValidString(value) || len(value) > contract.MaxInline {
			return errors.New("operation capacity strings must be bounded UTF-8")
		}
	}
	if !validOperationCapacityKind(r.Kind) || r.ID == "" || r.SessionID == "" || !r.Target.Valid() {
		return errors.New("versioned operation capacity identity required")
	}
	switch r.Kind {
	case "runner":
		if !filepath.IsAbs(r.Worktree) || r.Repository != "" || r.ResourceKey != "" {
			return errors.New("runner requires only its fixed workspace")
		}
	case "git":
		if !filepath.IsAbs(r.Repository) || r.Worktree != "" || r.ResourceKey != "" {
			return errors.New("Git operation requires only its repository")
		}
	default:
		if strings.TrimSpace(r.ResourceKey) == "" || len(r.ResourceKey) > contract.MaxInline || r.Repository != "" || r.Worktree != "" {
			return errors.New("external capacity requires its controller-owned resource key")
		}
	}
	return nil
}
func loadOperationReservationTx(ctx context.Context, q queryer, id string) (OperationReservation, error) {
	var r OperationReservation
	var raw []byte
	var session, wave, state string
	err := q.QueryRowContext(ctx, "SELECT session_id,wave_id,state,payload FROM kernel_dispatch_intents WHERE run_id=?", id).Scan(&session, &wave, &state, &raw)
	if err != nil {
		return r, err
	}
	return decodeOperationReservation(raw, id, session, wave, state)
}
func decodeOperationReservation(raw []byte, id, session, wave, state string) (OperationReservation, error) {
	var r OperationReservation
	var err error
	if err = json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	digest, err := contentDigest(r.Request)
	if err != nil {
		return r, err
	}
	if !validOperationCapacityKind(r.Kind) || r.Kind != r.Request.Kind || r.ID != id || r.Request.ID != id || r.SessionID != session || session != r.Request.SessionID || r.WaveID != wave || wave != r.Request.Target.WaveID || r.State != state || r.RequestDigest != digest {
		return r, errors.New("operation reservation identity conflict")
	}
	return r, nil
}
func (k *KernelStore) OperationReservation(ctx context.Context, id string) (OperationReservation, error) {
	return loadOperationReservationTx(ctx, k.DB, id)
}

type OperationReservationPage struct {
	Reservations []OperationReservation `json:"reservations"`
	NextAfter    string                 `json:"next_after"`
}

// PendingOperationReservations supports bounded startup/reconciliation scans.
// Callers retain the cursor across batches and wrap after its empty end marker.
func (k *KernelStore) PendingOperationReservations(ctx context.Context, after string, limit int) (OperationReservationPage, error) {
	page := OperationReservationPage{Reservations: []OperationReservation{}}
	if limit < 1 || limit > 1000 {
		return page, errors.New("operation reservation page limit must be 1..1000")
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT run_id,session_id,wave_id,state,payload FROM kernel_dispatch_intents WHERE state='held' AND run_id>? ORDER BY run_id", after)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, session, wave, state string
		var raw []byte
		if err = rows.Scan(&id, &session, &wave, &state, &raw); err != nil {
			return page, err
		}
		var header struct {
			Kind string `json:"kind"`
		}
		if err = json.Unmarshal(raw, &header); err != nil {
			return page, err
		}
		if header.Kind == "app_server" {
			continue
		}
		r, err := decodeOperationReservation(raw, id, session, wave, state)
		if err != nil {
			return page, err
		}
		if len(page.Reservations) == limit {
			page.NextAfter = page.Reservations[limit-1].ID
			return page, nil
		}
		page.Reservations = append(page.Reservations, r)
	}
	return page, rows.Err()
}

func operationCapacityOwnerTx(ctx context.Context, tx *sql.Tx, r OperationCapacityRequest, now time.Time) error {
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return err
	}
	if err = w.Validate(); err != nil {
		return err
	}
	b, err := sessionBindingTx(ctx, tx, r.SessionID)
	if err != nil {
		return err
	}
	if w.Target() != r.Target || w.Paused || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || !now.Before(w.Deadline) || b.Closed {
		return errors.New("operation source is stale, paused, expired or terminal")
	}
	if b.Kind == "orchestrator" {
		if w.OrchestratorSessionID != b.ID {
			return errors.New("operation parent does not own this Wave")
		}
	} else if b.Kind == "task" {
		if b.Target != w.Target() || b.ParentSessionID != w.OrchestratorSessionID || r.Kind != "runner" {
			return errors.New("Task can reserve only its own runner work")
		}
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return err
		}
		if task.Paused || task.State.Terminal() {
			return errors.New("Task operation is held")
		}
	} else {
		return errors.New("unknown operation source role")
	}
	if (r.Kind == "pr" || r.Kind == "cloud_review") && (w.Policy.Stage != "P" || !w.Policy.Product) {
		return errors.New("product operation requires trusted P policy")
	}
	return nil
}
func heldOperationCapacityTx(ctx context.Context, tx *sql.Tx, kind string) (int, error) {
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM kernel_dispatch_intents WHERE state='held'")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var raw []byte
		var header struct {
			Kind string `json:"kind"`
		}
		if err = rows.Scan(&raw); err != nil {
			return 0, err
		}
		if err = json.Unmarshal(raw, &header); err != nil {
			return 0, err
		}
		if header.Kind != "app_server" && !validOperationCapacityKind(header.Kind) {
			return 0, errors.New("unknown persisted reservation kind")
		}
		if header.Kind == kind {
			count++
		}
	}
	return count, rows.Err()
}
func repositoryResource(ctx context.Context, repo string) (string, error) {
	common, err := workspace.Git(ctx, repo, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return "", err
	}
	common = strings.TrimSpace(common)
	if !filepath.IsAbs(common) {
		return "", errors.New("Git returned non-absolute common repository identity")
	}
	return resourcePath(common)
}
func (k *KernelStore) ReserveOperationCapacity(ctx context.Context, request OperationCapacityRequest) (OperationReservation, error) {
	if err := validateOperationCapacityRequest(request); err != nil {
		return OperationReservation{}, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return OperationReservation{}, err
	}
	defer tx.Rollback()
	r, err := k.reserveOperationCapacityTx(ctx, tx, request)
	if err != nil {
		return r, err
	}
	return r, tx.Commit()
}
func (k *KernelStore) reserveOperationCapacityTx(ctx context.Context, tx *sql.Tx, request OperationCapacityRequest) (OperationReservation, error) {
	r, err := loadOperationReservationTx(ctx, tx, request.ID)
	if err == nil {
		if !reflect.DeepEqual(r.Request, request) {
			return r, errors.New("operation capacity identity reused with different inputs")
		}
		return r, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return r, err
	}
	if err = validateOperationCapacityRequest(request); err != nil {
		return r, err
	}
	if err = operationCapacityOwnerTx(ctx, tx, request, k.clock()); err != nil {
		return r, err
	}
	p, err := executionPolicyTx(ctx, tx)
	if err != nil {
		return r, err
	}
	// Acquire the DB write lock before counting/claiming shared capacity.
	if err = kernelRecordTx(ctx, tx, "kernel_execution_policy", "controller", "controller", p); err != nil {
		return r, err
	}
	limit := 0
	switch request.Kind {
	case "runner":
		limit = p.Limits.Runners
	case "pr":
		limit = p.Limits.PRs
	case "cloud_review":
		limit = p.Limits.CloudReviews
	}
	if limit > 0 {
		held, err := heldOperationCapacityTx(ctx, tx, request.Kind)
		if err != nil {
			return r, err
		}
		if held >= limit {
			return r, ErrExecutionCapacity
		}
	}
	r = OperationReservation{Kind: request.Kind, ID: request.ID, SessionID: request.SessionID, WaveID: request.Target.WaveID, State: "held", Request: request, PolicyRevision: p.Revision, Resources: []ResourceLease{}, CreatedAt: k.clock()}
	r.RequestDigest, err = contentDigest(request)
	if err != nil {
		return r, err
	}
	resources := []string{}
	switch request.Kind {
	case "runner":
		path, err := worktreeResource(ctx, request.Worktree)
		if err != nil {
			return r, err
		}
		if err = checkLiveWorkspaceTx(ctx, tx, path, ""); err != nil {
			return r, err
		}
		resources = append(resources, "worktree:"+path)
	case "git":
		path, err := repositoryResource(ctx, request.Repository)
		if err != nil {
			return r, err
		}
		resources = append(resources, "repository_git:"+path)
		tree, err := worktreeResource(ctx, request.Repository)
		if err != nil {
			return r, err
		}
		if err = checkLiveWorkspaceTx(ctx, tx, tree, ""); err != nil {
			return r, err
		}
		resources = append(resources, "worktree:"+tree)
	default:
		resources = append(resources, "external:"+request.Kind+":"+request.ResourceKey)
	}
	for _, resource := range resources {
		lease, err := claimResourceTx(ctx, tx, resource, r.ID, r.WaveID)
		if err != nil {
			return r, err
		}
		r.Resources = append(r.Resources, lease)
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO kernel_dispatch_intents(run_id,session_id,wave_id,state,payload) VALUES(?,?,?,'held',?)", r.ID, r.SessionID, r.WaveID, raw)
	return r, err
}

// Only the trusted process owner or external reconciler calls this port after
// verifying the outcome. Model/runner result bodies do not supply this proof.
func (k *KernelStore) ReleaseOperationCapacity(ctx context.Context, proof OperationReleaseProof) error {
	if proof.OperationID == "" || strings.TrimSpace(proof.ProofID) == "" || len(proof.ProofID) > contract.MaxInline || !utf8.ValidString(proof.ProofID) || strings.TrimSpace(proof.Reason) == "" || len(proof.Reason) > contract.MaxInline || !utf8.ValidString(proof.Reason) {
		return errors.New("verified operation release proof required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	r, err := loadOperationReservationTx(ctx, tx, proof.OperationID)
	if err != nil {
		return err
	}
	allowed := proof.Disposition == "not_started"
	switch r.Kind {
	case "runner", "git":
		allowed = allowed || proof.Disposition == "process_stopped"
	case "pr":
		allowed = allowed || proof.Disposition == "merged" || proof.Disposition == "closed"
	case "cloud_review":
		allowed = allowed || proof.Disposition == "completed" || proof.Disposition == "failed"
	}
	if !allowed {
		return errors.New("unknown/nonterminal outcome cannot release operation capacity")
	}
	if r.State == "released" {
		if r.Proof == nil || *r.Proof != proof {
			return errors.New("operation release proof identity conflict")
		}
		return nil
	}
	if r.State != "held" {
		return errors.New("invalid operation capacity state")
	}
	for _, lease := range r.Resources {
		result, err := tx.ExecContext(ctx, "UPDATE kernel_resource_leases SET state='released' WHERE resource=? AND run_id=? AND state='held' AND generation=?", lease.Resource, r.ID, lease.Generation)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.New("operation lease owner/generation changed")
		}
	}
	r.State, r.ReleasedAt, r.Proof = "released", k.clock(), &proof
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE kernel_dispatch_intents SET state='released',payload=? WHERE run_id=? AND state='held'", raw, r.ID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("operation capacity changed before release")
	}
	return tx.Commit()
}
