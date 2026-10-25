package storage

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

// ProductTarget is a controller-owned projection of product facts. It grants
// no publication or approval by itself. The P executor must recheck the source
// WorkItem, validation/gates and capacity immediately before external writes.
type ProductTarget struct {
	Target     contract.Target  `json:"target"`
	Repository string           `json:"repository"`
	BaseBranch string           `json:"base_branch"`
	WorkItem   domain.WorkItem  `json:"work_item"`
	Candidate  runner.Candidate `json:"candidate"`
	PRNumber   int              `json:"pr_number"`
}
type ProductTargetSnapshot struct {
	Value  ProductTarget `json:"value"`
	Digest string        `json:"digest"`
}

func productTargetID(target contract.Target, workItem string) string {
	return structureID("product-target-", target.WaveID, workItem)
}
func fixedSHA(sha string) bool { return len(sha) == 40 && strings.Trim(sha, "0123456789abcdef") == "" }
func (p ProductTarget) validate() error {
	w, c := p.WorkItem, p.Candidate
	if !p.Target.Valid() || w.ID == "" || w.RepositoryID <= 0 || w.ScopeRevision < 1 || w.PolicyRevision < 1 || w.Version < 1 || w.Branch == "" || w.Candidate != c.ID || c.ID == "" || !fixedSHA(c.SHA) || !fixedSHA(c.BaseSHA) || c.ScopeRevision != w.ScopeRevision || c.PolicyRevision != w.PolicyRevision || p.PRNumber < 0 {
		return errors.New("product target requires fixed controller WorkItem/candidate facts")
	}
	parts := strings.Split(p.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || p.BaseBranch == "" {
		return errors.New("product repository and default base required")
	}
	for _, value := range []string{w.ID, w.Branch, c.ID, p.Repository, p.BaseBranch} {
		if len(value) > 1024 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n\t ") {
			return errors.New("invalid product identity")
		}
	}
	return nil
}
func loadProductTargetTx(ctx context.Context, q queryer, target contract.Target, workItem string) (ProductTargetSnapshot, error) {
	var saved ProductTargetSnapshot
	if err := completionRecord(ctx, q, "product_target", productTargetID(target, workItem), &saved); err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if err = saved.Value.validate(); err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.Value.Target != target || saved.Value.WorkItem.ID != workItem {
		return saved, errors.New("product target binding conflict")
	}
	return saved, nil
}

// ReadProductTarget reads a trusted projection, including its CAS digest. It is
// not a model tool and does not grant permission to send an external request.
func (k *KernelStore) ReadProductTarget(ctx context.Context, target contract.Target, workItem string) (ProductTargetSnapshot, error) {
	return loadProductTargetTx(ctx, k.DB, target, workItem)
}

// SetProductTarget is a trusted controller port, unavailable to model tools.
// expected is empty only for initial registration. Compare-and-swap also covers
// pause/hold changes which need not increment the legacy WorkItem version.
func (k *KernelStore) SetProductTarget(ctx context.Context, expected string, p ProductTarget) (ProductTargetSnapshot, error) {
	var saved ProductTargetSnapshot
	if err := p.validate(); err != nil {
		return saved, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = k.setProductTargetTx(ctx, tx, expected, p)
	if err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func (k *KernelStore) setProductTargetTx(ctx context.Context, tx *sql.Tx, expected string, p ProductTarget) (ProductTargetSnapshot, error) {
	var saved ProductTargetSnapshot
	if err := p.validate(); err != nil {
		return saved, err
	}
	w, err := loadWave(ctx, tx, p.Target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.Target() != p.Target || w.Policy.Stage != "P" || !w.Policy.Product {
		return saved, errors.New("product registration requires current trusted P policy")
	}
	registration, registrationErr := loadProductWorkItemRegistration(ctx, tx, p.WorkItem.ID)
	if registrationErr != nil && !errors.Is(registrationErr, sql.ErrNoRows) {
		return saved, registrationErr
	}
	if registrationErr == nil && (!productRegistrationScope(registration, w) || registration.Value.Definition.RepositoryID != p.WorkItem.RepositoryID || registration.Value.Definition.Repository != p.Repository) {
		return saved, errors.New("product projection differs from registered source ownership")
	}
	var membership struct {
		Target      contract.Target                  `json:"target"`
		CandidateID string                           `json:"candidate_id"`
		SplitScope  *ProductSplitChildScopeReference `json:"split_scope,omitempty"`
	}
	err = completionRecord(ctx, tx, "wave_work_item", p.WorkItem.ID, &membership)
	if err == nil && membership.Target.WaveID != p.Target.WaveID {
		return saved, errors.New("product WorkItem already belongs to another Wave")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	var previous ProductTargetSnapshot
	err = completionRecord(ctx, tx, "product_target", productTargetID(p.Target, p.WorkItem.ID), &previous)
	if errors.Is(err, sql.ErrNoRows) {
		if expected != "" {
			return saved, errors.New("product target CAS expected existing registration")
		}
	} else if err != nil {
		return saved, err
	} else {
		previousDigest, e := contentDigest(previous.Value)
		if e != nil {
			return saved, e
		}
		if previous.Digest != previousDigest || expected != previous.Digest || previous.Value.WorkItem.Version > p.WorkItem.Version || previous.Value.WorkItem.RepositoryID != p.WorkItem.RepositoryID {
			return saved, errors.New("product target CAS conflict")
		}
	}
	// A projection refresh can select a new candidate, but cannot reinterpret an
	// existing immutable candidate ID, including after intervening candidates.
	candidateKey := structureID("product-candidate-", p.WorkItem.ID, p.Candidate.ID)
	var original runner.Candidate
	err = completionRecord(ctx, tx, "product_candidate", candidateKey, &original)
	if err == nil && original != p.Candidate {
		return saved, errors.New("immutable product candidate changed")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	if err = kernelRecordTx(ctx, tx, "product_candidate", candidateKey, p.WorkItem.ID, p.Candidate); err != nil {
		return saved, err
	}
	saved = ProductTargetSnapshot{Value: p}
	saved.Digest, err = contentDigest(p)
	if err != nil {
		return saved, err
	}
	if err = kernelRecordTx(ctx, tx, "product_target", productTargetID(p.Target, p.WorkItem.ID), p.Target.WaveID, saved); err != nil {
		return saved, err
	}
	if err = kernelRecordTx(ctx, tx, "product_target_observation", saved.Digest, p.Target.WaveID, saved); err != nil {
		return saved, err
	}
	membership.Target, membership.CandidateID = p.Target, p.Candidate.ID
	if err = kernelRecordTx(ctx, tx, "wave_work_item", p.WorkItem.ID, p.Target.WaveID, membership); err != nil {
		return saved, err
	}
	if err = k.refreshAssignedPRSubscriptionsTx(ctx, tx, w); err != nil {
		return saved, err
	}
	return saved, nil
}

type PRSubscriptionIntent struct {
	SourceKind   string          `json:"source_kind,omitempty"`
	WorkItemID   string          `json:"work_item_id,omitempty"`
	ResourceKey  string          `json:"resource_key,omitempty"`
	ID           string          `json:"id"`
	OperationID  string          `json:"operation_id"`
	SessionID    string          `json:"session_id"`
	TaskID       string          `json:"task_id"`
	Target       contract.Target `json:"target"`
	RepositoryID int64           `json:"repository_id"`
	PRNumber     int             `json:"pr_number"`
	State        string          `json:"state"`
	CreatedAt    time.Time       `json:"created_at"`
}
type PROperationIntent struct {
	Source           kernelToolOperation         `json:"source"`
	TaskID           string                      `json:"task_id"`
	Candidate        contract.CandidateReference `json:"candidate"`
	ProductDigest    string                      `json:"product_digest"`
	SubscriptionID   string                      `json:"subscription_id"`
	MarkerWorkItemID string                      `json:"marker_work_item_id"`
	State            string                      `json:"state"`
}

func (k *KernelStore) acceptPRIntentTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, b contract.SessionBinding, candidate contract.CandidateReference) (contract.ToolReply, error) {
	var reply contract.ToolReply
	w, err := loadWave(ctx, tx, op.Target.WaveID)
	if err != nil {
		return reply, err
	}
	if w.Policy.Stage != "P" || !w.Policy.Product || w.Target() != op.Target || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
		return reply, errors.New("PR tools require a current dispatchable product Wave")
	}
	if b.Kind == "task" {
		task, e := loadTask(ctx, tx, b.TaskID)
		if e != nil {
			return reply, e
		}
		if !slices.Contains(task.Spec.WorkItemIDs, candidate.WorkItemID) {
			return reply, errors.New("product WorkItem is outside Task assignment")
		}
	}
	saved, err := loadProductTargetTx(ctx, tx, op.Target, candidate.WorkItemID)
	if err != nil {
		return reply, err
	}
	p := saved.Value
	if candidate.CandidateID != p.Candidate.ID || candidate.SHA != p.Candidate.SHA || p.WorkItem.Paused || p.WorkItem.MergeHold || p.WorkItem.Phase != domain.Reviewing || p.WorkItem.Terminal() {
		return reply, errors.New("product candidate changed or WorkItem is held")
	}
	if op.Name == "pr_review_request" && p.PRNumber < 1 {
		return reply, errors.New("review request requires a controller-bound PR")
	}
	sub, err := k.ensureOperationSubscriptionTx(ctx, tx, op)
	if err != nil {
		return reply, err
	}
	watch := PRSubscriptionIntent{SourceKind: "operation", WorkItemID: candidate.WorkItemID, ID: sub.ID, OperationID: op.ID, SessionID: b.ID, TaskID: b.TaskID, Target: op.Target, RepositoryID: p.WorkItem.RepositoryID, PRNumber: p.PRNumber, State: "pending", CreatedAt: k.clock()}
	intent := PROperationIntent{Source: op, TaskID: b.TaskID, Candidate: candidate, ProductDigest: saved.Digest, SubscriptionID: sub.ID, MarkerWorkItemID: candidate.WorkItemID, State: "accepted"}
	if err = kernelRecordTx(ctx, tx, "product_pr_intent", op.ID, b.ID, intent); err != nil {
		return reply, err
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_subscription_intent", sub.ID, b.ID, watch); err != nil {
		return reply, err
	}
	return contract.ToolReply{SchemaVersion: 1, State: "accepted", OperationID: op.ID, SubscriptionIDs: []string{sub.ID}, ResourceID: candidate.WorkItemID}, nil
}
