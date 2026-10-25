package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func checkProductSplitSubscriptionTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, id string) error {
	expected := kernelOperationSubscription{ID: kernelOperationSubscriptionID(op.SessionID, op.ID), SessionID: op.SessionID, Target: op.Target, OperationID: op.ID}
	if id != expected.ID {
		return ErrProductSplitHeld
	}
	var session, wave, resource string
	var scope, plan int
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT session_id,wave_id,scope_revision,plan_revision,resource,payload FROM kernel_subscriptions WHERE id=?", id).Scan(&session, &wave, &scope, &plan, &resource, &raw); err != nil {
		return splitEvidenceErr(err)
	}
	var actual kernelOperationSubscription
	if err := json.Unmarshal(raw, &actual); err != nil {
		return errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	if actual != expected || session != op.SessionID || wave != op.Target.WaveID || scope != op.Target.ScopeRevision || plan != op.Target.PlanRevision || resource != "kernel:operation:"+op.ID {
		return ErrProductSplitHeld
	}
	return nil
}

// A trusted host preparation has no originating model operation. A model
// adoption must retain its actual accepted operation and artifact/preparation
// bindings; a missing source cannot be converted into the host-only path.
func productSplitNotificationSourceTx(ctx context.Context, tx *sql.Tx, registered ProductSplitRegistrations) (ProductSplitToolAdoption, bool, error) {
	var empty ProductSplitToolAdoption
	r := registered.Value.Request
	proposal, err := loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID})
	if err != nil {
		return empty, false, splitEvidenceErr(err)
	}
	id := proposal.Value.Request.Key
	var name, argument, session, thread, state string
	var payload, result []byte
	err = tx.QueryRowContext(ctx, "SELECT name,argument_digest,session_id,thread_id,state,payload,result FROM kernel_tool_operations WHERE id=?", id).Scan(&name, &argument, &session, &thread, &state, &payload, &result)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_tool_adoption' AND id=?)", id).Scan(&known); err != nil {
			return empty, false, err
		}
		if known || strings.HasPrefix(id, "kernel-operation-") {
			return empty, false, ErrProductSplitHeld
		}
		return empty, false, nil
	}
	if err != nil {
		return empty, false, err
	}
	var op kernelToolOperation
	var reply contract.ToolReply
	if json.Unmarshal(payload, &op) != nil || json.Unmarshal(result, &reply) != nil {
		return empty, false, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	if op.ID != id || name != "product_split" || op.Name != name || op.ArgumentDigest != argument || op.SessionID != session || op.ThreadID != thread || op.Target != r.Target || state != "completed" || reply.State != "completed" || reply.OperationID != id {
		return empty, false, ErrProductSplitHeld
	}
	var adoption ProductSplitToolAdoption
	if err := taskEvidenceRecord(ctx, tx, "product_split_tool_adoption", id, &adoption); err != nil {
		return empty, false, splitEvidenceErr(err)
	}
	request := contract.SplitProductRequest{SchemaVersion: 1, Target: op.Target, ProposalArtifactID: adoption.ArtifactID}
	digest, err := contentDigest(request)
	if err != nil {
		return empty, false, err
	}
	if op.ArgumentDigest != digest || adoption.Source != op || adoption.Preparation.Value.ProposalID != r.ProposalID || adoption.Preparation.Value.ProposalDigest != r.ProposalDigest || adoption.ParentHeadID != registered.Value.ParentHeadID {
		return empty, false, ErrProductSplitHeld
	}
	if err := verifyProductSplitReplayTx(ctx, tx, op, request, reply); err != nil {
		return empty, false, splitEvidenceErr(err)
	}
	var content struct {
		PreparationID string `json:"preparation_id"`
		Digest        string `json:"digest"`
		Children      int    `json:"children"`
		Stage         string `json:"stage"`
	}
	if json.Unmarshal([]byte(reply.Content), &content) != nil || content.PreparationID != adoption.PreparationID || content.Digest != adoption.PreparationDigest || content.Children != len(registered.Value.Children) || content.Stage != "prepared" {
		return empty, false, ErrProductSplitHeld
	}
	return adoption, true, nil
}
