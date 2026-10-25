package storage

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func requireProductNestedSplitSessionTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, r contract.NestedSplitProductRequest) error {
	if err := requireProductSplitSessionTx(ctx, tx, b, contract.SplitProductRequest{SchemaVersion: r.SchemaVersion, Target: r.Target, ProposalArtifactID: r.ProposalArtifactID}); err != nil {
		return err
	}
	var profile SessionToolProfile
	if err := taskEvidenceRecord(ctx, tx, "session_tool_profile", structureID("session-tools-", b.ID, fmt.Sprint(b.Generation)), &profile); err != nil {
		return err
	}
	if profile.Profile != "product_v5" || strings.TrimSpace(r.ParentScopeID) == "" || len(r.ParentScopeID) > 128 || !validSourceDigest(r.ParentScopeDigest) {
		return ErrProductSplitHeld
	}
	return nil
}

func (k *KernelStore) acceptProductNestedSplitTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, b contract.SessionBinding, r contract.NestedSplitProductRequest) (contract.ToolReply, error) {
	wave, artifact, proposal, err := k.productSplitToolArtifactTx(ctx, tx, op, b, r.ProposalArtifactID)
	if err != nil {
		return contract.ToolReply{}, err
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, proposal.WorkItemID)
	if err != nil {
		return contract.ToolReply{}, err
	}
	binding, err := productSplitChildScopeTx(ctx, tx, wave, registration)
	if err != nil {
		return contract.ToolReply{}, err
	}
	if binding == nil {
		return contract.ToolReply{}, ErrProductSplitHeld
	}
	proof, err := loadSplitParentScope(ctx, tx, *binding)
	if err != nil {
		return contract.ToolReply{}, splitEvidenceErr(err)
	}
	parent := ProductSplitParentScopeReference{ID: r.ParentScopeID, Digest: r.ParentScopeDigest}
	if proof.ID != parent.ID || proof.Digest != parent.Digest {
		return contract.ToolReply{}, ErrProductSplitHeld
	}
	request := ProductSplitProposalRequest{Target: op.Target, Key: op.ID, ParentWorkItemID: proposal.WorkItemID, RegistrationDigest: registration.Digest, Proposal: proposal}
	saved, err := k.prepareProductNestedSplitExecutionTx(ctx, tx, request, parent)
	if err != nil {
		return contract.ToolReply{}, err
	}
	// An independent complete parent copy retains sibling/ancestor prohibitions
	// even when this reference was originally prepared through a host port.
	receipt := productSplitParentScopeToolReceipt{Value: proof.Value, Source: op, ParentWorkItemID: proposal.WorkItemID, ParentScopeID: proof.ID, ParentScopeDigest: proof.Digest}
	if err := kernelRecordTx(ctx, tx, "product_split_parent_scope_tool", op.ID, op.Target.WaveID, receipt); err != nil {
		return contract.ToolReply{}, err
	}
	return k.finishProductSplitToolAdoptionTx(ctx, tx, op, artifact, saved, &parent)
}

func verifyProductNestedSplitReplayTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, r contract.NestedSplitProductRequest, reply contract.ToolReply) error {
	if err := verifyProductSplitReplayTx(ctx, tx, op, contract.SplitProductRequest{SchemaVersion: r.SchemaVersion, Target: r.Target, ProposalArtifactID: r.ProposalArtifactID}, reply); err != nil {
		return err
	}
	var adoption ProductSplitToolAdoption
	if err := taskEvidenceRecord(ctx, tx, "product_split_tool_adoption", op.ID, &adoption); err != nil {
		return splitEvidenceErr(err)
	}
	parent := ProductSplitParentScopeReference{ID: r.ParentScopeID, Digest: r.ParentScopeDigest}
	if op.Name != "product_split_nested" || op.Target != r.Target || adoption.ParentScopeProof == nil || *adoption.ParentScopeProof != parent || adoption.SubscriptionID == "" || adoption.Preparation.Value.Target == nil || *adoption.Preparation.Value.Target != op.Target {
		return ErrProductSplitHeld
	}
	nested, err := loadNestedSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: op.Target, ParentWorkItemID: adoption.Preparation.Value.ParentWorkItemID})
	if err != nil {
		return splitEvidenceErr(err)
	}
	digest, err := contentDigest(nested.Value.Proposal.Value.Request)
	if err != nil {
		return err
	}
	if nested.Value.ParentProof != parent || nested.Value.Proposal.Value.Request.Key != op.ID || nested.Value.Proposal.ID != adoption.Preparation.Value.ProposalID || nested.Value.Proposal.Digest != adoption.Preparation.Value.ProposalDigest || digest != adoption.Preparation.Value.RequestDigest {
		return ErrProductSplitHeld
	}
	var receipt productSplitParentScopeToolReceipt
	if err := taskEvidenceRecord(ctx, tx, "product_split_parent_scope_tool", op.ID, &receipt); err != nil {
		return splitEvidenceErr(err)
	}
	if receipt.Source != op || receipt.ParentScopeID != parent.ID || receipt.ParentScopeDigest != parent.Digest || receipt.ParentWorkItemID != adoption.Preparation.Value.ParentWorkItemID || !reflect.DeepEqual(receipt.Value, nested.Value.ProductSplitParentScopeValue) {
		return ErrProductSplitHeld
	}
	expected, err := productSplitToolReply(op, adoption.Preparation, adoption.SubscriptionID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(reply, expected) {
		return ErrProductSplitHeld
	}
	return nil
}
