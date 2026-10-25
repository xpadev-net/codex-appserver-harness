package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type productSplitParentScopeToolReceipt struct {
	Source            kernelToolOperation `json:"source"`
	ParentWorkItemID  string              `json:"parent_work_item_id"`
	ParentScopeID     string              `json:"parent_scope_id"`
	ParentScopeDigest string              `json:"parent_scope_digest"`
}

func requireProductSplitParentScopeSessionTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding) error {
	var profile SessionToolProfile
	if b.Kind != "orchestrator" {
		return ErrProductSplitHeld
	}
	if err := taskEvidenceRecord(ctx, tx, "session_tool_profile", structureID("session-tools-", b.ID, fmt.Sprint(b.Generation)), &profile); err != nil {
		return err
	}
	if profile.SessionID != b.ID || profile.Generation != b.Generation || profile.Profile != "product_v4" || profile.CreatedAt.IsZero() {
		return ErrProductSplitHeld
	}
	return nil
}

// The full saved parent proof includes sibling creation and seat witnesses.
// Only the requested child's public requirement data crosses the tool boundary.
func productSplitParentScopeToolReply(op kernelToolOperation, saved ProductSplitParentScope) (contract.ToolReply, error) {
	for _, binding := range saved.Value.Children {
		if binding.Value.Child.Value.WorkItemID != saved.Value.ParentWorkItemID {
			continue
		}
		raw, err := json.Marshal(struct {
			ParentScopeID     string                 `json:"parent_scope_id"`
			ParentScopeDigest string                 `json:"parent_scope_digest"`
			Scope             ProductSplitScopeInput `json:"scope"`
		}{saved.ID, saved.Digest, productSplitScopeInput(binding)})
		if err != nil {
			return contract.ToolReply{}, err
		}
		if len(raw) > contract.MaxInline {
			return contract.ToolReply{}, ErrProductSplitHeld
		}
		return contract.ToolReply{SchemaVersion: 1, State: "completed", OperationID: op.ID, SubscriptionIDs: []string{}, ResourceID: saved.ID, Content: string(raw)}, nil
	}
	return contract.ToolReply{}, ErrProductSplitHeld
}

func (k *KernelStore) acceptProductSplitParentScopeTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, request contract.SplitProductParentScopeRequest) (contract.ToolReply, error) {
	if request.SchemaVersion != 1 || request.Target != op.Target || request.ParentWorkItemID == "" || len(request.ParentWorkItemID) > 128 {
		return contract.ToolReply{}, ErrProductSplitHeld
	}
	saved, err := k.prepareProductSplitParentScopeTx(ctx, tx, request.Target, request.ParentWorkItemID)
	if err != nil {
		return contract.ToolReply{}, err
	}
	reply, err := productSplitParentScopeToolReply(op, saved)
	if err != nil {
		return contract.ToolReply{}, err
	}
	receipt := productSplitParentScopeToolReceipt{Source: op, ParentWorkItemID: request.ParentWorkItemID, ParentScopeID: saved.ID, ParentScopeDigest: saved.Digest}
	if err := kernelRecordTx(ctx, tx, "product_split_parent_scope_tool", op.ID, op.Target.WaveID, receipt); err != nil {
		return contract.ToolReply{}, err
	}
	return reply, nil
}

func (k *KernelStore) verifyProductSplitParentScopeReplayTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, request contract.SplitProductParentScopeRequest, reply contract.ToolReply) error {
	var receipt productSplitParentScopeToolReceipt
	if err := taskEvidenceRecord(ctx, tx, "product_split_parent_scope_tool", op.ID, &receipt); err != nil {
		return splitEvidenceErr(err)
	}
	if receipt.Source != op || receipt.ParentWorkItemID != request.ParentWorkItemID || request.Target != op.Target {
		return ErrProductSplitHeld
	}
	binding, err := splitChildSourceBindingTx(ctx, tx, op.Target, request.ParentWorkItemID)
	if err != nil {
		return err
	}
	saved, err := loadSplitParentScope(ctx, tx, binding)
	if err != nil {
		return splitEvidenceErr(err)
	}
	if saved.ID != receipt.ParentScopeID || saved.Digest != receipt.ParentScopeDigest || saved.Value.SavedAt.After(k.clock()) {
		return ErrProductSplitHeld
	}
	expected, err := productSplitParentScopeToolReply(op, saved)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(reply, expected) {
		return ErrProductSplitHeld
	}
	return nil
}
