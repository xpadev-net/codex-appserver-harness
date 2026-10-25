package storage

import (
	"context"
	"encoding/json"
	"strings"
)

// A cursor locates an opaque row, not authority. Decode one bounded witness at
// a time so a corrupt payload cannot abort JSON scans of healthy later rows.
func splitScopeWitnessCreation(ctx context.Context, q queryer, kind, key string) (string, error) {
	var id string
	var err error
	switch kind {
	case "product_nested_split_allocation_ledger", "product_nested_split_creation_ancestor", "product_nested_split_retry_ancestor":
		var raw []byte
		err = q.QueryRowContext(ctx, splitScopeWitnessSQL("SELECT payload FROM split_scope_witnesses WHERE kind=? AND id=?"), kind, key).Scan(&raw)
		if err == nil {
			var saved ProductNestedSplitAllocation
			if decodeErr := json.Unmarshal(raw, &saved); decodeErr != nil {
				return "", ErrTaskEvidence
			}
			if len(saved.Value.Children) > 0 {
				id = saved.Value.Children[0].Value.CreationID
			}
		}

	case "product_nested_split_scope_group":
		var raw []byte
		err = q.QueryRowContext(ctx, splitScopeWitnessSQL("SELECT payload FROM split_scope_witnesses WHERE kind=? AND id=?"), kind, key).Scan(&raw)
		if err == nil {
			var scopes ProductSplitChildScopes
			if decodeErr := json.Unmarshal(raw, &scopes); decodeErr != nil {
				return "", ErrTaskEvidence
			}
			if len(scopes.Value.Children) > 0 {
				id = scopes.Value.Children[0].Value.CreationID
			}
		}
	case "product_split_child_scope", "product_split_child_scope_history", "product_split_child_scope_admission", "product_split_child_source", "product_split_child_source_history", "product_split_child_source_policy", "product_split_child_source_policy_history":
		var binding ProductSplitChildScopeBinding
		err = taskEvidenceRecord(ctx, q, kind, key, &binding)
		id = binding.Value.CreationID
	case "product_split_child_scopes", "product_split_child_scopes_history", "product_split_child_scopes_admission":
		var scopes ProductSplitChildScopes
		err = taskEvidenceRecord(ctx, q, kind, key, &scopes)
		if len(scopes.Value.Children) > 0 {
			id = scopes.Value.Children[0].Value.CreationID
		}
	case "product_split_registrations", "product_split_registrations_history", "product_split_registrations_admission", "product_split_parent_scope", "product_split_parent_scope_history", "product_split_parent_scope_admission", "product_nested_split_proposal", "product_nested_split_proposal_history", "product_nested_split_proposal_admission", "product_nested_split_proposal_request", "product_nested_split_children", "product_nested_split_children_history", "product_nested_split_children_admission", "product_nested_split_child_intent", "product_nested_split_child_intent_history", "product_nested_split_child_intent_admission", "product_nested_split_allocation", "product_nested_split_allocation_history", "product_nested_split_allocation_admission", "product_nested_split_work_item_seat", "product_nested_split_child_scopes", "product_nested_split_child_scopes_history", "product_nested_split_child_scopes_admission", "product_nested_split_child_scope", "product_nested_split_child_scope_history", "product_nested_split_child_scope_admission", "product_nested_split_child_scope_source":
		var registered ProductSplitRegistrations
		err = taskEvidenceRecord(ctx, q, kind, key, &registered)
		if len(registered.Value.Children) > 0 {
			id = registered.Value.Children[0].Value.CreationID
		}
	case "product_split_child_scope_source":
		var source splitChildScopeSource
		err = taskEvidenceRecord(ctx, q, kind, key, &source)
		id = source.CreationID
	case "product_split_completion":
		var completion productSplitCompletion
		err = taskEvidenceRecord(ctx, q, kind, key, &completion)
		if len(completion.Value.Children) > 0 {
			id = completion.Value.Children[0].Value.CreationID
		}
	case "product_work_item_registration", "product_work_item_registration_version", "wave_work_item":
		var reference *ProductSplitChildScopeReference
		if kind == "wave_work_item" {
			var member struct {
				SplitScope *ProductSplitChildScopeReference `json:"split_scope"`
			}
			err = taskEvidenceRecord(ctx, q, kind, key, &member)
			reference = member.SplitScope
		} else {
			var registration ProductWorkItemRegistrationSnapshot
			err = taskEvidenceRecord(ctx, q, kind, key, &registration)
			reference = registration.Value.SplitScope
		}
		if reference != nil && len(reference.Creations) > 0 && len(reference.Creations) <= 33 {
			id = reference.Creations[len(reference.Creations)-1].ID
		}
	default:
		return "", ErrTaskEvidence
	}
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(id, "split-create-") || len(id) <= len("split-create-") || len(id) > 192 {
		return "", ErrTaskEvidence
	}
	return id, nil
}
