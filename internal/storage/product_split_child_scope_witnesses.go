package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Unknown content owned by this Wave can hide a seat or parent/child identity.
// Keep that Wave held; JSON probes of other Waves must not fail globally.
func splitScopeUnreadableForWave(ctx context.Context, q queryer, waveID string) (bool, error) {
	if tx, ok := q.(*sql.Tx); ok {
		return readSplitProof(ctx, tx, "splitScopeUnreadableForWave", waveID, func(read context.Context) (bool, error) {
			return splitScopeUnreadableForWaveUncached(read, tx, waveID)
		})
	}
	return splitScopeUnreadableForWaveUncached(ctx, q, waveID)
}

func splitScopeUnreadableForWaveUncached(ctx context.Context, q queryer, waveID string) (bool, error) {
	const witnessKinds = `('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_child_scope_source','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool','product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor','product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source','product_nested_split_scope_group','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_ancestor_group','product_split_completion','product_work_item_registration','product_work_item_registration_version','wave_work_item')`
	const flatKinds = `('product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool','product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor')`
	const compactKinds = `('product_work_item_registration','product_work_item_registration_version','wave_work_item','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_completion','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history')`
	query := `WITH split_scope_wave_rows(kind,payload,work_item_id) AS MATERIALIZED (
 SELECT kind,payload,work_item_id FROM main.records WHERE work_item_id=? AND kind IN ` + witnessKinds + `
 UNION ALL SELECT 'product_nested_split_allocation_ledger',payload,wave_id FROM kernel_budget_ledger WHERE wave_id=? AND kind='split_children' AND json_type(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.allocation') IS NOT NULL
 UNION ALL SELECT 'product_nested_split_creation_ancestor',ancestry,coalesce(json_extract(CASE WHEN json_valid(ancestry) THEN ancestry ELSE '{}' END,'$.value.request.target.wave_id'),work_item_id) FROM (
  SELECT work_item_id,CASE kind WHEN 'product_split_create' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.value.ancestry') WHEN 'product_split_create_event' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.value.ancestry') WHEN 'product_split_create_gate' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.ancestry') WHEN 'product_split_create_gate_history' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.ancestry') ELSE json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.ancestry') END AS ancestry
  FROM (SELECT kind,payload,work_item_id FROM main.records WHERE kind IN ('product_split_create','product_split_create_event','product_split_create_history','product_split_create_admission','product_split_create_gate','product_split_create_gate_history','product_split_create_retry_budget','product_split_created_resource') AND work_item_id=?
   UNION ALL SELECT kind,payload,work_item_id FROM main.records WHERE kind IN ('product_split_create','product_split_create_event','product_split_create_history','product_split_create_admission','product_split_create_gate','product_split_create_gate_history','product_split_create_retry_budget','product_split_created_resource') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.value.ancestry.value.request.target.wave_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.ancestry.value.request.target.wave_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.ancestry.value.request.target.wave_id'))=? AND work_item_id<>?)
 ) WHERE ancestry IS NOT NULL
 UNION ALL SELECT 'product_nested_split_retry_ancestor',json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.ancestry'),wave_id FROM kernel_budget_ledger WHERE wave_id=? AND kind='split_create_retry' AND json_type(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.ancestry') IS NOT NULL
 UNION ALL SELECT 'product_nested_split_scope_group',json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.scopes'),work_item_id FROM main.records WHERE work_item_id=? AND kind IN ('product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source')
 ), split_compact_ancestor_sets AS MATERIALIZED (
 SELECT kind,payload,
 CASE WHEN kind IN ('product_work_item_registration','product_work_item_registration_version') THEN json_extract(payload,'$.value.split_scope.ancestor_set') WHEN kind='wave_work_item' THEN json_extract(payload,'$.split_scope.ancestor_set') ELSE json_extract(payload,'$.value.ancestor_set') END AS ancestor_set,
 (kind IN ('product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission') OR (kind='product_split_completion' AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.children[0].value.child.value.depth')>=2,0))) AS required
 FROM split_scope_wave_rows WHERE kind IN ` + compactKinds + `
 ) SELECT EXISTS(SELECT 1 FROM split_scope_wave_rows WHERE kind IN ` + witnessKinds + ` AND CASE WHEN NOT json_valid(payload) THEN 1
 WHEN kind IN ('product_work_item_registration','product_work_item_registration_version') THEN coalesce(json_type(payload,'$.value')='object' AND json_extract(payload,'$.value.wave_id')=work_item_id AND json_type(payload,'$.value.definition.id')='text',0)=0
 WHEN kind='wave_work_item' THEN coalesce(json_type(payload,'$.target')='object' AND json_extract(payload,'$.target.wave_id')=work_item_id,0)=0
 WHEN kind='product_split_child_scope_source' THEN coalesce(json_type(payload,'$.request')='object' AND json_extract(payload,'$.request.target.wave_id')=work_item_id AND json_type(payload,'$.request.proposal_id')='text' AND json_type(payload,'$.parent_head_id')='text',0)=0
 ELSE coalesce(json_type(payload,'$.value.request')='object' AND json_extract(payload,'$.value.request.target.wave_id')=work_item_id AND json_type(payload,'$.value.request.proposal_id')='text' AND json_type(payload,'$.value.parent_head_id')='text',0)=0 END)
 OR EXISTS(SELECT 1 FROM split_scope_wave_rows r WHERE r.kind IN ` + flatKinds + ` AND json_valid(r.payload) AND CASE WHEN json_type(r.payload,'$.value.ancestor_witnesses') IS NOT NULL THEN coalesce(json_type(r.payload,'$.value.ancestor_witnesses')='array' AND json_array_length(r.payload,'$.value.ancestor_witnesses') BETWEEN 1 AND 128,0)=0 OR EXISTS(SELECT 1 FROM json_each(CASE WHEN json_type(r.payload,'$.value.ancestor_witnesses')='array' THEN json_extract(r.payload,'$.value.ancestor_witnesses') ELSE '[]' END) a WHERE coalesce(json_type(CASE WHEN json_valid(a.value) THEN a.value ELSE '{}' END,'$.request')='object' AND json_type(CASE WHEN json_valid(a.value) THEN a.value ELSE '{}' END,'$.parent_head_id')='text',0)=0) ELSE coalesce(json_extract(r.payload,'$.value.children[0].value.child.value.depth')>=2,0) END)
 OR EXISTS(SELECT 1 FROM split_compact_ancestor_sets r WHERE CASE WHEN ancestor_set IS NULL THEN required WHEN NOT json_valid(ancestor_set) THEN 1 WHEN coalesce(json_type(ancestor_set)='object' AND json_type(ancestor_set,'$.value.groups')='array' AND json_array_length(ancestor_set,'$.value.groups') BETWEEN 1 AND 128,0)=0 THEN 1 ELSE EXISTS(SELECT 1 FROM json_each(json_extract(ancestor_set,'$.value.groups')) a WHERE coalesce(json_type(CASE WHEN json_valid(a.value) THEN a.value ELSE '{}' END,'$.request')='object' AND json_type(CASE WHEN json_valid(a.value) THEN a.value ELSE '{}' END,'$.parent_head_id')='text',0)=0) END)`
	const waveBindings = 7
	if strings.Count(query, "?") != waveBindings {
		return false, fmt.Errorf("split scope witness bind count: %w", ErrProductSplitHeld)
	}
	args := make([]any, waveBindings)
	for i := range args {
		args[i] = waveID
	}
	var malformed bool
	if err := q.QueryRowContext(ctx, query, args...).Scan(&malformed); err != nil || malformed {
		return malformed, err
	}
	return splitLegacyScopeUnreadableForWave(ctx, q, waveID)
}

// Older history owners identify a proposal, not a Wave. Resolve that owner
// only from authenticated immutable proposal history. An unattributable old
// witness remains a prohibition until the original evidence is restored.
func splitLegacyScopeUnreadableForWave(ctx context.Context, q queryer, waveID string) (bool, error) {
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT json_group_array(work_item_id) FROM (SELECT DISTINCT work_item_id FROM records WHERE kind IN ('product_split_child_scope_history','product_split_child_scopes_history','product_split_registrations_history') AND work_item_id LIKE 'product-split-%'
 AND CASE WHEN NOT json_valid(payload) THEN 1 ELSE coalesce(json_type(payload,'$.value.request.target.wave_id')='text' AND json_extract(payload,'$.value.request.proposal_id')=work_item_id,0)=0 END LIMIT ?)`, maxProductCoverageMerges+1).Scan(&raw)
	if err != nil {
		return false, err
	}
	var owners []string
	if err := json.Unmarshal(raw, &owners); err != nil {
		return false, err
	}
	if len(owners) == 0 {
		return false, nil
	}
	wave, err := loadWave(ctx, q, waveID)
	if err == nil && (wave.Policy.Stage != "P" || !wave.Policy.Product) {
		return false, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	if len(owners) > maxProductCoverageMerges {
		return true, nil
	}
	for _, owner := range owners {
		var proposal ProductSplitProposal
		err := taskEvidenceRecord(ctx, q, "product_split_proposal_history", owner, &proposal)
		if err != nil {
			if err == sql.ErrNoRows || readinessHoldError(err) {
				return true, nil
			}
			return false, err
		}
		digest, err := contentDigest(proposal.Value)
		if err != nil {
			return false, err
		}
		if proposal.ID != owner || proposal.Digest != digest || owner != structureID("product-split-", splitProposalHead(proposal.Value.Request), digest) || !proposal.Value.Request.Target.Valid() || proposal.Value.CreatedAt.IsZero() {
			return true, nil
		}
		if proposal.Value.Request.Target.WaveID == waveID {
			return true, nil
		}
	}
	return false, nil
}

func splitChildScopeParentKnown(ctx context.Context, q queryer, head string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, splitScopeWitnessSQL(`SELECT EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool','product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor','product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source','product_nested_split_scope_group','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_ancestor_group','product_split_completion') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind='product_split_child_scope_source' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.parent_head_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.parent_head_id'))=?)`), head, head, head).Scan(&known)
	return known, err
}

const (
	splitCreationLeafKinds         = `('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history')`
	splitCreationGroupKinds        = `('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool','product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor','product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source','product_nested_split_scope_group','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_ancestor_group','product_split_completion')`
	splitCreationRegistrationKinds = `('product_work_item_registration','product_work_item_registration_version','wave_work_item')`
)

// Walk the negative witness relation once. Separate outer EXISTS branches
// otherwise repeat the complete ancestor expansion for every missing ID.
func splitChildScopeCreationKnownUncached(ctx context.Context, q queryer, id string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, splitScopeWitnessSQL(`SELECT EXISTS(SELECT 1 FROM split_scope_witnesses r WHERE
 (r.kind IN `+splitCreationLeafKinds+` AND EXISTS(SELECT 1 FROM json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.creations') c WHERE json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?))
 OR (r.kind IN `+splitCreationGroupKinds+` AND EXISTS(SELECT 1 FROM json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.children') b,json_each(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.creations') c WHERE json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?))
 OR (r.kind='product_split_child_scope_source' AND EXISTS(SELECT 1 FROM json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.creations') c WHERE json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?))
 OR (r.kind IN `+splitCreationRegistrationKinds+` AND EXISTS(SELECT 1 FROM json_each(coalesce(json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.split_scope.creations'),json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.split_scope.creations'))) c WHERE json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?)))`), id, id, id, id).Scan(&known)
	return known, err
}
func splitChildScopeIssueKnown(ctx context.Context, q queryer, key string, repositoryID int64, number int) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, splitScopeWitnessSQL(`SELECT EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind='product_split_child_scope_source' AND id=?)
 OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.issue.repository_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.issue.number')=?)
 OR EXISTS(SELECT 1 FROM split_scope_witnesses r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.children') b WHERE r.kind IN ('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool','product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor','product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source','product_nested_split_scope_group','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_ancestor_group','product_split_completion') AND json_extract(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.issue.repository_id')=? AND json_extract(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.issue.number')=?)
 OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.source_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.source_id'))=?)`), key, repositoryID, number, repositoryID, number, fmt.Sprintf("r%d-i%d", repositoryID, number)).Scan(&known)
	return known, err
}
func splitChildScopeParentExecutionHeld(ctx context.Context, q queryer, w domain.Wave, workID, allowedHead string) (bool, error) {
	malformed, err := splitScopeUnreadableForWave(ctx, q, w.ID)
	if err != nil || malformed {
		return malformed, err
	}
	var known bool
	err = q.QueryRowContext(ctx, splitScopeWitnessMaterializedSQL(`SELECT EXISTS(SELECT 1 FROM split_scope_witnesses WHERE
 (kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool','product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor','product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source','product_nested_split_scope_group','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_ancestor_group','product_split_completion')
 AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.target.wave_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.parent_work_item_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.target.scope_revision')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.target.policy_revision')=?
 AND (?='' OR coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.parent_head_id'),'')<>?))
 OR (kind='product_split_child_scope_source' AND work_item_id=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.request.parent_work_item_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.request.target.scope_revision')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.request.target.policy_revision')=?
 AND (?='' OR coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.parent_head_id'),'')<>?))
 OR (kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item')
 AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.target.wave_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.target.wave_id'))=? AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.parent_work_item_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.parent_work_item_id'))=? AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.target.scope_revision'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.target.scope_revision'))=? AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.target.policy_revision'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.target.policy_revision'))=?
 AND (?='' OR coalesce(coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.parent_head_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.parent_head_id')),'')<>?)))`), w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead).Scan(&known)
	return known, err
}
