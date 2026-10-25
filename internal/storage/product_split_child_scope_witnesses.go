package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Unknown content owned by this Wave can hide a seat or parent/child identity.
// Keep that Wave held; JSON probes of other Waves must not fail globally.
func splitScopeUnreadableForWave(ctx context.Context, q queryer, waveID string) (bool, error) {
	var malformed bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE work_item_id=? AND kind IN
 ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_child_scope_source','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_completion','product_work_item_registration','product_work_item_registration_version','wave_work_item')
 AND CASE WHEN NOT json_valid(payload) THEN 1
 WHEN kind IN ('product_work_item_registration','product_work_item_registration_version') THEN
 coalesce(json_type(payload,'$.value')='object' AND json_extract(payload,'$.value.wave_id')=work_item_id AND json_type(payload,'$.value.definition.id')='text',0)=0
 WHEN kind='wave_work_item' THEN
 coalesce(json_type(payload,'$.target')='object' AND json_extract(payload,'$.target.wave_id')=work_item_id,0)=0
 WHEN kind='product_split_child_scope_source' THEN
 coalesce(json_type(payload,'$.request')='object' AND json_extract(payload,'$.request.target.wave_id')=work_item_id AND json_type(payload,'$.request.proposal_id')='text' AND json_type(payload,'$.parent_head_id')='text',0)=0
 ELSE coalesce(json_type(payload,'$.value.request')='object' AND json_extract(payload,'$.value.request.target.wave_id')=work_item_id AND json_type(payload,'$.value.request.proposal_id')='text' AND json_type(payload,'$.value.parent_head_id')='text',0)=0 END)`, waveID).Scan(&malformed)
	if err != nil || malformed {
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
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_completion') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.parent_head_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.parent_head_id'))=?)`, head, head, head).Scan(&known)
	return known, err
}
func splitChildScopeCreationKnown(ctx context.Context, q queryer, id string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.creations') c WHERE r.kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history') AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.children') b,json_each(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.creations') c WHERE r.kind IN ('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_completion') AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.creations') c WHERE r.kind='product_split_child_scope_source' AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(coalesce(json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.split_scope.creations'),json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.split_scope.creations'))) c WHERE r.kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id')=?)`, id, id, id, id).Scan(&known)
	return known, err
}
func splitChildScopeIssueKnown(ctx context.Context, q queryer, key string, repositoryID int64, number int) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND id=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.issue.repository_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.issue.number')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.children') b WHERE r.kind IN ('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_completion') AND json_extract(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.issue.repository_id')=? AND json_extract(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.issue.number')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.source_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.source_id'))=?)`, key, repositoryID, number, repositoryID, number, fmt.Sprintf("r%d-i%d", repositoryID, number)).Scan(&known)
	return known, err
}
func splitChildScopeParentExecutionHeld(ctx context.Context, q queryer, w domain.Wave, workID, allowedHead string) (bool, error) {
	malformed, err := splitScopeUnreadableForWave(ctx, q, w.ID)
	if err != nil || malformed {
		return malformed, err
	}
	var known bool
	err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_completion')
 AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.target.wave_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.parent_work_item_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.target.scope_revision')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.target.policy_revision')=?
 AND (?='' OR coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.parent_head_id'),'')<>?))
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND work_item_id=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.request.parent_work_item_id')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.request.target.scope_revision')=? AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.request.target.policy_revision')=?
 AND (?='' OR coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.parent_head_id'),'')<>?))
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item')
 AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.target.wave_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.target.wave_id'))=? AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.parent_work_item_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.parent_work_item_id'))=? AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.target.scope_revision'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.target.scope_revision'))=? AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.target.policy_revision'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.target.policy_revision'))=?
 AND (?='' OR coalesce(coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.parent_head_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.parent_head_id')),'')<>?))`, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead).Scan(&known)
	return known, err
}
