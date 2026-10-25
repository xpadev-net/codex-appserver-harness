package storage

import (
	"context"
	"fmt"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func splitChildScopeParentKnown(ctx context.Context, q queryer, head string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission') AND json_extract(payload,'$.value.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND json_extract(payload,'$.parent_head_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(payload,'$.value.split_scope.parent_head_id'),json_extract(payload,'$.split_scope.parent_head_id'))=?)`, head, head, head).Scan(&known)
	return known, err
}
func splitChildScopeCreationKnown(ctx context.Context, q queryer, id string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records r,json_each(r.payload,'$.value.creations') c WHERE r.kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission') AND json_extract(c.value,'$.id')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(r.payload,'$.value.children') b,json_each(b.value,'$.value.creations') c WHERE r.kind IN ('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission') AND json_extract(c.value,'$.id')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(r.payload,'$.creations') c WHERE r.kind='product_split_child_scope_source' AND json_extract(c.value,'$.id')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(coalesce(json_extract(r.payload,'$.value.split_scope.creations'),json_extract(r.payload,'$.split_scope.creations'))) c WHERE r.kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND json_extract(c.value,'$.id')=?)`, id, id, id, id).Scan(&known)
	return known, err
}
func splitChildScopeIssueKnown(ctx context.Context, q queryer, key string, repositoryID int64, number int) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND id=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission') AND json_extract(payload,'$.value.issue.repository_id')=? AND json_extract(payload,'$.value.issue.number')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(r.payload,'$.value.children') b WHERE r.kind IN ('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission') AND json_extract(b.value,'$.value.issue.repository_id')=? AND json_extract(b.value,'$.value.issue.number')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(payload,'$.value.split_scope.source_id'),json_extract(payload,'$.split_scope.source_id'))=?)`, key, repositoryID, number, repositoryID, number, fmt.Sprintf("r%d-i%d", repositoryID, number)).Scan(&known)
	return known, err
}
func splitChildScopeParentExecutionHeld(ctx context.Context, q queryer, w domain.Wave, workID, allowedHead string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission')
 AND json_extract(payload,'$.value.request.target.wave_id')=? AND json_extract(payload,'$.value.request.parent_work_item_id')=? AND json_extract(payload,'$.value.request.target.scope_revision')=? AND json_extract(payload,'$.value.request.target.policy_revision')=?
 AND (?='' OR coalesce(json_extract(payload,'$.value.parent_head_id'),'')<>?))
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND work_item_id=? AND json_extract(payload,'$.request.parent_work_item_id')=? AND json_extract(payload,'$.request.target.scope_revision')=? AND json_extract(payload,'$.request.target.policy_revision')=?
 AND (?='' OR coalesce(json_extract(payload,'$.parent_head_id'),'')<>?))
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item')
 AND coalesce(json_extract(payload,'$.value.split_scope.request.target.wave_id'),json_extract(payload,'$.split_scope.request.target.wave_id'))=? AND coalesce(json_extract(payload,'$.value.split_scope.request.parent_work_item_id'),json_extract(payload,'$.split_scope.request.parent_work_item_id'))=? AND coalesce(json_extract(payload,'$.value.split_scope.request.target.scope_revision'),json_extract(payload,'$.split_scope.request.target.scope_revision'))=? AND coalesce(json_extract(payload,'$.value.split_scope.request.target.policy_revision'),json_extract(payload,'$.split_scope.request.target.policy_revision'))=?
 AND (?='' OR coalesce(coalesce(json_extract(payload,'$.value.split_scope.parent_head_id'),json_extract(payload,'$.split_scope.parent_head_id')),'')<>?))`, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead, w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead).Scan(&known)
	return known, err
}
