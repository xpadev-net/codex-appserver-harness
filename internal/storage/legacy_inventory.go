package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

type LegacyInventoryIssue struct {
	WorkItemID string `json:"work_item_id"`
	Reason     string `json:"reason"`
}

type LegacyInventoryLink struct {
	ChildID  string `json:"child_id"`
	ParentID string `json:"parent_id"`
	Kind     string `json:"kind"`
}

type LegacyInventoryThread struct {
	Role        string   `json:"role"`
	ThreadID    string   `json:"thread_id"`
	CWD         string   `json:"cwd"`
	Model       string   `json:"model"`
	Disposition string   `json:"disposition"`
	RunIDs      []string `json:"run_ids"`
}

// Baselines are carried values, not increments. The later family converter
// must deduplicate run IDs and inherited repair/baseline counters explicitly.
type LegacyInventoryBaseline struct {
	ValidationJobs int   `json:"validation_jobs"`
	ObservedTokens int64 `json:"observed_tokens"`
}

type LegacyInventoryEvidence struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type LegacyInventoryWork struct {
	WorkItem       domain.WorkItem          `json:"work_item"`
	RootWorkItemID string                   `json:"root_work_item_id"`
	Links          []LegacyInventoryLink    `json:"links"`
	ScopeDigest    string                   `json:"scope_digest"`
	PolicyDigest   string                   `json:"policy_digest"`
	Baseline       *LegacyInventoryBaseline `json:"baseline,omitempty"`
	// FinalAcceptance and CompletionProof are preserved verbatim as historical
	// evidence. They are not trusted as authorization by the Wave runtime.
	FinalAcceptance json.RawMessage           `json:"final_acceptance,omitempty"`
	CompletionProof json.RawMessage           `json:"completion_proof,omitempty"`
	FinalHistory    []LegacyInventoryEvidence `json:"final_history"`
	Operations      []domain.Operation        `json:"operations"`
	Threads         []LegacyInventoryThread   `json:"threads"`
	Issues          []LegacyInventoryIssue    `json:"issues"`
}

type LegacyInventoryValue struct {
	ImportID     string                `json:"import_id"`
	ImportDigest string                `json:"import_digest"`
	ArchiveSHA   string                `json:"archive_sha"`
	After        string                `json:"after"`
	NextAfter    string                `json:"next_after"`
	WorkItems    []LegacyInventoryWork `json:"work_items"`
}

type LegacyInventoryPage struct {
	Value  LegacyInventoryValue `json:"value"`
	Digest string               `json:"digest"`
}

type legacyInventoryCursor struct {
	ImportID string `json:"import_id"`
	Row      int64  `json:"row"`
}

// ReadLegacyMigrationInventory reads only the authenticated original archive.
// It creates no bindings, claims, budget, Tasks or dispatch permits. A thread
// marked historical must never be reused; an unresolved run requires actual
// process/provider reconciliation, not inference from this stored report.
func ReadLegacyMigrationInventory(ctx context.Context, root, after string, limit int) (LegacyInventoryPage, error) {
	var page LegacyInventoryPage
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return page, errors.New("canonical import root required")
	}
	if err := privateBackupPath(filepath.Join(root, "state.db")); err != nil {
		return page, err
	}
	if limit < 1 || limit > 128 || len(after) > 512 {
		return page, errors.New("bounded legacy inventory page required")
	}
	release, err := lockLegacyBackupSource(filepath.Join(root, "state.db"))
	if err != nil {
		return page, err
	}
	defer release()
	imported, err := InspectLegacyKernelImport(ctx, root)
	if err != nil {
		return page, err
	}
	var cursor legacyInventoryCursor
	if after != "" {
		raw, err := base64.RawURLEncoding.DecodeString(after)
		if err != nil {
			return page, err
		}
		if err := contract.DecodeStrict(raw, &cursor); err != nil || cursor.ImportID != imported.ID {
			return page, errors.New("inventory cursor belongs to another import")
		}
	}
	dsn, err := sqliteFileDSN(filepath.Join(root, "legacy-source.db"), "ro")
	if err != nil {
		return page, err
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return page, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SELECT rowid,id,repository_id,issue,data FROM work_items WHERE (?='' OR rowid>?) ORDER BY rowid LIMIT ?", after, cursor.Row, limit+1)
	if err != nil {
		return page, err
	}
	type sourceRow struct {
		row, repository int64
		id              string
		issue           int
		data            []byte
	}
	var found []sourceRow
	for rows.Next() {
		var row sourceRow
		if err := rows.Scan(&row.row, &row.id, &row.repository, &row.issue, &row.data); err != nil {
			rows.Close()
			return page, err
		}
		found = append(found, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return page, err
	}
	page.Value = LegacyInventoryValue{ImportID: imported.ID, ImportDigest: imported.Digest, ArchiveSHA: imported.Value.Request.Backup.SHA256, After: after, WorkItems: []LegacyInventoryWork{}}
	if len(found) > limit {
		last := found[limit-1].row
		raw, err := json.Marshal(legacyInventoryCursor{imported.ID, last})
		if err != nil {
			return page, err
		}
		page.Value.NextAfter = base64.RawURLEncoding.EncodeToString(raw)
		found = found[:limit]
	}
	for _, row := range found {
		entry := LegacyInventoryWork{FinalHistory: []LegacyInventoryEvidence{}, Operations: []domain.Operation{}, Threads: []LegacyInventoryThread{}, Issues: []LegacyInventoryIssue{}}
		if err := json.Unmarshal(row.data, &entry.WorkItem); err != nil || entry.WorkItem.ID != row.id || entry.WorkItem.RepositoryID != row.repository || entry.WorkItem.Issue != row.issue || row.id == "" || row.repository < 1 || row.issue < 1 {
			entry.WorkItem = domain.WorkItem{ID: row.id, RepositoryID: row.repository, Issue: row.issue}
			entry.Issues = append(entry.Issues, LegacyInventoryIssue{row.id, "work_item_identity_invalid"})
		} else {
			entry.RootWorkItemID, entry.Links, err = legacyInventoryLineage(ctx, db, entry.WorkItem)
			if err != nil {
				entry.Issues = append(entry.Issues, LegacyInventoryIssue{row.id, err.Error()})
			}
		}
		if err := fillLegacyInventoryWork(ctx, db, &entry); err != nil {
			return page, err
		}
		page.Value.WorkItems = append(page.Value.WorkItems, entry)
	}
	page.Digest, err = contentDigest(page.Value)
	return page, err
}

func legacyInventoryRecord(ctx context.Context, db *sql.DB, kind, id, owner string, out any) error {
	var recordedOwner string
	var payload []byte
	if err := db.QueryRowContext(ctx, "SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", kind, id).Scan(&recordedOwner, &payload); err != nil {
		return err
	}
	if recordedOwner != owner {
		return errors.New("legacy record owner differs")
	}
	return json.Unmarshal(payload, out)
}

func legacyInventoryWorkItem(ctx context.Context, db *sql.DB, id string) (domain.WorkItem, error) {
	var item domain.WorkItem
	var recordedID string
	var repository int64
	var issue int
	var raw []byte
	if err := db.QueryRowContext(ctx, "SELECT id,repository_id,issue,data FROM work_items WHERE id=?", id).Scan(&recordedID, &repository, &issue, &raw); err != nil {
		return item, err
	}
	if json.Unmarshal(raw, &item) != nil || item.ID != recordedID || recordedID != id || item.RepositoryID != repository || item.Issue != issue || repository < 1 || issue < 1 {
		return item, errors.New("work_item_identity_invalid")
	}
	return item, nil
}

func legacyInventoryLineage(ctx context.Context, db *sql.DB, item domain.WorkItem) (string, []LegacyInventoryLink, error) {
	var links []LegacyInventoryLink
	seen := map[string]bool{}
	for len(seen) < maxProductWorkItems {
		if seen[item.ID] {
			return "", links, errors.New("lineage_cycle")
		}
		seen[item.ID] = true
		parentID := ""
		for _, kind := range []string{"split_parent", "continued_from", "final_recovery_audit"} {
			var parent string
			var err error
			if kind == "final_recovery_audit" {
				var value struct {
					Previous string `json:"previous"`
				}
				err = legacyInventoryRecord(ctx, db, kind, item.ID, item.ID, &value)
				parent = value.Previous
			} else {
				err = legacyInventoryRecord(ctx, db, kind, item.ID, item.ID, &parent)
			}
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil || parent == "" || parentID != "" && parentID != parent {
				return "", links, errors.New("lineage_parent_ambiguous")
			}
			previous, err := legacyInventoryWorkItem(ctx, db, parent)
			if errors.Is(err, sql.ErrNoRows) {
				return "", links, errors.New("lineage_parent_missing")
			}
			if err != nil || previous.ID != parent || previous.RepositoryID != item.RepositoryID || kind != "split_parent" && previous.Issue != item.Issue {
				return "", links, errors.New("lineage_parent_identity_invalid")
			}
			links = append(links, LegacyInventoryLink{item.ID, parent, kind})
			parentID = parent
		}
		if parentID == "" {
			// Older partial continuations predate continued_from. The suffix
			// alone is not evidence: require identity and the exact residual scope.
			previousID := strings.TrimSuffix(item.ID, "-remaining")
			if previousID == item.ID {
				return item.ID, links, nil
			}
			previous, err := legacyInventoryWorkItem(ctx, db, previousID)
			if err != nil || previous.RepositoryID != item.RepositoryID || previous.Issue != item.Issue || previous.ScopeRevision > item.ScopeRevision || previous.PolicyRevision > item.PolicyRevision {
				return "", links, errors.New("legacy_continuation_unavailable")
			}
			var before, after verification.Scope
			if legacyInventoryRecord(ctx, db, "scope", previousID, previousID, &before) != nil || legacyInventoryRecord(ctx, db, "scope", item.ID, item.ID, &after) != nil || before.Validate() != nil || after.Validate() != nil || before.Revision != previous.ScopeRevision || after.Revision != item.ScopeRevision || len(before.Remaining) == 0 {
				return "", links, errors.New("legacy_continuation_scope_unavailable")
			}
			expected := before
			expected.Included = append([]string{}, before.Remaining...)
			expected.Remaining = []string{}
			expected.PreviouslyMerged = append(append([]string{}, before.PreviouslyMerged...), before.Included...)
			if !reflect.DeepEqual(expected, after) {
				return "", links, errors.New("legacy_continuation_scope_unavailable")
			}
			links = append(links, LegacyInventoryLink{item.ID, previousID, "continued_from"})
			parentID = previousID
		}
		var err error
		item, err = legacyInventoryWorkItem(ctx, db, parentID)
		if err != nil {
			return "", links, err
		}
	}
	return "", links, errors.New("lineage_depth_exceeded")
}

func fillLegacyInventoryWork(ctx context.Context, db *sql.DB, entry *LegacyInventoryWork) error {
	id := entry.WorkItem.ID
	issue := func(reason string) { entry.Issues = append(entry.Issues, LegacyInventoryIssue{id, reason}) }
	var scope verification.Scope
	finalKey := fmt.Sprintf("r%d-i%d", entry.WorkItem.RepositoryID, entry.WorkItem.Issue)
	for _, record := range []struct {
		kind string
		out  *json.RawMessage
	}{{"parent_final_state", &entry.FinalAcceptance}, {"parent_completion", &entry.CompletionProof}} {
		var owner string
		var payload []byte
		err := db.QueryRowContext(ctx, "SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", record.kind, finalKey).Scan(&owner, &payload)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if !json.Valid(payload) {
			issue(record.kind + "_identity_invalid")
			continue
		}
		if owner != id {
			ownerItem, ownerErr := legacyInventoryWorkItem(ctx, db, owner)
			ownerRoot := ""
			if ownerErr == nil && ownerItem.RepositoryID == entry.WorkItem.RepositoryID && ownerItem.Issue == entry.WorkItem.Issue {
				ownerRoot, _, ownerErr = legacyInventoryLineage(ctx, db, ownerItem)
			}
			if ownerErr != nil || ownerRoot != entry.RootWorkItemID {
				issue(record.kind + "_identity_invalid")
			}
			continue
		}
		*record.out = append(json.RawMessage(nil), payload...)
	}
	historyRows, err := db.QueryContext(ctx, "SELECT id,payload FROM records WHERE kind='parent_final_history' AND work_item_id=? ORDER BY id LIMIT 257", id)
	if err != nil {
		return err
	}
	for count := 0; historyRows.Next(); count++ {
		if count >= 256 {
			issue("parent_final_history_limit")
			break
		}
		var evidence LegacyInventoryEvidence
		if err := historyRows.Scan(&evidence.ID, &evidence.Payload); err != nil {
			historyRows.Close()
			return err
		}
		if !json.Valid(evidence.Payload) {
			issue("parent_final_history_invalid")
			continue
		}
		evidence.Payload = append(json.RawMessage(nil), evidence.Payload...)
		entry.FinalHistory = append(entry.FinalHistory, evidence)
	}
	if err := historyRows.Err(); err != nil {
		historyRows.Close()
		return err
	}
	historyRows.Close()
	if err := legacyInventoryRecord(ctx, db, "scope", id, id, &scope); err != nil || scope.Validate() != nil || scope.Revision != entry.WorkItem.ScopeRevision {
		issue("scope_unavailable")
	} else {
		entry.ScopeDigest, _ = contentDigest(scope)
	}
	if err := legacyInventoryRecord(ctx, db, "policy_snapshot", id, id, &entry.PolicyDigest); err != nil || !validSourceDigest(entry.PolicyDigest) {
		entry.PolicyDigest = ""
		issue("policy_snapshot_unavailable")
	}
	var baseline struct {
		ValidationJobs *int   `json:"validation_jobs"`
		ObservedTokens *int64 `json:"observed_tokens"`
	}
	if err := legacyInventoryRecord(ctx, db, "budget_baseline", id, id, &baseline); err == nil {
		if baseline.ValidationJobs == nil || baseline.ObservedTokens == nil || *baseline.ValidationJobs < 0 || *baseline.ObservedTokens < 0 {
			issue("budget_baseline_invalid")
		} else {
			entry.Baseline = &LegacyInventoryBaseline{*baseline.ValidationJobs, *baseline.ObservedTokens}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		issue("budget_baseline_invalid")
	} else {
		for _, link := range entry.Links {
			if link.ChildID == id && (link.Kind == "continued_from" || link.Kind == "final_recovery_audit") {
				issue("budget_baseline_missing")
				break
			}
		}
	}
	rows, err := db.QueryContext(ctx, "SELECT id,payload,state FROM operation_outbox WHERE work_item_id=? AND state NOT IN ('done','canceled') ORDER BY id LIMIT 257", id)
	if err != nil {
		return err
	}
	opCount := 0
	for rows.Next() {
		opCount++
		if opCount > 256 {
			issue("operation_history_limit")
			break
		}
		var op domain.Operation
		var opID, state string
		var payload []byte
		if err := rows.Scan(&opID, &payload, &state); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal(payload, &op); err != nil || op.ID != opID || op.WorkItemID != id {
			issue("operation_identity_invalid")
			continue
		}
		op.State = state
		entry.Operations = append(entry.Operations, op)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	runs, err := db.QueryContext(ctx, "SELECT id,payload FROM records WHERE kind='agent_run' AND work_item_id=? ORDER BY id LIMIT 257", id)
	if err != nil {
		return err
	}
	type runHeader struct {
		ID       string `json:"id"`
		ThreadID string `json:"thread_id"`
		State    string `json:"state"`
	}
	byThread := map[string][]runHeader{}
	runCount := 0
	runsTruncated := false
	runsUnassignable := false
	for runs.Next() {
		runCount++
		if runCount > 256 {
			issue("run_history_limit")
			runsTruncated = true
			break
		}
		var runID string
		var raw []byte
		var run runHeader
		if err := runs.Scan(&runID, &raw); err != nil {
			runs.Close()
			return err
		}
		if json.Unmarshal(raw, &run) != nil || run.ID != runID || run.ThreadID == "" {
			issue("run_identity_invalid")
			runsUnassignable = true
			continue
		}
		byThread[run.ThreadID] = append(byThread[run.ThreadID], run)
	}
	err = runs.Err()
	runs.Close()
	if err != nil {
		return err
	}
	matched := map[string]bool{}
	for _, role := range []string{"implementation", "verification"} {
		var session struct {
			WorkItemID string `json:"work_item_id"`
			Role       string `json:"role"`
			ThreadID   string `json:"thread_id"`
			CWD        string `json:"cwd"`
			Model      string `json:"model"`
		}
		err := legacyInventoryRecord(ctx, db, "role_thread", id+":"+role, id, &session)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil || session.WorkItemID != id || session.Role != role || session.ThreadID == "" {
			issue("thread_identity_invalid")
			continue
		}
		if matched[session.ThreadID] {
			issue("thread_alias")
		}
		matched[session.ThreadID] = true
		thread := LegacyInventoryThread{Role: role, ThreadID: session.ThreadID, CWD: session.CWD, Model: session.Model, Disposition: "historical", RunIDs: []string{}}
		for _, run := range byThread[session.ThreadID] {
			thread.RunIDs = append(thread.RunIDs, run.ID)
			switch run.State {
			case "completed", "failed", "interrupted", "blocked":
			default:
				thread.Disposition = "requires_reconciliation"
			}
		}
		if runsTruncated || runsUnassignable {
			thread.Disposition = "requires_reconciliation"
		}
		if thread.Disposition == "requires_reconciliation" {
			issue("thread_run_unresolved")
		}
		entry.Threads = append(entry.Threads, thread)
	}
	var orphanIDs []string
	for threadID := range byThread {
		if !matched[threadID] {
			orphanIDs = append(orphanIDs, threadID)
		}
	}
	sort.Strings(orphanIDs)
	for _, threadID := range orphanIDs {
		orphan := LegacyInventoryThread{ThreadID: threadID, Disposition: "requires_reconciliation", RunIDs: []string{}}
		for _, run := range byThread[threadID] {
			orphan.RunIDs = append(orphan.RunIDs, run.ID)
		}
		entry.Threads = append(entry.Threads, orphan)
		issue("run_thread_unbound")
	}
	sort.Slice(entry.Issues, func(i, j int) bool { return entry.Issues[i].Reason < entry.Issues[j].Reason })
	return nil
}
