package storage

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func legacyInventoryFixture(t *testing.T) (*Store, string, string) {
	t.Helper()
	ctx := context.Background()
	path := kernelPath(t)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	scope := verification.Scope{Revision: 1, OriginalRequest: "original request", Criteria: []verification.Criterion{{ID: "AC", Requirement: "whole requirement"}}, Included: []string{"AC"}, Remaining: []string{}, PreviouslyMerged: []string{}}
	policy := strings.Repeat("a", 64)
	root := work()
	root.RepairRounds = 2
	if err := s.ClaimRecords(ctx, root, map[string]any{"scope": scope, "policy_snapshot": policy}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, domain.Event{ID: "split", WorkItemID: root.ID, Kind: "plan_split", Ref: "proposal", ScopeRevision: 1, PolicyRevision: 1}); err != nil {
		t.Fatal(err)
	}
	child := work()
	child.ID = "child"
	child.Issue = 2
	local := scope
	local.OriginalRequest = "child local request"
	local.Criteria = []verification.Criterion{{ID: "AC", Requirement: "distinct child obligation sharing a local ID"}}
	if err := s.RegisterChildren(ctx, root.ID, []ChildRegistration{{Depth: 1, Item: child, Scope: local, Dependencies: []string{}, Source: map[string]string{"digest": "original"}, PolicySnapshot: policy}}); err != nil {
		t.Fatal(err)
	}
	planned, err := s.Get(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, domain.Event{ID: "split-confirmed", WorkItemID: root.ID, Kind: "split_confirmed", Ref: "proposal", Attempt: planned.Attempt, ScopeRevision: 1, PolicyRevision: 1}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"start", "implementation_ready", "candidate_ready", "validation_passed", "gates_passed", "merge_confirmed"} {
		current, err := s.Get(ctx, child.ID)
		if err != nil {
			t.Fatal(err)
		}
		candidate := current.Candidate
		if kind == "implementation_ready" || kind == "candidate_ready" {
			candidate = "fixture-candidate"
		}
		if err := s.Apply(ctx, domain.Event{ID: "child-" + kind, WorkItemID: child.ID, Kind: kind, Candidate: candidate, Attempt: current.Attempt, ScopeRevision: current.ScopeRevision, PolicyRevision: current.PolicyRevision}); err != nil {
			t.Fatal(err)
		}
	}

	next := work()
	next.ID = "child-remaining"
	next.Issue = 2
	next.RepairRounds = 2
	baseline := LegacyInventoryBaseline{ValidationJobs: 5, ObservedTokens: 100}
	if err := s.ClaimNextRecords(ctx, child.ID, next, map[string]any{"scope": local, "policy_snapshot": policy, "continued_from": child.ID, "budget_baseline": baseline}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, domain.Event{ID: "active-start", WorkItemID: next.ID, Kind: "start", ScopeRevision: 1, PolicyRevision: 1}); err != nil {
		t.Fatal(err)
	}
	if started, err := s.BeginDispatch(ctx, "active-start:implement"); err != nil || !started {
		t.Fatal("active fixture not admitted", err)
	}
	if err := s.FinishOperation(ctx, "active-start:implement", "unknown", nil); err != nil {
		t.Fatal(err)
	}
	finalKey := "r1-i1"
	failed := map[string]string{"state": "failed"}
	if err := s.Put(ctx, "parent_final_state", finalKey, root.ID, failed); err != nil {
		t.Fatal(err)
	}
	repair := work()
	repair.ID = "root-final-repair"
	repair.RepairRounds = 2
	if err := s.ClaimFinalRepair(ctx, root.ID, repair, map[string]any{"scope": scope, "policy_snapshot": policy, "budget_baseline": baseline, "final_recovery_audit": map[string]string{"previous": root.ID, "reason": "fixture final recovery", "evidence": "stored evidence"}}, finalKey, failed, map[string]string{"state": "recovering"}); err != nil {
		t.Fatal(err)
	}
	for _, thread := range []struct{ work, thread, state, run string }{{child.ID, "ended-thread", "completed", "ended-run"}, {next.ID, "pending-thread", "unknown", "active-start:implement"}} {
		if err := s.Put(ctx, "role_thread", thread.work+":implementation", thread.work, map[string]any{"work_item_id": thread.work, "role": "implementation", "thread_id": thread.thread, "cwd": "/legacy/workspace", "model": "legacy-model"}); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "agent_run", thread.run, thread.work, contract.SessionRun{ID: thread.run, ThreadID: thread.thread, TurnID: "observed-turn", State: thread.state}); err != nil {
			t.Fatal(err)
		}
	}
	return s, path, kernelPath(t)
}

func freezeLegacyInventory(t *testing.T, source, snapshot string) LegacyKernelImportRequest {
	t.Helper()
	ctx := context.Background()
	backup, err := BackupLegacyDatabase(ctx, source, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(kernelPath(t))
	r := LegacyKernelImportRequest{Key: "inventory-fixture", Source: snapshot, DestinationRoot: filepath.Join(parent, "imported"), Backup: backup}
	if _, err := PrepareLegacyKernelImport(ctx, r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestLegacyInventoryGroupsActualSplitContinuationRepairWithoutReusingEndedThreads(t *testing.T) {
	_, source, snapshot := legacyInventoryFixture(t)
	r := freezeLegacyInventory(t, source, snapshot)
	ctx := context.Background()
	before := backupFileBytes(t, filepath.Join(r.DestinationRoot, "state.db"))
	archive := backupFileBytes(t, filepath.Join(r.DestinationRoot, "legacy-source.db"))
	all := map[string]LegacyInventoryWork{}
	after := ""
	for {
		page, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		checksum, err := contentDigest(page.Value)
		if err != nil || checksum != page.Digest {
			t.Fatal("inventory digest differs", err)
		}
		again, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, after, 1)
		if err != nil || !reflect.DeepEqual(page, again) {
			t.Fatal("same snapshot page changed", err)
		}
		for _, entry := range page.Value.WorkItems {
			if _, duplicate := all[entry.WorkItem.ID]; duplicate {
				t.Fatal("pagination repeated work")
			}
			if entry.RootWorkItemID != "wi" || entry.ScopeDigest == "" || entry.PolicyDigest != strings.Repeat("a", 64) {
				t.Fatal("canonical root/local scope missing", entry)
			}
			all[entry.WorkItem.ID] = entry
		}
		if page.Value.NextAfter == "" {
			break
		}
		after = page.Value.NextAfter
	}
	if len(all) != 4 {
		t.Fatal("work lost", len(all))
	}
	if len(all["child"].Links) != 1 || all["child"].Links[0].Kind != "split_parent" {
		t.Fatal("split link lost")
	}
	if len(all["child-remaining"].Links) != 2 || all["child-remaining"].Links[0].Kind != "continued_from" {
		t.Fatal("continuation lineage lost")
	}
	if len(all["root-final-repair"].Links) != 1 || all["root-final-repair"].Links[0].Kind != "final_recovery_audit" {
		t.Fatal("repair lineage lost")
	}
	if all["child"].ScopeDigest == all["wi"].ScopeDigest {
		t.Fatal("local criterion ID was flattened to the parent obligation")
	}
	for _, id := range []string{"child-remaining", "root-final-repair"} {
		entry := all[id]
		if entry.WorkItem.RepairRounds != 2 || entry.Baseline == nil || entry.Baseline.ValidationJobs != 5 || entry.Baseline.ObservedTokens != 100 {
			t.Fatal("carried counters were reset or summed", id)
		}
	}
	if got := all["child"].Threads; len(got) != 1 || got[0].ThreadID != "ended-thread" || got[0].Disposition != "historical" {
		t.Fatal("ended thread promoted", got)
	}
	if got := all["child-remaining"].Threads; len(got) != 1 || got[0].Disposition != "requires_reconciliation" {
		t.Fatal("unknown run became a binding", got)
	}
	unknown := false
	for _, op := range all["child-remaining"].Operations {
		unknown = unknown || op.ID == "active-start:implement" && op.State == "unknown"
	}
	if !unknown {
		t.Fatal("unknown outbox omitted")
	}
	if !bytes.Equal(before, backupFileBytes(t, filepath.Join(r.DestinationRoot, "state.db"))) || !bytes.Equal(archive, backupFileBytes(t, filepath.Join(r.DestinationRoot, "legacy-source.db"))) {
		t.Fatal("read-only report changed migration data")
	}
	db := openLegacyImportDB(t, r.DestinationRoot, "ro")
	var count int
	if err := db.QueryRow("SELECT count(*) FROM kernel_sessions").Scan(&count); err != nil || count != 0 {
		t.Fatal("inventory bound a thread", count, err)
	}
}

func TestLegacyInventoryReportsUnavailableLineageInsteadOfInventingRoots(t *testing.T) {
	for _, mode := range []string{"missing", "cycle", "ambiguous", "foreign", "scope", "owner", "orphan", "baseline", "parent_columns"} {
		t.Run(mode, func(t *testing.T) {
			s, source, snapshot := legacyInventoryFixture(t)
			ctx := context.Background()
			target := "child"
			switch mode {
			case "missing":
				if err := s.Put(ctx, "split_parent", "child", "child", "absent"); err != nil {
					t.Fatal(err)
				}
			case "cycle":
				if err := s.Put(ctx, "split_parent", "wi", "wi", "child"); err != nil {
					t.Fatal(err)
				}
			case "ambiguous":
				target = "child-remaining"
				if err := s.Put(ctx, "split_parent", target, target, "wi"); err != nil {
					t.Fatal(err)
				}
			case "foreign":
				foreign := work()
				foreign.ID = "foreign"
				foreign.RepositoryID = 99
				foreign.Issue = 77
				if err := s.Claim(ctx, foreign); err != nil {
					t.Fatal(err)
				}
				if err := s.Put(ctx, "split_parent", "child", "child", foreign.ID); err != nil {
					t.Fatal(err)
				}
			case "scope":
				if _, err := s.DB.Exec("DELETE FROM records WHERE kind='scope' AND id='child'"); err != nil {
					t.Fatal(err)
				}
			case "owner":
				if _, err := s.DB.Exec("UPDATE records SET work_item_id='foreign' WHERE kind='split_parent' AND id='child'"); err != nil {
					t.Fatal(err)
				}
			case "baseline":
				target = "child-remaining"
				if err := s.Put(ctx, "budget_baseline", target, target, map[string]any{}); err != nil {
					t.Fatal(err)
				}
			case "parent_columns":
				if _, err := s.DB.Exec("UPDATE work_items SET repository_id=99 WHERE id='wi'"); err != nil {
					t.Fatal(err)
				}
			case "orphan":
				target = "wi"
				if err := s.Put(ctx, "agent_run", "orphan", target, contract.SessionRun{ID: "orphan", ThreadID: "unbound-thread", State: "unknown"}); err != nil {
					t.Fatal(err)
				}
			}
			r := freezeLegacyInventory(t, source, snapshot)
			page, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, entry := range page.Value.WorkItems {
				if entry.WorkItem.ID != target {
					continue
				}
				found = true
				if len(entry.Issues) == 0 {
					t.Fatal("unavailable evidence accepted", mode)
				}
				if mode != "scope" && mode != "orphan" && mode != "baseline" && entry.RootWorkItemID != "" {
					t.Fatal("failed lineage became an initial root", entry.RootWorkItemID)
				}
			}
			if !found {
				t.Fatal("damaged work silently omitted")
			}
		})
	}
}

func TestLegacyInventoryCursorIsBoundToImportAndReadsOnlyOriginalArchive(t *testing.T) {
	_, source, snapshot := legacyInventoryFixture(t)
	r := freezeLegacyInventory(t, source, snapshot)
	ctx := context.Background()
	page, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, "", 1)
	if err != nil || page.Value.NextAfter == "" {
		t.Fatal(err)
	}
	other := r
	other.Key = "other"
	other.DestinationRoot = filepath.Join(filepath.Dir(r.DestinationRoot), "other")
	if _, err := PrepareLegacyKernelImport(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadLegacyMigrationInventory(ctx, other.DestinationRoot, page.Value.NextAfter, 1); err == nil {
		t.Fatal("foreign cursor accepted")
	}
	db := openLegacyImportDB(t, r.DestinationRoot, "rw")
	if _, err := db.Exec("DELETE FROM work_items"); err != nil {
		t.Fatal(err)
	}
	original, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, "", 128)
	if err != nil || len(original.Value.WorkItems) != 4 {
		t.Fatal("mutable prepared rows replaced authenticated original", err)
	}
	if _, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, "not-a-cursor", 1); err == nil {
		t.Fatal("invalid cursor accepted")
	}
	if _, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, "", 129); err == nil {
		t.Fatal("unbounded page accepted")
	}
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 2 {
		t.Fatal(err)
	}
}

func TestLegacyInventoryTruncatedRunHistoryNeverClaimsThreadEnded(t *testing.T) {
	s, source, snapshot := legacyInventoryFixture(t)
	ctx := context.Background()
	if err := s.Put(ctx, "role_thread", "wi:implementation", "wi", map[string]any{"work_item_id": "wi", "role": "implementation", "thread_id": "many-runs", "cwd": "/legacy/workspace", "model": "legacy-model"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 257; i++ {
		id := fmt.Sprintf("history-%04d", i)
		state := "completed"
		if i == 256 {
			state = "unknown"
		}
		if err := s.Put(ctx, "agent_run", id, "wi", contract.SessionRun{ID: id, ThreadID: "many-runs", State: state}); err != nil {
			t.Fatal(err)
		}
	}
	r := freezeLegacyInventory(t, source, snapshot)
	page, err := ReadLegacyMigrationInventory(ctx, r.DestinationRoot, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range page.Value.WorkItems {
		if entry.WorkItem.ID != "wi" {
			continue
		}
		found = true
		limited := false
		for _, issue := range entry.Issues {
			limited = limited || issue.Reason == "run_history_limit"
		}
		if !limited || len(entry.Threads) != 1 || entry.Threads[0].Disposition != "requires_reconciliation" || len(entry.Threads[0].RunIDs) > 256 {
			t.Fatal("partial run history became ended-thread authority", entry)
		}
	}
	if !found {
		t.Fatal("bounded work was omitted")
	}
}
