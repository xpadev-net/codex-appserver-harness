package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func resourceScanFixture(t *testing.T) (*KernelStore, PRResourceScan) {
	t.Helper()
	k, _, id, _, proof, o := observedPRFixture(t, false)
	if _, err := k.BindObservedPRPublication(context.Background(), id, proof, o); err != nil {
		t.Fatal(err)
	}
	scan := PRResourceScan{ID: "scan-1", RepositoryID: o.RepositoryID, PRNumber: o.Number, HeadSHA: o.HeadSHA, StartedAt: k.clock()}
	for _, kind := range prSectionKinds {
		scan.Sections = append(scan.Sections, PRResourceSection{Kind: kind, Availability: "complete", Entities: []PRResourceEntity{}})
	}
	return k, scan
}
func setResourceSection(scan *PRResourceScan, kind string, entities ...PRResourceEntity) {
	for i := range scan.Sections {
		if scan.Sections[i].Kind == kind {
			scan.Sections[i].Entities = entities
			scan.Sections[i].Availability = "complete"
			scan.Sections[i].Reason = ""
			return
		}
	}
}
func TestPRResourceScanDiffsCanonicalJSONEditsDeletesReactionsAndDismissals(t *testing.T) {
	k, scan := resourceScanFixture(t)
	ctx := context.Background()
	setResourceSection(&scan, "issue_comment", PRResourceEntity{ID: "11", Data: json.RawMessage(`{"body":"first","user":{"id":8}}`)})
	setResourceSection(&scan, "review", PRResourceEntity{ID: "21", CandidateSHA: scan.HeadSHA, Data: json.RawMessage(`{"state":"APPROVED"}`)})
	setResourceSection(&scan, "reaction", PRResourceEntity{ID: "issue_comment:11:91", Data: json.RawMessage(`{"content":"+1"}`)})
	first, err := k.RecordPRResourceScan(ctx, "", scan)
	if err != nil || first.NotificationState != "pending" {
		t.Fatal(first, err)
	}
	oldVersionID := structureID("pr-state-", first.ResourceKey, first.Digest)
	scan.ID, scan.StartedAt = "scan-2", k.clock()
	setResourceSection(&scan, "issue_comment", PRResourceEntity{ID: "11", Data: json.RawMessage(`{ "user": {"id":8}, "body": "first" }`)})
	unchanged, err := k.RecordPRResourceScan(ctx, first.Digest, scan)
	if err != nil || unchanged.Digest != first.Digest || len(unchanged.Changes) != 0 || unchanged.NotificationState != "unchanged" {
		t.Fatal(unchanged, err)
	}
	scan.ID, scan.StartedAt = "scan-3", k.clock()
	setResourceSection(&scan, "issue_comment", PRResourceEntity{ID: "11", Data: json.RawMessage(`{"body":"edit","user":{"id":8}}`)})
	setResourceSection(&scan, "reaction")
	setResourceSection(&scan, "review", PRResourceEntity{ID: "21", CandidateSHA: scan.HeadSHA, Data: json.RawMessage(`{"state":"DISMISSED"}`)})
	changed, err := k.RecordPRResourceScan(ctx, unchanged.Digest, scan)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"issue_comment:11": "updated", "review:21": "updated", "reaction:issue_comment:11:91": "deleted"}
	if len(changed.Changes) != 3 {
		t.Fatal(changed)
	}
	for _, c := range changed.Changes {
		if want[c.Kind+":"+c.EntityID] != c.Action {
			t.Fatal(c)
		}
	}
	var historical PRResourceState
	if err = k.Record(ctx, "product_pr_state_version", oldVersionID, &historical); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range historical.Sections {
		if s.Kind == "issue_comment" && len(s.Entities) == 1 && strings.Contains(string(s.Entities[0].Data), "first") {
			found = true
		}
	}
	if !found {
		t.Fatal("prior edited/deleted data lost", historical)
	}
	var quality int
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&quality); err != nil || quality != 0 {
		t.Fatal("snapshot minted approval", quality, err)
	}
}
func TestPRResourceUnavailableSectionsRetainFactsAndNeverInventDeletion(t *testing.T) {
	k, scan := resourceScanFixture(t)
	ctx := context.Background()
	setResourceSection(&scan, "issue_comment", PRResourceEntity{ID: "11", Data: json.RawMessage(`{"body":"retained"}`)})
	first, err := k.RecordPRResourceScan(ctx, "", scan)
	if err != nil {
		t.Fatal(err)
	}
	scan.ID, scan.StartedAt = "partial", k.clock()
	for i := range scan.Sections {
		if scan.Sections[i].Kind == "issue_comment" {
			scan.Sections[i].Availability, scan.Sections[i].Reason, scan.Sections[i].Entities = "unavailable", "permission_denied", nil
		}
	}
	partial, err := k.RecordPRResourceScan(ctx, first.Digest, scan)
	if err != nil || len(partial.Changes) != 1 || partial.Changes[0].Action != "availability" {
		t.Fatal(partial, err)
	}
	state, err := k.PRResourceState(ctx, partial.ResourceKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range state.Sections {
		if s.Kind == "issue_comment" && (len(s.Entities) != 1 || s.Availability != "unavailable" || s.Reason != "permission_denied") {
			t.Fatal(s)
		}
	}
	scan.ID, scan.StartedAt = "restored", k.clock()
	setResourceSection(&scan, "issue_comment")
	deleted, err := k.RecordPRResourceScan(ctx, partial.Digest, scan)
	if err != nil || len(deleted.Changes) != 2 {
		t.Fatal(deleted, err)
	}
}
func TestPRResourceScanCASReplayAndAtomicFailureSurviveReopen(t *testing.T) {
	k, scan := resourceScanFixture(t)
	ctx := context.Background()
	first, err := k.RecordPRResourceScan(ctx, "", scan)
	if err != nil {
		t.Fatal(err)
	}
	again, err := k.RecordPRResourceScan(ctx, "", scan)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal(again, err)
	}
	scan.ID = "conflict"
	if _, err = k.RecordPRResourceScan(ctx, "", scan); err == nil {
		t.Fatal("stale scan overwrote state")
	}
	scan.ID = first.ScanID
	scan.HeadSHA = strings.Repeat("c", 40)
	if _, err = k.RecordPRResourceScan(ctx, "", scan); err == nil {
		t.Fatal("scan replay changed")
	}
	scan.ID, scan.StartedAt = "next", k.clock()
	if _, err = k.DB.Exec(`CREATE TRIGGER fail_scan_receipt BEFORE INSERT ON records WHEN NEW.kind='product_pr_scan' BEGIN SELECT RAISE(FAIL,'scan rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = k.RecordPRResourceScan(ctx, first.Digest, scan); err == nil {
		t.Fatal("partial scan committed")
	}
	state, err := k.PRResourceState(ctx, first.ResourceKey)
	if err != nil || state.Digest != first.Digest {
		t.Fatal(state, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER fail_scan_receipt"); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	second, err := k.RecordPRResourceScan(ctx, first.Digest, scan)
	if err != nil || second.Digest == first.Digest {
		t.Fatal(second, err)
	}
	stale := scan
	stale.ID = "older-after-reopen"
	stale.StartedAt = scan.StartedAt.Add(-time.Nanosecond)
	if _, err = k.RecordPRResourceScan(ctx, second.Digest, stale); err == nil {
		t.Fatal("older snapshot applied")
	}
}
func TestPRResourceScanRejectsInvalidUnboundedAndIncompleteInput(t *testing.T) {
	for _, mode := range []string{"unknown_section", "missing_section", "duplicate_section", "invalid_json", "duplicate_entity", "oversized", "availability", "unavailable_entities", "sha", "foreign", "future"} {
		t.Run(mode, func(t *testing.T) {
			k, scan := resourceScanFixture(t)
			switch mode {
			case "unknown_section":
				scan.Sections[0].Kind = "arbitrary_tool"
			case "missing_section":
				scan.Sections = scan.Sections[:7]
			case "duplicate_section":
				scan.Sections[1] = scan.Sections[0]
			case "invalid_json":
				setResourceSection(&scan, "pr", PRResourceEntity{ID: "1", Data: json.RawMessage(`{`)})
			case "duplicate_entity":
				e := PRResourceEntity{ID: "1", Data: json.RawMessage(`{}`)}
				setResourceSection(&scan, "pr", e, e)
			case "oversized":
				setResourceSection(&scan, "pr", PRResourceEntity{ID: "1", Data: json.RawMessage(fmt.Sprintf("%q", strings.Repeat("x", 64<<10)))})
			case "availability":
				scan.Sections[0].Availability = "partial"
			case "unavailable_entities":
				scan.Sections[0].Availability = "unavailable"
				scan.Sections[0].Reason = "denied"
				scan.Sections[0].Entities = []PRResourceEntity{{ID: "1", Data: json.RawMessage(`{}`)}}
			case "sha":
				scan.HeadSHA = "unfixed"
			case "foreign":
				scan.RepositoryID++
			case "future":
				scan.StartedAt = k.clock().Add(time.Hour)
			}
			if _, err := k.RecordPRResourceScan(context.Background(), "", scan); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}

func TestPRResourceScanRetainedUnavailableFactsStillCountTowardCapacity(t *testing.T) {
	k, scan := resourceScanFixture(t)
	ctx := context.Background()
	entities := make([]PRResourceEntity, 2048)
	for i := range entities {
		entities[i] = PRResourceEntity{ID: fmt.Sprint(i), Data: json.RawMessage(`{}`)}
	}
	setResourceSection(&scan, "issue_comment", entities...)
	first, err := k.RecordPRResourceScan(ctx, "", scan)
	if err != nil {
		t.Fatal(err)
	}
	scan.ID, scan.StartedAt = "over-retained", k.clock()
	for i := range scan.Sections {
		if scan.Sections[i].Kind == "issue_comment" {
			scan.Sections[i].Availability = "unavailable"
			scan.Sections[i].Reason = "permission_denied"
			scan.Sections[i].Entities = nil
		}
	}
	setResourceSection(&scan, "review", PRResourceEntity{ID: "new", Data: json.RawMessage(`{}`)})
	if _, err = k.RecordPRResourceScan(ctx, first.Digest, scan); err == nil {
		t.Fatal("unavailable facts evaded retained entity budget")
	}
	state, err := k.PRResourceState(ctx, first.ResourceKey)
	if err != nil || state.Digest != first.Digest {
		t.Fatal(state.Digest, err)
	}
}
