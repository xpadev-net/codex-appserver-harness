package product

import (
	"context"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkSourceRequestAdoption(t *testing.T, k *storage.KernelStore, w domain.Wave, node storage.ProductContinuation) {
	t.Helper()
	ctx := context.Background()
	observation, err := k.ReadProductSourceObservation(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.ProductSourceRequestAdoptionRequest{Target: w.Target(), DirectionID: w.DirectionID, ObservationID: observation.ID, ObservationDigest: observation.Digest, WorkItemID: node.Value.Request.Definition.ID}
	for _, mode := range []string{"observation", "direction", "work"} {
		wrong := request
		switch mode {
		case "observation":
			wrong.ObservationID = "unobserved"
		case "direction":
			wrong.DirectionID = "unapplied"
		case "work":
			wrong.WorkItemID = node.Value.ParentWorkItemID
		}
		if _, err := k.AdoptProductSourceRequest(ctx, wrong); err == nil {
			t.Fatal("unproven request adopted", mode)
		}
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_request_history BEFORE INSERT ON records WHEN NEW.kind='product_source_request_history' BEGIN SELECT RAISE(FAIL,'request adoption rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptProductSourceRequest(ctx, request); err == nil {
		t.Fatal("fault not reached")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_source_request','product_source_request_history','product_source_request_admission')").Scan(&count); err != nil || count != 0 {
		t.Fatal("partial request adoption", count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_request_history"); err != nil {
		t.Fatal(err)
	}
	adopted, err := k.AdoptProductSourceRequest(ctx, request)
	if err != nil || adopted.Value.RequestDigest != observation.Value.Facts.RequestDigest || adopted.Value.RequestDigest == adopted.Value.OriginalRequestDigest || adopted.Value.ContinuationDigest != node.Digest {
		t.Fatal(adopted, err)
	}
	replay, err := k.AdoptProductSourceRequest(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, adopted) {
		t.Fatal("adoption retry changed request", replay, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(current, w) {
		t.Fatal("adoption changed hold/Wave/budget", current, err)
	}
}

func checkAdoptedRequestTaskEvidence(t *testing.T, k *storage.KernelStore, w domain.Wave, node storage.ProductContinuation) {
	t.Helper()
	ctx := context.Background()
	work := contract.TaskSpec{ID: "new-request-work", Kind: "work", Instruction: "implement explicitly adopted Issue revision", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{node.Value.Request.Definition.ID}}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "new-request", Tasks: []contract.TaskSpec{work}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{work.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	admission, err := k.AdoptPlan(ctx, w.Target(), "adopted-source-plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	prep, err := k.ReadProductTaskPreparation(ctx, task.ID)
	if err != nil || prep.SourceRequest == nil || prep.SourceRequest.Value.RequestDigest == prep.SourceRequest.Value.OriginalRequestDigest {
		t.Fatal("new Task lacks adopted request", prep.SourceRequest, err)
	}
	adopted := *prep.SourceRequest
	request := adopted.Value.Request
	// Save exact trusted fixture records for restoration; never regenerate them.
	rows, err := k.DB.Query("SELECT kind,id,work_item_id,payload FROM records WHERE kind IN ('product_source_request','product_source_request_history','product_source_request_admission') OR (kind='product_source_observation_history' AND id=?) OR (kind='wave_direction' AND id=?)", request.ObservationID, request.DirectionID)
	if err != nil {
		t.Fatal(err)
	}
	type record struct {
		kind, id, owner string
		data            []byte
	}
	var records []record
	for rows.Next() {
		var r record
		if err := rows.Scan(&r.kind, &r.id, &r.owner, &r.data); err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for _, r := range records {
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", r.kind, r.id); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ReadProductTaskPreparation(ctx, task.ID); err == nil {
			t.Fatal("lost request evidence released preparation", r.kind)
		}
		if ready, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: w.Target()}); err != nil || len(ready.Tasks) != 0 {
			t.Fatal("lost request evidence released dispatch", r.kind, ready, err)
		}
		if _, err := k.AdoptProductSourceRequest(ctx, request); err == nil {
			t.Fatal("lost request evidence reconstructed", r.kind)
		}
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", r.kind, r.id, r.owner, r.data); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := k.ReadProductTaskPreparation(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(restored, prep) {
		t.Fatal("restored source changed captured preparation", restored, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	reopened, err := storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.ReadProductTaskPreparation(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(after, prep) {
		t.Fatal("adopted request lost on reopen", after, err)
	}
}
