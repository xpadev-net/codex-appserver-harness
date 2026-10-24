package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func publicationScopeFixture(t *testing.T, residual bool) (*KernelStore, domain.Wave, ProductWorkItemRegistrationSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, w, definition := productRegistrationFixture(t, "direct")
	definition.Criteria = []string{"AC"}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal(err)
	}
	var outside ProductWorkItemRegistrationSnapshot
	if residual {
		definition.ID, definition.Criteria = "remaining-work", []string{"extra"}
		var err error
		outside, err = k.RegisterProductWorkItem(ctx, w.Target(), "", definition)
		if err != nil {
			t.Fatal(err)
		}
	}
	return k, w, outside
}

func TestProductPublicationScopePersistsCompleteResidualWithoutExecution(t *testing.T) {
	ctx := context.Background()
	k, w, outside := publicationScopeFixture(t, true)
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work")
	if err != nil || !reflect.DeepEqual(saved.Value.OutsideCriteria, []string{"extra"}) || len(saved.Value.Outside) != 1 || !reflect.DeepEqual(saved.Value.Outside[0], outside) || len(saved.Value.Criteria) != 2 || saved.Value.Included.Value.Definition.ID != "work" {
		t.Fatal(saved, err)
	}
	for i := 0; i < 2; i++ {
		again, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work")
		if err != nil || !reflect.DeepEqual(again, saved) {
			t.Fatal("scope replay changed facts", again, err)
		}
		read, err := k.ReadProductPublicationScope(ctx, w.Target(), "work")
		if err != nil || !reflect.DeepEqual(read, saved) {
			t.Fatal(read, err)
		}
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("scope reset/spent budget", after, err)
	}
	for _, table := range []string{"kernel_tasks", "kernel_run_bindings", "kernel_dispatch_intents", "kernel_tool_operations"} {
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("scope admitted execution", table, count, err)
		}
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(current, w) {
		t.Fatal("scope accepted or replanned Wave", current, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if read, err := k.ReadProductPublicationScope(ctx, w.Target(), "work"); err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal("reopen changed scope", read, err)
	}
}

func TestProductPublicationScopeFullScopeHasNoPeerDependency(t *testing.T) {
	ctx := context.Background()
	k, w, definition := productRegistrationFixture(t, "direct")
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal(err)
	}
	definition.ID = "unrelated-peer"
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", definition.ID); err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work")
	if err != nil || len(saved.Value.OutsideCriteria) != 0 || len(saved.Value.Outside) != 0 {
		t.Fatal("full scope acquired peer dependency", saved, err)
	}
}

func TestProductPublicationScopeRequiresTrackedResidualAndRollsBack(t *testing.T) {
	ctx := context.Background()
	k, w, _ := publicationScopeFixture(t, false)
	if _, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work"); err == nil {
		t.Fatal("untracked residual admitted")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_publication_scope%'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", ProductWorkItemDefinition{ID: "remaining-work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"extra"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_scope BEFORE INSERT ON records WHEN NEW.kind='product_publication_scope' BEGIN SELECT RAISE(FAIL,'scope rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work"); err == nil || !strings.Contains(err.Error(), "scope rollback") {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_publication_scope%'").Scan(&count); err != nil || count != 0 {
		t.Fatal("partial scope saved", count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_scope"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work"); err != nil {
		t.Fatal(err)
	}
}

func TestProductPublicationScopeLostOrChangedEvidenceNeverRegenerates(t *testing.T) {
	for _, mode := range []string{"head", "history", "summary", "residual_head", "residual_head_and_membership", "residual_history", "membership", "new_contributor", "pause", "deadline", "source", "criterion"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, outside := publicationScopeFixture(t, true)
			saved, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work")
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			switch mode {
			case "head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_publication_scope'")
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_publication_scope_history'")
			case "summary":
				changed := saved
				changed.Value.OutsideCriteria = []string{}
				raw, err = json.Marshal(changed)
				if err == nil {
					_, err = k.DB.Exec("UPDATE records SET payload=? WHERE kind='product_publication_scope'", raw)
				}
			case "residual_head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", outside.Value.Definition.ID)
			case "residual_head_and_membership":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','wave_work_item') AND id=?", outside.Value.Definition.ID)
			case "residual_history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration_version' AND id=?", structureID("product-work-", outside.Value.Definition.ID, outside.Digest))
			case "membership":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='wave_work_item' AND id=?", outside.Value.Definition.ID)
			case "new_contributor":
				_, err = k.RegisterProductWorkItem(ctx, w.Target(), "", ProductWorkItemDefinition{ID: "another-contributor", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"extra"}})
			case "pause":
				_, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-scope", Target: w.Target(), Action: "pause", Reason: "fixture"})
			case "deadline":
				k.Now = func() time.Time { return w.Deadline }
			case "source", "criterion":
				changed := w
				if mode == "source" {
					changed.SourceID = "changed-source"
				} else {
					changed.Criteria[0].Requirement = "changed requirement"
				}
				tx, e := k.kernelTx(ctx)
				if e != nil {
					t.Fatal(e)
				}
				err = saveWave(ctx, tx, changed)
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductPublicationScope(ctx, w.Target(), "work"); err == nil {
				t.Fatal("unavailable/changed scope accepted")
			} else if mode == "history" && !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost saved history reported as unprepared scope", err)
			}
			if _, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work"); err == nil {
				t.Fatal("unavailable/changed scope regenerated")
			}
		})
	}
}

func TestProductPublicationScopeNewPlanKeepsOriginalScopeImmutable(t *testing.T) {
	ctx := context.Background()
	k, w, _ := publicationScopeFixture(t, true)
	plan := storagePlanFixture()
	plan.Phases[1].Tasks[0].Criteria = append(plan.Phases[1].Tasks[0].Criteria, "extra")
	if _, err := k.AdoptPlan(ctx, w.Target(), "first", plan); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	for i := range plan.Phases {
		phase := &plan.Phases[i]
		phase.ID = "next-" + phase.ID
		for j := range phase.Tasks {
			phase.Tasks[j].ID = "next-" + phase.Tasks[j].ID
			for d := range phase.Tasks[j].DependsOn {
				phase.Tasks[j].DependsOn[d].TaskID = "next-" + phase.Tasks[j].DependsOn[d].TaskID
			}
		}
		for j := range phase.Completion.RequiredTaskIDs {
			phase.Completion.RequiredTaskIDs[j] = "next-" + phase.Completion.RequiredTaskIDs[j]
		}
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "second", plan); err != nil {
		t.Fatal(err)
	}
	next, err := k.Wave(ctx, w.ID)
	if err != nil || next.PlanRevision == w.PlanRevision {
		t.Fatal(next, err)
	}
	updated, err := k.PrepareProductPublicationScope(ctx, next.Target(), "work")
	if err != nil || updated.ID == saved.ID || updated.Value.Target != next.Target() {
		t.Fatal(updated, err)
	}
	var original ProductPublicationScope
	if err := k.Record(ctx, "product_publication_scope_history", saved.ID, &original); err != nil || !reflect.DeepEqual(original, saved) {
		t.Fatal("new plan rewrote original scope", original, err)
	}
	if next.MainTurns != w.MainTurns || next.TasksCreated < w.TasksCreated || next.Replans != w.Replans+1 {
		t.Fatal("new scope reset Wave budget", next, w)
	}
}
