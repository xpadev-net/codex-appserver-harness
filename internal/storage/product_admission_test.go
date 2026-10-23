package storage

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productAdmissionFixture(t *testing.T, source string) (*KernelStore, ProductWaveRequest) {
	t.Helper()
	k, wave := waveStoreFixture(t)
	wave.Source, wave.SourceID = source, "product-source"
	wave.Policy.Stage, wave.Policy.Product = "P", true
	wave.Criteria = append(wave.Criteria, domain.WaveCriterion{ID: "extra", Requirement: "additional behavior"})
	return k, ProductWaveRequest{Wave: wave, WorkItems: []ProductWorkItemDefinition{
		{ID: "work-a", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"extra", "AC"}},
		{ID: "work-b", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}},
	}}
}

func TestProductAdmissionFirstReservedInputIncludesCompleteSource(t *testing.T) {
	for _, source := range []string{"direct", "github"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			k, request := productAdmissionFixture(t, source)
			wave, err := k.CreateProductWave(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			var runs int
			if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
				t.Fatal("source admission started a run", runs, err)
			}
			snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{wave.Target()})
			if err != nil {
				t.Fatal(err)
			}
			reserveInputRun(t, k, "main", snapshot)
			manifest := readSavedInput(t, k, "main", inputContext(t, snapshot).Waves[0].Manifest)
			if len(manifest.ProductWorkItems) != 2 || len(manifest.UnavailableProductWorkItems) != 0 {
				t.Fatal("initial reserved input lost source metadata", manifest.ProductWorkItems, manifest.UnavailableProductWorkItems)
			}
			for _, item := range manifest.ProductWorkItems {
				if item.Value.WaveID != wave.ID || item.Value.Source != source || item.Value.SourceID != request.Wave.SourceID {
					t.Fatal(item)
				}
			}
		})
	}
}

func TestProductAdmissionRollsBackWaveEventsOwnershipAndReceipt(t *testing.T) {
	for _, failure := range []string{"second_work_item", "receipt"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			k, request := productAdmissionFixture(t, "direct")
			condition := "NEW.kind='product_work_item_registration_version' AND NEW.work_item_id != '' AND (SELECT count(*) FROM records WHERE kind='product_work_item_registration_version')=1"
			if failure == "receipt" {
				condition = "NEW.kind='product_wave_admission'"
			}
			if _, err := k.DB.ExecContext(ctx, "CREATE TRIGGER fail_product_admission BEFORE INSERT ON records WHEN "+condition+" BEGIN SELECT RAISE(FAIL,'admission rollback'); END"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.CreateProductWave(ctx, request); err == nil || !strings.Contains(err.Error(), "admission rollback") {
				t.Fatal("injected failure not reached", err)
			}
			for _, query := range []string{
				"SELECT count(*) FROM kernel_waves", "SELECT count(*) FROM kernel_events", "SELECT count(*) FROM kernel_notifications",
				"SELECT count(*) FROM records WHERE kind IN ('product_wave_admission','product_work_item_registration','product_work_item_registration_version','wave_work_item','wave_operation_limits','notification_intent')",
			} {
				var count int
				if err := k.DB.QueryRowContext(ctx, query).Scan(&count); err != nil || count != 0 {
					t.Fatal("partial admission committed", query, count, err)
				}
			}
			if _, err := k.DB.ExecContext(ctx, "DROP TRIGGER fail_product_admission"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.CreateProductWave(ctx, request); err != nil {
				t.Fatal("rolled-back source identity cannot be retried", err)
			}
		})
	}
}

func TestProductAdmissionRetryIsReadOnlyAcrossPauseRevisionAndReopen(t *testing.T) {
	ctx := context.Background()
	k, request := productAdmissionFixture(t, "direct")
	wave, err := k.CreateProductWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	request.WorkItems[0], request.WorkItems[1] = request.WorkItems[1], request.WorkItems[0]
	request.WorkItems[1].Criteria = []string{"AC", "extra"}
	mustControl(t, k, operatorRequest(wave, "pause-source", "pause", ""))
	paused, err := k.CreateProductWave(ctx, request)
	if err != nil || !paused.Paused || paused.ID != wave.ID {
		t.Fatal(paused, err)
	}
	mustControl(t, k, operatorRequest(wave, "resume-source", "resume", ""))
	var before ProductWorkItemRegistrationSnapshot
	if err := k.Record(ctx, "product_work_item_registration", "work-a", &before); err != nil {
		t.Fatal(err)
	}
	direction, err := k.RequestWaveDirection(ctx, directionRequest(wave))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	wave, err = k.Wave(ctx, wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	definition := before.Value.Definition
	definition.Criteria = []string{"AC"}
	updated, err := k.RegisterProductWorkItem(ctx, wave.Target(), before.Digest, definition)
	if err != nil {
		t.Fatal(err)
	}
	var eventCount int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_events").Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replayed, err := k.CreateProductWave(ctx, request)
	if err != nil || !reflect.DeepEqual(wave, replayed) {
		t.Fatal("retry changed current Wave", replayed, wave, err)
	}
	var after ProductWorkItemRegistrationSnapshot
	if err := k.Record(ctx, "product_work_item_registration", definition.ID, &after); err != nil || !reflect.DeepEqual(updated, after) {
		t.Fatal("retry restored obsolete source metadata", after, err)
	}
	var afterCount int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_events").Scan(&afterCount); err != nil || afterCount != eventCount {
		t.Fatal("retry re-emitted admission events", afterCount, eventCount, err)
	}
}

func TestProductAdmissionRejectsConflictingReplayAndExistingWave(t *testing.T) {
	ctx := context.Background()
	k, request := productAdmissionFixture(t, "github")
	if _, err := k.CreateProductWave(ctx, request); err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.WorkItems = append([]ProductWorkItemDefinition{}, request.WorkItems...)
	changed.WorkItems[0].RepositoryID++
	if _, err := k.CreateProductWave(ctx, changed); err == nil {
		t.Fatal("source retry changed WorkItem ownership")
	}
	changed = request
	changed.Wave.SourceID = "second-source"
	if _, err := k.CreateProductWave(ctx, changed); err == nil {
		t.Fatal("new Wave stole existing WorkItems")
	}
	waves, err := k.Waves(ctx)
	if err != nil || len(waves) != 1 {
		t.Fatal("failed admission left a Wave", waves, err)
	}
	request.Wave.SourceID = "old-source"
	old, err := k.CreateWave(ctx, request.Wave)
	if err != nil {
		t.Fatal(err)
	}
	request.WorkItems[0].ID, request.WorkItems[1].ID = "new-a", "new-b"
	if _, err := k.CreateProductWave(ctx, request); err == nil {
		t.Fatal("visible Wave was retrofitted with initial ownership", old)
	}
	request.Wave.SourceID = "invalid-duplicate"
	request.WorkItems[1].ID = request.WorkItems[0].ID
	if _, err := k.CreateProductWave(ctx, request); err == nil {
		t.Fatal("duplicate source WorkItems accepted")
	}
	request.WorkItems[1].ID = "new-b"
	request.WorkItems[1].Criteria = []string{"unknown"}
	if _, err := k.CreateProductWave(ctx, request); err == nil {
		t.Fatal("unregistered source criterion accepted")
	}
	request.WorkItems[1].Criteria = []string{"AC"}
	request.Wave.Policy.Stage, request.Wave.Policy.Product = "K", false
	if _, err := k.CreateProductWave(ctx, request); err == nil {
		t.Fatal("kernel request promoted to product admission")
	}
}
