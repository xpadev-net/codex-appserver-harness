package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

// Physical owner/stop are real; the Issue GET observation is a storage fixture.
func splitObservedSourceFixture(t *testing.T) (*KernelStore, ProductSplitCreate) {
	t.Helper()
	ctx := context.Background()
	k, _, _, job := splitCreationFixture(t)
	identity := startSplitCreationOwner(t, k, job)
	stop, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := process.StopGroup(stop, identity); err != nil {
		t.Fatal(err)
	}
	stopped, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "source-ownership-stopped")
	if err != nil {
		t.Fatal(err)
	}
	observed, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: 900, Number: 99, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: time.Now().UTC()})
	if err != nil || stopped.State != "stopped" || observed.State != "observed" {
		t.Fatal(observed, err)
	}
	return k, observed
}
func splitRootRequest(repositoryID int64, number int, source, id string) ProductWaveRequest {
	return ProductWaveRequest{Wave: WaveRequest{OrchestratorSessionID: "main", Source: source, SourceID: fmt.Sprintf("r%d-i%d", repositoryID, number), OriginalRequest: "edited Issue body now contains a standalone root scope without child markers", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "standalone root claim"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: domain.DefaultKernelLimits()}}, SourcePolicyDigest: strings.Repeat("e", 64), WorkItems: []ProductWorkItemDefinition{{ID: id, Repository: "owner/repo", RepositoryID: repositoryID, Criteria: []string{"AC"}}}}
}
func TestProductSplitChildSourceCannotBecomeAnotherWaveOrDirectAlias(t *testing.T) {
	for _, mode := range []string{"github", "custom_work_id", "direct_alias", "foreign_repo_source", "foreign_repo_alias"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job := splitObservedSourceFixture(t)
			r := splitRootRequest(17, 99, "github", "r17-i99")
			if mode == "custom_work_id" {
				r.WorkItems[0].ID = "new-root-name"
			}
			if mode == "direct_alias" {
				r.Wave.Source = "direct"
				r.Wave.SourceID = "direct-new-request"
			}
			if mode == "foreign_repo_source" || mode == "foreign_repo_alias" {
				r.WorkItems[0].RepositoryID = 18
				r.WorkItems[0].Repository = "owner/another"
				if mode == "foreign_repo_source" {
					r.WorkItems[0].ID = "new-root-name"
				} else {
					r.Wave.Source = "direct"
					r.Wave.SourceID = "direct-other-repo"
				}
			}
			if _, err := k.CreateProductWave(ctx, r); !errors.Is(err, ErrProductSplitChildSourceHeld) {
				t.Fatal("known child admitted as new root", mode, err)
			}
			waves, err := k.Waves(ctx)
			if err != nil || len(waves) != 1 || waves[0].ID != job.Value.Request.Target.WaveID {
				t.Fatal("rejected source left Wave", waves, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_wave_admission'").Scan(&count); err != nil || count != 1 {
				t.Fatal("rejected source left admission", count, err)
			}
		})
	}
}
func TestProductSplitChildSourceCannotBeAddedThroughOrdinaryRegistration(t *testing.T) {
	ctx := context.Background()
	k, job := splitObservedSourceFixture(t)
	before, err := k.WaveOperationBudget(ctx, job.Value.Request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, job.Value.Request.Target, "", ProductWorkItemDefinition{ID: "r17-i99", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitChildSourceHeld) {
		t.Fatal("ordinary registration stole child Issue source", err)
	}
	after, err := k.WaveOperationBudget(ctx, job.Value.Request.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rejected alias changed budget", after, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration' AND id='r17-i99'").Scan(&count); err != nil || count != 0 {
		t.Fatal("alias registration persisted", count, err)
	}
}
func TestProductSplitChildSourceOwnershipSurvivesIndependentEvidenceLoss(t *testing.T) {
	for _, keep := range []string{"resource", "head", "event", "malformed_resource"} {
		t.Run(keep, func(t *testing.T) {
			ctx := context.Background()
			k, _ := splitObservedSourceFixture(t)
			if keep != "resource" && keep != "malformed_resource" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_created_resource'"); err != nil {
					t.Fatal(err)
				}
			}
			if keep != "head" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_create'"); err != nil {
					t.Fatal(err)
				}
			}
			if keep != "event" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_create_event'"); err != nil {
					t.Fatal(err)
				}
			}
			if keep == "malformed_resource" {
				if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='product_split_created_resource'", []byte("{")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.CreateProductWave(ctx, splitRootRequest(17, 99, "github", "r17-i99")); !errors.Is(err, ErrProductSplitChildSourceHeld) {
				t.Fatal("lost evidence freed numeric child source", keep, err)
			}
		})
	}
}
func TestProductSplitChildSourceGuardKeepsRepositoryAndIssueBoundaries(t *testing.T) {
	for _, mode := range []string{"another_issue", "another_repository"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _ := splitObservedSourceFixture(t)
			repositoryID, number := int64(17), 100
			if mode == "another_repository" {
				repositoryID, number = 18, 99
			}
			id := fmt.Sprintf("r%d-i%d", repositoryID, number)
			r := splitRootRequest(repositoryID, number, "github", id)
			if repositoryID == 18 {
				r.WorkItems[0].Repository = "owner/another"
			}
			if _, err := k.CreateProductWave(ctx, r); err != nil {
				t.Fatal("independent source held", mode, err)
			}
		})
	}
}
