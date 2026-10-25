package product

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type issueAdmissionAPI struct {
	issue        github.Issue
	dependencies map[string]github.Issue
	reads        []string
	issueReads   int
	metaReads    int
	repositoryID int64
	onFinal      func(*issueAdmissionAPI)
	fail         string
}

func (a *issueAdmissionAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.reads = append(a.reads, path)
	if path == a.fail {
		return nil, errors.New("source read unavailable")
	}
	if path == "" {
		a.metaReads++
		return json.Marshal(map[string]any{"id": a.repositoryID, "full_name": "owner/repo"})
	}
	if path == "issues/42" {
		a.issueReads++
		if a.issueReads%2 == 0 && a.onFinal != nil {
			a.onFinal(a)
		}
		return json.Marshal(a.issue)
	}
	if item, ok := a.dependencies[path]; ok {
		return json.Marshal(item)
	}
	return nil, errors.New("unexpected source path")
}
func (a *issueAdmissionAPI) ResourcePages(context.Context, string, string) ([]json.RawMessage, error) {
	return nil, errors.New("single Issue admission must not enumerate pool")
}

func issueAdmissionFixture(t *testing.T) (IssueAdmitter, *issueAdmissionAPI) {
	t.Helper()
	k, err := storage.OpenKernel(filepath.Join(t.TempDir(), "controller", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	if _, err := k.CreateOrchestrator(context.Background(), "main", "model", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	api := &issueAdmissionAPI{repositoryID: 17, fail: "never", dependencies: map[string]github.Issue{}}
	api.issue = github.Issue{Number: 42, Title: "product request", State: "open", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Body: "implement acceptance\n```harness-scope\n" + `{"revision":1,"original_request":"acceptance source","criteria":[{"id":"AC","requirement":"preserve expected behavior"}],"included":["AC"],"remaining":[],"previously_merged":[]}` + "\n```"}
	api.issue.Labels = append(api.issue.Labels, struct {
		Name string `json:"name"`
	}{Name: "ready"})
	return IssueAdmitter{Store: k, API: api, Policy: IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}, SessionID: "main", Limits: domain.DefaultKernelLimits()}, api
}

func assertNoIssueAdmission(t *testing.T, a IssueAdmitter) {
	t.Helper()
	waves, err := a.Store.Waves(context.Background())
	if err != nil || len(waves) != 0 {
		t.Fatal("rejected source left a Wave", waves, err)
	}
	var count int
	if err := a.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_work_item_registration','product_wave_admission')").Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected source left metadata", count, err)
	}
}

func TestIssueAdmissionUsesNumericRepositoryAndCommonProductPort(t *testing.T) {
	a, api := issueAdmissionFixture(t)
	ctx := context.Background()
	wave, err := a.Admit(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if wave.Source != "github" || wave.SourceID != "r17-i42" || wave.Policy.Stage != "P" || !wave.Policy.Product || wave.Policy.Limits != a.Limits || wave.OriginalRequest != api.issue.Title+"\n\n"+api.issue.Body {
		t.Fatal(wave)
	}
	if !reflect.DeepEqual(api.reads, []string{"", "issues/42", "issues/42", ""}) {
		t.Fatal("source was not verified at collection boundary", api.reads)
	}
	var saved storage.ProductWorkItemRegistrationSnapshot
	if err := a.Store.Record(ctx, "product_work_item_registration", "r17-i42", &saved); err != nil || saved.Value.WaveID != wave.ID || saved.Value.Definition.RepositoryID != 17 || !reflect.DeepEqual(saved.Value.Definition.Criteria, []string{"AC"}) {
		t.Fatal(saved, err)
	}
	replay, err := a.Admit(ctx, 42)
	if err != nil || !reflect.DeepEqual(wave, replay) {
		t.Fatal(replay, err)
	}
	api.issue.Body = strings.Replace(api.issue.Body, "preserve expected behavior", "changed requirement", 1)
	api.issue.UpdatedAt = api.issue.UpdatedAt.Add(time.Second)
	if _, err := a.Admit(ctx, 42); err == nil {
		t.Fatal("source edit silently replaced the active scope")
	}
	current, err := a.Store.Wave(ctx, wave.ID)
	if err != nil || !reflect.DeepEqual(wave, current) {
		t.Fatal("conflicting source retry mutated original Wave", current, err)
	}
}

func TestIssueAdmissionRejectsUnavailableHeldChangedAndUnsupportedLineage(t *testing.T) {
	for _, mode := range []string{"repository", "unavailable", "closed", "pr", "identity", "timestamp", "missing_ready", "hold", "bad_scope", "scope_revision", "partial", "merged", "changed_body", "changed_hold", "changed_repository", "policy"} {
		t.Run(mode, func(t *testing.T) {
			a, api := issueAdmissionFixture(t)
			switch mode {
			case "repository":
				api.repositoryID++
			case "unavailable":
				api.fail = "issues/42"
			case "closed":
				api.issue.State = "closed"
			case "pr":
				api.issue.PullRequest = json.RawMessage(`{"url":"pulls/42"}`)
			case "identity":
				api.issue.Number++
			case "timestamp":
				api.issue.UpdatedAt = time.Time{}
			case "missing_ready":
				api.issue.Labels = nil
			case "hold":
				label := api.issue.Labels[0]
				label.Name = "hold"
				api.issue.Labels = append(api.issue.Labels, label)
			case "bad_scope":
				api.issue.Body = "no acceptance contract"
			case "scope_revision":
				api.issue.Body = strings.Replace(api.issue.Body, `"revision":1`, `"revision":2`, 1)
			case "partial", "merged":
				api.issue.Body = strings.Replace(api.issue.Body, `"criteria":[`, `"criteria":[{"id":"AC2","requirement":"another"},`, 1)
				field := "remaining"
				if mode == "merged" {
					field = "previously_merged"
				}
				api.issue.Body = strings.Replace(api.issue.Body, `"`+field+`":[]`, `"`+field+`":["AC2"]`, 1)
			case "changed_body":
				api.onFinal = func(api *issueAdmissionAPI) { api.issue.Body += "\nsource edit" }
			case "changed_hold":
				api.onFinal = func(api *issueAdmissionAPI) { api.issue.Labels[0].Name = "hold" }
			case "changed_repository":
				api.onFinal = func(api *issueAdmissionAPI) { api.repositoryID++ }
			case "policy":
				a.Policy.HoldLabel = a.Policy.ReadyLabel
			}
			_, err := a.Admit(context.Background(), 42)
			if err == nil {
				t.Fatal("invalid source admitted")
			}
			if (mode == "scope_revision" || mode == "partial" || mode == "merged") && !strings.Contains(err.Error(), "lineage") {
				t.Fatal("lineage guard not reached", err)
			}
			if (mode == "changed_body" || mode == "changed_hold") && !strings.Contains(err.Error(), "Issue changed") {
				t.Fatal("fresh source guard not reached", err)
			}
			assertNoIssueAdmission(t, a)
		})
	}
}

func TestIssueAdmissionRequiresClosedDependenciesAndCompletedClaimedWave(t *testing.T) {
	for _, mode := range []string{"open", "unavailable", "claimed", "direct", "direct_canceled", "missing_membership", "corrupt_registration", "repository_conflict", "closed"} {
		t.Run(mode, func(t *testing.T) {
			a, api := issueAdmissionFixture(t)
			api.issue.Body += "\nharness-depends-on: 41\n"
			dependency := api.issue
			dependency.Number, dependency.State = 41, "closed"
			if mode == "open" {
				dependency.State = "open"
			}
			api.dependencies["issues/41"] = dependency
			if mode == "unavailable" {
				api.fail = "issues/41"
			}
			managed := mode != "open" && mode != "unavailable" && mode != "closed"
			if managed {
				source, sourceID := "direct", "direct-dependency"
				if mode == "claimed" {
					source, sourceID = "github", "r17-i41"
				}
				repositoryID := int64(17)
				if mode == "repository_conflict" {
					repositoryID = 18
				}
				owner, err := a.Store.CreateProductWave(context.Background(), storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: "main", Source: source, SourceID: sourceID, OriginalRequest: "prior dependency", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "prior"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: a.Limits}}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "r17-i41", Repository: "owner/repo", RepositoryID: repositoryID, Criteria: []string{"AC"}}}})
				if err != nil {
					t.Fatal(err)
				}
				if mode == "direct_canceled" {
					if _, err := a.Store.ApplyOperatorControl(context.Background(), storage.OperatorControl{Key: "cancel-dependency", Target: owner.Target(), Action: "cancel", Reason: "dependency canceled"}); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "missing_membership" {
					if _, err := a.Store.DB.Exec("DELETE FROM records WHERE kind='wave_work_item' AND id='r17-i41'"); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "corrupt_registration" {
					registration, err := a.Store.ReadProductWorkItemRegistration(context.Background(), "r17-i41")
					if err != nil {
						t.Fatal(err)
					}
					registration.Digest = "corrupt"
					if err := a.Store.Put(context.Background(), "product_work_item_registration", "r17-i41", owner.ID, registration); err != nil {
						t.Fatal(err)
					}
				}
			}

			_, err := a.Admit(context.Background(), 42)
			if (err == nil) != (mode == "closed") {
				t.Fatal("dependency gate mismatch", mode, err)
			}
			if mode == "direct" || mode == "direct_canceled" {
				if !strings.Contains(err.Error(), "no completed Wave") {
					t.Fatal("direct ownership guard was not reached", err)
				}
			}
			var admitted int
			if err := a.Store.DB.QueryRow("SELECT count(*) FROM kernel_waves WHERE source='github' AND source_id='r17-i42'").Scan(&admitted); err != nil || (admitted == 1) != (mode == "closed") {
				t.Fatal("dependency rejection persisted an admission", admitted, err)
			}

			if mode != "closed" && !managed {
				assertNoIssueAdmission(t, a)
			}
		})
	}
}

func TestIssueAdmissionDependencySyntaxAndCancellationFailClosed(t *testing.T) {
	for _, body := range []string{"harness-depends-on:", "harness-depends-on: 42", "harness-depends-on: 41,41", "harness-depends-on: bad", "harness-depends-on: 41\nharness-depends-on: 40"} {
		a, api := issueAdmissionFixture(t)
		api.issue.Body += "\n" + body
		if _, err := a.Admit(context.Background(), 42); err == nil {
			t.Fatal("invalid dependency syntax admitted", body)
		}
		assertNoIssueAdmission(t, a)
	}
	a, _ := issueAdmissionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Admit(ctx, 42); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled source collection admitted", err)
	}
	assertNoIssueAdmission(t, a)
}

func TestIssueAdmissionRejectsGeneratedSplitChildrenEvenWithEmbeddedRootScope(t *testing.T) {
	for _, marker := range []string{string([]byte{96, 96, 96}) + "harness-split-part", "<!-- harness-split-child:fixture -->"} {
		a, api := issueAdmissionFixture(t)
		api.issue.Body += "\n" + marker + "\n"
		if _, err := a.Admit(context.Background(), 42); err == nil {
			t.Fatal("split child became an independent Wave")
		}
		assertNoIssueAdmission(t, a)
	}
}
