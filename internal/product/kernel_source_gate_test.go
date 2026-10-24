package product

import (
	"context"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelSourceGateRefreshesIssueAtPublicationInsteadOfUsingCachedReady(t *testing.T) {
	for _, mode := range []string{"hold", "closed", "edited", "read_failed", "policy", "missing_policy", "changing"} {
		t.Run(mode, func(t *testing.T) {
			o, api, w := sourceObserverFixture(t)
			ctx := context.Background()
			saved := storage.ProductTargetSnapshot{Value: storage.ProductTarget{Target: w.Target(), Repository: o.Policy.Repository}}
			saved.Value.WorkItem.RepositoryID = o.Policy.RepositoryID
			g := KernelSourceGate{Store: o.Store, Policies: map[int64]IssueSourcePolicy{o.Policy.RepositoryID: o.Policy}, API: func(storage.ProductTarget) ResourceAPI { return api }}
			if err := g.Check(ctx, saved); err != nil {
				t.Fatal("initial ready source held", err)
			}
			if !reflect.DeepEqual(api.reads, []string{"", "issues/42", "issues/42", ""}) {
				t.Fatal(api.reads)
			}
			api.reads = nil
			original := api.issue
			switch mode {
			case "hold":
				api.issue.Labels = append(api.issue.Labels, struct {
					Name string `json:"name"`
				}{"hold"})
			case "closed":
				api.issue.State = "closed"
			case "edited":
				api.issue.Body += "\nchanged request"
			case "read_failed":
				api.fail = "issues/42"
			case "policy":
				changed := o.Policy
				changed.ReadyLabel = "other"
				g.Policies[changed.RepositoryID] = changed
			case "missing_policy":
				g.Policies = nil
			case "changing":
				api.onFinal = func(api *issueAdmissionAPI) { api.issue.Title += " changed" }
			}
			if err := g.Check(ctx, saved); err == nil {
				t.Fatal("cached ready authorized changed source", mode)
			}
			if mode != "missing_policy" {
				current, err := o.Store.Wave(ctx, w.ID)
				if err != nil || current.SourceHold == nil {
					t.Fatal("failed fresh source did not persist prohibition", current, err)
				}
			}
			api.issue, api.onFinal, api.fail = original, nil, "never"
			g.Policies = map[int64]IssueSourcePolicy{o.Policy.RepositoryID: o.Policy}
			if err := g.Check(ctx, saved); err != nil {
				t.Fatal("fresh restored source did not recover", err)
			}
			var runs int
			if err := o.Store.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
				t.Fatal("source gate launched model", runs, err)
			}
		})
	}
}

func TestKernelSourceGateUsesSavedDirectRequestWithoutGitHubRead(t *testing.T) {
	p, _, saved, _ := publicationFixture(t)
	g := KernelSourceGate{Store: p.Store, API: func(storage.ProductTarget) ResourceAPI { t.Fatal("direct source read GitHub"); return nil }}
	if err := g.Check(context.Background(), saved); err != nil {
		t.Fatal(err)
	}
	saved.Value.Target.PlanRevision++
	if err := g.Check(context.Background(), saved); err == nil {
		t.Fatal("stale direct target accepted")
	}
}
