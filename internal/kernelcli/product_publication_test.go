package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func publicationConfigFixture(t *testing.T) ProductExecutorConfig {
	t.Helper()
	c := productExecutorConfigFixture(t)
	r := contract.NewReport(c.Repositories[0].Name)
	r.RepositoryID, r.DefaultBranch = 17, "trunk"
	c.Publication = &ProductPublicationConfig{Workers: 2, ReviewRoutes: []ProductReviewRouteConfig{{RepositoryID: 17, Trigger: "@review please review", RequesterID: 71, CapabilitiesPath: operatorFile(t, r)}}}
	return c
}

func TestProductPublicationConfigIsExplicitPrivateAndBounded(t *testing.T) {
	root := cliRoot(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for _, mode := range []string{"workers_zero", "workers_large", "too_many_routes", "duplicate", "repository", "requester", "trigger", "closing", "missing_report", "relative_report", "public_report", "symlink_report", "hardlink_report", "invalid_report", "oversized_report", "source_repository", "source_duplicate", "source_labels", "source_limit", "actor_zero", "actor_duplicate", "actor_limit"} {
		t.Run(mode, func(t *testing.T) {
			c := publicationConfigFixture(t)
			route := &c.Publication.ReviewRoutes[0]
			switch mode {
			case "source_repository":
				c.Publication.SourcePolicies = []product.IssueSourcePolicy{{RepositoryID: 18, Repository: "owner/repo", ReadyLabel: "ready", HoldLabel: "hold"}}
			case "source_duplicate":
				c.Publication.SourcePolicies = []product.IssueSourcePolicy{{RepositoryID: 17, Repository: "owner/repo", ReadyLabel: "ready", HoldLabel: "hold"}, {RepositoryID: 17, Repository: "owner/repo", ReadyLabel: "ready", HoldLabel: "hold"}}
			case "source_labels":
				c.Publication.SourcePolicies = []product.IssueSourcePolicy{{RepositoryID: 17, Repository: "owner/repo", ReadyLabel: "ready", HoldLabel: "ready"}}
			case "source_limit":
				c.Publication.SourcePolicies = make([]product.IssueSourcePolicy, 129)
			case "actor_zero":
				route.CloudReviewerIDs = []int64{0}
			case "actor_duplicate":
				route.CloudReviewerIDs = []int64{91, 91}
			case "actor_limit":
				route.CloudReviewerIDs = make([]int64, 129)
			case "workers_zero":
				c.Publication.Workers = 0
			case "workers_large":
				c.Publication.Workers = 9
			case "too_many_routes":
				c.Publication.ReviewRoutes = make([]ProductReviewRouteConfig, 129)
			case "duplicate":
				c.Publication.ReviewRoutes = append(c.Publication.ReviewRoutes, *route)
			case "repository":
				route.RepositoryID = 18
			case "requester":
				route.RequesterID = 0
			case "trigger":
				route.Trigger = " "
			case "closing":
				route.Trigger = "Closes #42"
			case "missing_report":
				route.CapabilitiesPath = filepath.Join(t.TempDir(), "missing")
			case "relative_report":
				route.CapabilitiesPath = "report.json"
			case "public_report":
				if err := os.Chmod(route.CapabilitiesPath, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink_report":
				link := filepath.Join(t.TempDir(), "link")
				if err := os.Symlink(route.CapabilitiesPath, link); err != nil {
					t.Fatal(err)
				}
				route.CapabilitiesPath = link
			case "hardlink_report":
				if err := os.Link(route.CapabilitiesPath, filepath.Join(t.TempDir(), "link")); err != nil {
					t.Fatal(err)
				}
			case "invalid_report":
				if err := os.WriteFile(route.CapabilitiesPath, []byte(`{"unknown_field":true}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized_report":
				if err := os.Truncate(route.CapabilitiesPath, (1<<20)+1); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := loadProductExecutor(operatorFile(t, c), k, control.IsolatedConfig{}); err == nil {
				t.Fatal("invalid publication config accepted", mode)
			}
		})
	}
	for _, withRoutes := range []bool{false, true} {
		c := publicationConfigFixture(t)
		if !withRoutes {
			c.Publication.ReviewRoutes = []ProductReviewRouteConfig{}
		}
		worker, hint, err := loadProductExecutor(operatorFile(t, c), k, control.IsolatedConfig{})
		if err != nil || worker == nil || hint == nil {
			t.Fatal("explicit pipeline not configured", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := worker(ctx, k, func() {}, func(error) {}); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := worker(context.Background(), nil, func() {}, func(error) {}); err == nil {
			t.Fatal("pipeline accepted foreign store")
		}
	}
}

func TestProductReviewRoutesReloadPrivateReportsAndKeepSeparatePaths(t *testing.T) {
	c := publicationConfigFixture(t)
	c.Publication.ReviewRoutes[0].CloudReviewerIDs = []int64{93, 91}
	second := c.Repositories[0]
	second.Name = "owner/second"
	second.Policy.RepositoryID = 18
	c.Repositories = append(c.Repositories, second)
	report := contract.NewReport(second.Name)
	report.RepositoryID, report.DefaultBranch = 18, "trunk"
	c.Publication.ReviewRoutes = append(c.Publication.ReviewRoutes, ProductReviewRouteConfig{RepositoryID: 18, Trigger: "@other review", RequesterID: 72, CapabilitiesPath: operatorFile(t, report)})
	routes, hidden, err := c.Publication.routes(c.Repositories)
	if err != nil || len(hidden) != 2 {
		t.Fatal(routes, hidden, err)
	}
	if !slices.Equal(routes[17].CloudReviewerIDs, []int64{91, 93}) {
		t.Fatal("configured actor set was not frozen", routes[17].CloudReviewerIDs)
	}
	for i, route := range c.Publication.ReviewRoutes {
		observed, err := routes[route.RepositoryID].ReadReport(context.Background())
		if err != nil || observed.RepositoryID != route.RepositoryID || hidden[i] != route.CapabilitiesPath {
			t.Fatal(observed, hidden, err)
		}
	}
	path := c.Publication.ReviewRoutes[0].CapabilitiesPath
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	report.RepositoryID = 99
	changed, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	observed, err := routes[17].ReadReport(context.Background())
	if err != nil || observed.RepositoryID != 99 {
		t.Fatal("report was cached", observed, err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := routes[17].ReadReport(context.Background()); err == nil {
		t.Fatal("report privacy change ignored")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(c.Publication.ReviewRoutes[1].CapabilitiesPath, path); err != nil {
		t.Fatal(err)
	}
	if _, err := routes[17].ReadReport(context.Background()); err == nil {
		t.Fatal("report redirect followed")
	}
}

func TestProductPipelineFailureCancelsAndJoinsSibling(t *testing.T) {
	for _, externalFails := range []bool{false, true} {
		for _, failure := range []error{nil, errors.New("worker failed")} {
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			sibling := func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release
				return errors.Join(ctx.Err(), workspace.ErrGitStopUnconfirmed)
			}
			failed := func(context.Context) error { <-started; return failure }
			local, external := failed, sibling
			if externalFails {
				local, external = sibling, failed
			}
			done := make(chan error, 1)
			go func() { done <- runProductPipeline(context.Background(), local, external) }()
			select {
			case <-canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("sibling not canceled")
			}
			select {
			case err := <-done:
				t.Fatal("returned before sibling joined", err)
			default:
			}
			close(release)
			select {
			case err := <-done:
				if !errors.Is(err, workspace.ErrGitStopUnconfirmed) || failure != nil && !errors.Is(err, failure) {
					t.Fatal("lost shutdown failure", err)
				}
				if failure == nil && !strings.Contains(err.Error(), "stopped unexpectedly") {
					t.Fatal("unexpected worker exit hidden", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("sibling not joined")
			}
		}
	}
}

func TestProductPipelineDaemonKeepsLockUntilBothWorkersJoin(t *testing.T) {
	root := cliRoot(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	worker := func(ctx context.Context, _ *storage.KernelStore, _ func(), _ func(error)) error {
		return runProductPipeline(ctx, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return ctx.Err()
		})
	}
	done := make(chan error, 1)
	go func() {
		done <- runDaemon(ctx, k, control.Runtime{Store: k, Factory: neverStartFactory{make(chan control.Launch, 1)}}, io.Discard, worker)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline did not start")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline did not cancel")
	}
	select {
	case err := <-done:
		t.Fatal("daemon returned before Git/HTTP owner joined", err)
	default:
	}
	if unlock, err := controllerLock(root); err == nil {
		unlock()
		t.Fatal("controller lock released early")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline not joined")
	}
	unlock, err := controllerLock(root)
	if err != nil {
		t.Fatal("controller lock retained after join", err)
	}
	unlock()
}
