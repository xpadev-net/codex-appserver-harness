package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func mergeConfigFixture(t *testing.T) ProductExecutorConfig {
	t.Helper()
	c := publicationConfigFixture(t)
	c.Publication.ReviewRoutes[0].CloudReviewerIDs = []int64{91}
	c.Publication.Merge = &ProductMergeConfig{Workers: 2, Repositories: []ProductMergeRouteConfig{{RepositoryID: 17, Method: "merge", CIRefMode: "head", RequiredChecks: []gate.RequiredCheck{{Name: "unit", AppID: 9, AllowedConclusions: []string{"success"}}}, TrustedReviewerIDs: []int64{91}, RequiredApprovals: 1}}}
	return c
}

func TestProductMergeConfigRequiresExplicitBoundedIdentityAndStrictPolicies(t *testing.T) {
	k, _, err := openKernel(context.Background(), cliRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for _, mode := range []string{"workers_zero", "workers_large", "routes_empty", "routes_large", "duplicate", "unknown_repo", "missing_route", "no_assigned_actor", "method", "ci_ref", "checks_empty", "checks_large", "check_duplicate", "check_blank", "check_newline", "issuer", "conclusion", "actors_empty", "actor_zero", "actor_duplicate", "actor_unassigned", "approvals_zero", "approvals_large", "report_repository", "report_name", "report_base", "report_version", "report_future"} {
		t.Run(mode, func(t *testing.T) {
			c := mergeConfigFixture(t)
			m := c.Publication.Merge
			route := &m.Repositories[0]
			switch mode {
			case "workers_zero":
				m.Workers = 0
			case "workers_large":
				m.Workers = 9
			case "routes_empty":
				m.Repositories = nil
			case "routes_large":
				m.Repositories = make([]ProductMergeRouteConfig, 129)
			case "duplicate":
				m.Repositories = append(m.Repositories, *route)
			case "unknown_repo":
				route.RepositoryID = 18
			case "missing_route":
				c.Publication.ReviewRoutes = nil
			case "no_assigned_actor":
				c.Publication.ReviewRoutes[0].CloudReviewerIDs = nil
			case "method":
				route.Method = "force"
			case "ci_ref":
				route.CIRefMode = "any_green"
			case "checks_empty":
				route.RequiredChecks = nil
			case "checks_large":
				route.RequiredChecks = make([]gate.RequiredCheck, 129)
			case "check_duplicate":
				route.RequiredChecks = append(route.RequiredChecks, route.RequiredChecks[0])
			case "check_blank":
				route.RequiredChecks[0].Name = " "
			case "check_newline":
				route.RequiredChecks[0].Name = "unit\nother"
			case "issuer":
				route.RequiredChecks[0].AppID = 0
			case "conclusion":
				route.RequiredChecks[0].AllowedConclusions = []string{"neutral"}
			case "actors_empty":
				route.TrustedReviewerIDs = nil
			case "actor_zero":
				route.TrustedReviewerIDs = []int64{0}
			case "actor_duplicate":
				route.TrustedReviewerIDs = []int64{91, 91}
			case "actor_unassigned":
				route.TrustedReviewerIDs = []int64{92}
			case "approvals_zero":
				route.RequiredApprovals = 0
			case "approvals_large":
				route.RequiredApprovals = 2
			default:
				report := contract.NewReport("owner/repo")
				report.RepositoryID, report.DefaultBranch = 17, "trunk"
				switch mode {
				case "report_repository":
					report.RepositoryID = 18
				case "report_name":
					report.Repository = "owner/other"
				case "report_base":
					report.DefaultBranch = "other"
				case "report_version":
					report.Version = 2
				case "report_future":
					report.ObservedAt = time.Now().Add(time.Hour)
				}
				c.Publication.ReviewRoutes[0].CapabilitiesPath = operatorFile(t, report)
			}
			if _, _, err := loadProductExecutor(operatorFile(t, c), k, control.IsolatedConfig{}); err == nil {
				t.Fatal("invalid merge configuration accepted", mode)
			}
		})
	}
}

func TestProductMergePoliciesKeepPrivateReportsLiveAndUnknownCapabilitiesHeld(t *testing.T) {
	c := mergeConfigFixture(t)
	routes, _, err := c.Publication.routes(c.Repositories)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.Publication.Merge.policies(c.Repositories, routes)
	if err != nil {
		t.Fatal("unknown capability report must allow safe configured waiting", err)
	}
	c.Publication.Merge.Repositories[0].TrustedReviewerIDs[0] = 99
	c.Publication.Merge.Repositories[0].RequiredChecks[0].AllowedConclusions[0] = "neutral"
	if p.cloud[17].Policy.TrustedReviewerIDs[0] != 91 || p.merge[17].RequiredChecks[0].AllowedConclusions[0] != "success" {
		t.Fatal("host policy retained caller-owned mutable slices")
	}
	k, _, err := openKernel(context.Background(), cliRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	driver := productMergeDriver(product.Executor{Store: k}, p, nil, "/unused-trusted-executable")
	saved := storage.ProductTargetSnapshot{Value: storage.ProductTarget{Repository: "owner/repo", BaseBranch: "trunk"}}
	saved.Value.WorkItem.RepositoryID = 17
	for _, state := range []contract.State{contract.Unknown, contract.Unsupported} {
		r := contract.NewReport("owner/repo")
		r.RepositoryID, r.DefaultBranch = 17, "trunk"
		r.Capabilities["cloud_native_approval"] = contract.Capability{State: state, Reason: "fixture never grants approval"}
		raw, _ := json.Marshal(r)
		if err := os.WriteFile(c.Publication.ReviewRoutes[0].CapabilitiesPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
		current, err := p.cloud[17].ReadReport(context.Background())
		if err != nil || current.Capabilities["cloud_native_approval"].State != state {
			t.Fatal("capability reader cached an old report", current, err)
		}
		if _, err := driver.Gate(context.Background(), saved, "unused-request"); err == nil || !strings.Contains(err.Error(), "capability contract") {
			t.Fatal("configured native policy converted unknown into permission", err)
		}
	}
}

func TestConfiguredMergePipelineOwnsPrivateAuthorityAndJoinsShutdownWithoutModel(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			ctx := context.Background()
			k, _, err := openKernel(ctx, cliRoot(t))
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			c := mergeConfigFixture(t)
			if !enabled {
				c.Publication.Merge = nil
			}
			worker, _, err := loadProductExecutorWithObserver(operatorFile(t, c), k, control.IsolatedConfig{}, true)
			if err != nil {
				t.Fatal(err)
			}
			var before int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&before); err != nil {
				t.Fatal(err)
			}
			run, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- worker(run, k, func() {}, func(error) {}) }()
			path := filepath.Join(k.Root, "merge-authorize.sock")
			if enabled {
				deadline := time.Now().Add(5 * time.Second)
				for {
					info, err := os.Stat(path)
					if err == nil && info.Mode()&os.ModeSocket != 0 && info.Mode().Perm()&0077 == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("configured authority never started", err)
					}
					time.Sleep(5 * time.Millisecond)
				}
				other, err := product.ListenMergeHTTPAuthority(k, func(context.Context, storage.ProductMerge) (storage.ProductMergeGateObservation, error) {
					return storage.ProductMergeGateObservation{}, storage.ErrProductMergeHeld
				})
				if err == nil {
					other.Close()
					t.Fatal("duplicate merge authority replaced active owner")
				}
				dir, err := os.Open(k.Root)
				if err != nil {
					t.Fatal(err)
				}
				defer dir.Close()
				// Directory-FD path also covers long test roots, like production.
				conn, err := net.DialTimeout("unix", filepath.Join("/proc/self/fd", strconv.Itoa(int(dir.Fd())), "merge-authorize.sock"), time.Second)
				if err != nil {
					t.Fatal(err)
				}
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				if err := json.NewEncoder(conn).Encode(map[string]string{"merge_id": "unregistered", "spec_digest": strings.Repeat("a", 64)}); err != nil {
					t.Fatal(err)
				}
				var reply map[string]any
				err = json.NewDecoder(io.LimitReader(conn, 1<<20)).Decode(&reply)
				conn.Close()
				if err != nil || reply["error"] == nil || reply["error"] == "" {
					t.Fatal("active authority accepted unregistered caller", reply, err)
				}
			} else {
				time.Sleep(50 * time.Millisecond)
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("publication alone opened a merge authority", err)
				}
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("authority socket retained after pipeline shutdown", err)
			}
			var after int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&after); err != nil || after != before {
				t.Fatal("configured merge pipeline launched a model", before, after, err)
			}
		})
	}
}

func TestMergePipelineJoinsOtherOwnedWorkersBeforeReturning(t *testing.T) {
	ctx := context.Background()
	k, _, err := openKernel(ctx, cliRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	driver := product.MergeDriver{Store: k, Gate: func(context.Context, storage.ProductTargetSnapshot, string) (storage.ProductMergeGateObservation, error) {
		return storage.ProductMergeGateObservation{}, storage.ErrProductMergeHeld
	}, Transport: product.MergeHTTPTransport{Store: k, Executable: "/unused-trusted-executable"}}
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	other := func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	}
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runProductMergePipeline(run, driver, map[int64]string{17: "merge"}, 1, time.Hour, nil, nil, other)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline never started")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("owned worker not canceled")
	}
	select {
	case err := <-done:
		t.Fatal("pipeline returned before owned cleanup", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
