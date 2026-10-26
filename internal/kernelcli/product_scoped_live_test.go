package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

type liveProductSplit struct {
	Binding    storage.ProductSplitChildScopeBinding
	Registered storage.ProductSplitRegistrations
	Policy     product.IssueSourcePolicy
	Directory  string
}

// Creation and subsequent source observation both use production subprocess
// transports. This private local adapter substitutes only the remote API.
func prepareLiveProductSplit(t *testing.T, ctx context.Context, k *storage.KernelStore, w domain.Wave, cli, base string) liveProductSplit {
	t.Helper()
	directory := prepareLiveProductGitHub(t, k, base, 2)
	registration, err := k.ReadProductWorkItemRegistration(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: "work", ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
	for _, part := range []struct{ key, requirement string }{
		{"positive", "Multiply must return the exact product for positive and zero non-overflowing integer inputs."},
		{"negative", "Multiply must return the exact product for non-overflowing integer inputs with one or both operands negative."},
	} {
		criterion := "multiply-" + part.key
		proposal.Children = append(proposal.Children, splitting.Child{Key: part.key, Title: part.key + " multiplication", Requirement: part.requirement, Criteria: []verification.Criterion{{ID: criterion, Requirement: part.requirement}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: criterion, PartID: part.key, PartCount: 2, Basis: part.requirement}}, DependsOn: []string{}})
	}
	prepared, err := k.PrepareProductSplitExecution(ctx, storage.ProductSplitProposalRequest{Target: w.Target(), Key: "native-scoped-split", ParentWorkItemID: "work", RegistrationDigest: registration.Digest, Proposal: proposal})
	if err != nil {
		t.Fatal("prepare complete native split", err)
	}
	var request storage.ProductSplitChildPlanRequest
	for _, ref := range prepared.Value.Creations {
		job, err := k.ReadProductSplitCreate(ctx, ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		request = job.Value.Request
		got, err := (product.SplitHTTPTransport{Store: k, Executable: cli}).Start(ctx, job.ID)
		if err != nil || got.State != "stopped" || got.Stop == nil {
			t.Fatalf("controlled production Issue process did not stop: state=%s error=%v", got.State, err)
		}
		bound, _, err := (product.SplitCreationReconciler{Store: k, API: product.GitHubSplitResourceAPI, MaxPages: 32}).Reconcile(ctx, job.ID)
		if err != nil || bound.State != "observed" {
			t.Fatal("controlled production Issue GET binding did not complete", err)
		}
	}
	if _, err := k.BindProductSplitChildScopes(ctx, request); err != nil {
		t.Fatal("bind native local scopes", err)
	}
	registered, err := k.RegisterProductSplitChildren(ctx, request)
	if err != nil {
		t.Fatal("register native children", err)
	}
	out := liveProductSplit{Registered: registered, Policy: product.IssueSourcePolicy{Repository: "fixture/local-product", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}, Directory: directory}
	for _, binding := range registered.Value.Children {
		if binding.Value.Child.Value.Child.Key == "positive" {
			out.Binding = binding
		}
	}
	if out.Binding.ID == "" || len(registered.Value.Children) != 2 {
		t.Fatal("native local part missing")
	}
	return out
}

func prepareLiveProductGitHub(t *testing.T, k *storage.KernelStore, base string, limit int) string {
	t.Helper()
	if limit < 2 || limit > 32 {
		t.Fatal("bounded controlled Issue count required")
	}
	directory := filepath.Join(k.Root, "native-github")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal("create private controlled GitHub adapter")
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate controlled API helper source")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	helper := filepath.Join(directory, "gh")
	build := exec.Command("go", "build", "-o", helper, "./internal/kernelcli/testdata/livegh")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build controlled GitHub adapter: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	t.Setenv("HARNESS_LIVE_SCOPED_GITHUB", directory)
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	head, err := json.Marshal(map[string]any{"ref": "refs/heads/trunk", "object": map[string]string{"sha": base, "type": "commit"}})
	if err != nil || os.WriteFile(filepath.Join(directory, "head.json"), head, 0600) != nil {
		t.Fatal("save controlled repository HEAD")
	}
	if err := os.WriteFile(filepath.Join(directory, "posts-limit"), []byte(strconv.Itoa(limit)), 0600); err != nil {
		t.Fatal(err)
	}
	return directory
}

func liveProductSplitPosts(t *testing.T, directory string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(directory, "posts"))
	if err != nil {
		t.Fatal("read controlled API creation count")
	}
	count, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal("invalid controlled API count")
	}
	return count
}

// A gh shim executes this test subprocess. It never accesses a credential or
// network and rejects every endpoint/mutation outside this controlled fixture.
func TestLiveScopedGitHubFixture(t *testing.T) {
	directory := os.Getenv("HARNESS_LIVE_SCOPED_GITHUB")
	if directory == "" {
		t.Skip("controlled GitHub subprocess only")
	}
	fail := func() { os.Exit(89) }
	if !filepath.IsAbs(directory) {
		fail()
	}
	limitRaw, err := os.ReadFile(filepath.Join(directory, "posts-limit"))
	limit, parseErr := strconv.Atoi(string(limitRaw))
	if err != nil || parseErr != nil || limit < 2 || limit > 32 {
		fail()
	}
	args := []string{}
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	if len(args) < 4 || args[0] != "api" || args[1] != "--method" {
		fail()
	}
	method, endpoint := args[2], ""
	for _, arg := range args[3:] {
		if strings.HasPrefix(arg, "repos/") {
			endpoint = arg
			break
		}
	}
	const repository = "repos/fixture/local-product"
	if method == "GET" && endpoint == repository {
		_, _ = io.WriteString(os.Stdout, `{"id":17,"full_name":"fixture/local-product","default_branch":"trunk","clone_url":"https://github.com/fixture/local-product.git"}`)
		os.Exit(0)
	}
	if method == "GET" && endpoint == repository+"/git/ref/heads/trunk" {
		raw, err := os.ReadFile(filepath.Join(directory, "head.json"))
		if err != nil {
			fail()
		}
		_, _ = os.Stdout.Write(raw)
		os.Exit(0)
	}
	if method == "GET" && endpoint == repository+"/issues?state=all&per_page=100&page=1" {
		issues := []json.RawMessage{}
		for number := 101; number <= 100+limit; number++ {
			raw, err := os.ReadFile(filepath.Join(directory, fmt.Sprintf("issue-%d.json", number)))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				fail()
			}
			issues = append(issues, json.RawMessage(raw))
		}
		raw, err := json.Marshal(issues)
		if err != nil {
			fail()
		}
		_, _ = os.Stdout.Write(raw)
		os.Exit(0)
	}
	if method == "GET" && strings.HasPrefix(endpoint, repository+"/issues/") {
		number, err := strconv.Atoi(strings.TrimPrefix(endpoint, repository+"/issues/"))
		if err != nil || number < 101 || number > 100+limit {
			fail()
		}
		raw, err := os.ReadFile(filepath.Join(directory, fmt.Sprintf("issue-%d.json", number)))
		if err != nil {
			fail()
		}
		_, _ = os.Stdout.Write(raw)
		os.Exit(0)
	}
	if method != "POST" || endpoint != repository+"/issues" {
		fail()
	}
	var request struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if err != nil || json.Unmarshal(raw, &request) != nil || request.Title == "" || !strings.Contains(request.Body, "harness-split-child:") {
		fail()
	}
	lock, err := os.OpenFile(filepath.Join(directory, "counter.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		fail()
	}
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX) != nil {
		fail()
	}
	count := 0
	prior, err := os.ReadFile(filepath.Join(directory, "posts"))
	if err == nil {
		count, err = strconv.Atoi(strings.TrimSpace(string(prior)))
		if err != nil {
			fail()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		fail()
	}
	if count < 0 || count >= limit {
		fail()
	}
	number := 101 + count
	issue := struct {
		ID        int64               `json:"id"`
		Number    int                 `json:"number"`
		Title     string              `json:"title"`
		Body      string              `json:"body"`
		State     string              `json:"state"`
		UpdatedAt time.Time           `json:"updated_at"`
		Labels    []map[string]string `json:"labels"`
	}{int64(1000 + number), number, request.Title, request.Body, "open", time.Now().UTC(), []map[string]string{{"name": "ready"}}}
	encoded, err := json.Marshal(issue)
	if err != nil {
		fail()
	}
	file, err := os.OpenFile(filepath.Join(directory, fmt.Sprintf("issue-%d.json", number)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fail()
	}
	if _, err := file.Write(encoded); err != nil {
		fail()
	}
	if err := file.Close(); err != nil {
		fail()
	}
	if err := os.WriteFile(filepath.Join(directory, "posts"), []byte(strconv.Itoa(count+1)), 0600); err != nil {
		fail()
	}
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) != nil {
		fail()
	}
	_ = lock.Close()
	response, _ := json.Marshal(map[string]any{"id": issue.ID, "number": number})
	_, _ = os.Stdout.Write(response)
	os.Exit(0)
}
