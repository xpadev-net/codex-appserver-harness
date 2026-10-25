package product

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func splitHTTPFixture(t *testing.T, source string) (*storage.KernelStore, storage.ProductSplitCreate, *IssueSourcePolicy, string) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	k, err := storage.OpenKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	workspace := filepath.Join(root, "workspace")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := k.CreateOrchestrator(ctx, "main", "configured", workspace, "read-only"); err != nil {
		t.Fatal(err)
	}
	sourceID, parentID := "direct-request", "split-root"
	var policy *IssueSourcePolicy
	pin := ""
	if source == "github" {
		sourceID = "r17-i42"
		parentID = sourceID
		policy = &IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}
		pin = policy.Digest()
	}
	w, err := k.CreateProductWave(ctx, storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: "main", Source: source, SourceID: sourceID, OriginalRequest: "parent title\n\nparent body", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "whole behavior"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: domain.DefaultKernelLimits()}}, SourcePolicyDigest: pin, WorkItems: []storage.ProductWorkItemDefinition{{ID: parentID, Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if source == "github" {
		_, err = k.ObserveProductSource(ctx, w.Target(), "", storage.ProductSourceFacts{RepositoryID: 17, IssueNumber: 42, PolicyDigest: pin, Available: true, IssueState: "open", Ready: true, RequestDigest: storage.ProductSourceRequestDigest(w.OriginalRequest), UpdatedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatal(err)
		}
	}
	registration, err := k.ReadProductWorkItemRegistration(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: parentID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{
		{Key: "first", Title: "first work", Requirement: "first half", Criteria: []verification.Criterion{{ID: "first-ac", Requirement: "first behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "first-ac", PartID: "one", PartCount: 2, Basis: "first part"}}, DependsOn: []string{}},
		{Key: "second", Title: "second work", Requirement: "remaining half", Criteria: []verification.Criterion{{ID: "second-ac", Requirement: "remaining behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "second-ac", PartID: "two", PartCount: 2, Basis: "remaining part"}}, DependsOn: []string{"first"}},
	}}
	saved, err := k.PrepareProductSplitProposal(ctx, storage.ProductSplitProposalRequest{Target: w.Target(), Key: "http-split", ParentWorkItemID: parentID, RegistrationDigest: registration.Digest, Proposal: proposal})
	if err != nil {
		t.Fatal(err)
	}
	r := storage.ProductSplitChildPlanRequest{Target: w.Target(), ParentWorkItemID: parentID, ProposalID: saved.ID, ProposalDigest: saved.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	job, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "http-controller")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	repo := []byte("{\"id\":17,\"full_name\":\"owner/repo\",\"default_branch\":\"master\"}")
	issue := []byte("{\"id\":42,\"number\":42,\"title\":\"parent title\",\"body\":\"parent body\",\"state\":\"open\",\"updated_at\":\"2026-10-01T00:00:00Z\",\"labels\":[{\"name\":\"ready\"}]}")
	for name, body := range map[string][]byte{"repo.json": repo, "source.json": issue} {
		if err := os.WriteFile(filepath.Join(bin, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	script := `#!/bin/sh
case "$*" in
 "api --method GET repos/owner/repo")
  printf 'GET:repository\n' >> "$HARNESS_SPLIT_HTTP_FIXTURE/calls"
  /bin/cat "$HARNESS_SPLIT_HTTP_FIXTURE/repo.json";;
 "api --method GET repos/owner/repo/issues/42")
  printf 'GET:source\n' >> "$HARNESS_SPLIT_HTTP_FIXTURE/calls"
  /bin/cat "$HARNESS_SPLIT_HTTP_FIXTURE/source.json";;
 "api --method POST repos/owner/repo/issues --input -")
  printf 'POST\n' >> "$HARNESS_SPLIT_HTTP_FIXTURE/calls"
  /bin/cat > "$HARNESS_SPLIT_HTTP_FIXTURE/request.json"
  if [ "$HARNESS_SPLIT_HTTP_MODE" = "oversize" ];then /bin/head -c 2097152 /dev/zero;exit 0;fi
  if [ "$HARNESS_SPLIT_HTTP_MODE" = "unknown" ]; then printf '{';exit 17;fi
  if [ "$HARNESS_SPLIT_HTTP_MODE" = "hang" ]; then
   trap '' TERM
   /bin/sleep 100 &
   echo "$!" > "$HARNESS_SPLIT_HTTP_FIXTURE/descendant.pid"
   wait
  fi
  printf '{"id":101,"number":11}';;
 *) exit 88;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("HARNESS_SPLIT_HTTP_FIXTURE", bin)
	t.Setenv("HARNESS_SPLIT_HTTP_MODE", "success")
	return k, job, policy, bin
}
func splitHTTPCalls(t *testing.T, bin string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(bin, "calls"))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func splitTestTransport(t *testing.T, k *storage.KernelStore, policy *IssueSourcePolicy) SplitHTTPTransport {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return SplitHTTPTransport{Store: k, Executable: exe, SourcePolicy: policy}
}

func TestSplitHTTPTransportSendsExactBodyOnceAndJoinsPhysicalGroup(t *testing.T) {
	for _, source := range []string{"direct", "github"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			k, job, policy, bin := splitHTTPFixture(t, source)
			transport := splitTestTransport(t, k, policy)
			stopped, err := transport.Start(ctx, job.ID)
			if err != nil || stopped.State != "stopped" || stopped.HTTP == nil || stopped.Stop == nil || stopped.Issue != nil {
				t.Fatal("response became authority before GET", stopped, err)
			}
			alive, err := process.GroupAlive(stopped.HTTP.Process)
			if err != nil || alive {
				t.Fatal("lease returned before group stopped", alive, err)
			}
			result, err := ReadSplitHTTPResult(ctx, k, job.ID)
			if err != nil || result.Disposition != "response_received" || result.Response.ID != 101 || result.Response.Number != 11 {
				t.Fatal(result, err)
			}
			raw, err := os.ReadFile(filepath.Join(bin, "request.json"))
			if err != nil {
				t.Fatal(err)
			}
			var body struct {
				Title string
				Body  string
			}
			if err := json.Unmarshal(raw, &body); err != nil || body.Title != job.Value.Child.Value.Child.Title || body.Body != job.Value.Child.Value.Body {
				t.Fatal("body/marker changed in native request", body, err)
			}
			calls := splitHTTPCalls(t, bin)
			expected := "GET:repository\nPOST\n"
			if source == "github" {
				expected = "GET:repository\nGET:repository\nGET:source\nGET:source\nGET:repository\nPOST\n"
			}
			if calls != expected {
				t.Fatal("native request ordering", calls, expected)
			}
			if _, err := transport.Start(ctx, job.ID); !errors.Is(err, storage.ErrProductSplitHeld) {
				t.Fatal("completed transport resent", err)
			}
			if _, err := ExecuteSplitHTTP(ctx, filepath.Join(k.Root, "split-http", job.ID, "spec.json")); !errors.Is(err, storage.ErrProductSplitHeld) {
				t.Fatal("wrapper replay resent", err)
			}
			if err := os.Remove(filepath.Join(k.Root, "split-http", job.ID, "result.json")); err != nil {
				t.Fatal(err)
			}
			recovered, err := (SplitHTTPTransport{Store: k}).Recover(ctx, job.ID)
			if err != nil || !reflect.DeepEqual(recovered, stopped) || splitHTTPCalls(t, bin) != calls {
				t.Fatal("missing file caused replay or reaping", recovered, err)
			}
			var registrations int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&registrations); err != nil || registrations != 1 {
				t.Fatal("transport activated child WorkItem", registrations, err)
			}
		})
	}
}

func TestSplitHTTPUnknownResponseNeverResends(t *testing.T) {
	ctx := context.Background()
	k, job, policy, bin := splitHTTPFixture(t, "direct")
	t.Setenv("HARNESS_SPLIT_HTTP_MODE", "unknown")
	transport := splitTestTransport(t, k, policy)
	stopped, err := transport.Start(ctx, job.ID)
	if err != nil || stopped.State != "stopped" || stopped.Issue != nil {
		t.Fatal(stopped, err)
	}
	result, err := ReadSplitHTTPResult(ctx, k, job.ID)
	if err != nil || result.Disposition != "outcome_unknown" || result.Response != nil {
		t.Fatal(result, err)
	}
	before := splitHTTPCalls(t, bin)
	if strings.Count(before, "POST\n") != 1 {
		t.Fatal(before)
	}
	if _, err := transport.Start(ctx, job.ID); !errors.Is(err, storage.ErrProductSplitHeld) {
		t.Fatal("unknown result retried POST", err)
	}
	if _, err := transport.Recover(ctx, job.ID); err != nil || splitHTTPCalls(t, bin) != before {
		t.Fatal("recovery caused native request", err)
	}
}

func TestSplitHTTPUnactivatedRecoveryAndLaunchFailureSealAdmission(t *testing.T) {
	for _, mode := range []string{"recover", "launch_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, policy, bin := splitHTTPFixture(t, "direct")
			transport := splitTestTransport(t, k, policy)
			var got storage.ProductSplitCreate
			var err error
			if mode == "recover" {
				got, err = transport.Recover(ctx, job.ID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				transport.Executable = filepath.Join(bin, "absent")
				got, err = transport.Start(ctx, job.ID)
				if err == nil {
					t.Fatal("expected launch failure")
				}
			}
			if got.State != "aborted" || got.HTTP != nil || got.Abort == nil || splitHTTPCalls(t, bin) != "" {
				t.Fatal("unactivated intent not sealed", got, err)
			}
			if _, err := transport.Start(ctx, job.ID); !errors.Is(err, storage.ErrProductSplitHeld) {
				t.Fatal("sealed intent reactivated", err)
			}
		})
	}
}

func TestSplitHTTPPauseBeforeSpecWaitsWithoutConsumingLaunch(t *testing.T) {
	ctx := context.Background()
	k, job, policy, bin := splitHTTPFixture(t, "direct")
	r := job.Value.Request
	transport := splitTestTransport(t, k, policy)
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-http", Target: r.Target, Action: "pause", Reason: "hold native launch"}); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Start(ctx, job.ID); !errors.Is(err, storage.ErrProductSplitHeld) {
		t.Fatal("pause launched wrapper", err)
	}
	current, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || current.State != "prepared" || current.Abort != nil {
		t.Fatal("pause consumed prepared intent", current, err)
	}
	if splitHTTPCalls(t, bin) != "" {
		t.Fatal("paused request reached gh")
	}
	if _, err := os.Lstat(filepath.Join(k.Root, "split-http", job.ID, "spec.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pause created immutable spec", err)
	}
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "resume-http", Target: r.Target, Action: "resume", Reason: "resume native launch"}); err != nil {
		t.Fatal(err)
	}
	if got, err := transport.Start(ctx, job.ID); err != nil || got.State != "stopped" {
		t.Fatal("resumed launch failed", got, err)
	}
}

func TestSplitHTTPClosedSourceAndPolicyDriftCannotPOST(t *testing.T) {
	for _, mode := range []string{"closed", "policy"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, policy, bin := splitHTTPFixture(t, "github")
			transport := splitTestTransport(t, k, policy)
			if mode == "closed" {
				path := filepath.Join(bin, "source.json")
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(strings.Replace(string(raw), "\"open\"", "\"closed\"", 1)), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				changed := *policy
				changed.ReadyLabel = "another-policy"
				transport.SourcePolicy = &changed
			}
			got, err := transport.Start(ctx, job.ID)
			if strings.Contains(splitHTTPCalls(t, bin), "POST") {
				t.Fatal("unready source reached POST")
			}
			if mode == "closed" {
				if err != nil || got.State != "aborted" || got.HTTP != nil {
					t.Fatal("unready wrapper was not sealed", got, err)
				}
				w, err := k.Wave(ctx, job.Value.Request.Target.WaveID)
				if err != nil || w.SourceHold == nil || w.SourceHold.Reason != "closed" {
					t.Fatal("source prohibition lost", w.SourceHold, err)
				}
			} else if !errors.Is(err, storage.ErrProductSplitHeld) || splitHTTPCalls(t, bin) != "" {
				t.Fatal("policy conflict performed native read", got, err)
			}
		})
	}
}

func TestSplitHTTPSpecRejectsOverwritePublicSymlinkAndUnknownFields(t *testing.T) {
	ctx := context.Background()
	k, job, policy, _ := splitHTTPFixture(t, "direct")
	spec, err := WriteSplitHTTPSpec(ctx, k, job.ID, policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteSplitHTTPSpec(ctx, k, job.ID, policy); err == nil {
		t.Fatal("immutable spec overwritten")
	}
	raw, err := os.ReadFile(spec.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(spec.Path(), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadSplitHTTPSpec(spec.Path()); err == nil {
		t.Fatal("public spec accepted")
	}
	if err := os.Chmod(spec.Path(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(spec.Path()); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(k.Root, "linked-spec.json")
	if err := os.WriteFile(target, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, spec.Path()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadSplitHTTPSpec(spec.Path()); err == nil {
		t.Fatal("symlink spec accepted")
	}
	if err := os.Remove(spec.Path()); err != nil {
		t.Fatal(err)
	}
	unknown := strings.Replace(string(raw), "{", "{\"untrusted_command\":\"ignored\",", 1)
	if err := os.WriteFile(spec.Path(), []byte(unknown), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadSplitHTTPSpec(spec.Path()); err == nil {
		t.Fatal("unknown spec command accepted")
	}
}

func TestSplitHTTPCancellationJoinsNativeDescendantsBeforeRelease(t *testing.T) {
	k, job, policy, bin := splitHTTPFixture(t, "direct")
	t.Setenv("HARNESS_SPLIT_HTTP_MODE", "hang")
	transport := splitTestTransport(t, k, policy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		job storage.ProductSplitCreate
		err error
	}
	done := make(chan outcome, 1)
	go func() { got, err := transport.Start(ctx, job.ID); done <- outcome{got, err} }()
	var descendant process.Identity
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(filepath.Join(bin, "descendant.pid")); err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err != nil {
				t.Fatal(err)
			}
			descendant = process.Identify(pid)
			if descendant.Valid() && process.Alive(descendant) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !descendant.Valid() || !process.Alive(descendant) {
		cancel()
		<-done
		t.Fatal("native descendant did not start")
	}
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) || got.job.State != "stopped" || got.job.HTTP == nil || got.job.Stop == nil {
			t.Fatal("cancellation inferred completion without stopping", got.job, got.err)
		}
		alive, err := process.GroupAlive(got.job.HTTP.Process)
		if err != nil || alive || process.Alive(descendant) {
			t.Fatal("native descendant outlived lease release", alive, err)
		}
		if strings.Count(splitHTTPCalls(t, bin), "POST\n") != 1 {
			t.Fatal("canceled POST repeated")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("cancellation cleanup did not join native group")
	}
}

func TestSplitHTTPRestartRecoversRegisteredOwnerWithLostResult(t *testing.T) {
	ctx := context.Background()
	k, job, policy, bin := splitHTTPFixture(t, "direct")
	transport := splitTestTransport(t, k, policy)
	spec, err := WriteSplitHTTPSpec(ctx, k, job.ID, policy)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(transport.Executable, "_split-http", spec.Path())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	identity := process.Identify(cmd.Process.Pid)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = process.StopGroup(cleanup, identity)
		_ = cmd.Wait()
	}()
	wait, cancel := context.WithTimeout(ctx, 8*time.Second)
	err = transport.waitResult(wait, job.ID, identity)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(spec.directory(), "result.json")); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenExistingKernel(filepath.Join(k.Root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stopped, err := (SplitHTTPTransport{Store: reopened}).Recover(ctx, job.ID)
	if err != nil || stopped.State != "stopped" || stopped.Issue != nil || stopped.HTTP.Process != identity {
		t.Fatal("lost result was not kept unknown", stopped, err)
	}
	alive, err := process.GroupAlive(identity)
	if err != nil || alive {
		t.Fatal("recovery did not join original owner", alive, err)
	}
	before := splitHTTPCalls(t, bin)
	if _, err := (SplitHTTPTransport{Store: reopened}).Recover(ctx, job.ID); err != nil || splitHTTPCalls(t, bin) != before {
		t.Fatal("restarted recovery resent", err)
	}
}

func TestSplitHTTPDelayedUnactivatedWrapperCannotPOSTAfterSeal(t *testing.T) {
	ctx := context.Background()
	k, job, policy, bin := splitHTTPFixture(t, "direct")
	transport := splitTestTransport(t, k, policy)
	spec, err := WriteSplitHTTPSpec(ctx, k, job.ID, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := transport.Recover(ctx, job.ID); err != nil || got.State != "aborted" {
		t.Fatal(got, err)
	}
	cmd := exec.Command(transport.Executable, "_split-http", spec.Path())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err == nil {
		t.Fatal("delayed wrapper accepted sealed intent")
	}
	if splitHTTPCalls(t, bin) != "" {
		t.Fatal("sealed wrapper reached native request")
	}
}

func TestSplitHTTPOutputCannotBypassLimitThroughIOCopy(t *testing.T) {
	var output splitHTTPOutput
	// LimitedReader has no WriterTo; io.Copy may choose destination ReadFrom.
	// Embedding bytes.Buffer would expose ReadFrom and bypass Write's limit.
	_, err := io.Copy(&output, io.LimitReader(strings.NewReader(strings.Repeat("x", mergeHTTPBytes+1)), mergeHTTPBytes+1))
	if err == nil || len(output.Bytes()) > mergeHTTPBytes {
		t.Fatal("native output exceeded bound", len(output.Bytes()), err)
	}
}
func TestSplitHTTPOversizedNativeResponseStaysUnknownWithoutRetry(t *testing.T) {
	ctx := context.Background()
	k, job, policy, bin := splitHTTPFixture(t, "direct")
	t.Setenv("HARNESS_SPLIT_HTTP_MODE", "oversize")
	stopped, err := splitTestTransport(t, k, policy).Start(ctx, job.ID)
	if err != nil || stopped.State != "stopped" {
		t.Fatal(stopped, err)
	}
	result, err := ReadSplitHTTPResult(ctx, k, job.ID)
	if err != nil || result.Disposition != "outcome_unknown" || result.Response != nil {
		t.Fatal(result, err)
	}
	if strings.Count(splitHTTPCalls(t, bin), "POST\n") != 1 {
		t.Fatal("oversize response repeated native POST")
	}
}
