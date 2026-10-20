package runner

import (
	"context"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"os"
	"path/filepath"
	"testing"
)

func spec(t *testing.T) Spec {
	t.Helper()
	ctx := context.Background()
	repo := t.TempDir()
	for _, a := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"config", "commit.gpgsign", "false"}, {"commit", "--allow-empty", "-m", "base"}} {
		if _, err := workspace.Git(ctx, repo, a...); err != nil {
			t.Fatal(err)
		}
	}
	sha, err := workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return Spec{JobID: "job", Repository: repo, Root: t.TempDir(), Candidate: Candidate{ID: "c", SHA: sha, BaseSHA: sha, ScopeRevision: 1, PolicyRevision: 1}, ProfileID: "unit", Profile: policy.Profile{Revision: 1, Argv: []string{"sh", "-c", "printf PASS"}, TimeoutSeconds: 2, SuccessPattern: "PASS"}, Launcher: []string{"env"}}
}
func TestRunnerOutcomesAndPersistence(t *testing.T) {
	for _, v := range []struct {
		name, script, status string
		timeout              int
	}{{"pass", "printf PASS", "passed", 2}, {"fail", "exit 7", "failed", 2}, {"timeout", "sleep 10", "timeout", 1}, {"empty", "true", "failed", 2}, {"env", "test -z \"$GH_TOKEN\" && printf PASS", "passed", 2}} {
		t.Run(v.name, func(t *testing.T) {
			s := spec(t)
			s.Profile.Argv = []string{"sh", "-c", v.script}
			s.Profile.TimeoutSeconds = v.timeout
			t.Setenv("GH_TOKEN", "must-not-inherit")
			r, _ := Execute(context.Background(), s)
			if r.Status != v.status {
				t.Fatalf("status %s, reason %s", r.Status, r.Reason)
			}
			saved, err := LoadResult(s.Root, s.JobID)
			if err != nil || saved.Status != v.status || saved.TestedSHA != s.Candidate.SHA {
				t.Fatal(saved, err)
			}
			if _, err = Execute(context.Background(), s); err == nil {
				t.Fatal("job repeated")
			}
			if _, err = os.Stat(filepath.Join(s.Root, s.JobID, "spec.json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestRequiredProfilesAndSHA(t *testing.T) {
	s := spec(t)
	r, err := Execute(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	v := policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": s.Profile}}
	if !Passed(s.Candidate, v, []Result{r}) {
		t.Fatal("valid evidence rejected")
	}
	r.TestedSHA = "stale"
	if Passed(s.Candidate, v, []Result{r}) {
		t.Fatal("stale sha accepted")
	}
	if Passed(s.Candidate, v, nil) {
		t.Fatal("empty results accepted")
	}
	r.TestedSHA = s.Candidate.SHA
	r.ExitCode = nil
	if Passed(s.Candidate, v, []Result{r}) {
		t.Fatal("missing exit accepted")
	}
}
func TestOrphanIsNotRerun(t *testing.T) {
	s := spec(t)
	os.Mkdir(filepath.Join(s.Root, s.JobID), 0700)
	if _, err := LoadResult(s.Root, s.JobID); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err := Execute(context.Background(), s); err == nil {
		t.Fatal("orphan rerun")
	}
}
func TestTruncatedSuccessEvidenceIsInconclusive(t *testing.T) {
	s := spec(t)
	s.Profile.Argv = []string{"sh", "-c", "head -c 8500000 /dev/zero; printf PASS"}
	r, err := Execute(context.Background(), s)
	if err == nil || r.Status != "unknown" || !r.LogTruncated {
		t.Fatal("truncated evidence misclassified", r, err)
	}
}
