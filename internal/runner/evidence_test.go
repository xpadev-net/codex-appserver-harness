package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Execute the actual detached wrapper contract without building another binary.
func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "_runner" {
		raw, err := os.ReadFile(os.Args[2])
		var s Spec
		if err == nil {
			err = json.Unmarshal(raw, &s)
		}
		if err == nil {
			_, err = Execute(context.Background(), s)
		}
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func stoppedCompletionFixture(t *testing.T, script string, timeout int, configure ...func(*Spec)) (Spec, Completion) {
	t.Helper()
	s := spec(t)
	s.Profile.Argv = []string{"sh", "-c", script}
	s.Profile.TimeoutSeconds = timeout
	for _, apply := range configure {
		apply(&s)
	}
	path := filepath.Join(s.Root, "launch.json")
	if err := WriteJSON(path, s); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Launch(context.Background(), exe, path); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for {
		c, err := ReadStoppedResult(context.Background(), s)
		if err == nil {
			return s, c
		}
		if time.Now().After(deadline) {
			t.Fatal("completion not confirmed", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStoppedCompletionMeasuresDetachedResultAndLog(t *testing.T) {
	for _, tc := range []struct {
		script, status string
		timeout        int
	}{{"printf PASS", "passed", 3}, {"exit 7", "failed", 3}, {"sleep 20", "timeout", 1}, {"true", "failed", 3}} {
		t.Run(tc.status+tc.script, func(t *testing.T) {
			s, c := stoppedCompletionFixture(t, tc.script, tc.timeout)
			if c.Result.Status != tc.status || c.Result.TestedSHA != s.Candidate.SHA || c.ResultDigest == "" || c.LogDigest == "" || c.Validate(s) != nil {
				t.Fatal(c)
			}
			raw, err := os.ReadFile(filepath.Join(s.Root, s.JobID, "result.json"))
			if err != nil || digestBytes(raw) != c.ResultDigest {
				t.Fatal("result digest mismatch", err)
			}
		})
	}
}

func TestStoppedCompletionDistinguishesCancelAndInfrastructure(t *testing.T) {
	for _, status := range []string{"canceled", "infrastructure_failure"} {
		t.Run(status, func(t *testing.T) {
			_, c := stoppedCompletionFixture(t, "sleep 20", 3, func(s *Spec) {
				if status == "infrastructure_failure" {
					s.Launcher = []string{"/missing/isolation-launcher"}
				} else if err := os.WriteFile(filepath.Join(s.Root, s.JobID+".cancel"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			})
			if c.Result.Status != status {
				t.Fatal(c)
			}
		})
	}
}

func TestStoppedCompletionRejectsChangedEvidenceAndLiveWrapper(t *testing.T) {
	for _, mode := range []string{"candidate", "profile", "spec", "empty_log", "unknown", "live_wrapper", "missing_profile", "missing_lifecycle", "log_symlink", "synthetic"} {
		t.Run(mode, func(t *testing.T) {
			s, c := stoppedCompletionFixture(t, "printf PASS", 3)
			saveResult := false
			switch mode {
			case "candidate":
				c.Result.Candidate.SHA = strings.Repeat("e", 40)
				saveResult = true
			case "profile":
				c.Result.ProfileRevision++
				saveResult = true
			case "spec":
				changed := s
				changed.Profile.Argv = []string{"untrusted"}
				if err := WriteJSON(filepath.Join(s.Root, s.JobID, "spec.json"), changed); err != nil {
					t.Fatal(err)
				}
			case "empty_log":
				if err := os.WriteFile(c.Result.LogPath, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				c.Result.Status = "unknown"
				saveResult = true
			case "synthetic":
				c.Result.ResolutionEvidence = "operator statement"
				saveResult = true
			case "live_wrapper", "missing_profile":
				life, err := readLifecycle(s)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "live_wrapper" {
					life.Process = identity(os.Getpid())
				} else {
					life.Profile = ProcessIdentity{}
				}
				if err := saveLifecycle(s, life); err != nil {
					t.Fatal(err)
				}
			case "missing_lifecycle":
				if err := os.Remove(filepath.Join(s.Root, s.JobID+".lifecycle.json")); err != nil {
					t.Fatal(err)
				}
			case "log_symlink":
				if err := os.Rename(c.Result.LogPath, c.Result.LogPath+".moved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(c.Result.LogPath+".moved", c.Result.LogPath); err != nil {
					t.Fatal(err)
				}
			}
			if saveResult {
				if err := WriteJSON(filepath.Join(s.Root, s.JobID, "result.json"), c.Result); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ReadStoppedResult(context.Background(), s); err == nil {
				t.Fatal("unverified completion accepted", mode)
			}
		})
	}
}
