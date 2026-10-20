// Package runner executes fixed candidates without LLM calls. Linux process groups
// terminate profile descendants; deployment launchers supply credential isolation.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Candidate struct {
	ID             string `json:"id"`
	SHA            string `json:"sha"`
	BaseSHA        string `json:"base_sha"`
	ScopeRevision  int    `json:"scope_revision"`
	PolicyRevision int    `json:"policy_revision"`
}
type Spec struct {
	JobID      string         `json:"job_id"`
	Repository string         `json:"repository"`
	Root       string         `json:"root"`
	Candidate  Candidate      `json:"candidate"`
	ProfileID  string         `json:"profile_id"`
	Profile    policy.Profile `json:"profile"`
	Launcher   []string       `json:"launcher"`
}
type Result struct {
	JobID           string    `json:"job_id"`
	Candidate       Candidate `json:"candidate"`
	TestedSHA       string    `json:"tested_sha"`
	ProfileID       string    `json:"profile_id"`
	ProfileRevision int       `json:"profile_revision"`
	Status          string    `json:"status"`
	ExitCode        *int      `json:"exit_code"`
	Started         time.Time `json:"started"`
	Finished        time.Time `json:"finished"`
	LogPath         string    `json:"log_path"`
	Environment     string    `json:"environment"`
	Reason          string    `json:"reason,omitempty"`
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var sha = regexp.MustCompile(`^[0-9a-f]{40,64}$`)

func (s Spec) Validate() error {
	if !identifier.MatchString(s.JobID) || s.Candidate.ID == "" || !sha.MatchString(s.Candidate.SHA) || !sha.MatchString(s.Candidate.BaseSHA) || s.Candidate.ScopeRevision < 1 || s.Candidate.PolicyRevision < 1 || !filepath.IsAbs(s.Root) || !filepath.IsAbs(s.Repository) || !identifier.MatchString(s.ProfileID) || s.Profile.Revision < 1 || len(s.Profile.Argv) == 0 || s.Profile.Argv[0] == "" || s.Profile.TimeoutSeconds < 1 || len(s.Launcher) == 0 || s.Launcher[0] == "" {
		return errors.New("invalid runner specification or isolation launcher missing")
	}
	if s.Profile.SuccessPattern != "" {
		_, err := regexp.Compile(s.Profile.SuccessPattern)
		return err
	}
	return nil
}
func WriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, path)
}
func LoadResult(root, id string) (Result, error) {
	var r Result
	if !identifier.MatchString(id) {
		return r, errors.New("invalid job id")
	}
	b, err := os.ReadFile(filepath.Join(root, id, "result.json"))
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	if err == nil && r.JobID != id {
		err = errors.New("job identity mismatch")
	}
	return r, err
}

// Execute claims a job directory exclusively. An existing directory without result is
// unknown, never evidence the old job stopped and never grounds for an automatic rerun.
func Execute(ctx context.Context, s Spec) (r Result, err error) {
	if err = s.Validate(); err != nil {
		return r, err
	}
	dir := filepath.Join(s.Root, s.JobID)
	if err = os.MkdirAll(s.Root, 0700); err != nil {
		return r, err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return r, fmt.Errorf("job already exists or unavailable; reconcile: %w", err)
	}
	r = Result{JobID: s.JobID, Candidate: s.Candidate, ProfileID: s.ProfileID, ProfileRevision: s.Profile.Revision, Status: "infrastructure_failure", Started: time.Now().UTC(), Environment: runtime.GOOS + "/" + runtime.GOARCH, LogPath: filepath.Join(dir, "output.log")}
	defer func() {
		r.Finished = time.Now().UTC()
		if err != nil {
			r.Reason = err.Error()
		}
		if saveErr := WriteJSON(filepath.Join(dir, "result.json"), r); saveErr != nil {
			err = errors.Join(err, saveErr)
		}
	}()
	if err = WriteJSON(filepath.Join(dir, "spec.json"), s); err != nil {
		return r, err
	}
	checkout := filepath.Join(dir, "checkout")
	if _, err = workspace.Git(ctx, s.Repository, "clone", "--no-local", "--no-checkout", "--", s.Repository, checkout); err != nil {
		return r, err
	}
	if _, err = workspace.Git(ctx, checkout, "-c", "core.hooksPath=/dev/null", "checkout", "--detach", s.Candidate.SHA); err != nil {
		return r, err
	}
	r.TestedSHA, err = workspace.Resolve(ctx, checkout, "HEAD")
	if err != nil || r.TestedSHA != s.Candidate.SHA {
		return r, errors.New("checkout SHA mismatch")
	}
	if err = workspace.Clean(ctx, checkout); err != nil {
		return r, err
	}
	log, err := os.OpenFile(r.LogPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return r, err
	}
	defer log.Close()
	output := &boundedLog{writer: log, limit: 8 << 20}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(s.Profile.TimeoutSeconds)*time.Second)
	defer cancel()
	argv := append(append([]string{}, s.Launcher...), s.Profile.Argv...)
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = checkout
	home := filepath.Join(dir, "home")
	if err = os.Mkdir(home, 0700); err != nil {
		return r, err
	}
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "HARNESS_WORKSPACE=" + checkout, "HARNESS_JOB_ID=" + s.JobID}
	cmd.Stdout = output
	cmd.Stderr = output
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	if err = cmd.Start(); err != nil {
		return r, err
	}
	waitErr := cmd.Wait()
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if ctx.Err() != nil {
		r.Status = "canceled"
		return r, ctx.Err()
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		r.Status = "timeout"
		return r, runCtx.Err()
	}
	code := cmd.ProcessState.ExitCode()
	r.ExitCode = &code
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			r.Status = "failed"
		} else {
			r.Status = "unknown"
		}
		return r, waitErr
	}
	if err = log.Sync(); err != nil {
		return r, err
	}
	if s.Profile.SuccessPattern != "" {
		b, readErr := os.ReadFile(r.LogPath)
		if readErr != nil {
			return r, readErr
		}
		if !regexp.MustCompile(s.Profile.SuccessPattern).Match(b) {
			r.Status = "failed"
			return r, errors.New("required success evidence missing")
		}
	}
	r.Status = "passed"
	return r, nil
}

type boundedLog struct {
	mu      sync.Mutex
	writer  io.Writer
	limit   int
	written int
}

func (b *boundedLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	original := len(p)
	left := b.limit - b.written
	if left > 0 {
		if len(p) > left {
			p = p[:left]
		}
		n, err := b.writer.Write(p)
		b.written += n
		if err != nil {
			return n, err
		}
	}
	return original, nil
}

// Passed requires the complete trusted profile set and exact candidate/revisions.
func Passed(c Candidate, v policy.Verification, results []Result) bool {
	if v.Validate() != nil || c.PolicyRevision != v.Revision {
		return false
	}
	for _, id := range v.RequiredProfiles {
		found := false
		for _, r := range results {
			if r.Candidate == c && r.TestedSHA == c.SHA && r.ProfileID == id && r.ProfileRevision == v.Profiles[id].Revision && r.Status == "passed" && r.ExitCode != nil && *r.ExitCode == 0 && !r.Started.IsZero() && !r.Finished.IsZero() && r.LogPath != "" {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Launch starts a durable runner subprocess; the parent only polls result.json.
func Launch(ctx context.Context, executable, specPath string) (*os.Process, error) {
	if strings.TrimSpace(specPath) == "" {
		return nil, errors.New("spec path required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := exec.Command(executable, "_runner", specPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go cmd.Wait()
	return cmd.Process, nil
}
