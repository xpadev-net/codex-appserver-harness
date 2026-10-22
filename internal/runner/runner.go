// Package runner executes fixed candidates without LLM calls. Linux process groups
// terminate profile descendants; deployment launchers supply credential isolation.
package runner

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
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
	LauncherContract string         `json:"launcher_contract"`
	JobID            string         `json:"job_id"`
	Repository       string         `json:"repository"`
	Root             string         `json:"root"`
	Candidate        Candidate      `json:"candidate"`
	ProfileID        string         `json:"profile_id"`
	Profile          policy.Profile `json:"profile"`
	ForbiddenPaths   []string       `json:"forbidden_paths"`
	Launcher         []string       `json:"launcher"`
}
type Result struct {
	ResolutionEvidence string    `json:"resolution_evidence,omitempty"`
	ResolutionState    string    `json:"resolution_state,omitempty"`
	ResolvedAt         time.Time `json:"resolved_at,omitempty"`
	JobID              string    `json:"job_id"`
	Candidate          Candidate `json:"candidate"`
	TestedSHA          string    `json:"tested_sha"`
	ProfileID          string    `json:"profile_id"`
	ProfileRevision    int       `json:"profile_revision"`
	Status             string    `json:"status"`
	ExitCode           *int      `json:"exit_code"`
	Started            time.Time `json:"started"`
	Finished           time.Time `json:"finished"`
	LogPath            string    `json:"log_path"`
	Environment        string    `json:"environment"`
	LogTruncated       bool      `json:"log_truncated"`
	Reason             string    `json:"reason,omitempty"`
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var sha = regexp.MustCompile(`^[0-9a-f]{40,64}$`)

func (s Spec) Validate() error {
	if s.LauncherContract != "argv_passthrough_v1" || !identifier.MatchString(s.JobID) || s.Candidate.ID == "" || !sha.MatchString(s.Candidate.SHA) || !sha.MatchString(s.Candidate.BaseSHA) || s.Candidate.ScopeRevision < 1 || s.Candidate.PolicyRevision < 1 || !filepath.IsAbs(s.Root) || !filepath.IsAbs(s.Repository) || !identifier.MatchString(s.ProfileID) || s.Profile.Revision < 1 || len(s.Profile.Argv) == 0 || s.Profile.Argv[0] == "" || s.Profile.TimeoutSeconds < 1 || len(s.Launcher) == 0 || s.Launcher[0] == "" {
		return errors.New("invalid runner specification or isolation launcher missing")
	}
	return s.Profile.ValidateEvidence()
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
	if os.IsNotExist(err) {
		b, err = os.ReadFile(filepath.Join(root, id+".resolved.json"))
	}
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
	lease, err := jobLock(ctx, s, true)
	if err != nil {
		return r, err
	}
	defer lease.Close()
	if _, err = LoadResult(s.Root, s.JobID); err == nil {
		return r, errors.New("job already completed or resolved")
	} else if !os.IsNotExist(err) {
		return r, err
	}
	dir := filepath.Join(s.Root, s.JobID)
	if err = os.MkdirAll(s.Root, 0700); err != nil {
		return r, err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return r, fmt.Errorf("job already exists or unavailable; reconcile: %w", err)
	}
	life := Lifecycle{State: "preparing", Process: identity(os.Getpid())}
	if err = saveLifecycle(s, life); err != nil {
		return r, err
	}
	jobCtx, stopJob := context.WithCancel(ctx)
	defer stopJob()
	ctx = jobCtx
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				if _, err := os.Stat(filepath.Join(s.Root, s.JobID+".cancel")); err == nil {
					stopJob()
					return
				}
			}
		}
	}()
	var profile process.Identity
	r = Result{JobID: s.JobID, Candidate: s.Candidate, ProfileID: s.ProfileID, ProfileRevision: s.Profile.Revision, Status: "infrastructure_failure", Started: time.Now().UTC(), Environment: runtime.GOOS + "/" + runtime.GOARCH, LogPath: filepath.Join(dir, "output.log")}
	defer func() {
		if profile.PID > 0 {
			stopping, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			stopErr := process.WaitStopped(stopping, profile)
			cancel()
			if stopErr != nil {
				r.Status = "unknown"
				r.Finished = time.Now().UTC()
				err = errors.Join(err, stopErr)
				return
			}
		}
		r.Finished = time.Now().UTC()
		if ctx.Err() != nil && r.Status == "infrastructure_failure" {
			r.Status = "canceled"
		}
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
	if strings.Contains(s.Profile.Argv[0], "/") && !filepath.IsAbs(s.Profile.Argv[0]) {
		entry := filepath.Join(checkout, s.Profile.Argv[0])
		if rel, relErr := filepath.Rel(checkout, entry); relErr != nil || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return r, errors.New("profile entrypoint escapes checkout")
		}
		data, readErr := os.ReadFile(entry)
		if readErr != nil {
			return r, readErr
		}
		if s.Profile.EntryPointSHA256 == "" || fmt.Sprintf("%x", sha256.Sum256(data)) != s.Profile.EntryPointSHA256 {
			return r, errors.New("trusted profile entrypoint changed; explicit policy revision required")
		}
	}
	log, err := os.OpenFile(r.LogPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return r, err
	}
	defer log.Close()
	output := &boundedLog{writer: log, limit: 8 << 20}
	redacted := newRedactedLog(output, secretValues())
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(s.Profile.TimeoutSeconds)*time.Second)
	defer cancel()
	argv := append(append([]string{}, s.Launcher...), s.Profile.Argv...)
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = checkout
	home := filepath.Join(dir, "home")
	if err = os.Mkdir(home, 0700); err != nil {
		return r, err
	}
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "HARNESS_SANDBOX_HOME=" + home, "HARNESS_LAUNCH_MODE=runner", "HARNESS_WORKSPACE=" + checkout, "HARNESS_JOB_ID=" + s.JobID}
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR", "CURL_CA_BUNDLE", "NODE_EXTRA_CA_CERTS"} {
		if value, ok := os.LookupEnv(name); ok {
			cmd.Env = append(cmd.Env, name+"="+value)
		}
	}
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return r, err
	}
	expected := fmt.Sprintf("harness-launcher:%x", nonce)
	probeArgs := append(append([]string{}, s.Launcher...), "sh", "-c", `test "$PWD" = "$HARNESS_WORKSPACE" && test "$1" = 'literal $ ; space' && printf '%s' "$2"`, "probe", "literal $ ; space", expected)
	canary := exec.CommandContext(runCtx, probeArgs[0], probeArgs[1:]...)
	canary.Dir = checkout
	canary.Env = cmd.Env
	observed, probeErr := canary.Output()
	if probeErr != nil || !bytes.Equal(observed, []byte(expected)) {
		return r, errors.New("launcher does not preserve command argv/cwd/environment")
	}
	exitArgs := append(append([]string{}, s.Launcher...), "sh", "-c", "exit 37")
	exitProbe := exec.CommandContext(runCtx, exitArgs[0], exitArgs[1:]...)
	exitProbe.Dir = checkout
	exitProbe.Env = cmd.Env
	var exitError *exec.ExitError
	if exitErr := exitProbe.Run(); !errors.As(exitErr, &exitError) || exitError.ExitCode() != 37 {
		return r, errors.New("launcher does not preserve command exit status")
	}
	if len(s.ForbiddenPaths) > 0 {
		args := append(append([]string{}, s.Launcher...), "sh", "-c", `for p in "$@"; do test ! -e "$p" || exit 1; done`, "probe")
		args = append(args, s.ForbiddenPaths...)
		probe := exec.CommandContext(runCtx, args[0], args[1:]...)
		probe.Dir = checkout
		probe.Env = cmd.Env
		if probe.Run() != nil {
			return r, errors.New("runner credential isolation probe failed")
		}
	}
	cmd.Stdout = redacted
	cmd.Stderr = redacted
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var groupKillOnce sync.Once
	var groupKillErr error
	cmd.Cancel = func() error {
		groupKillOnce.Do(func() { groupKillErr = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
		return groupKillErr
	}
	cmd.WaitDelay = 2 * time.Second
	if err = cmd.Start(); err != nil {
		return r, err
	}
	life.State = "running"
	profile = identity(cmd.Process.Pid)
	life.Profile = profile
	if err = saveLifecycle(s, life); err != nil {
		_ = cmd.Cancel()
		_ = cmd.Wait()
		return r, err
	}
	leaderErr := waitLeaderExit(runCtx, profile)
	// Signal once before Wait can reap the original leader, including normal
	// completion. A late context watcher cannot signal a recycled group ID.
	_ = cmd.Cancel()
	waitErr := errors.Join(leaderErr, cmd.Wait())
	if flushErr := redacted.Flush(); flushErr != nil {
		waitErr = errors.Join(waitErr, flushErr)
	}
	r.LogTruncated = output.truncated
	if cmd.ProcessState != nil {
		code := cmd.ProcessState.ExitCode()
		r.ExitCode = &code
	}
	life.State = "quiesced"
	if err = saveLifecycle(s, life); err != nil {
		return r, err
	}
	if ctx.Err() != nil {
		r.Status = "canceled"
		return r, ctx.Err()
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		r.Status = "timeout"
		return r, runCtx.Err()
	}
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
	{
		if output.truncated {
			r.Status = "unknown"
			return r, errors.New("output truncated; success evidence inconclusive")
		}
		b, readErr := os.ReadFile(r.LogPath)
		if readErr != nil {
			return r, readErr
		}
		match := regexp.MustCompile(s.Profile.SuccessPattern).FindIndex(b)
		if len(b) == 0 || match == nil || match[0] == match[1] {
			r.Status = "failed"
			return r, errors.New("required success evidence missing")
		}
	}
	r.Status = "passed"
	return r, nil
}

type boundedLog struct {
	mu        sync.Mutex
	writer    io.Writer
	limit     int
	written   int
	truncated bool
}

func (b *boundedLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	original := len(p)
	left := b.limit - b.written
	if original > left {
		b.truncated = true
	}
	if left > 0 {
		if len(p) > left {
			p = p[:left]
		}
		n, err := b.writer.Write(p)
		b.written += n
		if err == nil && n != len(p) {
			err = io.ErrShortWrite
		}
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
		var latest *Result
		for i := range results {
			r := &results[i]
			if r.Candidate == c && r.ProfileID == id && (latest == nil || r.Started.After(latest.Started) || (r.Started.Equal(latest.Started) && r.JobID > latest.JobID)) {
				latest = r
			}
		}
		if latest == nil {
			return false
		}
		r := latest
		if r.TestedSHA != c.SHA || r.ProfileRevision != v.Profiles[id].Revision || r.Status != "passed" || r.ExitCode == nil || *r.ExitCode != 0 || r.Started.IsZero() || r.Finished.IsZero() || r.Finished.Before(r.Started) || r.LogPath == "" {
			return false
		}
	}
	return true
}

// Launch starts a durable runner subprocess; the parent only polls result.json.
func Launch(ctx context.Context, executable, specPath string) (*os.Process, error) {
	return LaunchWithWake(ctx, executable, specPath, "")
}

// LaunchWithWake passes the private notification path only to the trusted
// wrapper argv, never into the profile specification or its environment.
func LaunchWithWake(ctx context.Context, executable, specPath, socketPath string) (*os.Process, error) {
	if socketPath != "" && (!filepath.IsAbs(socketPath) || filepath.Base(socketPath) != ipc.SocketName) {
		return nil, errors.New("absolute controller socket path required")
	}
	if strings.TrimSpace(specPath) == "" {
		return nil, errors.New("spec path required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(specPath)
	if err != nil {
		return nil, err
	}
	var spec Spec
	if err = json.Unmarshal(b, &spec); err != nil {
		return nil, err
	}
	if err = spec.Validate(); err != nil {
		return nil, err
	}
	lease, err := jobLock(ctx, spec, false)
	if err != nil {
		return nil, err
	}
	defer lease.Close()
	if _, err = LoadResult(spec.Root, spec.JobID); err == nil {
		return nil, errors.New("job already completed or resolved")
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	life, err := inspectLocked(spec)
	if err != nil {
		return nil, err
	}
	if life.State != "not_started" {
		return nil, errors.New("existing runner requires reconciliation")
	}
	life = Lifecycle{State: "launching"}
	if err = saveLifecycle(spec, life); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	args := []string{"_runner", specPath}
	if socketPath != "" {
		args = append(args, "--wake-socket", socketPath)
	}
	cmd := exec.Command(executable, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	life.Process = identity(cmd.Process.Pid)
	persistErr := saveLifecycle(spec, life)
	go cmd.Wait()
	return cmd.Process, persistErr
}
