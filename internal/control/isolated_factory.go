package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// IsolatedConfig is common to main and Task sessions. K has no GitHub repository
// or merge-capability prerequisite. IsolationVerified is a trusted operator
// attestation about descendant containment; per-launch probes remain mandatory.
type IsolatedConfig struct {
	PrivateRoot       string
	Model             string
	CodexBinary       string
	WorkerCodexHome   string // Dedicated authentication/configuration template.
	Launcher          []string
	LauncherContract  string
	IsolationVerified bool
	GitDirectories    []string // Canonical approved metadata roots for linked worktrees.
}

type IsolatedFactory struct{ Config IsolatedConfig }

// NewIsolatedRuntime binds the private-path probe policy to the actual database
// root and freezes launcher arguments. Per-session path/version/isolation checks
// remain inside Open, after the managed launch reservation.
func NewIsolatedRuntime(store *storage.KernelStore, config IsolatedConfig) (Runtime, error) {
	if store == nil {
		return Runtime{}, errors.New("Kernel store required")
	}
	root, err := existingAbsolute(config.PrivateRoot)
	if err != nil || root != store.Root {
		return Runtime{}, errors.New("runtime private root must match the Kernel store")
	}
	config.PrivateRoot = root
	config.Launcher = append([]string{}, config.Launcher...)
	config.GitDirectories = append([]string{}, config.GitDirectories...)
	return Runtime{Store: store, Factory: IsolatedFactory{Config: config}}, nil
}

func pathContains(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
func existingAbsolute(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("canonical absolute runtime path required")
	}
	return filepath.EvalSymlinks(path)
}

// Open runs only after the runtime has durably reserved this preparation and
// crossed its launch boundary. Probes invoke no model turn. Parent and Task get
// identical executable, environment policy and app-server flags; the approved
// binding supplies CWD/model/thread sandbox through the common SessionDriver.
func (f IsolatedFactory) Open(ctx context.Context, launch Launch) (Process, error) {
	fail := func(err error) (Process, error) { return nil, errors.Join(ErrNotStarted, err) }
	c := f.Config
	if !c.IsolationVerified || c.LauncherContract != "argv_passthrough_v1" || len(c.Launcher) == 0 || c.Model == "" || c.Model != launch.Binding.Model {
		return fail(errors.New("verified common launcher contract and matching model required"))
	}
	root, err := existingAbsolute(c.PrivateRoot)
	if err != nil {
		return fail(err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return fail(errors.New("private controller root required"))
	}
	workspace, err := existingAbsolute(launch.Binding.CWD)
	if err != nil {
		return fail(err)
	}
	if workspace != launch.Binding.CWD {
		return fail(errors.New("session binding must use its canonical workspace"))
	}
	workspaceInfo, err := os.Stat(workspace)
	if err != nil || !workspaceInfo.IsDir() {
		return fail(errors.New("fixed session workspace required"))
	}
	binary, err := existingAbsolute(c.CodexBinary)
	if err != nil {
		return fail(err)
	}
	launcher, err := existingAbsolute(c.Launcher[0])
	if err != nil {
		return fail(err)
	}
	auth, err := existingAbsolute(c.WorkerCodexHome)
	if err != nil {
		return fail(err)
	}
	if info, err := os.Stat(auth); err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return fail(errors.New("private dedicated worker template required"))
	}
	for _, program := range []string{binary, launcher} {
		if info, err := os.Stat(program); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Mode().Perm()&0022 != 0 {
			return fail(errors.New("trusted non-writable runtime executable required"))
		}
	}
	for _, trusted := range []string{binary, launcher, auth} {
		if pathContains(root, trusted) || pathContains(workspace, trusted) {
			return fail(errors.New("runtime executable/auth paths overlap writable state or workspace"))
		}
	}
	if pathContains(workspace, root) || pathContains(auth, root) || pathContains(auth, workspace) || pathContains(auth, binary) || pathContains(auth, launcher) {
		return fail(errors.New("runtime workspace/state/executable/auth paths overlap"))
	}
	if pathContains(root, workspace) && !pathContains(filepath.Join(root, "workspaces"), workspace) {
		return fail(errors.New("session workspace must not expose controller internals"))
	}
	controllerHome := os.Getenv("CODEX_HOME")
	userHome, err := os.UserHomeDir()
	if err != nil {
		return fail(err)
	}
	if controllerHome == "" {
		controllerHome = filepath.Join(userHome, ".codex")
	}
	controllerHome, err = filepath.Abs(controllerHome)
	if err != nil {
		return fail(err)
	}
	if canonical, err := filepath.EvalSymlinks(controllerHome); err == nil && (pathContains(canonical, auth) || pathContains(auth, canonical)) {
		return fail(errors.New("dedicated worker Codex home required"))
	}
	sessionKey := sha256.Sum256([]byte(launch.Binding.ID))
	home := filepath.Join(root, "worker-homes", fmt.Sprintf("session-%x", sessionKey[:16]))
	if pathContains(workspace, home) {
		return fail(errors.New("sandbox home must be outside the session workspace"))
	}
	if err = os.MkdirAll(home, 0700); err != nil {
		return fail(err)
	}
	actualHome, err := filepath.EvalSymlinks(home)
	if err != nil || actualHome != home {
		return fail(errors.New("sandbox home must not redirect through a symlink"))
	}
	profile, err := sessionProfile(auth, home)
	if err != nil {
		return fail(err)
	}
	gitdir, err := isolatedGitDirectory(ctx, workspace, home, root, auth, c.GitDirectories)
	if err != nil {
		return fail(err)
	}
	environment := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "HARNESS_SANDBOX_HOME=" + home, "HARNESS_LAUNCH_MODE=worker", "CODEX_HOME=" + profile, "HARNESS_WORKSPACE=" + workspace, "HARNESS_CODEX_BINARY=" + binary, "HARNESS_GIT_DIR=" + gitdir, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=/bin/false", "SSH_ASKPASS=/bin/false"}
	for _, name := range []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value, ok := os.LookupEnv(name); ok {
			environment = append(environment, name+"="+value)
		}
	}
	// A real sentinel makes the boundary probe meaningful even before the daemon
	// has created its socket. The launcher mounts only the allowed descendants.
	sentinel, err := os.CreateTemp(root, ".launch-probe-")
	if err != nil {
		return fail(err)
	}
	probePath := sentinel.Name()
	if err = sentinel.Close(); err != nil {
		_ = os.Remove(probePath)
		return fail(err)
	}
	defer os.Remove(probePath)
	hidden := []string{probePath, auth, filepath.Join(root, "state.db"), filepath.Join(root, ipc.SocketName), filepath.Join(userHome, ".config", "gh"), filepath.Join(userHome, ".git-credentials"), filepath.Join(controllerHome, "auth.json")}
	if path := os.Getenv("GH_CONFIG_DIR"); path != "" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return fail(err)
		}
		hidden = append(hidden, absolute)
	}
	if path := os.Getenv("XDG_CONFIG_HOME"); path != "" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return fail(err)
		}
		hidden = append(hidden, filepath.Join(absolute, "gh"))
	}
	args := append([]string{}, c.Launcher[1:]...)
	probe := append(append([]string{}, args...), "sh", "-c", `for path in "$@"; do test ! -e "$path" || exit 1; done`, "probe")
	probe = append(probe, hidden...)
	if _, err = isolatedProbe(ctx, launcher, probe, environment, workspace); err != nil {
		return fail(errors.New("worker isolation probe failed; app-server was not started"))
	}
	versionArgs := append(append([]string{}, args...), binary, "--version")
	version, err := isolatedProbe(ctx, launcher, versionArgs, environment, workspace)
	if err != nil || strings.TrimSpace(version) != "codex-cli 0.163.0" {
		return fail(errors.New("isolated native Codex version does not match the pinned protocol"))
	}
	command := append(append([]string{}, args...), binary, "app-server", "--listen", "stdio://", "--disable", "goals", "--disable", "multi_agent", "--disable", "multi_agent_v2")
	return (CommandFactory{Build: func(context.Context, Launch) (Command, error) {
		return Command{Program: launcher, Arguments: command, Environment: environment, ContainmentVerified: true}, nil
	}}).Open(ctx, launch)
}

type probeOutput struct {
	bytes     []byte
	truncated bool
}

func (b *probeOutput) Write(p []byte) (int, error) {
	n := min(len(p), 4096-len(b.bytes))
	b.bytes = append(b.bytes, p[:n]...)
	b.truncated = b.truncated || n < len(p)
	return len(p), nil
}
func isolatedProbe(ctx context.Context, program string, args, environment []string, cwd string) (string, error) {
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Env, cmd.Dir = environment, cwd
	var output probeOutput
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil {
		return "", err
	}
	if output.truncated {
		return "", errors.New("runtime probe output exceeded its bound")
	}
	return string(output.bytes), nil
}

func isolatedGitDirectory(ctx context.Context, workspace, home, root, auth string, approved []string) (string, error) {
	git, err := exec.LookPath("git")
	if err != nil {
		return "", err
	}
	environment := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0"}
	inside, err := isolatedProbe(ctx, git, []string{"rev-parse", "--is-inside-work-tree"}, environment, workspace)
	if err != nil {
		// A repository marker that cannot be resolved is uncertainty, never a
		// reason to mount arbitrary metadata or ignore a corrupted worktree.
		if _, statErr := os.Lstat(filepath.Join(workspace, ".git")); !os.IsNotExist(statErr) {
			return "", errors.New("session Git metadata cannot be resolved")
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		empty := filepath.Join(home, "empty-git-metadata")
		if err = os.MkdirAll(empty, 0700); err != nil {
			return "", err
		}
		canonical, err := filepath.EvalSymlinks(empty)
		if err != nil || canonical != empty {
			return "", errors.New("empty Git metadata directory must not redirect")
		}
		return empty, nil
	}
	if strings.TrimSpace(inside) != "true" {
		return "", errors.New("session must be a worktree or a plain directory")
	}
	dir, err := isolatedProbe(ctx, git, []string{"rev-parse", "--path-format=absolute", "--git-common-dir"}, environment, workspace)
	if err != nil {
		return "", err
	}
	canonical, err := existingAbsolute(strings.TrimSpace(dir))
	if err != nil {
		return "", err
	}
	if pathContains(auth, canonical) || pathContains(canonical, root) || pathContains(canonical, auth) || pathContains(root, canonical) && !pathContains(workspace, canonical) {
		return "", errors.New("Git metadata overlaps private controller/auth paths")
	}
	if !pathContains(workspace, canonical) {
		matched := false
		for _, path := range approved {
			resolved, err := existingAbsolute(path)
			if err != nil || resolved != path {
				return "", errors.New("approved Git metadata must use a canonical existing path")
			}
			matched = matched || resolved == canonical
		}
		if !matched {
			return "", errors.New("external Git metadata has not been approved for this runtime")
		}
	}
	if err = validateGitAuthentication(ctx, git, environment, workspace, nil); err != nil {
		return "", err
	}
	// The entire approved common directory is readable, including configuration
	// belonging to OTHER linked worktrees. Validate those standard files too.
	configs := []string{filepath.Join(canonical, "config"), filepath.Join(canonical, "config.worktree")}
	entries, err := os.ReadDir(filepath.Join(canonical, "worktrees"))
	if err != nil && !os.IsNotExist(err) {
		return "", errors.New("linked worktree configuration cannot be inspected")
	}
	if len(entries) > 2048 {
		return "", errors.New("too many linked worktree configurations")
	}
	for _, entry := range entries {
		if entry.IsDir() {
			configs = append(configs, filepath.Join(canonical, "worktrees", entry.Name(), "config.worktree"))
		}
	}
	for _, path := range configs {
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() {
			return "", errors.New("Git configuration must be regular")
		}
		if err = validateGitAuthentication(ctx, git, environment, workspace, []string{"--file", path}); err != nil {
			return "", err
		}
	}
	return canonical, nil
}

func validateGitAuthentication(ctx context.Context, git string, environment []string, workspace string, selector []string) error {
	// Read-only metadata is still readable by the worker. Refuse effective Git
	// authentication configuration instead of mounting controller credentials.
	// Host/global inputs are suppressed in environment. Omitting --local keeps
	// extensions.worktreeConfig in the effective view, alongside common config.
	names, err := isolatedProbe(ctx, git, append(append([]string{"config"}, selector...), "--includes", "--name-only", "--list"), environment, workspace)
	if err != nil {
		return errors.New("Git configuration cannot be checked")
	}
	for _, rawName := range strings.Fields(names) {
		name := strings.ToLower(rawName)
		if strings.HasPrefix(name, "http.") {
			if split := strings.LastIndex(rawName, "."); split > len("http.") {
				scope := rawName[len("http."):split]
				if strings.Contains(scope, "://") {
					u, err := url.Parse(scope)
					if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
						return errors.New("Git HTTP scope carries inline authentication")
					}
				}
			}
		}
		if strings.HasPrefix(name, "credential.") || strings.HasPrefix(name, "url.") || name == "core.sshcommand" || strings.HasPrefix(name, "remote.") && strings.HasSuffix(name, ".proxy") || strings.HasPrefix(name, "http.") && (strings.HasSuffix(name, "extraheader") || strings.HasSuffix(name, "cookiefile") || strings.HasSuffix(name, "proxy") || strings.HasSuffix(name, "sslkey") || strings.HasSuffix(name, "sslcert")) {
			return errors.New("Git authentication settings cannot be mounted in a worker")
		}
		if strings.HasPrefix(name, "remote.") && (strings.HasSuffix(name, ".url") || strings.HasSuffix(name, ".pushurl")) {
			value, err := isolatedProbe(ctx, git, append(append([]string{"config"}, selector...), "--includes", "--get-all", rawName), environment, workspace)
			if err != nil {
				return errors.New("Git remote configuration cannot be checked")
			}
			for _, remote := range strings.Split(strings.TrimSpace(value), "\n") {
				if strings.Contains(remote, "://") {
					u, err := url.Parse(remote)
					if err != nil || u.RawQuery != "" || u.Fragment != "" || u.User != nil && (u.Scheme != "ssh" || u.User.Username() != "git") {
						return errors.New("Git remote carries unsupported inline authentication")
					}
					if u.User != nil {
						if _, set := u.User.Password(); set {
							return errors.New("Git remote carries inline authentication")
						}
					}
				} else if user, _, found := strings.Cut(remote, "@"); found && user != "git" {
					return errors.New("Git remote carries unsupported inline authentication")
				}
			}
		}
	}
	return nil
}
