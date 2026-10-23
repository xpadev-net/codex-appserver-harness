package kernelcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// RuntimeConfig contains host-selected launch policy, never model output or
// credentials. SessionAuthFile references the authorized session's token source.
type RuntimeConfig struct {
	ProductTools       bool     `json:"product_tools,omitempty"`
	ProductToolProfile string   `json:"product_tool_profile,omitempty"`
	SchemaVersion      int      `json:"schema_version"`
	CodexBinary        string   `json:"codex_binary"`
	WorkerCodexHome    string   `json:"worker_codex_home"`
	SessionAuthFile    string   `json:"session_auth_file,omitempty"`
	Launcher           []string `json:"launcher"`
	LauncherContract   string   `json:"launcher_contract"`
	IsolationVerified  bool     `json:"isolation_verified"`
	GitDirectories     []string `json:"git_directories"`
}

func loadRuntime(path, root, model string) (control.IsolatedConfig, error) {
	var c RuntimeConfig
	raw, err := readFile(path, 64<<10, true)
	if err != nil {
		return control.IsolatedConfig{}, err
	}
	if err = contract.DecodeStrict(raw, &c); err != nil {
		return control.IsolatedConfig{}, err
	}
	if c.SchemaVersion != 1 || !c.IsolationVerified || c.LauncherContract != "argv_passthrough_v1" || len(c.Launcher) == 0 {
		return control.IsolatedConfig{}, errors.New("versioned verified isolation launcher required")
	}
	paths := []string{c.CodexBinary, c.WorkerCodexHome, c.Launcher[0]}
	if c.SessionAuthFile != "" {
		paths = append(paths, c.SessionAuthFile)
	}
	paths = append(paths, c.GitDirectories...)
	for _, p := range paths {
		actual, err := filepath.EvalSymlinks(p)
		if err != nil {
			return control.IsolatedConfig{}, err
		}
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || actual != p {
			return control.IsolatedConfig{}, errors.New("canonical absolute runtime paths required")
		}
	}
	return control.IsolatedConfig{PrivateRoot: root, Model: model, CodexBinary: c.CodexBinary, WorkerCodexHome: c.WorkerCodexHome, SessionAuthFile: c.SessionAuthFile, Launcher: c.Launcher, LauncherContract: c.LauncherContract, IsolationVerified: c.IsolationVerified, GitDirectories: c.GitDirectories, ProductTools: c.ProductTools, ProductToolProfile: c.ProductToolProfile}, nil
}

// The process launch reservation already owns this session's fixed workspace.
// No user-supplied path is created and no existing symlink is followed.
type workspaceFactory struct {
	control.Factory
	root string
}

func (f workspaceFactory) Open(ctx context.Context, launch control.Launch) (control.Process, error) {
	fail := func(err error) (control.Process, error) { return nil, errors.Join(control.ErrNotStarted, err) }
	id := launch.Binding.ID
	if id == "" || filepath.Base(id) != id || id == "." || id == ".." || launch.Binding.CWD != filepath.Join(f.root, "workspaces", id) {
		return fail(errors.New("fixed session workspace required"))
	}
	for _, path := range []string{filepath.Join(f.root, "workspaces"), launch.Binding.CWD} {
		if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return fail(err)
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fail(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || info.Mode().Perm()&0077 != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
			return fail(errors.New("private same-user session workspace required"))
		}
	}
	return f.Factory.Open(ctx, launch)
}

func controllerLock(root string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(root, "controller.lock"), os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !ok || st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 {
		f.Close()
		return nil, errors.New("private same-user controller lock required")
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("another controller owns this state root")
	}
	return func() { _ = f.Close() }, nil
}

// Background workers are controller-owned and must join all child work before
// returning. Wake and report are nonblocking hints, never authority.
type daemonWorker func(context.Context, *storage.KernelStore, func(), func(error)) error

func runDaemon(ctx context.Context, k *storage.KernelStore, runtime control.Runtime, diagnostics io.Writer, background ...daemonWorker) error {
	unlock, err := controllerLock(k.Root)
	if err != nil {
		return err
	}
	defer unlock()
	runtime.Factory = workspaceFactory{Factory: runtime.Factory, root: k.Root}
	source := control.NewKernelSource(k)
	loop := control.NewLoop(runtime, source)
	reporting, stopReporting := context.WithCancel(context.WithoutCancel(ctx))
	defer stopReporting()
	report := diagnosticReporter(reporting, diagnostics)
	source.Report, loop.Report = report, report
	server, err := ipc.Listen(k.Root, k, loop.Wake)
	if err != nil {
		return err
	}
	defer server.Close()
	serving, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 2+len(background))
	start := func(name string, run func() error) {
		go func() {
			err := run()
			if err == nil && serving.Err() == nil {
				err = fmt.Errorf("%s stopped unexpectedly", name)
			}
			done <- err
		}()
	}
	start("kernel IPC server", func() error { return server.Serve(serving) })
	start("kernel dispatch loop", func() error { return loop.Run(serving) })
	for _, worker := range background {
		start("kernel background worker", func() error { return worker(serving, k, loop.Wake, report) })
	}
	var failures []error
	for remaining := 2 + len(background); remaining > 0; remaining-- {
		failures = append(failures, withoutCancellation(<-done))
		cancel()
	}
	// Retain the controller lock and store until every worker and Runtime's
	// physical shutdown have completed, including cancellation of external reads.
	return errors.Join(failures...)
}

// Cancellation is an expected stop request, but a joined failure of physical
// shutdown must remain observable to the operator.
func withoutCancellation(err error) error {
	if err == context.Canceled {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var remaining []error
		for _, child := range joined.Unwrap() {
			remaining = append(remaining, withoutCancellation(child))
		}
		return errors.Join(remaining...)
	}
	return err
}
