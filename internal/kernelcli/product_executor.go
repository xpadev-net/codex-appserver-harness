package kernelcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type ProductExecutorConfig struct {
	Publication       *ProductPublicationConfig     `json:"publication,omitempty"`
	SchemaVersion     int                           `json:"schema_version"`
	EverySeconds      int                           `json:"every_seconds"`
	IsolationVerified bool                          `json:"isolation_verified"`
	Repositories      []product.ExecutionRepository `json:"repositories"`
}

func loadProductExecutor(path string, k *storage.KernelStore, runtime control.IsolatedConfig) (daemonWorker, func(), error) {
	if path == "" {
		return nil, nil, nil
	}
	raw, err := readFile(path, 1<<20, true)
	if err != nil {
		return nil, nil, err
	}
	var c ProductExecutorConfig
	if err := contract.DecodeStrict(raw, &c); err != nil {
		return nil, nil, err
	}
	if k == nil || c.SchemaVersion != 1 || c.EverySeconds < 1 || c.EverySeconds > 3600 || !c.IsolationVerified || len(c.Repositories) == 0 || len(c.Repositories) > 128 {
		return nil, nil, errors.New("product executor schema 1, verified isolation, repositories 1..128 and every_seconds 1..3600 required")
	}
	routes, reportPaths, err := c.Publication.routes(c.Repositories)
	if err != nil {
		return nil, nil, err
	}
	sources, err := c.Publication.sources(c.Repositories)
	if err != nil {
		return nil, nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, nil, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, err
	}
	controllerHome := os.Getenv("CODEX_HOME")
	if controllerHome == "" {
		controllerHome = filepath.Join(home, ".codex")
	}
	controllerHome, err = filepath.Abs(controllerHome)
	if err != nil {
		return nil, nil, err
	}
	database := filepath.Join(k.Root, databaseName)
	hidden := []string{database, database + "-wal", database + "-shm", filepath.Join(k.Root, ipc.SocketName), filepath.Join(home, ".config", "gh"), filepath.Join(home, ".git-credentials"), filepath.Join(controllerHome, "auth.json")}
	for _, p := range []string{runtime.WorkerCodexHome, runtime.SessionAuthFile, os.Getenv("GH_CONFIG_DIR")} {
		if p != "" {
			absolute, err := filepath.Abs(p)
			if err != nil {
				return nil, nil, err
			}
			hidden = append(hidden, absolute)
		}
	}
	if p := os.Getenv("XDG_CONFIG_HOME"); p != "" {
		absolute, err := filepath.Abs(p)
		if err != nil {
			return nil, nil, err
		}
		hidden = append(hidden, filepath.Join(absolute, "gh"))
	}
	hidden = append(hidden, runtime.GitDirectories...)
	hidden = append(hidden, reportPaths...)
	repositories := map[int64]product.ExecutionRepository{}
	for _, repo := range c.Repositories {
		parts := strings.Split(repo.Name, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(repo.Name+repo.BaseBranch, "\x00\r\n\t ") || repo.BaseBranch == "" || len(repo.InitialSHA) != 40 || strings.Trim(repo.InitialSHA, "0123456789abcdef") != "" {
			return nil, nil, errors.New("trusted repository identity, base branch and fixed initial SHA required")
		}
		policy := repo.Policy
		policy.WaveID, policy.PolicyRevision = "host-executor-config", policy.Verification.Revision
		if err := policy.Validate(); err != nil {
			return nil, nil, err
		}
		if _, exists := repositories[policy.RepositoryID]; exists {
			return nil, nil, errors.New("duplicate product repository ID")
		}
		if err := repo.Runner.Validate(); err != nil {
			return nil, nil, err
		}
		if repo.Runner.LauncherContract != "argv_passthrough_v1" {
			return nil, nil, errors.New("known product runner launcher contract required")
		}
		for _, p := range []string{policy.Repository, repo.Runner.Launcher[0]} {
			actual, err := filepath.EvalSymlinks(p)
			if err != nil || !filepath.IsAbs(p) || actual != p {
				return nil, nil, errors.New("canonical existing product repository and launcher required")
			}
		}
		repo.Runner.ForbiddenPaths = append(slices.Clone(repo.Runner.ForbiddenPaths), hidden...)
		repo.Runner.ForbiddenPaths = append(repo.Runner.ForbiddenPaths, policy.Repository)
		slices.Sort(repo.Runner.ForbiddenPaths)
		repo.Runner.ForbiddenPaths = slices.Compact(repo.Runner.ForbiddenPaths)
		if err := repo.Runner.Validate(); err != nil {
			return nil, nil, err
		}
		repositories[policy.RepositoryID] = repo
	}
	executor := product.Executor{Store: k, Repositories: repositories, Runner: product.Runner{Store: k, Executable: exe, SocketPath: filepath.Join(k.Root, ipc.SocketName)}}
	every := time.Duration(c.EverySeconds) * time.Second
	w := product.NewExecutionWorker(executor, every)
	return func(ctx context.Context, store *storage.KernelStore, wake func(), report func(error)) error {
		if store != k {
			return errors.New("product executor store owner mismatch")
		}
		w.OnMutation, w.Report = wake, report
		if c.Publication == nil {
			return w.Run(ctx)
		}
		dispatcher := productPublicationDispatcher(executor, routes, sources, c.Publication.Workers, every)
		dispatcher.OnMutation, dispatcher.Report = wake, report
		if len(sources) == 0 {
			return runProductPipeline(ctx, w.Run, dispatcher.Run)
		}
		return runProductPipeline(ctx, w.Run, dispatcher.Run, func(ctx context.Context) error { return runPublicationSources(ctx, k, sources, every, wake, report) })
	}, w.Hint, nil
}
