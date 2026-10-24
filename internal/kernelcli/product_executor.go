package kernelcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	return loadProductExecutorWithObserver(path, k, runtime, false)
}

// An explicitly configured standalone observer supplies its own worker count
// and cadence. Otherwise publication includes PR observation automatically.
type productExecutorPipeline struct {
	Worker       daemonWorker
	Hint         func()
	FinalGate    func(context.Context, contract.Target) error
	ConfigDigest string
}

func loadProductExecutorWithObserver(path string, k *storage.KernelStore, runtime control.IsolatedConfig, externalObserver bool) (daemonWorker, func(), error) {
	pipeline, err := loadProductExecutorPipeline(path, k, runtime, externalObserver)
	return pipeline.Worker, pipeline.Hint, err
}
func loadProductExecutorPipeline(path string, k *storage.KernelStore, runtime control.IsolatedConfig, externalObserver bool) (productExecutorPipeline, error) {
	if path == "" {
		return productExecutorPipeline{}, nil
	}
	raw, err := readFile(path, 1<<20, true)
	if err != nil {
		return productExecutorPipeline{}, err
	}
	var c ProductExecutorConfig
	if err := contract.DecodeStrict(raw, &c); err != nil {
		return productExecutorPipeline{}, err
	}
	if k == nil || c.SchemaVersion != 1 || c.EverySeconds < 1 || c.EverySeconds > 3600 || !c.IsolationVerified || len(c.Repositories) == 0 || len(c.Repositories) > 128 {
		return productExecutorPipeline{}, errors.New("product executor schema 1, verified isolation, repositories 1..128 and every_seconds 1..3600 required")
	}
	routes, reportPaths, err := c.Publication.routes(c.Repositories)
	if err != nil {
		return productExecutorPipeline{}, err
	}
	sources, err := c.Publication.sources(c.Repositories)
	if err != nil {
		return productExecutorPipeline{}, err
	}
	var mergeConfig *ProductMergeConfig
	if c.Publication != nil {
		mergeConfig = c.Publication.Merge
	}
	mergePolicies, err := mergeConfig.policies(c.Repositories, routes)
	if err != nil {
		return productExecutorPipeline{}, err
	}
	exe, err := os.Executable()
	if err != nil {
		return productExecutorPipeline{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return productExecutorPipeline{}, err
	}
	controllerHome := os.Getenv("CODEX_HOME")
	if controllerHome == "" {
		controllerHome = filepath.Join(home, ".codex")
	}
	controllerHome, err = filepath.Abs(controllerHome)
	if err != nil {
		return productExecutorPipeline{}, err
	}
	database := filepath.Join(k.Root, databaseName)
	hidden := []string{database, database + "-wal", database + "-shm", filepath.Join(k.Root, ipc.SocketName), filepath.Join(home, ".config", "gh"), filepath.Join(home, ".git-credentials"), filepath.Join(controllerHome, "auth.json")}
	hidden = append(hidden, product.MergeHTTPProtectedPaths(k.Root)...)
	for _, p := range []string{runtime.WorkerCodexHome, runtime.SessionAuthFile, os.Getenv("GH_CONFIG_DIR")} {
		if p != "" {
			absolute, err := filepath.Abs(p)
			if err != nil {
				return productExecutorPipeline{}, err
			}
			hidden = append(hidden, absolute)
		}
	}
	if p := os.Getenv("XDG_CONFIG_HOME"); p != "" {
		absolute, err := filepath.Abs(p)
		if err != nil {
			return productExecutorPipeline{}, err
		}
		hidden = append(hidden, filepath.Join(absolute, "gh"))
	}
	modelHidden := append(slices.Clone(hidden), reportPaths...)
	hidden = append(hidden, runtime.GitDirectories...)
	hidden = append(hidden, reportPaths...)
	repositories := map[int64]product.ExecutionRepository{}
	for _, repo := range c.Repositories {
		parts := strings.Split(repo.Name, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(repo.Name+repo.BaseBranch, "\x00\r\n\t ") || repo.BaseBranch == "" || len(repo.InitialSHA) != 40 || strings.Trim(repo.InitialSHA, "0123456789abcdef") != "" {
			return productExecutorPipeline{}, errors.New("trusted repository identity, base branch and fixed initial SHA required")
		}
		policy := repo.Policy
		policy.WaveID, policy.PolicyRevision = "host-executor-config", policy.Verification.Revision
		if err := policy.Validate(); err != nil {
			return productExecutorPipeline{}, err
		}
		if _, exists := repositories[policy.RepositoryID]; exists {
			return productExecutorPipeline{}, errors.New("duplicate product repository ID")
		}
		if err := repo.Runner.Validate(); err != nil {
			return productExecutorPipeline{}, err
		}
		if repo.Runner.LauncherContract != "argv_passthrough_v1" {
			return productExecutorPipeline{}, errors.New("known product runner launcher contract required")
		}
		for _, p := range []string{policy.Repository, repo.Runner.Launcher[0]} {
			actual, err := filepath.EvalSymlinks(p)
			if err != nil || !filepath.IsAbs(p) || actual != p {
				return productExecutorPipeline{}, errors.New("canonical existing product repository and launcher required")
			}
		}
		repo.Runner.ForbiddenPaths = append(slices.Clone(repo.Runner.ForbiddenPaths), hidden...)
		repo.Runner.ForbiddenPaths = append(repo.Runner.ForbiddenPaths, policy.Repository)
		slices.Sort(repo.Runner.ForbiddenPaths)
		repo.Runner.ForbiddenPaths = slices.Compact(repo.Runner.ForbiddenPaths)
		if err := repo.Runner.Validate(); err != nil {
			return productExecutorPipeline{}, err
		}
		repositories[policy.RepositoryID] = repo
	}
	if err := k.ConfigureProductIsolationPaths(modelHidden); err != nil {
		return productExecutorPipeline{}, err
	}
	modelRuntime, err := productModelRuntime(k, runtime, repositories)
	if err != nil {
		return productExecutorPipeline{}, err
	}
	executor := product.Executor{Store: k, Repositories: repositories, Runner: product.Runner{Store: k, Executable: exe, SocketPath: filepath.Join(k.Root, ipc.SocketName)}}
	if c.Publication != nil {
		sourceGate := product.KernelSourceGate{Store: k, Policies: sources, API: product.GitHubResourceAPI}
		executor.BasePlanner = &product.BasePlanner{Executor: executor, Source: sourceGate.Check, API: product.GitHubResourceAPI}
	}
	every := time.Duration(c.EverySeconds) * time.Second
	w := product.NewExecutionWorker(executor, every)
	finalPlanner, finalRunner := productFinalPipeline(executor, mergePolicies, sources, exe)
	finalGate := func(ctx context.Context, target contract.Target) error {
		if mergeConfig == nil {
			return storage.ErrProductFinalPlanHeld
		}
		_, err := finalPlanner.Prepare(ctx, target)
		return err
	}
	sum := sha256.Sum256(raw)
	return productExecutorPipeline{FinalGate: finalGate, ConfigDigest: hex.EncodeToString(sum[:]), Worker: func(ctx context.Context, store *storage.KernelStore, wake func(), report func(error)) error {
		if store != k {
			return errors.New("product executor store owner mismatch")
		}
		if modelRuntime != nil {
			revoke, err := k.BindProductModelRuntime(*modelRuntime)
			if err != nil {
				return err
			}
			defer revoke()
			if wake != nil {
				wake()
			}
		}
		w.OnMutation, w.Report = wake, report
		if c.Publication == nil {
			return w.Run(ctx)
		}
		dispatcher := productPublicationDispatcher(executor, routes, sources, c.Publication.Workers, every)
		dispatcher.OnMutation, dispatcher.Report = wake, report
		completion := product.CloudCompletionWorker{Reader: product.CloudCompletionReader{Store: k, API: product.GitHubResourceAPI, Routes: routes}, Workers: c.Publication.Workers, Every: every, OnMutation: wake, Report: report}
		reconciliation := product.MergeReconciliationWorker{Reconciler: product.MergeReconciler{Store: k, API: product.GitHubResourceAPI}, Workers: c.Publication.Workers, Every: every, OnMutation: wake, Report: report}
		workers := []func(context.Context) error{w.Run, dispatcher.Run, completion.Run, reconciliation.Run}
		if !externalObserver {
			observer := product.Observer{Store: k, API: product.GitHubResourceAPI, Workers: c.Publication.Workers, Every: every, OnMutation: wake, Report: report}
			workers = append(workers, observer.Run)
		}
		if len(sources) > 0 {
			workers = append(workers, func(ctx context.Context) error { return runPublicationSources(ctx, k, sources, every, wake, report) })
		}
		if mergeConfig != nil {
			final := product.FinalVerificationWorker{Final: finalRunner, Repositories: repositories, Every: every, OnMutation: wake, Report: report}
			workers = append(workers, final.Run)
			driver := productMergeDriver(executor, mergePolicies, sources, exe)
			return runProductMergePipeline(ctx, driver, mergePolicies.methods, mergeConfig.Workers, every, wake, report, workers...)
		}
		return runProductPipeline(ctx, workers...)
	}, Hint: w.Hint}, nil
}
