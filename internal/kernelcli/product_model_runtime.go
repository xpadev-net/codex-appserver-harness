package kernelcli

import (
	"errors"
	"os"
	"path/filepath"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Only the current isolated product_v2 runtime activates model dispatch. A
// standalone local executor remains useful without enabling model turns.
func productModelRuntime(k *storage.KernelStore, runtime control.IsolatedConfig, repositories map[int64]product.ExecutionRepository) (*storage.ProductModelRuntime, error) {
	if !runtime.ProductTools || runtime.ProductToolProfile == "" || runtime.ProductToolProfile == "product_v1" {
		return nil, nil
	}
	if runtime.ProductToolProfile != "product_v2" || !runtime.IsolationVerified || runtime.Model == "" || runtime.LauncherContract != "argv_passthrough_v1" || len(runtime.Launcher) == 0 {
		return nil, errors.New("complete isolated product_v2 model runtime required")
	}

	canonical := func(path string) bool {
		actual, err := filepath.EvalSymlinks(path)
		return err == nil && filepath.IsAbs(path) && filepath.Clean(path) == path && actual == path
	}
	for _, program := range []string{runtime.CodexBinary, runtime.Launcher[0]} {
		info, err := os.Stat(program)
		if !canonical(program) || err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Mode().Perm()&0022 != 0 {
			return nil, errors.New("canonical protected product runtime executables required")
		}
	}
	home, err := os.Stat(runtime.WorkerCodexHome)
	if !canonical(runtime.WorkerCodexHome) || err != nil || !home.IsDir() || home.Mode().Perm()&0077 != 0 {
		return nil, errors.New("canonical private product worker home required")
	}
	if runtime.SessionAuthFile != "" {
		info, err := os.Stat(runtime.SessionAuthFile)
		if !canonical(runtime.SessionAuthFile) || err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("canonical private product session authentication source required")
		}
	}
	ids := make([]int64, 0, len(repositories))
	for id := range repositories {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	config := storage.ProductModelRuntime{Model: runtime.Model, ToolProfile: runtime.ProductToolProfile, Repositories: []storage.ProductModelRepository{}}
	for _, id := range ids {
		repo := repositories[id]
		config.Repositories = append(config.Repositories, storage.ProductModelRepository{Name: repo.Name, BaseBranch: repo.BaseBranch, InitialSHA: repo.InitialSHA, Policy: repo.Policy, Runner: repo.Runner})
	}
	isolated, err := control.NewIsolatedRuntime(k, runtime)
	if err != nil {
		return nil, err
	}
	config.RuntimeDigest = isolated.ConfigurationDigest
	return &config, nil
}
