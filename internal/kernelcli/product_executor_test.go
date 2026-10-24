package kernelcli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func productExecutorConfigFixture(t *testing.T) ProductExecutorConfig {
	t.Helper()
	launcher := filepath.Join(t.TempDir(), "launcher")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexec \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return ProductExecutorConfig{SchemaVersion: 1, EverySeconds: 60, IsolationVerified: true, Repositories: []product.ExecutionRepository{{Name: "owner/repo", BaseBranch: "trunk", InitialSHA: strings.Repeat("a", 40), Policy: storage.ProductVerificationPolicy{RepositoryID: 17, Repository: t.TempDir(), VerifierPaths: []string{"tests/"}, Verification: policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"true"}, TimeoutSeconds: 30, SuccessPattern: "PASS"}}}}, Runner: storage.ProductRunnerRuntime{Root: filepath.Join(t.TempDir(), "jobs"), LauncherContract: "argv_passthrough_v1", Launcher: []string{launcher}, ForbiddenPaths: []string{}}}}}
}

func TestProductExecutorConfigRequiresExplicitPrivateBoundedOptIn(t *testing.T) {
	if worker, hint, err := loadProductExecutor("", nil, control.IsolatedConfig{}); worker != nil || hint != nil || err != nil {
		t.Fatal("executor enabled implicitly", err)
	}
	root := cliRoot(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for _, mode := range []string{"version", "interval", "isolation", "repositories", "duplicate", "launcher", "sha", "profile", "public_file"} {
		t.Run(mode, func(t *testing.T) {
			c := productExecutorConfigFixture(t)
			switch mode {
			case "version":
				c.SchemaVersion = 2
			case "interval":
				c.EverySeconds = 0
			case "isolation":
				c.IsolationVerified = false
			case "repositories":
				c.Repositories = nil
			case "duplicate":
				c.Repositories = append(c.Repositories, c.Repositories[0])
			case "launcher":
				c.Repositories[0].Runner.Launcher = []string{}
			case "sha":
				c.Repositories[0].InitialSHA = "trunk"
			case "profile":
				c.Repositories[0].Policy.Verification.Profiles["unit"] = policy.Profile{}
			}
			file := operatorFile(t, c)
			if mode == "public_file" {
				if err := os.Chmod(file, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := loadProductExecutor(file, k, control.IsolatedConfig{}); err == nil {
				t.Fatal("invalid product executor accepted")
			}
		})
	}
	file := operatorFile(t, productExecutorConfigFixture(t))
	worker, hint, err := loadProductExecutor(file, k, control.IsolatedConfig{})
	if err != nil || worker == nil || hint == nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "list", "--root", root, "--product-executor", file); err == nil {
		t.Fatal("product executor flag accepted by reader")
	}
}
