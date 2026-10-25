package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func writeUpgradeSource(t *testing.T, cfg Config) (string, string, []byte) {
	t.Helper()
	parent := t.TempDir()
	if err := os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	source, target := filepath.Join(parent, "v1.json"), filepath.Join(parent, "v2.json")
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return source, target, raw
}

func TestConfigUpgradePreservesLegacyPolicyBudgetAndCLIWithoutOpeningDB(t *testing.T) {
	for _, implicit := range []bool{true, false} {
		t.Run(map[bool]string{true: "serial", false: "explicit"}[implicit], func(t *testing.T) {
			c, _, _, _ := setup(t)
			c.Config.RequiredChecks = []gate.RequiredCheck{}
			if implicit {
				c.Config.Execution = nil
			} else {
				execution := domain.DefaultExecutionLimits()
				c.Config.Execution = &execution
			}
			ctx := context.Background()
			oldDigest := c.policyDigest()
			if err := c.Store.Put(ctx, "policy_snapshot", "w", "w", oldDigest); err != nil {
				t.Fatal(err)
			}
			source, target, original := writeUpgradeSource(t, c.Config)
			dbPath := filepath.Join(c.Config.Root, "state.db")
			before, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			wal, err := os.ReadFile(dbPath + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			if err := CLI(ctx, []string{"upgrade-config", "--config", source, "--output", target}); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(target)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Version != 2 || cfg.Execution == nil || cfg.ExecutionLimits() != c.Config.ExecutionLimits() || (&Controller{Config: cfg}).policyDigest() != oldDigest || cfg.UpgradeOrigin.ImplicitExecution != implicit {
				t.Fatal("conversion reset policy identity or changed capacities", cfg)
			}
			var saved string
			if err := c.Store.Record(ctx, "policy_snapshot", "w", &saved); err != nil || saved != oldDigest {
				t.Fatal("saved policy snapshot changed", err)
			}
			for path, expected := range map[string][]byte{source: original, dbPath: before, dbPath + "-wal": wal} {
				actual, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(expected, actual) {
					t.Fatal("upgrade modified source or database", path, err)
				}
			}
			first, err := UpgradeTrustedConfig(ctx, source, target)
			if err != nil {
				t.Fatal(err)
			}
			second, err := UpgradeTrustedConfig(ctx, source, target)
			if err != nil || !reflect.DeepEqual(first, second) {
				t.Fatal("exact replay changed receipt", err)
			}
			cfg.Limits.Tokens++
			if err := cfg.Validate(); err == nil {
				t.Fatal("policy edit retained old approval identity")
			}
			cfg.UpgradeOrigin = nil
			if err := cfg.Validate(); err != nil || (&Controller{Config: cfg}).policyDigest() == oldDigest {
				t.Fatal("explicit new policy could not use normal adoption", err)
			}
		})
	}
}

func TestConfigUpgradePublicationFailureReplayAndConflict(t *testing.T) {
	for _, point := range []string{"before_publish", "after_publish", "competing_publish", "cancel"} {
		t.Run(point, func(t *testing.T) {
			c, _, _, _ := setup(t)
			c.Config.RequiredChecks = []gate.RequiredCheck{}
			source, target, original := writeUpgradeSource(t, c.Config)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("injected interruption")
			_, err := upgradeTrustedConfig(ctx, source, target, func(at string) error {
				if point == at {
					return injected
				}
				if at == "before_publish" && point == "competing_publish" {
					return os.WriteFile(target, []byte("competing config"), 0600)
				}
				if at == "before_publish" && point == "cancel" {
					cancel()
				}
				return nil
			})
			if err == nil {
				t.Fatal("injected interruption succeeded")
			}
			if point == "competing_publish" {
				if _, err := UpgradeTrustedConfig(context.Background(), source, target); err == nil {
					t.Fatal("conflicting destination overwritten")
				}
				actual, _ := os.ReadFile(target)
				if string(actual) != "competing config" {
					t.Fatal("competing file changed")
				}
			} else {
				if point != "after_publish" {
					if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("unpublished conversion became visible", err)
					}
				}
				if _, err := UpgradeTrustedConfig(context.Background(), source, target); err != nil {
					t.Fatal("interrupted conversion could not replay", err)
				}
			}
			actual, err := os.ReadFile(source)
			if err != nil || !bytes.Equal(original, actual) {
				t.Fatal("source was overwritten", err)
			}
			staged, err := filepath.Glob(filepath.Join(filepath.Dir(target), ".config-upgrade-*"))
			if err != nil || len(staged) != 0 {
				t.Fatal("abandoned staging file", staged, err)
			}
		})
	}
}

func TestConfigUpgradeRejectsUnknownOrUnsafeInputs(t *testing.T) {
	c, _, _, _ := setup(t)
	c.Config.RequiredChecks = []gate.RequiredCheck{}
	for _, mode := range []string{"unknown_version", "v2_source", "implicit_v2", "source_symlink", "source_hardlink", "source_writable", "target_symlink", "target_in_root", "same_file", "unknown_field", "changed_origin"} {
		t.Run(mode, func(t *testing.T) {
			cfg := c.Config
			source, target, raw := writeUpgradeSource(t, cfg)
			switch mode {
			case "unknown_version", "v2_source", "implicit_v2":
				cfg.Version = 99
				if mode != "unknown_version" {
					cfg.Version = 2
				}
				if mode == "v2_source" {
					execution := domain.LegacyExecutionLimits()
					cfg.Execution = &execution
				}
				raw, _ = json.Marshal(cfg)
				if err := os.WriteFile(source, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "source_symlink", "source_hardlink":
				link := source + ".link"
				var err error
				if mode == "source_symlink" {
					err = os.Symlink(source, link)
				} else {
					err = os.Link(source, link)
				}
				if err != nil {
					t.Fatal(err)
				}
				source = link
			case "source_writable":
				if err := os.Chmod(source, 0666); err != nil {
					t.Fatal(err)
				}
			case "target_symlink":
				if err := os.Symlink(source, target); err != nil {
					t.Fatal(err)
				}
			case "target_in_root":
				target = filepath.Join(cfg.Root, "config-v2.json")
			case "same_file":
				target = source
			case "unknown_field":
				raw = append(append([]byte{}, raw[:len(raw)-1]...), []byte(`,"unknown":true}`)...)
				if err := os.WriteFile(source, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "changed_origin":
				if _, err := UpgradeTrustedConfig(context.Background(), source, target); err != nil {
					t.Fatal(err)
				}
				loaded, err := Load(target)
				if err != nil {
					t.Fatal(err)
				}
				loaded.Model = "different-model"
				raw, _ = json.Marshal(loaded)
				if err := os.WriteFile(target, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := Load(target); err == nil {
					t.Fatal("changed origin accepted")
				}
			}
			if _, err := UpgradeTrustedConfig(context.Background(), source, target); err == nil {
				t.Fatal("unsafe conversion accepted", mode)
			}
		})
	}
}

func TestConfigUpgradeExistingPolicyVersionStillAdmitsAndAdoptsWork(t *testing.T) {
	c, _, _, _ := setup(t)
	ctx := context.Background()
	c.Config.RequiredChecks = []gate.RequiredCheck{}
	// Establish an actual v1 policy revision before upgrading. Revision 1 from
	// the older fixture remains untouched, as does its existing WorkItem.
	c.Config.Verification.Revision++
	if err := c.ensurePolicy(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := c.Store.Records(ctx, "policy_version", "")
	if err != nil {
		t.Fatal(err)
	}
	oldDigest := c.policyDigest()
	source, target, _ := writeUpgradeSource(t, c.Config)
	if _, err := UpgradeTrustedConfig(ctx, source, target); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(target)
	if err != nil {
		t.Fatal(err)
	}
	c.Config = cfg
	scope := verification.Scope{Revision: 1, OriginalRequest: "new admitted request", Criteria: []verification.Criterion{{ID: "AC", Requirement: "behavior"}}, Included: []string{"AC"}, Remaining: []string{}, PreviouslyMerged: []string{}}
	item := domain.WorkItem{ID: "after-upgrade", RepositoryID: 1, Issue: 2, ScopeRevision: 1, PolicyRevision: cfg.Verification.Revision, Phase: domain.Ready}
	if err := c.Enqueue(ctx, item, scope, github.Issue{Number: 2, State: "open", Body: scope.OriginalRequest}); err != nil {
		t.Fatal("upgraded config conflicts with its own immutable policy version", err)
	}
	// This is the same policy registration used by the operator adopt path.
	if err := c.ensurePolicy(ctx); err != nil {
		t.Fatal("adoption policy registration failed", err)
	}
	scope.Revision++
	if err := c.Store.Adopt(ctx, item.ID, scope.Revision, cfg.Verification.Revision, map[string]any{"scope": scope, "policy_snapshot": c.policyDigest()}); err != nil {
		t.Fatal("existing policy could not adopt a new scope", err)
	}
	after, err := c.Store.Records(ctx, "policy_version", "")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("old policy records rewritten", err)
	}
	var saved string
	if err := c.Store.Record(ctx, "policy_snapshot", item.ID, &saved); err != nil || saved != oldDigest {
		t.Fatal("new work lost preserved identity", err)
	}
}
