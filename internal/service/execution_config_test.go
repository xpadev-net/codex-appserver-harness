package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
)

func TestExecutionConfigPreservesLegacySerialAndExplicitNewCapacities(t *testing.T) {
	c, _, _, _ := setup(t)
	c.Config.RequiredChecks = []gate.RequiredCheck{}
	path := filepath.Join(filepath.Dir(c.Config.Root), "trusted-config.json")
	for _, legacy := range []bool{true, false} {
		cfg := c.Config
		expected := domain.DefaultExecutionLimits()
		if legacy {
			cfg.Execution = nil
			expected = domain.LegacyExecutionLimits()
		} else {
			cfg.Execution = &expected
		}
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		loaded, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.ExecutionLimits() != expected {
			t.Fatal("configuration changed concurrency on load", loaded.ExecutionLimits(), expected)
		}
	}
	invalid := domain.DefaultExecutionLimits()
	invalid.Turns = 0
	c.Config.Execution = &invalid
	raw, err := json.Marshal(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(path); err == nil {
		t.Fatal("zero process capacity accepted")
	}
}
