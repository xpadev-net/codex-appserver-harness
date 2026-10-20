package service

import (
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Limits struct {
	NetworkRetries int   `json:"network_retries"`
	ValidationJobs int   `json:"validation_jobs"`
	RepairRounds   int   `json:"repair_rounds"`
	RunnerRetries  int   `json:"runner_retries"`
	ReviewRequests int   `json:"review_requests"`
	SplitDepth     int   `json:"split_depth"`
	SplitChildren  int   `json:"split_children"`
	Tokens         int64 `json:"tokens"`
	TurnSeconds    int   `json:"turn_seconds"`
	ReviewSeconds  int   `json:"review_seconds"`
}
type Config struct {
	CIRefMode         string               `json:"ci_ref_mode"`
	PriorityLabels    []string             `json:"priority_labels"`
	LogRetentionDays  int                  `json:"log_retention_days"`
	MaxArtifactBytes  int64                `json:"max_artifact_bytes"`
	Version           int                  `json:"version"`
	Repository        string               `json:"repository"`
	RepositoryID      int64                `json:"repository_id"`
	RepositoryPath    string               `json:"repository_path"`
	BaseBranch        string               `json:"base_branch"`
	Root              string               `json:"root"`
	Model             string               `json:"model"`
	CodexBinary       string               `json:"codex_binary"`
	WorkerCodexHome   string               `json:"worker_codex_home"`
	Launcher          []string             `json:"launcher"`
	IsolationVerified bool                 `json:"isolation_verified"`
	CapabilitiesPath  string               `json:"capabilities_path"`
	ReadyLabel        string               `json:"ready_label"`
	HoldLabel         string               `json:"hold_label"`
	Verification      policy.Verification  `json:"verification"`
	VerifierPaths     []string             `json:"verifier_paths"`
	Review            review.Policy        `json:"review"`
	ReviewTrigger     string               `json:"review_trigger"`
	RequesterID       int64                `json:"requester_id"`
	RequiredChecks    []gate.RequiredCheck `json:"required_checks"`
	MergeEnabled      bool                 `json:"merge_enabled"`
	MergeMethod       string               `json:"merge_method"`
	Limits            Limits               `json:"limits"`
	ReconcileSeconds  int                  `json:"reconcile_seconds"`
}

func Load(path string) (Config, error) {
	var c Config
	info, err := os.Stat(path)
	if err != nil {
		return c, err
	}
	if info.Mode().Perm()&0022 != 0 {
		return c, errors.New("trusted config must not be writable by other users")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err = contract.DecodeStrict(b, &c); err != nil {
		return c, err
	}
	c.RepositoryPath = canonical(c.RepositoryPath)
	c.Root = canonical(c.Root)
	c.WorkerCodexHome = canonical(c.WorkerCodexHome)
	if len(c.Launcher) > 0 {
		c.Launcher[0] = canonical(c.Launcher[0])
	}
	c.CodexBinary = canonical(c.CodexBinary)
	if c.CapabilitiesPath != "" {
		c.CapabilitiesPath = canonical(c.CapabilitiesPath)
	}
	if err = c.Validate(); err != nil {
		return c, err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return c, err
	}
	if within(c.RepositoryPath, canonical(absolute)) || within(c.Root, canonical(absolute)) || within(c.WorkerCodexHome, canonical(absolute)) {
		return c, errors.New("trusted configuration must be outside the repository")
	}
	return c, nil
}
func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
func (c Config) Validate() error {
	if within(c.Root, c.WorkerCodexHome) || within(c.WorkerCodexHome, c.Root) || within(c.WorkerCodexHome, c.RepositoryPath) || within(c.Root, c.CodexBinary) || within(c.WorkerCodexHome, c.CodexBinary) || (len(c.Launcher) > 0 && (within(c.Root, c.Launcher[0]) || within(c.WorkerCodexHome, c.Launcher[0]))) {
		return errors.New("trusted executable/auth/state paths must not overlap worker-writable paths")
	}
	if c.CapabilitiesPath != "" && (!filepath.IsAbs(c.CapabilitiesPath) || within(c.RepositoryPath, c.CapabilitiesPath) || within(c.Root, c.CapabilitiesPath) || within(c.WorkerCodexHome, c.CapabilitiesPath)) {
		return errors.New("capability evidence must be outside repository and worker-writable paths")
	}
	if (c.CIRefMode != "head" && c.CIRefMode != "test_merge") || len(c.PriorityLabels) == 0 || c.LogRetentionDays < 1 || c.MaxArtifactBytes < 1 || !regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`).MatchString(c.Repository) || c.Version != 1 || c.RepositoryID < 1 || !strings.Contains(c.Repository, "/") || c.BaseBranch == "" || c.Model == "" || !filepath.IsAbs(c.RepositoryPath) || !filepath.IsAbs(c.Root) || !filepath.IsAbs(c.CodexBinary) || !filepath.IsAbs(c.WorkerCodexHome) || within(c.RepositoryPath, c.Root) || within(c.Root, c.RepositoryPath) || within(c.RepositoryPath, c.WorkerCodexHome) || len(c.Launcher) == 0 || !filepath.IsAbs(c.Launcher[0]) || c.ReconcileSeconds < 1 || c.ReadyLabel == "" || c.HoldLabel == "" {
		return errors.New("invalid configuration or execution paths overlap trusted repository")
	}
	if c.Limits.NetworkRetries < 0 || c.Limits.ValidationJobs < 1 || c.Limits.RepairRounds < 1 || c.Limits.RunnerRetries < 0 || c.Limits.ReviewRequests < 1 || c.Limits.SplitDepth < 1 || c.Limits.SplitChildren < 2 || c.Limits.Tokens < 1 || c.Limits.TurnSeconds < 1 || c.Limits.ReviewSeconds < 1 {
		return errors.New("all execution budgets must be configured")
	}
	if len(c.VerifierPaths) == 0 {
		return errors.New("verifier path policy required")
	}
	return c.Verification.Validate()
}

func canonical(path string) string {
	if path == "" {
		return path
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	if real, err := filepath.EvalSymlinks(absolute); err == nil {
		return real
	}
	parent := filepath.Dir(absolute)
	if parent == absolute {
		return absolute
	}
	return filepath.Join(canonical(parent), filepath.Base(absolute))
}
