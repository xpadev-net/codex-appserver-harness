package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func (c *Controller) policyDigest() string {
	data, _ := json.Marshal(c.Config)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
func (c *Controller) VerifyRuntime(ctx context.Context) error {
	remote, err := workspace.Git(ctx, c.Config.RepositoryPath, "remote", "get-url", "origin")
	if err != nil {
		return err
	}
	value := strings.TrimSpace(remote)
	var path string
	if strings.HasPrefix(value, "git@github.com:") {
		path = strings.TrimPrefix(value, "git@github.com:")
	} else {
		u, err := url.Parse(value)
		if err != nil || u.Hostname() != "github.com" || u.User != nil || u.Scheme != "https" {
			return errors.New("origin must identify the configured GitHub repository without embedded credentials")
		}
		path = strings.TrimPrefix(u.Path, "/")
	}
	if !strings.EqualFold(strings.TrimSuffix(path, ".git"), c.Config.Repository) {
		return errors.New("local origin does not match configured GitHub repository")
	}
	b, err := exec.CommandContext(ctx, "gh", "api", "repos/"+c.Config.Repository).Output()
	if err != nil {
		return err
	}
	var metadata struct {
		ID int64 `json:"id"`
	}
	if err = json.Unmarshal(b, &metadata); err != nil {
		return err
	}
	if metadata.ID != c.Config.RepositoryID {
		return errors.New("GitHub repository numeric identity changed")
	}
	version, err := exec.CommandContext(ctx, c.Config.CodexBinary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != "codex-cli 0.163.0" {
		return errors.New("Codex binary does not match pinned protocol")
	}
	return nil
}
func (c *Controller) artifactBudget() error {
	var total int64
	err := filepath.WalkDir(filepath.Join(c.Config.Root, "jobs"), func(_ string, entry fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if total >= c.Config.MaxArtifactBytes {
		return errors.New("artifact budget exhausted; inspect retention and run gc")
	}
	return nil
}
func (c *Controller) GC(ctx context.Context) error {
	items, err := c.Store.List(ctx)
	if err != nil {
		return err
	}
	for _, w := range items {
		if !w.Terminal() {
			continue
		}
		records, err := c.Store.Records(ctx, "validation", w.ID)
		if err != nil {
			return err
		}
		for _, data := range records {
			var result runner.Result
			if err = json.Unmarshal(data, &result); err != nil {
				return err
			}
			if result.Finished.IsZero() || time.Since(result.Finished) < time.Duration(c.Config.LogRetentionDays)*24*time.Hour {
				continue
			}
			if !within(filepath.Join(c.Config.Root, "jobs"), canonical(result.LogPath)) {
				return errors.New("refusing to delete evidence outside job root")
			}
			if err = os.Remove(result.LogPath); err != nil && !os.IsNotExist(err) {
				return err
			}
			if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`).MatchString(result.JobID) {
				return errors.New("invalid retained job id")
			}
			for _, entry := range []string{"checkout", "home"} {
				path := filepath.Join(c.Config.Root, "jobs", result.JobID, entry)
				if err = os.RemoveAll(path); err != nil {
					return err
				}
			}
			if err = c.Store.Put(ctx, "evidence_retention", result.JobID, w.ID, map[string]any{"logs": "expired", "expired_at": time.Now().UTC()}); err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *Controller) PendingModelWork(ctx context.Context, id string) bool {
	w, err := c.Store.Get(ctx, id)
	return err == nil && (w.Phase == domain.Implementing || w.Phase == domain.Verifying)
}
func forbiddenPaths(c Config) []string {
	home, _ := os.UserHomeDir()
	paths := []string{filepath.Join(c.Root, "state.db"), filepath.Join(c.Root, ipc.SocketName), filepath.Join(home, ".config", "gh"), filepath.Join(home, ".git-credentials")}
	if p := os.Getenv("GH_CONFIG_DIR"); p != "" {
		paths = append(paths, p)
	}
	if p := os.Getenv("XDG_CONFIG_HOME"); p != "" {
		paths = append(paths, filepath.Join(p, "gh"))
	}
	return paths
}
func (c *Controller) ensurePolicy(ctx context.Context) error {
	return c.Store.EnsureRecord(ctx, "policy_version", fmt.Sprintf("%d:%d", c.Config.RepositoryID, c.Config.Verification.Revision), c.Config)
}

type writeGuardKey struct{}
type writeGuard struct {
	WorkItem   domain.WorkItem
	Kind       string
	ObservedAt time.Time
}

func (c *Controller) writeContext(ctx context.Context, w domain.WorkItem, kind string) context.Context {
	return context.WithValue(ctx, writeGuardKey{}, writeGuard{WorkItem: w, Kind: kind, ObservedAt: time.Now().UTC()})
}

// beforeWrite runs after the adapter's final repository-ID lookup and immediately
// before the HTTP mutation, including after slow push or request reconciliation.
func (c *Controller) beforeWrite(ctx context.Context, _, _ string) error {
	guard, ok := ctx.Value(writeGuardKey{}).(writeGuard)
	if !ok {
		return errors.New("missing controller write guard")
	}
	if guard.Kind == "merge" && time.Since(guard.ObservedAt) > time.Minute {
		return errors.New("merge facts expired before send")
	}
	if guard.Kind != "close_issue" {
		if _, err := c.current(ctx, guard.WorkItem); err != nil {
			return err
		}
	}
	latest, err := c.Store.Get(ctx, guard.WorkItem.ID)
	if err != nil {
		return err
	}
	if latest.Candidate != guard.WorkItem.Candidate || latest.ScopeRevision != guard.WorkItem.ScopeRevision || latest.PolicyRevision != guard.WorkItem.PolicyRevision {
		return errors.New("candidate or scope changed before send")
	}
	allowed := false
	switch guard.Kind {
	case "publish", "review":
		allowed = latest.Phase == domain.Reviewing
	case "merge":
		allowed = latest.Phase == domain.MergePending && !latest.MergeHold
	case "split":
		allowed = latest.Phase == domain.Blocked && latest.Reason == "split_pending" && !latest.Paused
	case "close_issue":
		allowed = (latest.Phase == domain.Merged || latest.Phase == domain.Split) && !latest.Paused && !latest.MergeHold
	}
	if !allowed {
		return errors.New("operator/workflow state prohibits request")
	}
	return nil
}
