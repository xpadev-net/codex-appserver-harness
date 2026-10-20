package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 || os.Args[1] != "doctor" {
		return fmt.Errorf("usage: harness doctor --repo OWNER/REPO [--codex codex]")
	}
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	repo := fs.String("repo", "", "GitHub repository")
	binary := fs.String("codex", "codex", "pinned Codex executable")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`).MatchString(*repo) {
		return fmt.Errorf("--repo OWNER/REPO is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r := contract.NewReport(*repo)
	var metadata struct {
		ID            int64  `json:"id"`
		DefaultBranch string `json:"default_branch"`
	}
	b, err := exec.CommandContext(ctx, "gh", "api", "repos/"+*repo).Output()
	if err == nil {
		err = json.Unmarshal(b, &metadata)
	}
	if err == nil && metadata.ID > 0 && metadata.DefaultBranch != "" {
		r.RepositoryID = metadata.ID
		r.DefaultBranch = metadata.DefaultBranch
		r.Capabilities["github_read"] = contract.Capability{State: contract.Verified, Reason: "repository metadata retrieved", Evidence: "gh api repos/" + *repo}
	}
	b, err = exec.CommandContext(ctx, *binary, "--version").Output()
	if err == nil {
		r.CodexVersion = strings.TrimSpace(string(b))
	}
	if r.CodexVersion != "codex-cli 0.163.0" {
		r.Capabilities["app_server"] = contract.Capability{State: contract.Unsupported, Reason: "binary does not match protocol manifest"}
	} else {
		c, err := codex.Start(ctx, *binary)
		if err == nil {
			defer c.Close()
			err = c.Initialize(ctx)
		}
		if err == nil {
			r.Capabilities["app_server"] = contract.Capability{State: contract.Verified, Reason: "initialize/initialized completed", Evidence: r.CodexVersion}
		}
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		contract.Report
		AutoMergeReady bool `json:"auto_merge_ready"`
	}{r, r.AutoMergeReady()})
}
