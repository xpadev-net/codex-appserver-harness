package kernelcli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func TestProductWorkspaceCLIUsesPrivateHostRequestAndNoModelFactory(t *testing.T) {
	ctx := context.Background()
	root := cliRoot(t)
	k, main, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	w, err := k.CreateProductWave(ctx, storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: main.SessionID, Source: "direct", SourceID: "cli-preparation", OriginalRequest: "implement the registered behavior", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "preserve expected behavior"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: main.Limits}}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	spec := contract.TaskSpec{ID: "implementation", Kind: "work", Instruction: "implement the registered behavior", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{"work"}}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "cli-workspace-plan", contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "implementation", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}})
	if err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "cli test"}, {"config", "user.email", "cli@example.invalid"}, {"-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base"}} {
		if _, err := workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	sha, err := workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "product-workspace.json")
	input := ProductWorkspaceFile{SchemaVersion: 1, Request: storage.ProductWorkspaceRequest{TaskID: admitted.CreatedTaskIDs[0], RepositoryID: 17, Repository: repo, BaseSHA: sha}}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, raw, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "prepare-product-task", "--root", root, "--file", file); err == nil {
		t.Fatal("public preparation request accepted")
	}
	if err := os.Chmod(file, 0600); err != nil {
		t.Fatal(err)
	}
	unknown := strings.TrimSuffix(string(raw), "}") + `,"sandbox":"danger-full-access"}`
	if err := os.WriteFile(file, []byte(unknown), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "prepare-product-task", "--root", root, "--file", file); err == nil {
		t.Fatal("request injected sandbox permission")
	}
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := cliCall(t, "prepare-product-task", "--root", root, "--file", file)
	if err != nil {
		t.Fatal(output, err)
	}
	var prepared storage.ProductWorkspace
	if err := json.Unmarshal([]byte(output), &prepared); err != nil || prepared.State != "ready" || prepared.Proof == nil || prepared.Proof.HeadSHA != sha {
		t.Fatal(output, err)
	}
	if err := workspace.Verify(ctx, repo, prepared.Value.CWD, prepared.Value.Branch); err != nil {
		t.Fatal(err)
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("preparation launched model", runs, err)
	}
	view, err := cliCall(t, "product-workspace", "--root", root, "--id", input.Request.TaskID)
	if err != nil || string(view) != string(output) {
		t.Fatal("read-only workspace view changed receipt", string(view), err)
	}
	replay, err := cliCall(t, "prepare-product-task", "--root", root, "--file", file)
	if err != nil || string(replay) != string(output) {
		t.Fatal("CLI retry changed preparation", replay, err)
	}
}
