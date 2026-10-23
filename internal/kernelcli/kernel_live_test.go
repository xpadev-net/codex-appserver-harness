package kernelcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Opt-in native K integration. Normal tests never read authentication or invoke
// a model. The explicit root is new and private; no existing database is reused.
func TestLiveKernelCLIFixReviewAcceptance(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_KERNEL") != "1" {
		t.Skip("explicit native K model exercise only")
	}
	binary, model, root, output := os.Getenv("HARNESS_LIVE_CODEX_BINARY"), os.Getenv("HARNESS_LIVE_MODEL"), os.Getenv("HARNESS_LIVE_KERNEL_ROOT"), os.Getenv("HARNESS_LIVE_KERNEL_EVIDENCE")
	if !filepath.IsAbs(binary) || model == "" || !filepath.IsAbs(root) || !filepath.IsAbs(output) {
		t.Fatal("absolute binary, NEW private root, evidence file and model required")
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatal("native K requires a new root")
	}
	auth := os.Getenv("HARNESS_LIVE_SESSION_AUTH_FILE")
	if auth == "" {
		home := os.Getenv("CODEX_HOME")
		if home == "" {
			user, err := os.UserHomeDir()
			if err != nil {
				t.Fatal("resolve current session")
			}
			home = filepath.Join(user, ".codex")
		}
		auth = filepath.Join(home, "auth.json")
	}
	auth, err := filepath.EvalSymlinks(auth)
	if err != nil {
		t.Fatal("current session auth source unavailable")
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Clean(filepath.Join(cwd, "../../scripts/isolate-linux"))
	template := t.TempDir()
	if err = os.Chmod(template, 0700); err != nil {
		t.Fatal(err)
	}
	runtimeFile := filepath.Join(t.TempDir(), "runtime.json")
	config := RuntimeConfig{SchemaVersion: 1, CodexBinary: binary, WorkerCodexHome: template, SessionAuthFile: auth, Launcher: []string{launcher}, LauncherContract: "argv_passthrough_v1", IsolationVerified: true, GitDirectories: []string{}}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(runtimeFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	if err = Run(ctx, []string{"init", "--root", root, "--model", model}, io.Discard, io.Discard); err != nil {
		t.Fatal("initialize native root failed")
	}
	tokenLimit := domain.DefaultKernelLimits().Tokens
	if value := os.Getenv("HARNESS_LIVE_KERNEL_TOKENS"); value != "" {
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil || parsed < 1 {
			t.Fatal("positive explicit native token limit required")
		}
		tokenLimit = parsed
		config, loadErr := loadConfig(root)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		config.Limits.Tokens = tokenLimit
		if loadErr = config.validate(); loadErr != nil {
			t.Fatal(loadErr)
		}
		raw, encodeErr := json.MarshalIndent(config, "", "  ")
		if encodeErr != nil || os.WriteFile(filepath.Join(root, configName), append(raw, '\n'), 0600) != nil {
			t.Fatal("configure new native root budget failed")
		}
	}
	request := DirectRequest{SchemaVersion: 1, RequestID: "native-k-local-result-contract", Request: `K native exercise with no repository, WorkItem, PR, or external lookup. Source data is [4,7,10]. Deliver a short immutable report containing data=[4,7,10], sum=21, average=7; final content must contain no NEEDS_FIX marker.
Plan an initial Phase containing TWO independent Tasks: draft and source-check, with no dependency between them. draft publishes the initial report with the intentional NEEDS_FIX seed; source-check separately verifies the source arithmetic. Then a separate read-only review Task reviews ONLY draft as its direct artifact subject. Then a final report Task summarizes the final reviewed candidate without introducing an unreviewed replacement deliverable.
Configure a saved FixReviewTemplate with input_task_id=review, classifications=[artifact_defect], max_rounds=2. Fix instructions remove the marker entirely and preserve the exact source/calculations. Review instructions independently check the exact current candidate digest against BOTH original criteria. The first review must truthfully report the seeded marker as artifact_defect; it is not an exemption from final acceptance. Let the kernel expand fresh fix/review Tasks without ordinary intermediate main replanning. After execution completion, propose verified accept with the corrected artifact and exact clean reviewer ID for both criteria. Do not waive any gate, fabricate evidence, change policy, write files, or use extra agents.`, Criteria: []domain.WaveCriterion{{ID: "AC_CALC", Requirement: "Final immutable report contains data=[4,7,10], sum=21 and average=7."}, {ID: "AC_CLEAN", Requirement: "Final accepted report contains no NEEDS_FIX marker."}}}
	requestFile := filepath.Join(t.TempDir(), "request.json")
	data, err = json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(requestFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = Run(ctx, []string{"create", "--root", root, "--file", requestFile}, io.Discard, io.Discard); err != nil {
		t.Fatal("create native Wave failed")
	}
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	page, err := k.WavePage(ctx, "", 1)
	if err != nil || len(page.Waves) != 1 {
		t.Fatal("new native Wave missing")
	}
	waveID := page.Waves[0].ID
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, []string{"run", "--root", root, "--runtime", runtimeFile}, io.Discard, io.Discard)
	}()
	evidence := map[string]any{"stage": "K native CLI fix/review acceptance", "model": model, "binary_version": "codex-cli 0.163.0", "executed_tests": 1, "repository": nil, "candidate": nil, "credential_values_recorded": false, "private_paths_recorded": false, "acceptance": "not_accepted", "token_limit": tokenLimit}
	t.Cleanup(func() {
		cancel()
		select {
		case stopErr := <-done:
			if stopErr != nil {
				t.Error("native daemon returned an error during shutdown")
			}
		case <-time.After(25 * time.Second):
			t.Error("native daemon shutdown unconfirmed")
		}
		clean := context.Background()
		tasks, readErr := k.Tasks(clean, waveID)
		kinds := []string{}
		sessions := map[string]bool{}
		for _, task := range tasks {
			kinds = append(kinds, task.Spec.Kind)
			sessions[task.SessionID] = true
		}
		evidence["task_kinds"] = kinds
		evidence["task_sessions_unique"] = len(sessions) == len(tasks)
		var live int
		if err := k.DB.QueryRowContext(clean, "SELECT count(*) FROM records WHERE kind='managed_process' AND json_extract(payload,'$.state')<>'stopped'").Scan(&live); err == nil {
			evidence["all_managed_processes_stopped"] = live == 0
		}
		var runs, mainRuns int
		_ = k.DB.QueryRowContext(clean, "SELECT count(*) FROM records WHERE kind='agent_run'").Scan(&runs)
		_ = k.DB.QueryRowContext(clean, "SELECT count(*) FROM records WHERE kind='agent_run' AND work_item_id='main'").Scan(&mainRuns)
		evidence["model_runs"] = runs
		evidence["main_runs"] = mainRuns
		var subject storage.SessionAuthentication
		if err := k.Record(clean, "session_authentication", "main", &subject); err == nil {
			digest := sha256.Sum256([]byte(subject.AccountID))
			evidence["authentication_subject_sha256"] = hex.EncodeToString(digest[:])
		}
		evidence["observed_at"] = time.Now().UTC()
		if readErr == nil && !t.Failed() {
			evidence["acceptance"] = "completed_with_verified_evidence"
		}
		k.Close()
		raw, encodeErr := json.MarshalIndent(evidence, "", "  ")
		if encodeErr != nil || os.WriteFile(output, append(raw, '\n'), 0600) != nil {
			t.Error("write sanitized native evidence failed")
		}
	})
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		view, readErr := k.InspectWave(ctx, waveID, "", "", 128)
		if readErr != nil {
			t.Fatal("native observation failed")
		}
		if view.Budget.ObservedTokens >= tokenLimit {
			t.Fatal("native configured token budget exhausted; keep report dispatch held")
		}
		if view.Main.DispatchHold != nil && view.Main.ProcessStopped {
			t.Fatal("native main output requires explicit reconciliation")
		}
		if view.Wave.State == domain.WaveCompleted {
			evidence["wave_state"] = view.Wave.State
			evidence["fix_rounds"] = view.Wave.FixRounds
			evidence["main_turns"] = view.Wave.MainTurns
			if view.Wave.FixRounds < 1 {
				t.Fatal("seeded native defect did not require a fresh fix")
			}
			// The final acceptance proof must exist; model text alone is insufficient.
			var completionRaw []byte
			if err = k.DB.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='wave_completion' AND work_item_id=?", waveID).Scan(&completionRaw); err != nil {
				t.Fatal("verified Wave completion proof missing")
			}
			var completion storage.WaveCompletion
			if err = json.Unmarshal(completionRaw, &completion); err != nil || len(completion.Proof.Proposal.Outputs) == 0 {
				t.Fatal("native acceptance outputs missing")
			}
			finalDigests := []string{}
			for _, id := range completion.Proof.Proposal.Outputs {
				artifact, body, readErr := k.ReadOperatorArtifact(ctx, id, 0, 16384)
				if readErr != nil {
					t.Fatal("accepted native artifact unreadable")
				}
				producer, readErr := k.Task(ctx, artifact.TaskID)
				if readErr != nil || producer.Spec.Kind != "fix" {
					t.Fatal("accepted artifact did not come from fresh fix")
				}
				if strings.Contains(string(body), "NEEDS_FIX") {
					t.Fatal("accepted native artifact still contains seed")
				}
				normalized := strings.ToLower(strings.ReplaceAll(string(body), " ", ""))
				if !strings.Contains(normalized, "sum=21") || !strings.Contains(normalized, "average=7") {
					t.Fatal("accepted native arithmetic absent")
				}
				digest := sha256.Sum256(body)
				if hex.EncodeToString(digest[:]) != artifact.Digest {
					t.Fatal("accepted native artifact digest mismatch")
				}
				finalDigests = append(finalDigests, artifact.Digest)
			}
			evidence["accepted_artifact_digests"] = finalDigests

			t.Logf("native K verified: main_turns=%d fix_rounds=%d", view.Wave.MainTurns, view.Wave.FixRounds)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("native K acceptance timed out")
		case <-ticker.C:
		}
	}
}
