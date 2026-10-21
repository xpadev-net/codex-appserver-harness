package contract_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// This standalone S-stage probe intentionally does not implement the Wave driver.
// Normal tests never start a model or read authentication files.
func TestLiveDynamicToolResume(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_SESSION") != "1" {
		t.Skip("explicit live session probe only")
	}
	binary, auth, model, output := os.Getenv("HARNESS_LIVE_CODEX_BINARY"), os.Getenv("HARNESS_LIVE_WORKER_CODEX_HOME"), os.Getenv("HARNESS_LIVE_MODEL"), os.Getenv("HARNESS_LIVE_EVIDENCE")
	if !filepath.IsAbs(binary) || !filepath.IsAbs(auth) || !filepath.IsAbs(output) || model == "" {
		t.Fatal("absolute native binary, dedicated Codex home, evidence path and model required")
	}
	obs := map[string]any{"acceptance": "AT-050", "stage": "preflight", "state": "unknown", "observed_at": time.Now().UTC(), "model": model, "authentication": "operator-provided dedicated Codex home", "credential_values_recorded": false, "executed_tests": 1, "completed_turns": 0, "output_schema_fallback": false}
	defer func() {
		if t.Failed() {
			obs["state"] = "unknown"
		}
		b, err := json.MarshalIndent(obs, "", "  ")
		if err != nil {
			t.Error("encode evidence")
			return
		}
		if err = os.WriteFile(output, append(b, '\n'), 0600); err != nil {
			t.Error("write sanitized evidence")
		}
	}()
	controllerHome := os.Getenv("CODEX_HOME")
	if controllerHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal("resolve controller home")
		}
		controllerHome = filepath.Join(home, ".codex")
	}
	a, err := filepath.EvalSymlinks(auth)
	if err != nil {
		t.Fatal("dedicated Codex home unavailable")
	}
	c, err := filepath.EvalSymlinks(controllerHome)
	if err != nil {
		t.Fatal("controller Codex home unavailable")
	}
	if a == c {
		t.Fatal("use a separate Codex home containing only the explicitly selected authentication/configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Clean(filepath.Join(cwd, "../../scripts/isolate-linux"))
	script, err := os.ReadFile(launcher)
	if err != nil {
		t.Fatal("read launcher")
	}
	obs["launcher"] = "scripts/isolate-linux"
	obs["launcher_sha256"] = fmt.Sprintf("%x", sha256.Sum256(script))
	version, err := exec.CommandContext(ctx, binary, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != "codex-cli 0.163.0" {
		t.Fatal("pinned native Codex version required")
	}
	obs["binary_version"] = strings.TrimSpace(string(version))
	generated := t.TempDir()
	if err = exec.CommandContext(ctx, binary, "app-server", "generate-json-schema", "--experimental", "--out", generated).Run(); err != nil {
		t.Fatal("experimental schema generation failed")
	}
	hashes := map[string]string{}
	for _, name := range []string{"v1/InitializeParams.json", "v2/ThreadStartParams.json", "v2/ThreadResumeParams.json", "DynamicToolCallParams.json", "DynamicToolCallResponse.json"} {
		data, err := os.ReadFile(filepath.Join(generated, name))
		if err != nil {
			t.Fatal("generated schema missing", name)
		}
		hashes[name] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	obs["experimental_schema_sha256"] = hashes
	workspace, home, private := t.TempDir(), t.TempDir(), t.TempDir()
	if err = exec.CommandContext(ctx, "git", "init", "--quiet", workspace).Run(); err != nil {
		t.Fatal("create credential-free probe repository")
	}
	sentinel := filepath.Join(private, "state.db")
	if err = os.WriteFile(sentinel, []byte("private probe sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	environment := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "HARNESS_SANDBOX_HOME=" + home, "HARNESS_LAUNCH_MODE=worker", "CODEX_HOME=" + auth, "HARNESS_WORKSPACE=" + workspace, "HARNESS_CODEX_BINARY=" + binary, "HARNESS_GIT_DIR=" + filepath.Join(workspace, ".git")}
	for _, key := range []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value, ok := os.LookupEnv(key); ok {
			environment = append(environment, key+"="+value)
		}
	}
	run := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, launcher, args...)
		cmd.Env = environment
		return cmd.Output()
	}
	obs["stage"] = "isolation"
	for _, hidden := range []string{sentinel, filepath.Join(controllerHome, "auth.json"), filepath.Join(os.Getenv("HOME"), ".config/gh"), filepath.Join(os.Getenv("HOME"), ".git-credentials")} {
		if _, err = run("sh", "-c", `test ! -e "$1"`, "probe", hidden); err != nil {
			t.Fatal("controller/authentication boundary is not hidden")
		}
	}
	obs["controller_paths_hidden"] = true
	// A 401 is observed, not inferred from a failed command or absent token.
	ghEnv := append(append([]string{}, environment...), "GH_TOKEN=unauthenticated-boundary-probe")
	githubStatuses := map[string]string{}
	for _, endpoint := range []string{"user", "repos/xpadev-net/codex-appserver-harness"} {
		cmd := exec.CommandContext(ctx, launcher, "gh", "api", endpoint, "-H", "Authorization:", "--jq", "has(\"permissions\")")
		cmd.Env = ghEnv
		body, runErr := cmd.Output()
		status, allowed := anonymousGitHubBoundary(endpoint, body, runErr)
		githubStatuses[endpoint] = status
		obs["github_boundary_status"] = githubStatuses
		if !allowed {
			t.Fatal("GitHub credential boundary is inconclusive; no model turn started")
		}
	}
	args := []string{binary, "app-server", "--listen", "stdio://", "--disable", "goals", "--disable", "multi_agent", "--disable", "multi_agent_v2"}
	stopped := map[*codex.Client]bool{}
	stop := func(client *codex.Client) {
		if !stopped[client] {
			_ = client.Close()
			stopped[client] = true
		}
	}
	start := func() *codex.Client {
		client, err := codex.StartCommand(ctx, launcher, args, environment)
		if err != nil {
			t.Fatal("isolated app-server launch failed")
		}
		t.Cleanup(func() { stop(client) })
		if err = client.Call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "harness_session_spike", "version": "0.1.0"}, "capabilities": map[string]bool{"experimentalApi": true}}, nil); err != nil {
			t.Fatal("experimental initialization failed")
		}
		if err = client.Send(map[string]any{"method": "initialized"}); err != nil {
			t.Fatal("initialization notification failed")
		}
		return client
	}
	obs["stage"] = "thread_start"
	first := start()
	var account struct {
		Account struct {
			Type  string `json:"type"`
			Email string `json:"email"`
		} `json:"account"`
	}
	if err = first.Call(ctx, "account/read", map[string]bool{"refreshToken": false}, &account); err != nil || account.Account.Type != "chatgpt" || account.Account.Email == "" {
		t.Fatal("authenticated ChatGPT probe identity not established")
	}
	obs["authentication_type"] = account.Account.Type
	obs["authentication_subject_sha256"] = fmt.Sprintf("%x", sha256.Sum256([]byte(account.Account.Email)))
	tool := map[string]any{"type": "function", "name": "harness_probe_nonce", "description": "Return a harmless controller-generated acknowledgement. No side effects.", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"nonce": map[string]string{"type": "string"}}, "required": []string{"nonce"}, "additionalProperties": false}, "deferLoading": false}
	var started struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err = first.Call(ctx, "thread/start", map[string]any{"cwd": workspace, "model": model, "approvalPolicy": "never", "sandbox": "read-only", "dynamicTools": []any{tool}}, &started); err != nil || started.Thread.ID == "" {
		t.Fatal("thread/start failed")
	}
	thread := started.Thread.ID
	obs["thread_id"] = thread
	rounds := []spikeTurnEvidence{}
	obs["stage"] = "first_turn_unknown_until_completed"
	rounds = append(rounds, spikeTurn(t, ctx, first, thread, 100, "This task remains incomplete after this turn; it will continue in a new process.", obs))
	obs["turns"] = rounds
	obs["completed_turns"] = 1
	// Close waits for the first process. Resume cannot use an in-memory session.
	stop(first)
	obs["first_process_stopped"] = true
	obs["stage"] = "new_process_resume"
	second := start()
	var resumed struct {
		Thread struct {
			ID    string `json:"id"`
			Turns []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"turns"`
		} `json:"thread"`
	}
	if err = second.Call(ctx, "thread/resume", map[string]any{"threadId": thread, "cwd": workspace, "model": model, "approvalPolicy": "never", "sandbox": "read-only"}, &resumed); err != nil || resumed.Thread.ID != thread {
		t.Fatal("same-thread resume failed")
	}
	found := false
	for _, turn := range resumed.Thread.Turns {
		if turn.ID == rounds[0].TurnID && turn.Status == "completed" {
			found = true
		}
	}
	if !found {
		t.Fatal("resumed history does not contain the completed first turn")
	}
	obs["resumed_history_verified"] = true
	obs["stage"] = "resumed_turn_unknown_until_completed"
	rounds = append(rounds, spikeTurn(t, ctx, second, thread, 101, "Continue the same incomplete task. Use its persisted dynamic tool again.", obs))
	if rounds[0].TurnID == rounds[1].TurnID || rounds[0].CallID == rounds[1].CallID {
		t.Fatal("new turn/call identity required")
	}
	obs["turns"] = rounds
	obs["completed_turns"] = 2
	stop(second)
	obs["second_process_stopped"] = true
	obs["stage"] = "completed"
	obs["state"] = "verified"
}

func anonymousGitHubBoundary(endpoint string, body []byte, err error) (string, bool) {
	if exit, ok := err.(*exec.ExitError); ok && regexp.MustCompile(`HTTP 401`).Match(exit.Stderr) {
		return "http_401", true
	}
	if strings.HasPrefix(endpoint, "repos/") && err == nil && strings.TrimSpace(string(body)) == "false" {
		return "anonymous_public_metadata", true
	}
	return "inconclusive_or_authenticated", false
}

func TestAnonymousGitHubBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, body string
		err                  error
		allowed              bool
	}{
		{"user_unauthenticated", "user", "", &exec.ExitError{Stderr: []byte("HTTP 401")}, true},
		{"repository_unauthenticated", "repos/owner/repo", "", &exec.ExitError{Stderr: []byte("HTTP 401")}, true},
		{"public_metadata", "repos/owner/repo", "false\n", nil, true},
		{"repository_permissions", "repos/owner/repo", "true\n", nil, false},
		{"authenticated_user", "user", "false\n", nil, false},
		{"transport_failure", "repos/owner/repo", "false", fmt.Errorf("transport failed"), false},
		{"forbidden_is_not_anonymous", "user", "", &exec.ExitError{Stderr: []byte("HTTP 403")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, allowed := anonymousGitHubBoundary(tc.endpoint, []byte(tc.body), tc.err); allowed != tc.allowed {
				t.Fatal("incorrect boundary classification")
			}
		})
	}
}

type spikeTurnEvidence struct {
	ThreadID                string          `json:"thread_id"`
	TurnID                  string          `json:"turn_id"`
	CallID                  string          `json:"call_id"`
	RequestID               json.RawMessage `json:"tool_request_id"`
	StartRequestID          int             `json:"turn_start_request_id"`
	ResponseSent            bool            `json:"response_sent"`
	ToolCompleted           bool            `json:"tool_completed"`
	AcknowledgementVerified bool            `json:"acknowledgement_verified"`
	Status                  string          `json:"status"`
}

func spikeNonce(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func spikeTurn(t *testing.T, ctx context.Context, client *codex.Client, thread string, rpcID int, continuation string, observation map[string]any) spikeTurnEvidence {
	t.Helper()
	requestNonce, replyNonce := spikeNonce(t), spikeNonce(t)
	evidence := spikeTurnEvidence{ThreadID: thread, StartRequestID: rpcID}
	defer func() { observation["last_turn"] = evidence }()
	prompt := fmt.Sprintf("%s Call harness_probe_nonce exactly once with nonce %s. Use no other tools and do not inspect files or authentication. After the tool response, return only its acknowledgement text, unchanged, then finish this turn.", continuation, requestNonce)
	if err := client.Send(map[string]any{"id": rpcID, "method": "turn/start", "params": map[string]any{"threadId": thread, "input": []any{map[string]string{"type": "text", "text": prompt}}}}); err != nil {
		t.Fatal("turn/start send failed; start unknown")
	}
	acknowledged, final := false, ""
	bind := func(id string) {
		if id == "" || (evidence.TurnID != "" && evidence.TurnID != id) {
			t.Fatal("turn identity mismatch")
		}
		evidence.TurnID = id
	}
	for {
		msg, err := client.Next(ctx)
		if err != nil {
			t.Fatal("turn stream incomplete; start/result unknown")
		}
		if len(msg.ID) > 0 && msg.Method != "" {
			if msg.Method != "item/tool/call" || evidence.ResponseSent {
				t.Fatal("unsupported or repeated server request")
			}
			var p struct {
				ThreadID  string          `json:"threadId"`
				TurnID    string          `json:"turnId"`
				CallID    string          `json:"callId"`
				Tool      string          `json:"tool"`
				Namespace *string         `json:"namespace"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if json.Unmarshal(msg.Params, &p) != nil || p.ThreadID != thread || p.Tool != "harness_probe_nonce" || p.Namespace != nil || p.CallID == "" {
				t.Fatal("unexpected dynamic tool request")
			}
			var a struct {
				Nonce string `json:"nonce"`
			}
			if contract.DecodeStrict(p.Arguments, &a) != nil || a.Nonce != requestNonce {
				t.Fatal("invalid tool arguments")
			}
			bind(p.TurnID)
			evidence.CallID = p.CallID
			evidence.RequestID = msg.ID
			if err = client.Send(map[string]any{"id": msg.ID, "result": map[string]any{"success": true, "contentItems": []any{map[string]string{"type": "inputText", "text": "ACK:" + replyNonce}}}}); err != nil {
				t.Fatal("tool response send failed")
			}
			evidence.ResponseSent = true
			continue
		}
		if len(msg.ID) > 0 {
			if string(msg.ID) != fmt.Sprint(rpcID) || len(msg.Error) > 0 {
				t.Fatal("turn/start response failed or mismatched")
			}
			var r struct {
				Turn struct {
					ID string `json:"id"`
				} `json:"turn"`
			}
			if json.Unmarshal(msg.Result, &r) != nil {
				t.Fatal("invalid turn/start response")
			}
			bind(r.Turn.ID)
			acknowledged = true
			continue
		}
		if msg.Method != "item/completed" && msg.Method != "turn/completed" {
			continue
		}
		var n struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Turn     struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"turn"`
			Item struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				Text    string `json:"text"`
				Tool    string `json:"tool"`
				Status  string `json:"status"`
				Success *bool  `json:"success"`
			} `json:"item"`
		}
		if json.Unmarshal(msg.Params, &n) != nil || n.ThreadID != thread {
			t.Fatal("notification identity mismatch")
		}
		if msg.Method == "turn/completed" {
			bind(n.Turn.ID)
			evidence.Status = n.Turn.Status
			evidence.AcknowledgementVerified = strings.Contains(final, "ACK:"+replyNonce)
			if n.Turn.Status != "completed" || !acknowledged || !evidence.ResponseSent || !evidence.ToolCompleted || !evidence.AcknowledgementVerified {
				t.Fatalf("tool round trip or normal completion not established: status=%s start_ack=%v response_sent=%v tool_completed=%v acknowledgement=%v", evidence.Status, acknowledged, evidence.ResponseSent, evidence.ToolCompleted, evidence.AcknowledgementVerified)
			}
			return evidence
		}
		bind(n.TurnID)
		switch n.Item.Type {
		case "dynamicToolCall":
			if !evidence.ResponseSent || n.Item.ID != evidence.CallID || n.Item.Tool != "harness_probe_nonce" || n.Item.Status != "completed" || n.Item.Success == nil || !*n.Item.Success {
				t.Fatal("tool completion mismatch")
			}
			evidence.ToolCompleted = true
		case "agentMessage":
			final = n.Item.Text
		case "commandExecution", "fileChange", "mcpToolCall":
			t.Fatal("unexpected side-effecting tool item")
		}
	}
}
