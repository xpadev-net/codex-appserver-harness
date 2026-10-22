package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestContextReconstructsExactLargePersistedContract(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.OriginalRequest = strings.Repeat("日本語\n\x00<&>😀", 6000)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, adopted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	criteria, _ := json.Marshal(w.Criteria)
	policy, _ := json.Marshal(w.Policy)
	for _, session := range []string{"main", task.SessionID} {
		for section, want := range map[string]string{"request": request.OriginalRequest, "criteria": string(criteria), "policy": string(policy), "direction": ""} {
			t.Run(session+"/"+section, func(t *testing.T) {
				r := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: section, Limit: contract.MaxContextRead}
				var out strings.Builder
				for {
					got, err := k.ReadContext(ctx, session, r)
					if err != nil {
						t.Fatal(err)
					}
					if got.Target != w.Target() || got.Section != section || got.Bytes != int64(len(want)) || got.Digest != fmt.Sprintf("%x", sha256.Sum256([]byte(want))) || got.Offset != r.Offset || !utf8.ValidString(got.Content) || len(got.Content) > contract.MaxContextRead {
						t.Fatal("incorrect context binding", got)
					}
					out.WriteString(got.Content)
					if got.NextOffset == nil {
						break
					}
					if *got.NextOffset != r.Offset+int64(len(got.Content)) || *got.NextOffset <= r.Offset {
						t.Fatal("nonprogressing cursor", got)
					}
					r.Offset = *got.NextOffset
				}
				if out.String() != want {
					t.Fatal("context was lost or modified")
				}
			})
		}
	}
	for _, mutation := range []string{"stale", "foreign-main", "foreign-task", "mid-rune", "beyond", "negative", "zero-limit", "oversize", "unknown-section", "short-rune"} {
		t.Run(mutation, func(t *testing.T) {
			session := "main"
			r := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "request", Limit: contract.MaxContextRead}
			switch mutation {
			case "stale":
				r.Target.ScopeRevision++
			case "foreign-main", "foreign-task":
				other := request
				other.SourceID = mutation
				otherWave, err := k.CreateWave(ctx, other)
				if err != nil {
					t.Fatal(err)
				}
				r.Target = otherWave.Target()
				if mutation == "foreign-main" {
					foreignWorkspace := filepath.Join(t.TempDir(), "workspace")
					if err := os.Mkdir(foreignWorkspace, 0700); err != nil {
						t.Fatal(err)
					}
					if _, err := k.CreateOrchestrator(ctx, "foreign-main", "configured", foreignWorkspace, "read-only"); err != nil {
						t.Fatal(err)
					}
					session = "foreign-main"
				} else {
					session = task.SessionID
				}
			case "mid-rune":
				r.Offset = 1
			case "beyond":
				r.Offset = int64(len(request.OriginalRequest) + 1)
			case "negative":
				r.Offset = -1
			case "zero-limit":
				r.Limit = 0
			case "oversize":
				r.Limit = contract.MaxContextRead + 1
			case "unknown-section":
				r.Section = "/etc/passwd"
			case "short-rune":
				r.Limit = 1
			}
			if _, err := k.ReadContext(ctx, session, r); err == nil {
				t.Fatal("invalid context read allowed")
			}
		})
	}
}

func TestContextDirectionIsAppliedScopeOnly(t *testing.T) {
	ctx := context.Background()
	k, w, _ := mailboxFixture(t)
	d := mustDirection(t, k, w)
	r := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "direction", Limit: contract.MaxContextRead}
	got, err := k.ReadContext(ctx, "main", r)
	if err != nil || got.Content != "" {
		t.Fatal("queued instruction leaked", got, err)
	}
	if _, err = k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReadContext(ctx, "main", r); err == nil {
		t.Fatal("old scope accepted")
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.Target = w.Target()
	got, err = k.ReadContext(ctx, "main", r)
	if err != nil || got.Content != d.Request.Instruction {
		t.Fatal(got, err)
	}
}

func TestContextToolBindsRuntimeAndBoundsEscapedReply(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"orchestrator", "task"} {
		t.Run(kind, func(t *testing.T) {
			k, w, _, b, run := kernelToolFixture(t, kind)
			// Control characters exercise worst-case JSON expansion without changing the
			// saved target, run snapshot or source authority.
			w.OriginalRequest = strings.Repeat("\x00", contract.MaxContextRead) + "tail"
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = saveWave(ctx, tx, w); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			r := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "request", Limit: contract.MaxContextRead}
			call := kernelCall(t, b, run, "context_read", r)
			gateway := contract.KernelGateway{Backend: k}
			reply, err := gateway.Handle(ctx, call)
			if err != nil || reply.State != "completed" || len(reply.Content) > contract.MaxInline {
				t.Fatal(reply, err)
			}
			var got contract.ContextSlice
			if err = json.Unmarshal([]byte(reply.Content), &got); err != nil || got.Content != w.OriginalRequest[:contract.MaxContextRead] || got.NextOffset == nil {
				t.Fatal(got, err)
			}
			replay, err := gateway.Handle(ctx, call)
			if err != nil || !reflect.DeepEqual(reply, replay) {
				t.Fatal(replay, err)
			}
			call.CallID = "foreign"
			call.RunID = "another-run"
			if _, err = gateway.Handle(ctx, call); err == nil {
				t.Fatal("foreign run read context")
			}
			var operations, artifacts int
			if err = k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&operations); err != nil || operations != 1 {
				t.Fatal(operations, err)
			}
			if err = k.DB.QueryRow("SELECT count(*) FROM kernel_artifacts").Scan(&artifacts); err != nil || artifacts != 0 {
				t.Fatal(artifacts, err)
			}
		})
	}
}
