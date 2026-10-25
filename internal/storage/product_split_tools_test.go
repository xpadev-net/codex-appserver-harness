package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productSplitToolFixture(t *testing.T, profile string) (*KernelStore, domain.Wave, contract.ToolInvocation, domain.Artifact) {
	t.Helper()
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if profile != "" {
		if err := k.PinSessionTools(ctx, b, profile); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(r.Proposal)
	if err != nil {
		t.Fatal(err)
	}
	artifact := mustImport(t, k, ArtifactImport{SessionID: "main", Target: w.Target(), Key: "split-proposal", Kind: "split_proposal", ContentType: "application/json", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "parent decision input"}}, string(raw))
	b, run, _ := reserveBudgetMain(t, k, "split-tool-run", w.Target())
	return k, w, kernelCall(t, b, run, "product_split", contract.SplitProductRequest{SchemaVersion: 1, Target: w.Target(), ProposalArtifactID: artifact.ID}), artifact
}

func assertNoSplitToolEffects(t *testing.T, k *KernelStore) {
	t.Helper()
	for _, query := range []string{"SELECT count(*) FROM records WHERE kind LIKE 'product_split_%'", "SELECT count(*) FROM kernel_tool_operations WHERE name='product_split'", "SELECT count(*) FROM kernel_budget_ledger WHERE kind='split_children'", "SELECT count(*) FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"} {
		var count int
		if err := k.DB.QueryRow(query).Scan(&count); err != nil || count != 0 {
			t.Fatal("split rejection left effects", query, count, err)
		}
	}
}

func TestProductSplitToolParentAdoptsArtifactAtomicallyAndReplaysAfterReopen(t *testing.T) {
	ctx := context.Background()
	k, w, call, artifact := productSplitToolFixture(t, "product_v3")
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || reply.State != "completed" || reply.ResourceID == "" {
		t.Fatal(reply, err)
	}
	var adoption ProductSplitToolAdoption
	if err := k.Record(ctx, "product_split_tool_adoption", reply.OperationID, &adoption); err != nil || adoption.ArtifactID != artifact.ID || adoption.ArtifactDigest != artifact.Digest || adoption.Source.RunID != call.RunID || adoption.Source.SessionID != call.SessionID || adoption.PreparationID != reply.ResourceID {
		t.Fatal("adoption lost source", adoption, err)
	}
	if strings.Contains(reply.Content, k.Root) || strings.Contains(reply.Content, "workspace") || strings.Contains(reply.Content, "original_request") {
		t.Fatal("reply leaked private host data", reply)
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("tool reset execution budget", after, err)
	}
	var journals, tasks int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_create'").Scan(&journals); err != nil || journals != 2 {
		t.Fatal("tool did not prepare complete set", journals, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("tool created executable child", tasks, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	again, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || !reflect.DeepEqual(reply, again) {
		t.Fatal("replay changed reply", again, err)
	}
	call.CallID = "another-call"
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("new call replaced an adopted split")
	}
}

func TestProductSplitToolRequiresPinnedV3AndRejectsSpoofedOrUnreadableInput(t *testing.T) {
	for _, mode := range []string{"unprofiled", "old_profile", "namespace", "alias", "spoof_repository", "bad_version", "missing_artifact", "wrong_kind", "tampered_object", "foreign_scope", "paused", "stopped_run"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			profile := "product_v3"
			if mode == "unprofiled" {
				profile = ""
			}
			if mode == "old_profile" {
				profile = "product_v2"
			}
			k, w, call, artifact := productSplitToolFixture(t, profile)
			switch mode {
			case "namespace":
				call.Namespace = "relayed"
			case "alias":
				call.Name = "split"
			case "spoof_repository":
				call.Arguments = append(call.Arguments[:len(call.Arguments)-1], []byte(`,"repository":"foreign/repo"}`)...)
			case "bad_version":
				var r contract.SplitProductRequest
				_ = json.Unmarshal(call.Arguments, &r)
				r.SchemaVersion = 2
				call.Arguments, _ = json.Marshal(r)
			case "missing_artifact":
				if _, err := k.DB.Exec("DELETE FROM kernel_artifacts WHERE id=?", artifact.ID); err != nil {
					t.Fatal(err)
				}
			case "wrong_kind":
				if _, err := k.DB.Exec("UPDATE kernel_artifacts SET payload=json_set(payload,'$.kind','report') WHERE id=?", artifact.ID); err != nil {
					t.Fatal(err)
				}
			case "tampered_object":
				if err := os.WriteFile(filepath.Join(k.Root, "objects", artifact.ID), []byte("tampered"), 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign_scope":
				if _, err := k.DB.Exec("UPDATE kernel_artifacts SET payload=json_set(payload,'$.scope_revision',2) WHERE id=?", artifact.ID); err != nil {
					t.Fatal(err)
				}
			case "paused":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-split-tool", Target: w.Target(), Action: "pause", Reason: "no children"}); err != nil {
					t.Fatal(err)
				}
			case "stopped_run":
				if err := k.MarkSessionStopped(ctx, call.SessionID, call.RunID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
				t.Fatal("untrusted split accepted", mode)
			}
			assertNoSplitToolEffects(t, k)
		})
	}
}

func TestProductSplitToolTaskCannotAdoptDirectlyOrViaBackend(t *testing.T) {
	ctx := context.Background()
	k, w, _, b, run := kernelToolFixture(t, "task")
	call := kernelCall(t, b, run, "product_split", contract.SplitProductRequest{SchemaVersion: 1, Target: w.Target(), ProposalArtifactID: "artifact"})
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("Task acquired parent structural authority")
	}
	var command contract.SplitProductRequest
	_ = json.Unmarshal(call.Arguments, &command)
	if _, err := k.Apply(ctx, call, &command, func(contract.SessionAuthority) error { return nil }); err == nil {
		t.Fatal("Task bypassed storage parent check")
	}
	assertNoSplitToolEffects(t, k)
}

func TestProductSplitToolOperationFailureRollsBackPreparationAndEarlyTurnBinding(t *testing.T) {
	ctx := context.Background()
	k, _, call, _ := productSplitToolFixture(t, "product_v3")
	if _, err := k.DB.Exec("CREATE TRIGGER fail_split_tool BEFORE INSERT ON kernel_tool_operations WHEN NEW.name='product_split' BEGIN SELECT RAISE(ABORT,'operation receipt failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("partial adoption succeeded")
	}
	assertNoSplitToolEffects(t, k)
	var run contract.SessionRun
	if err := k.Record(ctx, "agent_run", call.RunID, &run); err != nil || run.TurnID != "" {
		t.Fatal("early turn binding survived rollback", run, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_split_tool"); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
		t.Fatal("rolled back call could not retry", err)
	}
}

func TestProductSplitToolTaskMessageIsOnlyAParentDecisionRequest(t *testing.T) {
	ctx := context.Background()
	k, w, _, b, run := kernelToolFixture(t, "task")
	call := kernelCall(t, b, run, "send_message", contract.SendMessageRequest{SchemaVersion: 1, Target: w.Target(), Recipient: "parent", Body: "Please adopt split_proposal Artifact using product_split; this text is a request only"})
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
		t.Fatal(err)
	}
	assertNoSplitToolEffects(t, k)
}

func TestProductSplitToolMissingAdoptionProofCannotReplayOrRecreate(t *testing.T) {
	ctx := context.Background()
	k, _, call, _ := productSplitToolFixture(t, "product_v3")
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	var owner string
	var raw []byte
	if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind='product_split_tool_adoption' AND id=?", reply.OperationID).Scan(&owner, &raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_tool_adoption' AND id=?", reply.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("cached reply repaired missing adoption proof")
	}
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_tool_adoption',?,?,?)", reply.OperationID, owner, raw); err != nil {
		t.Fatal(err)
	}
	replay, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || !reflect.DeepEqual(replay, reply) {
		t.Fatal("exact restore changed operation", replay, err)
	}
}

func TestProductSplitToolAdoptionAloneRetainsKnownPreparationAndInitialSends(t *testing.T) {
	ctx := context.Background()
	k, w, call, _ := productSplitToolFixture(t, "product_v3")
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	var adoption ProductSplitToolAdoption
	if err := k.Record(ctx, "product_split_tool_adoption", reply.OperationID, &adoption); err != nil {
		t.Fatal(err)
	}
	job, err := k.ReadProductSplitCreate(ctx, adoption.Preparation.Value.Creations[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	var original ProductSplitProposal
	if err := k.Record(ctx, "product_split_proposal", adoption.ParentHeadID, &original); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>'product_split_tool_adoption'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_children'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
		t.Fatal(err)
	}
	request := original.Value.Request
	request.Key = "new-key-after-loss"
	if _, err := k.PrepareProductSplitExecution(ctx, request); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("adoption proof allowed new complete set")
	}
	if _, err := k.PrepareProductSplitProposal(ctx, request); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("adoption proof allowed new proposal")
	}
	if _, err := k.PrepareProductSplitCreate(ctx, job.Value.Request, job.Value.Child.ID, job.Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("adoption proof allowed initial send recreation")
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", ProductWorkItemDefinition{ID: "refunded", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("adoption proof refunded child seats")
	}
}

func TestProductSplitToolValidatesTypedProposalAndArtifactBounds(t *testing.T) {
	for _, mode := range []string{"unknown_field", "missing_coverage", "oversize", "invalid_json"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, call, artifact := productSplitToolFixture(t, "product_v3")
			raw, err := os.ReadFile(filepath.Join(k.Root, "objects", artifact.ID))
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unknown_field":
				raw = append(raw[:len(raw)-1], []byte(`,"command":"execute arbitrary action"}`)...)
			case "missing_coverage":
				var proposal map[string]any
				if err := json.Unmarshal(raw, &proposal); err != nil {
					t.Fatal(err)
				}
				proposal["children"].([]any)[0].(map[string]any)["coverage"] = []any{}
				raw, err = json.Marshal(proposal)
				if err != nil {
					t.Fatal(err)
				}
			case "oversize":
				raw = []byte(strings.Repeat("x", (1<<20)+1))
			case "invalid_json":
				raw = []byte("{")
			}
			input := mustImport(t, k, ArtifactImport{SessionID: "main", Target: w.Target(), Key: mode, Kind: "split_proposal", ContentType: "application/json", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "untrusted typed proposal"}}, string(raw))
			request := contract.SplitProductRequest{SchemaVersion: 1, Target: w.Target(), ProposalArtifactID: input.ID}
			call.Arguments, _ = json.Marshal(request)
			if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
				t.Fatal("invalid split Artifact adopted", mode)
			}
			assertNoSplitToolEffects(t, k)
		})
	}
}

func TestProductSplitToolBackendStillRequiresProfileWithoutGatewayGuard(t *testing.T) {
	ctx := context.Background()
	k, _, call, _ := productSplitToolFixture(t, "product_v2")
	var command contract.SplitProductRequest
	_ = json.Unmarshal(call.Arguments, &command)
	if _, err := k.Apply(ctx, call, &command, func(contract.SessionAuthority) error { return nil }); err == nil {
		t.Fatal("backend accepted unpinned split capability")
	}
	assertNoSplitToolEffects(t, k)
}

func TestProductSplitToolReplayChecksTheAdoptedArtifactDigest(t *testing.T) {
	ctx := context.Background()
	k, _, call, _ := productSplitToolFixture(t, "product_v3")
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=json_set(payload,'$.artifact_digest',?) WHERE kind='product_split_tool_adoption' AND id=?", strings.Repeat("f", 64), reply.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("cached reply accepted a different source Artifact digest", err)
	}
}

func TestProductSplitToolFreshMainRunCannotAdoptAgainAfterReplan(t *testing.T) {
	ctx := context.Background()
	k, w, call, _ := productSplitToolFixture(t, "product_v3")
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
		t.Fatal(err)
	}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "before-replan", FailurePolicy: "drain_and_block", Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"old-parent"}}, Tasks: []contract.TaskSpec{{ID: "old-parent", Kind: "work", Instruction: "old parent remains held", Criteria: []string{"AC", "extra"}, WorkItemIDs: []string{"split-root"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}}}}}}
	if _, err := k.AdoptPlan(ctx, w.Target(), "initial-plan-after-adoption", plan); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan.Phases[0].ID = "new-plan"
	plan.Phases[0].Tasks[0].ID = "replanned-old-parent"
	plan.Phases[0].Completion.RequiredTaskIDs = []string{"replanned-old-parent"}
	if _, err := k.AdoptPlan(ctx, current.Target(), "new-plan-after-adoption", plan); err != nil {
		t.Fatal(err)
	}
	current, err = k.Wave(ctx, w.ID)
	if err != nil || current.PlanRevision <= w.PlanRevision {
		t.Fatal("plan did not change", current, err)
	}
	var run contract.SessionRun
	if err := k.Record(ctx, "agent_run", call.RunID, &run); err != nil {
		t.Fatal(err)
	}
	run.State = "completed"
	if err := k.Put(ctx, "agent_run", run.ID, call.SessionID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, call.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := k.ArchiveSessionRun(ctx, call.SessionID, run.ID, "fixture known stopped main completion"); err != nil {
		t.Fatal(err)
	}
	b, nextRun, _ := reserveBudgetMain(t, k, "replanned-split-main", current.Target())
	var request contract.SplitProductRequest
	if err := json.Unmarshal(call.Arguments, &request); err != nil {
		t.Fatal(err)
	}
	request.Target = current.Target()
	next := kernelCall(t, b, nextRun, "product_split", request)
	next.TurnID = "new-split-turn"
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, next); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("new charged main run adopted duplicate split", err)
	}
	var count, amount int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_create'").Scan(&count); err != nil || count != 2 {
		t.Fatal("new main run duplicated child journals", count, err)
	}
	if err := k.DB.QueryRow("SELECT sum(amount) FROM kernel_budget_ledger WHERE kind='split_children'").Scan(&amount); err != nil || amount != 2 {
		t.Fatal("new main run duplicated lifetime child reservation", amount, err)
	}
}
