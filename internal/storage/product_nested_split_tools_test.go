package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func productNestedSplitToolFixture(t *testing.T, profile string) (*KernelStore, ProductSplitProposalRequest, contract.ToolInvocation, contract.NestedSplitProductRequest) {
	t.Helper()
	ctx := context.Background()
	k, r, _, scopeCall := productSplitParentScopeToolFixture(t, profile)
	// A host-prepared reference is also valid; only Main may explicitly adopt it.
	proof, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(r.Proposal)
	if err != nil {
		t.Fatal(err)
	}
	artifact := mustImport(t, k, ArtifactImport{SessionID: "main", Target: r.Target, Key: "nested-model-proposal", Kind: "split_proposal", ContentType: "application/json", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "child split decision"}}, string(raw))
	request := contract.NestedSplitProductRequest{SchemaVersion: 1, Target: r.Target, ProposalArtifactID: artifact.ID, ParentScopeID: proof.ID, ParentScopeDigest: proof.Digest}
	call := scopeCall
	call.Name, call.CallID = "product_split_nested", "nested-adopt"
	call.Arguments, _ = json.Marshal(request)
	return k, r, call, request
}

func TestProductNestedSplitToolAtomicallyAdoptsCompleteSetAndAuditsReplay(t *testing.T) {
	ctx := context.Background()
	k, r, call, request := productNestedSplitToolFixture(t, "product_v5")
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || reply.State != "completed" || len(reply.SubscriptionIDs) != 1 {
		t.Fatal(reply, err)
	}
	var adoption ProductSplitToolAdoption
	if err := k.Record(ctx, "product_split_tool_adoption", reply.OperationID, &adoption); err != nil {
		t.Fatal(err)
	}
	if adoption.ParentScopeProof == nil || adoption.ParentScopeProof.ID != request.ParentScopeID || adoption.ParentScopeProof.Digest != request.ParentScopeDigest || len(adoption.Preparation.Value.Creations) != 2 {
		t.Fatal("incomplete qualified adoption")
	}
	for _, ref := range adoption.Preparation.Value.Creations {
		job, err := k.ReadProductSplitCreate(ctx, ref.ID)
		if err != nil || job.Value.Ancestry == nil || job.Value.Ancestry.Value.ParentProof != *adoption.ParentScopeProof || job.Value.Child.Value.Depth != 2 || job.Value.OwnerID == "" {
			t.Fatal("creation lost canonical ancestry", err)
		}
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("adoption reset operation budget", err)
	}
	var seats, tasks int
	if err := k.DB.QueryRow("SELECT amount FROM kernel_budget_ledger WHERE wave_id=? AND operation_key=? AND kind='split_children'", r.Target.WaveID, adoption.Preparation.Value.ProposalID).Scan(&seats); err != nil || seats != 2 {
		t.Fatal(seats, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("preparation created executable Tasks", tasks, err)
	}
	if strings.Contains(reply.Content, k.Root) || strings.Contains(reply.Content, "ancestry") {
		t.Fatal("private evidence leaked")
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	now := source.CheckedAt.Add(2 * time.Minute)
	k.Now = func() time.Time { return now }
	again, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || !reflect.DeepEqual(again, reply) {
		t.Fatal("reopen replay changed preparation", again, err)
	}
	sourceAfter, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(source, sourceAfter) {
		t.Fatal("cached completion refreshed source", err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_split_parent_scope_tool", reply.OperationID},
		{"product_split_parent_scope", r.ParentWorkItemID},
		{"product_nested_split_proposal", adoption.ParentHeadID},
		{"product_split_preparation", adoption.ParentHeadID},
		{"product_split_tool_adoption", reply.OperationID},
	} {
		var owner string
		var raw []byte
		if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
			t.Fatal(err)
		}
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
			t.Fatal(err)
		}
		if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
			t.Fatal("replay repaired missing original", loss.kind)
		}
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
			t.Fatal(err)
		}
	}
	call.CallID = "new-decision"
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("new call replaced an adopted nested set")
	}
}

func TestProductNestedSplitToolRejectsUnqualifiedAdoptionAndRollsBack(t *testing.T) {
	for _, mode := range []string{"v4", "wrong_parent_id", "wrong_parent_digest", "missing_parent", "hold", "stale_source", "paused", "late_failure", "root_tool"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			profile := "product_v5"
			if mode == "v4" {
				profile = "product_v4"
			}
			k, r, call, request := productNestedSplitToolFixture(t, profile)
			switch mode {
			case "wrong_parent_id":
				request.ParentScopeID += "-wrong"
			case "wrong_parent_digest":
				request.ParentScopeDigest = strings.Repeat("f", 64)
			case "missing_parent":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_parent_scope' AND id=?", r.ParentWorkItemID); err != nil {
					t.Fatal(err)
				}
			case "hold":
				source, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				facts := source.Value.Facts
				facts.Hold = true
				if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, source.ID, facts); err != nil {
					t.Fatal(err)
				}
			case "stale_source":
				now := k.clock().Add(2 * time.Minute)
				k.Now = func() time.Time { return now }
			case "paused":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "nested-tool-pause", Target: r.Target, Action: "pause", Reason: "operator"}); err != nil {
					t.Fatal(err)
				}
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_tool BEFORE INSERT ON kernel_tool_operations WHEN NEW.name='product_split_nested' BEGIN SELECT RAISE(ABORT,'late nested tool failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			call.Arguments, _ = json.Marshal(request)
			if mode == "root_tool" {
				call.Name = "product_split"
				call.Arguments, _ = json.Marshal(contract.SplitProductRequest{SchemaVersion: 1, Target: r.Target, ProposalArtifactID: request.ProposalArtifactID})
			}
			if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
				t.Fatal("unqualified nested adoption accepted", mode)
			}
			for _, query := range []string{
				"SELECT count(*) FROM records WHERE kind LIKE 'product_nested_split_%'",
				"SELECT count(*) FROM records WHERE kind='product_split_tool_adoption'",
				"SELECT count(*) FROM kernel_tool_operations WHERE name IN ('product_split_nested','product_split')",
			} {
				var count int
				if err := k.DB.QueryRow(query).Scan(&count); err != nil || count != 0 {
					t.Fatal("failed adoption leaked effects", query, count, err)
				}
			}
		})
	}
}

func registerNestedSplitToolFixture(t *testing.T) (*KernelStore, ProductSplitChildPlanRequest, contract.ToolInvocation, contract.ToolReply) {
	t.Helper()
	ctx := context.Background()
	k, r, call, _ := productNestedSplitToolFixture(t, "product_v5")
	now := k.clock()
	k.Now = func() time.Time { return now }
	source, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, source.ID, source.Value.Facts); err != nil {
		t.Fatal(err)
	}
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	var adoption ProductSplitToolAdoption
	if err := k.Record(ctx, "product_split_tool_adoption", reply.OperationID, &adoption); err != nil {
		t.Fatal(err)
	}
	plan := ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: adoption.Preparation.Value.ProposalID, ProposalDigest: adoption.Preparation.Value.ProposalDigest}
	for i, ref := range adoption.Preparation.Value.Creations {
		job, err := k.ReadProductSplitCreate(ctx, ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(k.Root, "split-http-started.json")); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		identity := startSplitCreationOwner(t, k, job, 20*time.Second)
		stop, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = process.StopGroup(stop, identity)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "nested-notice-fixture-stop"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: int64(500 + i), Number: 50 + i, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: k.clock()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := k.BindProductSplitChildScopes(ctx, plan); err != nil {
		t.Fatal(err)
	}
	return k, plan, call, reply
}

func TestProductNestedSplitToolRegistrationQueuesQualifiedCompletionOnce(t *testing.T) {
	ctx := context.Background()
	k, r, call, reply := registerNestedSplitToolFixture(t)
	finishSplitMainWait(t, k, call, reply, r.Target)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_notice BEFORE INSERT ON records WHEN NEW.kind='product_split_completion' BEGIN SELECT RAISE(ABORT,'late nested notice failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); err == nil {
		t.Fatal("failed completion acknowledged registration")
	}
	var partial int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_completion')").Scan(&partial); err != nil || partial != 0 {
		t.Fatal("partial nested registration/notice", partial, err)
	}
	if len(splitNotices(t, k, call.SessionID)) != 0 {
		t.Fatal("failed registration emitted notification")
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_nested_notice"); err != nil {
		t.Fatal(err)
	}
	registered, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := loadProductSplitCompletionTx(ctx, tx, reply.OperationID)
	tx.Rollback()
	if err != nil || completion.Value.Summary.ParentWorkItemID != r.ParentWorkItemID || len(completion.Value.Summary.Children) != 2 || completion.Value.Source.Name != "product_split_nested" || completion.Value.AncestorSet == nil || len(completion.Value.AncestorSet.Value.Groups) != 2 || completion.Value.SubscriptionID != reply.SubscriptionIDs[0] {
		t.Fatal("nested completion lost source or local children", err)
	}
	notices := splitNotices(t, k, call.SessionID)
	if len(notices) != 1 || notices[0].Split == nil || !reflect.DeepEqual(*notices[0].Split, productSplitCompletionSummary(registered)) || notices[0].SubscriptionID != reply.SubscriptionIDs[0] || notices[0].OperationID != reply.OperationID {
		t.Fatal("nested completion did not wake its registered parent", notices)
	}
	for _, child := range completion.Value.Summary.Children {
		if child.Depth != 2 {
			t.Fatal("nested depth flattened", child)
		}
	}
	replay, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil || !reflect.DeepEqual(replay, registered) {
		t.Fatal("registration replay changed set", err)
	}
	var events int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='product_split_registered'").Scan(&events); err != nil || events != 1 {
		t.Fatal("completion emitted twice", events, err)
	}

	for _, query := range []string{
		"DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%') AND kind<>'product_split_completion'",
		"DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version') AND json_type(payload,'$.value.split_scope')='object'",
		"DELETE FROM records WHERE kind='wave_work_item' AND json_type(payload,'$.split_scope')='object'",
		"DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')",
		"DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'",
	} {
		if _, err := k.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	for _, group := range completion.Value.AncestorSet.Value.Groups {
		for _, child := range group.Children {
			if known, err := productSplitChildExecutionHeld(ctx, k.DB, child.Value.Child.Value.WorkItemID); err != nil || !known {
				t.Fatal("completion forgot ancestor/sibling identity", known, err)
			}
			if known, err := splitChildScopeIssueKnown(ctx, k.DB, "", child.Value.Issue.RepositoryID, child.Value.Issue.Number); err != nil || !known {
				t.Fatal("completion allowed ancestor/sibling Issue to become Root", known, err)
			}
			for _, creation := range child.Value.Creations {
				if known, err := splitChildScopeCreationKnown(ctx, k.DB, creation.ID); err != nil || !known {
					t.Fatal("completion forgot consumed creation", known, err)
				}
			}
		}
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); err == nil {
		t.Fatal("completion repaired missing registration")
	}
}
