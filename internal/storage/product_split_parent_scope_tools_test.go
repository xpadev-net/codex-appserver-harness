package storage

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func productSplitParentScopeToolFixture(t *testing.T, profile string) (*KernelStore, ProductSplitProposalRequest, ProductSplitChildScopeBinding, contract.ToolInvocation) {
	t.Helper()
	k, r, binding, _ := nestedSplitContextFixture(t)
	b, err := k.SessionBinding(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if profile != "" {
		if err := k.PinSessionTools(context.Background(), b, profile); err != nil {
			t.Fatal(err)
		}
	}
	b, run, _ := reserveBudgetMain(t, k, "parent-scope-tool-run", r.Target)
	call := kernelCall(t, b, run, "product_split_parent_scope", contract.SplitProductParentScopeRequest{SchemaVersion: 1, Target: r.Target, ParentWorkItemID: r.ParentWorkItemID})
	return k, r, binding, call
}

func TestProductSplitParentScopeToolPublicLocalScopeAndAuditedReplay(t *testing.T) {
	ctx := context.Background()
	k, r, binding, call := productSplitParentScopeToolFixture(t, "product_v4")
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || reply.State != "completed" {
		t.Fatal(reply, err)
	}
	var public struct {
		ParentScopeID     string                 `json:"parent_scope_id"`
		ParentScopeDigest string                 `json:"parent_scope_digest"`
		Scope             ProductSplitScopeInput `json:"scope"`
	}
	if err := contract.DecodeStrict([]byte(reply.Content), &public); err != nil {
		t.Fatal(err)
	}
	saved, err := k.ReadProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || public.ParentScopeID != saved.ID || public.ParentScopeDigest != saved.Digest || reply.ResourceID != saved.ID || !reflect.DeepEqual(public.Scope, productSplitScopeInput(binding)) {
		t.Fatal(public, err)
	}
	for _, private := range []string{k.Root, "creations", "ancestor_witnesses", "registration_digest", "original_registration"} {
		if strings.Contains(reply.Content, private) {
			t.Fatal("private proof leaked", private)
		}
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("scope tool changed budget", err)
	}
	var tasks, adoptions int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal(tasks, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_tool_adoption'").Scan(&adoptions); err != nil || adoptions != 0 {
		t.Fatal(adoptions, err)
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
	replay, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || !reflect.DeepEqual(reply, replay) {
		t.Fatal("replay changed response", replay, err)
	}
	sourceAfter, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(source, sourceAfter) {
		t.Fatal("replay renewed source", err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_split_parent_scope_tool", reply.OperationID},
		{"product_split_parent_scope", r.ParentWorkItemID},
		{"product_split_parent_scope_history", saved.ID},
		{"product_split_parent_scope_admission", r.ParentWorkItemID},
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
			t.Fatal("replay repaired lost proof", loss.kind)
		}
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
			t.Fatal(err)
		}
	}
	forged := reply
	forged.Content = `{"parent_scope_id":"forged"}`
	raw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE kernel_tool_operations SET result=? WHERE id=?", raw, reply.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("forged cached content accepted")
	}
}

func TestProductSplitParentScopeToolRejectsUnqualifiedCallsAndRollsBack(t *testing.T) {
	for _, mode := range []string{"unprofiled", "v3", "root", "foreign_child", "namespace", "stale_target", "paused", "stale_source", "late_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			profile := "product_v4"
			if mode == "unprofiled" {
				profile = ""
			}
			if mode == "v3" {
				profile = "product_v3"
			}
			k, r, binding, call := productSplitParentScopeToolFixture(t, profile)
			var request contract.SplitProductParentScopeRequest
			if err := json.Unmarshal(call.Arguments, &request); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "root":
				request.ParentWorkItemID = binding.Value.Request.ParentWorkItemID
			case "foreign_child":
				request.ParentWorkItemID = "unregistered-child"
			case "namespace":
				call.Namespace = "relay"
			case "stale_target":
				request.Target.ScopeRevision++
			case "paused":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "scope-tool-pause", Target: r.Target, Action: "pause", Reason: "operator"}); err != nil {
					t.Fatal(err)
				}
			case "stale_source":
				now := k.clock().Add(2 * time.Minute)
				k.Now = func() time.Time { return now }
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_scope_tool BEFORE INSERT ON kernel_tool_operations WHEN NEW.name='product_split_parent_scope' BEGIN SELECT RAISE(ABORT,'late scope tool failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			call.Arguments, _ = json.Marshal(request)
			if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
				t.Fatal("unqualified call accepted", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool')").Scan(&count); err != nil || count != 0 {
				t.Fatal("failed call leaked partial proof", count, err)
			}
		})
	}
}

func TestProductV4StillAdoptsRootSplitWithV3Wire(t *testing.T) {
	k, _, call, _ := productSplitToolFixture(t, "product_v4")
	reply, err := (contract.KernelGateway{Backend: k}).Handle(context.Background(), call)
	if err != nil || reply.State != "completed" || reply.ResourceID == "" {
		t.Fatal(reply, err)
	}
}
