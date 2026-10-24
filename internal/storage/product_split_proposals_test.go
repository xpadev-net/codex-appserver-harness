package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func productSplitProposalFixture(t *testing.T, extraRoots ...string) (*KernelStore, domain.Wave, ProductSplitProposalRequest) {
	t.Helper()
	k, r := waveStoreFixture(t)
	r.Policy.Stage, r.Policy.Product = "P", true
	r.Criteria = append(r.Criteria, domain.WaveCriterion{ID: "extra", Requirement: "second original requirement"})
	definition := ProductWorkItemDefinition{ID: "split-root", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC", "extra"}}
	definitions := []ProductWorkItemDefinition{definition}
	for _, id := range extraRoots {
		other := definition
		other.ID = id
		definitions = append(definitions, other)
	}
	w, err := k.CreateProductWave(context.Background(), ProductWaveRequest{Wave: r, WorkItems: definitions})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := k.ReadProductWorkItemRegistration(context.Background(), definition.ID)
	if err != nil {
		t.Fatal(err)
	}
	p := splitting.Proposal{SchemaVersion: 1, WorkItemID: definition.ID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{
		{Key: "first", Title: "first work", Requirement: "preserve first original requirement", Criteria: []verification.Criterion{{ID: "first-ac", Requirement: "first original behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "first-ac"}}, DependsOn: []string{}},
		{Key: "second", Title: "second work", Requirement: "preserve second original requirement", Criteria: []verification.Criterion{{ID: "second-ac", Requirement: "second original behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "extra", ChildCriterion: "second-ac"}}, DependsOn: []string{"first"}},
	}}
	return k, w, ProductSplitProposalRequest{Target: w.Target(), Key: "split-request", ParentWorkItemID: definition.ID, RegistrationDigest: registration.Digest, Proposal: p}
}
func cloneSplitRequest(t *testing.T, r ProductSplitProposalRequest) ProductSplitProposalRequest {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var clone ProductSplitProposalRequest
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func TestProductSplitProposalPreservesParentAndBudgetsAndReplaysAfterReopen(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil || saved.Value.Depth != 0 || saved.Value.Limits.SplitDepth != 2 || saved.Value.Limits.SplitChildren != 8 || saved.Value.InitialRegistrationDigest != r.RegistrationDigest || saved.Value.ParentScopeDigest == "" || saved.Value.SourceDigest == "" {
		t.Fatal(saved, err)
	}
	replay, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil || !reflect.DeepEqual(replay, saved) {
		t.Fatal("split retry changed facts", replay, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(current, w) {
		t.Fatal("proposal changed parent Wave", current, err)
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(after, budget) {
		t.Fatal("proposal reset lifetime budgets", after, err)
	}
	for _, query := range []string{"SELECT count(*) FROM kernel_tasks", "SELECT count(*) FROM kernel_run_bindings", "SELECT count(*) FROM kernel_tool_operations", "SELECT count(*) FROM records WHERE kind IN ('product_target','product_workspace')"} {
		var n int
		if err := k.DB.QueryRow(query).Scan(&n); err != nil || n != 0 {
			t.Fatal("proposal created work or authority", query, n, err)
		}
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&n); err != nil || n != 1 {
		t.Fatal("proposal created children", n, err)
	}
	changed := cloneSplitRequest(t, r)
	changed.Proposal.Children[0].Requirement = "another proposal cannot replace saved intent"
	if _, err := k.PrepareProductSplitProposal(ctx, changed); err == nil {
		t.Fatal("split intent overwritten")
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	reopened, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil || !reflect.DeepEqual(reopened, saved) {
		t.Fatal("split retry changed after restart", reopened, err)
	}
}

func TestProductSplitProposalRejectsLossScopeAndPolicyViolationsWithoutPartialSave(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	for _, mode := range []string{"missing_criterion", "overlap", "cycle", "unknown_parent", "unknown_child", "too_many", "wrong_scope", "wrong_registration", "part_count", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			bad := cloneSplitRequest(t, r)
			switch mode {
			case "missing_criterion":
				bad.Proposal.Children[1].Coverage = nil
			case "overlap":
				bad.Proposal.Children[1].Coverage[0].ParentCriterion = "AC"
			case "cycle":
				bad.Proposal.Children[0].DependsOn = []string{"second"}
			case "unknown_parent":
				bad.Proposal.Children[0].Coverage[0].ParentCriterion = "unknown"
			case "unknown_child":
				bad.Proposal.Children[0].Coverage[0].ChildCriterion = "unknown"
			case "too_many":
				for len(bad.Proposal.Children) <= 8 {
					bad.Proposal.Children = append(bad.Proposal.Children, bad.Proposal.Children[0])
				}
			case "wrong_scope":
				bad.Proposal.ScopeRevision++
			case "wrong_registration":
				bad.RegistrationDigest = strings.Repeat("e", 64)
			case "part_count":
				bad.Proposal.Children[0].Coverage[0].PartID = "missing-part"
				bad.Proposal.Children[0].Coverage[0].PartCount = 2
				bad.Proposal.Children[0].Coverage[0].Basis = "first of two original parts"
			case "oversize":
				bad.Proposal.Children[0].Requirement = strings.Repeat("x", 1<<20)
			}
			if _, err := k.PrepareProductSplitProposal(ctx, bad); err == nil {
				t.Fatal("invalid split admitted", mode)
			}
		})
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_proposal'").Scan(&count); err != nil || count != 0 {
		t.Fatal("invalid split left intent", count, err)
	}
	limits, err := k.WaveOperationLimits(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	limits.SplitDepth, limits.SplitChildren = 0, 0
	if err := k.Put(ctx, "wave_operation_limits", w.ID, w.ID, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitProposal(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("disabled split limit upgraded", err)
	}
}

func TestProductSplitProposalMissingProofNeverReconstructsAndRollbackIsAtomic(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_split_history BEFORE INSERT ON records WHEN NEW.kind='product_split_proposal_history' BEGIN SELECT RAISE(FAIL,'split history rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitProposal(ctx, r); err == nil {
		t.Fatal("split transaction fault not reached")
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_split_proposal','product_split_proposal_history','product_split_proposal_admission','product_split_proposal_request')").Scan(&n); err != nil || n != 0 {
		t.Fatal("split partially committed", n, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_split_history"); err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_split_proposal", splitProposalHead(r)},
		{"product_split_proposal_history", saved.ID},
		{"product_split_proposal_admission", splitProposalHead(r)},
		{"product_split_proposal_request", splitProposalRequestID(r)},
		{"product_work_item_registration", r.ParentWorkItemID},
		{"product_work_item_registration_version", structureID("product-work-", r.ParentWorkItemID, r.RegistrationDigest)},
		{"wave_work_item", r.ParentWorkItemID},
		{"product_wave_admission", structureID("product-admission-", w.Source, w.SourceID)},
		{"wave_operation_limits", w.ID},
	} {
		var owner string
		var raw []byte
		if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
			t.Fatal(err)
		}
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
			t.Fatal(err)
		}
		if _, err := k.PrepareProductSplitProposal(ctx, r); err == nil {
			t.Fatal("lost proof reconstructed", loss.kind)
		}
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil || !reflect.DeepEqual(restored, saved) {
		t.Fatal("proof inspection rewrote intent", restored, err)
	}
}

func TestProductSplitProposalKeepsAllPartsAndRejectsCrossParentRequestReuse(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t, "other-root")
	r.Proposal.Children[0].Coverage[0] = splitting.Coverage{ParentCriterion: "AC", ChildCriterion: "first-ac", PartID: "one", PartCount: 2, Basis: "first bounded part of original AC"}
	r.Proposal.Children[1].Coverage[0] = splitting.Coverage{ParentCriterion: "AC", ChildCriterion: "second-ac", PartID: "two", PartCount: 2, Basis: "remaining bounded part of original AC"}
	r.Proposal.Children[1].Criteria = append(r.Proposal.Children[1].Criteria, verification.Criterion{ID: "extra-ac", Requirement: "complete original extra criterion"})
	r.Proposal.Children[1].Coverage = append(r.Proposal.Children[1].Coverage, splitting.Coverage{ParentCriterion: "extra", ChildCriterion: "extra-ac"})
	saved, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil || !reflect.DeepEqual(saved.Value.Request.Proposal, r.Proposal) {
		t.Fatal("complete partition was flattened or lost", saved, err)
	}
	other := cloneSplitRequest(t, r)
	registration, err := k.ReadProductWorkItemRegistration(ctx, "other-root")
	if err != nil {
		t.Fatal(err)
	}
	other.ParentWorkItemID, other.Proposal.WorkItemID = "other-root", "other-root"
	other.RegistrationDigest = registration.Digest
	if _, err := k.PrepareProductSplitProposal(ctx, other); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("Wave request key reused for another parent", err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_proposal' AND work_item_id=?", w.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal("conflicting parent got an intent", count, err)
	}
}

func TestProductSplitProposalRequestWitnessPreventsNewKeyAfterIndexLoss(t *testing.T) {
	ctx := context.Background()
	k, _, r := productSplitProposalFixture(t)
	saved, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	type record struct {
		kind, id, owner string
		raw             []byte
	}
	lost := []record{{kind: "product_split_proposal", id: splitProposalHead(r)}, {kind: "product_split_proposal_history", id: saved.ID}, {kind: "product_split_proposal_admission", id: splitProposalHead(r)}}
	for i := range lost {
		row := &lost[i]
		if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", row.kind, row.id).Scan(&row.owner, &row.raw); err != nil {
			t.Fatal(err)
		}
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", row.kind, row.id); err != nil {
			t.Fatal(err)
		}
	}
	changed := cloneSplitRequest(t, r)
	changed.Key = "new-key-cannot-repair-loss"
	if _, err := k.PrepareProductSplitProposal(ctx, changed); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("new key recreated a known split", err)
	}
	if _, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("known split became absent", err)
	}
	for _, row := range lost {
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", row.kind, row.id, row.owner, row.raw); err != nil {
			t.Fatal(err)
		}
	}
	read, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal("read reconstructed or changed proposal", read, err)
	}
}

func TestProductSplitProposalPausedWaveAllowsAuditButNoNewIntent(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t, "other-root")
	saved, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-split", Target: w.Target(), Action: "pause", Reason: "review proposal before any child creation"}); err != nil {
		t.Fatal(err)
	}
	read, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal("pause hid immutable proposal audit", read, err)
	}
	other := cloneSplitRequest(t, r)
	registration, err := k.ReadProductWorkItemRegistration(ctx, "other-root")
	if err != nil {
		t.Fatal(err)
	}
	other.Key = "other-parent"
	other.ParentWorkItemID, other.Proposal.WorkItemID = "other-root", "other-root"
	other.RegistrationDigest = registration.Digest
	if _, err := k.PrepareProductSplitProposal(ctx, other); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("pause admitted another split intent", err)
	}
}

func TestProductSplitProposalCapturesParentCandidateAndRejectsMovedOrigin(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	// Typed host projection fixture only; no physical Git or quality is claimed.
	product := ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "master", WorkItem: domain.WorkItem{ID: r.ParentWorkItemID, RepositoryID: 17, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, Phase: domain.Verifying, Candidate: "original", Branch: "harness/root", Version: 1}, Candidate: runner.Candidate{ID: "original", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision}}
	origin, err := k.SetProductTarget(ctx, "", product)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductSplitProposal(ctx, r)
	if err != nil || saved.Value.Product == nil || !reflect.DeepEqual(*saved.Value.Product, origin) {
		t.Fatal("parent candidate was not bound", saved, err)
	}
	product.Candidate.ID, product.Candidate.SHA, product.WorkItem.Candidate = "later", strings.Repeat("c", 40), "later"
	product.WorkItem.Version++
	if _, err := k.SetProductTarget(ctx, origin.Digest, product); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("changed candidate reused split intent", err)
	}
	if _, err := k.PrepareProductSplitProposal(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("changed candidate rewrote split intent", err)
	}
}
