package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func ancestorGroupFixture(t *testing.T) ProductSplitAncestorGroup {
	t.Helper()
	_, wave, request := productSplitProposalFixture(t)
	r := ProductSplitChildPlanRequest{Target: wave.Target(), ParentWorkItemID: request.ParentWorkItemID, ProposalID: "proposal", ProposalDigest: strings.Repeat("a", 64)}
	group := ProductSplitAncestorGroup{Request: r, ParentHeadID: splitProposalHead(request), ParentWorkItemID: "work-1", SourcePolicyDigest: strings.Repeat("b", 64), ScopeSetID: "scope", ScopeSetDigest: strings.Repeat("c", 64), Children: []ProductSplitAncestorChild{}}
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("split-create-%d", i)
		group.Children = append(group.Children, ProductSplitAncestorChild{Value: ProductSplitAncestorChildValue{Child: splitChildSourceChild{ID: fmt.Sprintf("intent-%d", i), Value: splitChildSourceWork{WorkItemID: fmt.Sprintf("work-%d", i)}}, Issue: splitChildSourceIssue{RepositoryID: 17, ID: int64(101 + i), Number: 11 + i}, InitialCreationID: id, CreationID: id, Creations: []ProductSplitChildCreationReference{{ID: id, Digest: strings.Repeat("d", 64)}}}})
	}
	return group
}
func cloneAncestorGroup(t *testing.T, v ProductSplitAncestorGroup) ProductSplitAncestorGroup {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out ProductSplitAncestorGroup
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSplitAncestorWitnessesCanonicalizeWithoutRecursiveOrPrivateData(t *testing.T) {
	group := ancestorGroupFixture(t)
	reversed := cloneAncestorGroup(t, group)
	reversed.Children[0], reversed.Children[1] = reversed.Children[1], reversed.Children[0]
	got, err := canonicalSplitAncestorGroups(group.Request.Target.WaveID, []ProductSplitAncestorGroup{reversed, group})
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], group) {
		t.Fatal("unstable/duplicate witness", got, err)
	}
	other := cloneAncestorGroup(t, group)
	other.ParentWorkItemID = "work-0"
	both, err := canonicalSplitAncestorGroups(group.Request.Target.WaveID, []ProductSplitAncestorGroup{other, group})
	if err != nil || len(both) != 2 {
		t.Fatal("independent policy admission lost", err)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{`"body"`, `"scope"`, `"candidate"`, `"registrations"`, `"ancestry"`, `"workspace"`} {
		if strings.Contains(string(raw), private+":") {
			t.Fatal("recursive/private payload", private)
		}
	}
	if len(raw) > 4096 {
		t.Fatal("compact fixture expanded", len(raw))
	}
}

func TestSplitAncestorWitnessesRejectConflictsAndBrokenCreationChains(t *testing.T) {
	original := ancestorGroupFixture(t)
	for _, mode := range []string{"wrong_wave", "wrong_head", "lost_pin", "duplicate_child", "aliased_issue", "broken_chain", "foreign_marker", "shared_creation", "changed_pin", "changed_child", "parent_seat_overflow", "retry_overflow"} {
		t.Run(mode, func(t *testing.T) {
			group := cloneAncestorGroup(t, original)
			groups := []ProductSplitAncestorGroup{group}
			switch mode {
			case "wrong_wave":
				group.Request.Target.WaveID = "foreign-wave"
			case "wrong_head":
				group.ParentHeadID = "foreign-head"
			case "lost_pin":
				group.SourcePolicyDigest = ""
			case "duplicate_child":
				group.Children[1] = group.Children[0]
			case "aliased_issue":
				group.Children[1].Value.Issue = group.Children[0].Value.Issue
			case "broken_chain":
				group.Children[0].Value.Creations[0].Attempt = 1
			case "foreign_marker":
				group.ParentWorkItemID = "foreign-parent"
			case "shared_creation":
				group.Children[1].Value.Creations = append([]ProductSplitChildCreationReference{}, group.Children[0].Value.Creations...)
				group.Children[1].Value.InitialCreationID = group.Children[0].Value.InitialCreationID
				group.Children[1].Value.CreationID = group.Children[0].Value.CreationID
			case "changed_pin":
				other := cloneAncestorGroup(t, group)
				other.SourcePolicyDigest = strings.Repeat("e", 64)
				groups = append(groups, other)
			case "changed_child":
				other := cloneAncestorGroup(t, group)
				other.ScopeSetID = "other-scope"
				other.Children[0].Value.Issue.Number++
				groups = append(groups, other)
			case "parent_seat_overflow":
				group.ParentWorkItemID = group.Request.ParentWorkItemID
				group.Children = nil
				for i := 0; i < maxProductWorkItems; i++ {
					v := cloneAncestorGroup(t, original).Children[0]
					id := fmt.Sprintf("split-create-bound-%d", i)
					v.Value.Child.ID = fmt.Sprintf("intent-bound-%d", i)
					v.Value.Child.Value.WorkItemID = fmt.Sprintf("work-bound-%d", i)
					v.Value.Issue.ID = int64(1000 + i)
					v.Value.Issue.Number = 1000 + i
					v.Value.InitialCreationID = id
					v.Value.CreationID = id
					v.Value.Creations = []ProductSplitChildCreationReference{{ID: id, Digest: strings.Repeat("a", 64)}}
					group.Children = append(group.Children, v)
				}
			case "retry_overflow":
				for i := range group.Children {
					for n := 1; n <= 17; n++ {
						child := &group.Children[i].Value
						prior := child.Creations[len(child.Creations)-1].ID
						id := fmt.Sprintf("split-create-retry-%d-%d", i, n)
						child.Creations = append(child.Creations, ProductSplitChildCreationReference{ID: id, Digest: strings.Repeat("a", 64), Attempt: n, PreviousID: prior})
						child.CreationID = id
					}
				}
			}
			groups[0] = group
			if _, err := canonicalSplitAncestorGroups(original.Request.Target.WaveID, groups); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("invalid prohibition set accepted", err)
			}
		})
	}
}

func TestProductSplitAncestorWitnessInspectionAuthenticatesScopeAndDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	k, r := nestedObservedScopesFixture(t)
	if _, err := k.BindProductSplitChildScopes(ctx, r); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&before); err != nil {
		t.Fatal(err)
	}
	witness, err := k.InspectProductSplitAncestorWitnesses(ctx, r)
	if err != nil || len(witness.Value.Groups) != 2 || witness.ID == "" || witness.Digest == "" {
		t.Fatal(witness, err)
	}
	var afterFirst int
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&afterFirst); err != nil || afterFirst != before {
		t.Fatal("initial inspection wrote records", before, afterFirst, err)
	}
	work := map[string]bool{}
	for _, group := range witness.Value.Groups {
		for _, child := range group.Children {
			work[child.Value.Child.Value.WorkItemID] = true
		}
	}
	if len(work) != 4 {
		t.Fatal("ancestor/own identities dropped", work)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-ancestor-inspection", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	// Operator control itself writes records; compare only the inspection calls.
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&before); err != nil {
		t.Fatal(err)
	}
	replay, err := k.InspectProductSplitAncestorWitnesses(ctx, r)
	if err != nil || !reflect.DeepEqual(replay, witness) {
		t.Fatal("audit changed witnesses", err)
	}
	var after int
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&after); err != nil || after != before {
		t.Fatal("inspection wrote records", before, after, err)
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); !readinessHoldError(err) {
		t.Fatal("inspection granted registration", err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_nested_split_child_scopes' AND id=?", r.ProposalID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.InspectProductSplitAncestorWitnesses(ctx, r); !readinessHoldError(err) {
		t.Fatal("missing authoritative scope reconstructed", err)
	}
}
