package splitting

import (
	"context"
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"path/filepath"
	"testing"
)

func fixture() (verification.Scope, Proposal) {
	s := verification.Scope{Revision: 1, OriginalRequest: "requirements", Criteria: []verification.Criterion{{ID: "A", Requirement: "first"}, {ID: "B", Requirement: "second"}}, Included: []string{"A", "B"}}
	p := Proposal{SchemaVersion: 1, WorkItemID: "w", ScopeRevision: 1, Outcome: "split", Children: []Child{{Key: "a", Title: "first", Requirement: "first requirement", Criteria: []verification.Criterion{{ID: "a1", Requirement: "first"}}, Coverage: []Coverage{{ParentCriterion: "A", ChildCriterion: "a1"}}, DependsOn: []string{}}, {Key: "b", Title: "second", Requirement: "second requirement", Criteria: []verification.Criterion{{ID: "b1", Requirement: "second"}}, Coverage: []Coverage{{ParentCriterion: "B", ChildCriterion: "b1"}}, DependsOn: []string{"a"}}}}
	return s, p
}
func TestSplitCoverageAndDAG(t *testing.T) {
	s, p := fixture()
	if err := p.Validate("w", s, 0, 2, 8); err != nil {
		t.Fatal(err)
	}
	p.Children[0].DependsOn = []string{"b"}
	if err := p.Validate("w", s, 0, 2, 8); err == nil {
		t.Fatal("cycle accepted")
	}
	_, p = fixture()
	p.Children[1].Coverage[0].ParentCriterion = "A"
	if err := p.Validate("w", s, 0, 2, 8); err == nil {
		t.Fatal("duplicate coverage accepted")
	}
	_, p = fixture()
	p.Children = p.Children[:1]
	if err := p.Validate("w", s, 0, 2, 8); err == nil {
		t.Fatal("dropped requirement accepted")
	}
	_, p = fixture()
	if err := p.Validate("w", s, 2, 2, 8); err == nil {
		t.Fatal("depth budget bypass")
	}
	if r := ReadyChildren(p, map[string]bool{}); len(r) != 1 || r[0] != "a" {
		t.Fatal(r)
	}
}

type creator struct {
	issues map[string]int
	calls  int
	lose   bool
}

func (c *creator) FindChild(_ context.Context, key string) (int, error) { return c.issues[key], nil }
func (c *creator) CreateChild(_ context.Context, _, _, marker string) (int, error) {
	c.calls++
	number := 100 + c.calls
	c.issues[marker] = number
	if c.lose {
		c.lose = false
		return 0, errors.New("response lost")
	}
	return number, nil
}
func TestChildCreationRecovery(t *testing.T) {
	ctx := context.Background()
	s, _ := storage.Open(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	_, p := fixture()
	api := &creator{issues: map[string]int{}, lose: true}
	if _, err := CreateChildren(ctx, s, api, p); err == nil {
		t.Fatal("lost response succeeded")
	}
	progress, err := CreateChildren(ctx, s, api, p)
	if err != nil || len(progress.Children) != 2 || api.calls != 2 {
		t.Fatal(progress, api.calls, err)
	}
	if _, err = CreateChildren(ctx, s, api, p); err != nil || api.calls != 2 {
		t.Fatal("duplicate child created", err)
	}
}
func TestPartialAndParentCompletion(t *testing.T) {
	s, _ := fixture()
	s.Included = []string{"A"}
	s.Remaining = []string{"B"}
	next, err := NextScope(s)
	if err != nil || len(next.PreviouslyMerged) != 1 || next.Included[0] != "B" {
		t.Fatal(next, err)
	}
	children := []Completion{{1, domain.Merged, []string{"A"}}, {1, domain.Merged, []string{"B"}}}
	if ParentComplete(s, children, 1, false) {
		t.Fatal("missing final acceptance accepted")
	}
	if !ParentComplete(s, children, 1, true) {
		t.Fatal("complete parent rejected")
	}
	children[1].Phase = domain.Canceled
	if ParentComplete(s, children, 1, true) {
		t.Fatal("unmerged child accepted")
	}
}
func TestCreatedChildMustStillMatchMarker(t *testing.T) {
	ctx := context.Background()
	s, _ := storage.Open(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	_, p := fixture()
	api := &creator{issues: map[string]int{}}
	if _, err := CreateChildren(ctx, s, api, p); err != nil {
		t.Fatal(err)
	}
	delete(api.issues, "harness-child:w:a")
	if _, err := CreateChildren(ctx, s, api, p); err == nil {
		t.Fatal("missing child accepted")
	}
	if api.calls != 2 {
		t.Fatal("missing child recreated")
	}
}
func TestSingleBroadCriterionPartitionRequiresAllDeclaredParts(t *testing.T) {
	scope, proposal := fixture()
	scope.Criteria = scope.Criteria[:1]
	scope.Included = []string{"A"}
	proposal.Children[0].Coverage = []Coverage{{ParentCriterion: "A", ChildCriterion: "a1", PartID: "first", PartCount: 2, Basis: "first part of original requirement"}}
	proposal.Children[1].Coverage = []Coverage{{ParentCriterion: "A", ChildCriterion: "b1", PartID: "second", PartCount: 2, Basis: "remaining part of original requirement"}}
	if err := proposal.Validate("w", scope, 0, 2, 8); err != nil {
		t.Fatal(err)
	}
	proposal.Children[1].Coverage[0].PartCount = 3
	if err := proposal.Validate("w", scope, 0, 2, 8); err == nil {
		t.Fatal("partition count mismatch accepted")
	}
}
