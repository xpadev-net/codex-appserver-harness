package storage

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func TestProductLocalReviewRequiresEveryLocalPartUnderExactScope(t *testing.T) {
	binding := ProductSplitChildScopeBinding{Value: ProductSplitChildScopeValue{ScopeDigest: strings.Repeat("a", 64), Scope: verification.Scope{Included: []string{"parse-left", "validate-left", "AC"}}}}
	complete := &contract.ProductLocalReview{ScopeDigest: binding.Value.ScopeDigest, Criteria: []contract.CriterionResult{}}
	for _, id := range binding.Value.Scope.Included {
		complete.Criteria = append(complete.Criteria, contract.CriterionResult{CriterionID: id, Status: "claimed", EvidenceIDs: []string{"local-report"}, Basis: "independent evaluation"})
	}
	if !productLocalReviewMatches(&binding, 4, complete) {
		t.Fatal("complete local scope denied")
	}
	if productLocalReviewMatches(&binding, 1, nil) {
		t.Fatal("original Wave claims satisfied a local review")
	}
	for _, mode := range []string{"missing_part", "foreign_scope", "parent_only", "duplicate"} {
		local := *complete
		local.Criteria = append([]contract.CriterionResult{}, complete.Criteria...)
		switch mode {
		case "missing_part":
			local.Criteria = local.Criteria[:2]
		case "foreign_scope":
			local.ScopeDigest = strings.Repeat("b", 64)
		case "parent_only":
			local.Criteria = []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", EvidenceIDs: []string{"local-report"}, Basis: "original criterion"}}
		case "duplicate":
			local.Criteria[1] = local.Criteria[0]
		}
		if productLocalReviewMatches(&binding, 4, &local) {
			t.Fatal("incomplete/foreign review passed", mode)
		}
	}
	if productLocalReviewMatches(nil, 4, complete) || !productLocalReviewMatches(nil, 1, nil) {
		t.Fatal("local review leaked into Root contract")
	}
	result := contract.TaskResult{Artifacts: []string{"summary"}, CriterionResults: []contract.CriterionResult{{EvidenceIDs: []string{"summary"}}}, ProductLocalReview: complete}
	if refs := resultArtifactReferences(result); !reflect.DeepEqual(refs, []string{"summary", "local-report"}) {
		t.Fatal("local evidence escaped artifact grants/retention", refs)
	}
}
