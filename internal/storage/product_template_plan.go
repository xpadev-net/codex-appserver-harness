package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productRepairPhase(waveID, key, local string, task contract.TaskSpec) contract.PhaseSpec {
	return contract.PhaseSpec{ID: structureID("product-repair-phase-", waveID, key+":"+local), Tasks: []contract.TaskSpec{task}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{task.ID}}, FailurePolicy: "drain_and_block"}
}

func (k *KernelStore) buildProductTemplatePlanTx(ctx context.Context, tx *sql.Tx, w domain.Wave, current contract.PlanProposal, position int, key string, sourceResults []string, reviews []domain.Task, reports map[string]ProductTaskReviewSnapshot, owners []productTemplateOwner, verifier domain.Task, binding *ProductTemplateBinding) (contract.PlanProposal, map[string]templateReviewLineage, error) {
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: append([]contract.PhaseSpec{}, current.Phases[:position+1]...), Templates: append([]contract.FixReviewTemplate{}, current.Templates...)}
	lineages := map[string]templateReviewLineage{}
	registration, err := loadProductWorkItemRegistration(ctx, tx, binding.WorkItemID)
	if err != nil {
		return plan, lineages, err
	}
	contextResults := append(append([]string{}, sourceResults...), verifier.ResultID)
	sort.Strings(contextResults)
	if len(contextResults) > contract.MaxTaskSourceResults {
		return plan, lineages, fmt.Errorf("%w: product finding batch exceeds source bound", ErrTemplateNeedsMain)
	}
	inputs := []string{}
	inputSet := map[string]bool{}
	for _, review := range reviews {
		var result contract.TaskResult
		if err := taskEvidenceRecord(ctx, tx, "task_result", review.ResultID, &result); err != nil {
			return plan, lineages, err
		}
		for _, id := range append(append([]string{}, review.Spec.InputArtifactIDs...), resultArtifactReferences(result)...) {
			inputSet[id] = true
		}
	}
	for id := range inputSet {
		if _, err := k.currentArtifactTx(ctx, tx, w, id); err != nil {
			return plan, lineages, err
		}
		inputs = append(inputs, id)
	}
	sort.Strings(inputs)
	previous := verifier.LocalID
	for _, owner := range owners {
		root, err := loadTask(ctx, tx, structureID("task-", w.ID, owner.Root))
		if err != nil {
			return plan, lineages, err
		}
		local := structureID("product-repair-", w.ID, key+":"+owner.Template.ID+":"+owner.Kind)
		criteria := append([]string{}, root.Spec.Criteria...)
		if owner.Kind == "verification" {
			criteria = append([]string{}, registration.Value.Definition.Criteria...)
		}
		deps := []contract.TaskDependency{{TaskID: previous}}
		for _, review := range reviews {
			deps = append(deps, contract.TaskDependency{TaskID: review.LocalID})
		}
		instruction := owner.Template.FixInstruction + "\nApply only these classified finding IDs: " + strings.Join(owner.Findings, ", ")
		task := contract.TaskSpec{ID: local, Kind: owner.Kind, Instruction: instruction, Criteria: criteria, DependsOn: deps, InputArtifactIDs: append([]string{}, inputs...), SourceResultIDs: append([]string{}, contextResults...), WorkItemIDs: []string{binding.WorkItemID}}
		plan.Phases = append(plan.Phases, productRepairPhase(w.ID, key, local, task))
		binding.Repairs = append(binding.Repairs, local)
		previous = local
	}
	// Implementation fixes always get a separate fresh verifier. A test-defect
	// repair already is a verifier with the v3 plan/test-path sealing contract.
	if owners[len(owners)-1].Kind != "verification" {
		local := structureID("product-reverify-", w.ID, key)
		task := contract.TaskSpec{ID: local, Kind: "verification", Instruction: verifier.Spec.Instruction + "\nIndependently revalidate the repaired Candidate against the original requirements and pinned mandatory profiles.", Criteria: append([]string{}, registration.Value.Definition.Criteria...), DependsOn: []contract.TaskDependency{{TaskID: previous}}, InputArtifactIDs: append([]string{}, inputs...), SourceResultIDs: append([]string{}, contextResults...), WorkItemIDs: []string{binding.WorkItemID}}
		plan.Phases = append(plan.Phases, productRepairPhase(w.ID, key, local, task))
		previous = local
	}
	binding.Verifier = previous
	phase := contract.PhaseSpec{ID: structureID("product-rereview-phase-", w.ID, key), Tasks: []contract.TaskSpec{}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, FailurePolicy: "drain_and_block"}
	roles := map[string]string{}
	rootReplacements := map[string][]string{}
	templates := map[string]bool{}
	addReview := func(local, root, role, instruction string, criteria, artifacts []string) {
		task := contract.TaskSpec{ID: local, Kind: "review", Instruction: instruction, Criteria: append([]string{}, criteria...), DependsOn: []contract.TaskDependency{{TaskID: previous}}, InputArtifactIDs: append([]string{}, artifacts...), SourceResultIDs: []string{}, WorkItemIDs: []string{binding.WorkItemID}}
		phase.Tasks = append(phase.Tasks, task)
		phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, local)
		roles[role] = local
		lineages[local] = templateReviewLineage{RootReviewLocalID: root, ReviewRoleID: role}
	}
	for _, owner := range owners {
		if templates[owner.Template.ID] {
			continue
		}
		templates[owner.Template.ID] = true
		root, err := loadTask(ctx, tx, structureID("task-", w.ID, owner.Root))
		if err != nil {
			return plan, lineages, err
		}
		local := structureID("product-owner-review-", w.ID, key+":"+owner.Template.ID)
		addReview(local, owner.Root, "template:"+owner.Template.ID, owner.Template.ReviewInstruction, root.Spec.Criteria, root.Spec.InputArtifactIDs)
		rootReplacements[owner.Root] = append(rootReplacements[owner.Root], local)
	}
	for _, review := range reviews {
		lineage, err := productTemplateRoot(ctx, tx, review)
		if err != nil {
			return plan, lineages, err
		}
		replacements := []string{}
		if local := roles[lineage.ReviewRoleID]; local != "" {
			replacements = append(replacements, local)
		} else if strings.HasPrefix(lineage.ReviewRoleID, "root:") && reports[review.ID].Value.Review.Verdict == "findings" && len(rootReplacements[lineage.RootReviewLocalID]) > 0 {
			replacements = append(replacements, rootReplacements[lineage.RootReviewLocalID]...)
		} else {
			local := structureID("product-role-review-", w.ID, key+":"+review.ID)
			addReview(local, lineage.RootReviewLocalID, lineage.ReviewRoleID, review.Spec.Instruction, review.Spec.Criteria, review.Spec.InputArtifactIDs)
			replacements = append(replacements, local)
		}
		for _, local := range replacements {
			binding.Replacements[review.ID] = append(binding.Replacements[review.ID], structureID("task-", w.ID, local))
		}
	}
	groups, err := k.remapProductReviewPhasesTx(ctx, tx, w, current, key, binding, reviews, phase)
	if err != nil {
		return plan, lineages, err
	}
	plan.Phases = append(plan.Phases, groups...)
	if err := k.appendProductFuturePhasesTx(ctx, tx, w, current, position, key, binding, reviews, &plan, lineages); err != nil {
		return plan, lineages, err
	}
	return plan, lineages, nil
}
