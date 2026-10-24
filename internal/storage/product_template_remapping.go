package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func (k *KernelStore) productFutureTaskUnusedTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) error {
	if task.State != contract.TaskReady || task.Attempts != 0 {
		return fmt.Errorf("%w: later product Task already progressed", ErrTemplateNeedsMain)
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return err
	}
	b, err := sessionBindingTx(ctx, tx, task.SessionID)
	if err != nil {
		return err
	}
	var active string
	var stopped, creating, used, held bool
	if err := tx.QueryRowContext(ctx, `SELECT active_run_id,process_stopped,creating,EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=?),EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE session_id=? AND state='held') FROM kernel_sessions WHERE id=?`, b.ID, b.ID, b.ID).Scan(&active, &stopped, &creating, &used, &held); err != nil {
		return err
	}
	if b.Closed || b.ThreadID != "" || b.PreparationID != "" || active != "" || !stopped || creating || used || held {
		return fmt.Errorf("%w: later product session is not unused", ErrTemplateNeedsMain)
	}
	var live bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='managed_process' AND json_extract(payload,'$.state')<>'stopped' AND json_extract(payload,'$.request.binding.id')=?)`, b.ID).Scan(&live); err != nil {
		return err
	}
	if live {
		return fmt.Errorf("%w: later product process is not stopped", ErrTemplateNeedsMain)
	}
	return nil
}

func uniqueProductDependencies(deps []contract.TaskDependency, self string) ([]contract.TaskDependency, error) {
	seen := map[string]string{}
	out := []contract.TaskDependency{}
	for _, dep := range deps {
		if dep.TaskID == self {
			return out, fmt.Errorf("%w: review role would depend on itself", ErrTemplateNeedsMain)
		}
		if selector, ok := seen[dep.TaskID]; ok {
			if selector != dep.ArtifactID {
				return out, fmt.Errorf("%w: conflicting explicit dependency selectors", ErrTemplateNeedsMain)
			}
			continue
		}
		seen[dep.TaskID] = dep.ArtifactID
		out = append(out, dep)
	}
	return out, nil
}

func (k *KernelStore) remapProductReviewPhasesTx(ctx context.Context, tx *sql.Tx, w domain.Wave, current contract.PlanProposal, key string, binding *ProductTemplateBinding, reviews []domain.Task, phase contract.PhaseSpec) ([]contract.PhaseSpec, error) {
	byID := map[string]domain.Task{}
	byLocal := map[string]domain.Task{}
	positions := map[string]int{}
	for _, review := range reviews {
		byID[review.ID], byLocal[review.LocalID] = review, review
	}
	for index, oldPhase := range current.Phases {
		for _, spec := range oldPhase.Tasks {
			positions[structureID("task-", w.ID, spec.ID)] = index
		}
	}
	newLocals := map[string]string{}
	for _, review := range phase.Tasks {
		newLocals[structureID("task-", w.ID, review.ID)] = review.ID
	}
	origins := map[string][]string{}
	for old, ids := range binding.Replacements {
		for _, id := range ids {
			local := newLocals[id]
			if local == "" {
				return nil, ErrTaskEvidence
			}
			origins[local] = append(origins[local], old)
		}
	}
	groups := map[int][]contract.TaskSpec{}
	indexes := []int{}
	for _, review := range phase.Tasks {
		deps := append([]contract.TaskDependency{}, review.DependsOn...)
		position := 0
		for _, id := range origins[review.ID] {
			old, ok := byID[id]
			if !ok {
				return nil, ErrTaskEvidence
			}
			if positions[id] > position {
				position = positions[id]
			}
			for _, artifactID := range review.InputArtifactIDs {
				a, err := k.currentArtifactTx(ctx, tx, w, artifactID)
				if err != nil {
					return nil, err
				}
				if _, peer := byID[a.TaskID]; peer {
					return nil, fmt.Errorf("%w: explicit peer report input needs parent remapping", ErrTemplateNeedsMain)
				}
			}
			for _, dep := range old.Spec.DependsOn {
				if peer, ok := byLocal[dep.TaskID]; ok {
					if dep.ArtifactID != "" {
						return nil, fmt.Errorf("%w: explicit peer report selector needs parent remapping", ErrTemplateNeedsMain)
					}
					for _, next := range binding.Replacements[peer.ID] {
						deps = append(deps, contract.TaskDependency{TaskID: newLocals[next]})
					}
					continue
				}
				prior, err := loadTask(ctx, tx, structureID("task-", w.ID, dep.TaskID))
				if err != nil {
					return nil, err
				}
				input, err := k.taskResultInputTx(ctx, tx, w, prior.ResultID)
				if err != nil {
					return nil, err
				}
				if input.ProductCandidate != nil && input.ProductCandidate.Value.Reference.WorkItemID == binding.WorkItemID {
					if dep.ArtifactID != "" {
						return nil, fmt.Errorf("%w: explicit superseded product selector needs parent remapping", ErrTemplateNeedsMain)
					}
					continue // Fresh final verifier is already the subject dependency.
				}
				deps = append(deps, dep)
			}
		}
		var err error
		review.DependsOn, err = uniqueProductDependencies(deps, review.ID)
		if err != nil {
			return nil, err
		}
		if _, ok := groups[position]; !ok {
			indexes = append(indexes, position)
		}
		groups[position] = append(groups[position], review)
	}
	sort.Ints(indexes)
	out := []contract.PhaseSpec{}
	for _, index := range indexes {
		group := contract.PhaseSpec{ID: structureID("product-rereview-phase-", w.ID, fmt.Sprintf("%s:%d", key, index)), Tasks: groups[index], Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, FailurePolicy: "drain_and_block"}
		for _, task := range group.Tasks {
			group.Completion.RequiredTaskIDs = append(group.Completion.RequiredTaskIDs, task.ID)
		}
		out = append(out, group)
	}
	return out, nil
}

func (k *KernelStore) appendProductFuturePhasesTx(ctx context.Context, tx *sql.Tx, w domain.Wave, current contract.PlanProposal, position int, key string, binding *ProductTemplateBinding, reviews []domain.Task, plan *contract.PlanProposal, lineages map[string]templateReviewLineage) error {
	clones := map[string]string{}
	for _, phase := range current.Phases[position+1:] {
		for _, task := range phase.Tasks {
			clones[task.ID] = structureID("product-future-", w.ID, key+":"+task.ID)
		}
	}
	if len(clones) == 0 {
		return nil
	}
	binding.FutureClones = map[string]string{}
	for old, next := range clones {
		binding.FutureClones[structureID("task-", w.ID, old)] = structureID("task-", w.ID, next)
	}
	for i := range plan.Templates {
		if next := clones[plan.Templates[i].InputTaskID]; next != "" {
			plan.Templates[i].InputTaskID = next
		}
	}
	replacementLocals := map[string][]string{}
	for old, ids := range binding.Replacements {
		task, err := loadTask(ctx, tx, old)
		if err != nil {
			return err
		}
		for _, id := range ids {
			for _, phase := range plan.Phases {
				for _, spec := range phase.Tasks {
					if structureID("task-", w.ID, spec.ID) == id {
						replacementLocals[task.LocalID] = append(replacementLocals[task.LocalID], spec.ID)
					}
				}
			}
		}
	}
	retiredArtifacts := map[string]bool{}
	for _, review := range reviews {
		var result contract.TaskResult
		if err := taskEvidenceRecord(ctx, tx, "task_result", review.ResultID, &result); err != nil {
			return err
		}
		for _, id := range resultArtifactReferences(result) {
			a, err := loadArtifact(ctx, tx, id)
			if err != nil {
				return err
			}
			if a.TaskID == review.ID {
				retiredArtifacts[id] = true
			}
		}
	}
	for _, oldPhase := range current.Phases[position+1:] {
		phase := contract.PhaseSpec{ID: structureID("product-future-phase-", w.ID, key+":"+oldPhase.ID), Tasks: []contract.TaskSpec{}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, FailurePolicy: oldPhase.FailurePolicy}
		for _, old := range oldPhase.Tasks {
			task := old
			task.ID = clones[old.ID]
			task.DependsOn = []contract.TaskDependency{}
			task.SourceResultIDs = []string{}
			for _, id := range old.InputArtifactIDs {
				if retiredArtifacts[id] {
					return fmt.Errorf("%w: explicit superseded report input needs parent remapping", ErrTemplateNeedsMain)
				}
			}
			for _, dep := range old.DependsOn {
				if next := clones[dep.TaskID]; next != "" {
					task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: next, ArtifactID: dep.ArtifactID})
					continue
				}
				if replacements := replacementLocals[dep.TaskID]; len(replacements) > 0 {
					if dep.ArtifactID != "" {
						return fmt.Errorf("%w: explicit future review selector needs parent remapping", ErrTemplateNeedsMain)
					}
					for _, next := range replacements {
						task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: next})
					}
					continue
				}
				prior, err := loadTask(ctx, tx, structureID("task-", w.ID, dep.TaskID))
				if err != nil {
					return err
				}
				input, err := k.taskResultInputTx(ctx, tx, w, prior.ResultID)
				if err != nil {
					return err
				}
				if input.ProductCandidate != nil && input.ProductCandidate.Value.Reference.WorkItemID == binding.WorkItemID {
					if dep.ArtifactID != "" {
						return fmt.Errorf("%w: explicit future Candidate selector needs parent remapping", ErrTemplateNeedsMain)
					}
					task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: binding.Verifier})
					continue
				}
				task.DependsOn = append(task.DependsOn, dep)
			}
			for _, id := range old.SourceResultIDs {
				input, err := k.taskResultInputTx(ctx, tx, w, id)
				if err != nil {
					return err
				}
				if replacements := binding.Replacements[input.TaskID]; len(replacements) > 0 {
					for _, next := range replacementLocals[input.SourceSpec.ID] {
						task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: next})
					}
					continue
				}
				if input.ProductCandidate != nil && input.ProductCandidate.Value.Reference.WorkItemID == binding.WorkItemID {
					task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: binding.Verifier})
					continue
				}
				task.SourceResultIDs = append(task.SourceResultIDs, id)
			}
			if len(task.SourceResultIDs) == 0 {
				task.SourceResultIDs = nil
			}
			var err error
			task.DependsOn, err = uniqueProductDependencies(task.DependsOn, task.ID)
			if err != nil {
				return err
			}
			phase.Tasks = append(phase.Tasks, task)
			phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, task.ID)
			oldID, newID := structureID("task-", w.ID, old.ID), structureID("task-", w.ID, task.ID)
			if err := kernelRecordTx(ctx, tx, "template_future_clone", newID, w.ID, templateTaskClone{OriginalID: oldID, ExpansionKey: key}); err != nil {
				return err
			}
			var lineage templateReviewLineage
			if err := taskEvidenceRecord(ctx, tx, "product_template_review_lineage", oldID, &lineage); err == nil {
				if next := clones[lineage.RootReviewLocalID]; next != "" {
					lineage.RootReviewLocalID = next
				}
				lineages[task.ID] = lineage
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		plan.Phases = append(plan.Phases, phase)
	}
	return nil
}
