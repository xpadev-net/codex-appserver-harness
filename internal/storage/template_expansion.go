package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrTemplateNeedsMain = errors.New("template expansion requires parent replanning")

type TemplateExpansionRequest struct {
	Target       contract.Target `json:"target"`
	ReviewTaskID string          `json:"review_task_id"`
}
type TemplateExpansion struct {
	Request         TemplateExpansionRequest `json:"request"`
	Key             string                   `json:"key"`
	Round           int                      `json:"round"`
	InputDigest     string                   `json:"input_digest"`
	FindingBatchID  string                   `json:"finding_batch_id"`
	SourceResultIDs []string                 `json:"source_result_ids"`
	TemplateIDs     []string                 `json:"template_ids"`
	Admission       PlanAdmission            `json:"admission"`
}
type templateReviewLineage struct {
	RootReviewLocalID string `json:"root_review_local_id"`
	TemplateDigest    string `json:"template_digest"`
	Round             int    `json:"round"`
	ExpansionKey      string `json:"expansion_key"`
	ReviewRoleID      string `json:"review_role_id"`
}
type templateAssignment struct {
	Template          contract.FixReviewTemplate `json:"template"`
	RootReviewLocalID string                     `json:"root_review_local_id"`
	FindingIDs        []string                   `json:"finding_ids"`
}

// ExpandReviewTemplates executes only K's saved fix/review language. It does not
// call a model, accept a generated plan, or authorize a Task to create Tasks.
// P's verification/runner/candidate transitions are connected by Task_7/8.
func (k *KernelStore) ExpandReviewTemplates(ctx context.Context, request TemplateExpansionRequest) (TemplateExpansion, error) {
	var receipt TemplateExpansion
	if !request.Target.Valid() || request.ReviewTaskID == "" {
		return receipt, errors.New("versioned review expansion request required")
	}
	requestKey, err := contentDigest(request)
	if err != nil {
		return receipt, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "template_expansion_request", requestKey, &receipt)
	if err == nil {
		return receipt, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	wave, err := loadWave(ctx, tx, request.Target.WaveID)
	if err != nil {
		return receipt, err
	}
	if wave.Target() != request.Target || wave.Paused || wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted || !k.clock().Before(wave.Deadline) {
		return receipt, errors.New("stale/paused/terminal/expired template target")
	}
	if wave.Policy.Stage != "K" || wave.Policy.Product {
		return receipt, fmt.Errorf("%w: product expansion needs independent verification adapter", ErrTemplateNeedsMain)
	}
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_plans WHERE wave_id=? AND revision=?", wave.ID, wave.PlanRevision).Scan(&raw); err != nil {
		return receipt, err
	}
	var current PlanAdmission
	if err = json.Unmarshal(raw, &current); err != nil {
		return receipt, err
	}
	source, err := loadTask(ctx, tx, request.ReviewTaskID)
	if err != nil {
		return receipt, err
	}
	if source.WaveID != wave.ID || source.Spec.Kind != "review" {
		return receipt, errors.New("template source must be this Wave's review Task")
	}
	sourceResult, err := k.completedTaskResultTx(ctx, tx, wave, source)
	if err != nil {
		return receipt, err
	}
	if sourceResult.Review == nil || sourceResult.Review.Verdict != "findings" {
		return receipt, fmt.Errorf("%w: source has no actionable findings", ErrTemplateNeedsMain)
	}
	phasePosition := -1
	for i, p := range current.Plan.Phases {
		if structureID("phase-", wave.ID, p.ID) == source.PhaseID {
			phasePosition = i
			break
		}
	}
	if phasePosition < 0 {
		return receipt, errors.New("source review is not bound to current plan")
	}

	inputDigest := sourceResult.Review.TargetDigest
	results := []string{}
	sourceTaskIDs := []string{}
	reviews := []contract.ReviewResult{}
	assignments := map[string]*templateAssignment{}
	round := 0
	for _, spec := range reviewSpecsThrough(current.Plan, phasePosition) {
		if spec.Kind != "review" {
			continue
		}
		task, err := loadTask(ctx, tx, structureID("task-", wave.ID, spec.ID))
		if err != nil {
			return receipt, err
		}
		result, err := k.completedTaskResultTx(ctx, tx, wave, task)
		if err != nil {
			return receipt, err
		}
		if result.Review == nil || result.Review.TargetDigest != inputDigest {
			continue
		}
		results = append(results, task.ResultID)
		sourceTaskIDs = append(sourceTaskIDs, task.ID)
		review := *result.Review
		review.Findings = append([]contract.ReviewFinding{}, review.Findings...)
		sort.Slice(review.Findings, func(i, j int) bool { return review.Findings[i].ID < review.Findings[j].ID })
		reviews = append(reviews, review)
		root := task.LocalID
		nextRound := 1
		var lineage templateReviewLineage
		err = completionRecord(ctx, tx, "template_review_lineage", task.ID, &lineage)
		if err == nil {
			root = lineage.RootReviewLocalID
			nextRound = lineage.Round + 1
		} else if !errors.Is(err, sql.ErrNoRows) {
			return receipt, err
		}
		if nextRound > round {
			round = nextRound
		}
		if result.Review.Verdict != "findings" {
			continue
		}
		templates := []contract.FixReviewTemplate{}
		for _, template := range current.Plan.Templates {
			if template.InputTaskID == root {
				templates = append(templates, template)
			}
		}
		if len(templates) == 0 {
			return receipt, fmt.Errorf("%w: no saved template for review", ErrTemplateNeedsMain)
		}
		definitions, err := contentDigest(templates)
		if err != nil {
			return receipt, err
		}
		if lineage.TemplateDigest != "" && lineage.TemplateDigest != definitions {
			return receipt, fmt.Errorf("%w: template authority changed", ErrTemplateNeedsMain)
		}
		for _, finding := range review.Findings {
			matching := []contract.FixReviewTemplate{}
			for _, template := range templates {
				for _, classification := range template.Classifications {
					if classification == finding.Classification {
						matching = append(matching, template)
						break
					}
				}
			}
			if len(matching) != 1 {
				return receipt, fmt.Errorf("%w: unsupported or ambiguous finding classification", ErrTemplateNeedsMain)
			}
			template := matching[0]
			owner := assignments[template.ID]
			if owner == nil {
				owner = &templateAssignment{Template: template, RootReviewLocalID: root, FindingIDs: []string{}}
				assignments[template.ID] = owner
			}
			owner.FindingIDs = append(owner.FindingIDs, task.ID+":"+finding.ID)
		}
	}
	if len(assignments) == 0 || round < 1 {
		return receipt, fmt.Errorf("%w: no classified findings", ErrTemplateNeedsMain)
	}
	sort.Strings(results)
	sort.Slice(reviews, func(i, j int) bool {
		left, _ := contentDigest(reviews[i])
		right, _ := contentDigest(reviews[j])
		return left < right
	})
	batch, err := contentDigest(struct {
		Results []string
		Reviews []contract.ReviewResult
	}{results, reviews})
	if err != nil {
		return receipt, err
	}
	owners := make([]templateAssignment, 0, len(assignments))
	for _, a := range assignments {
		sort.Strings(a.FindingIDs)
		owners = append(owners, *a)
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].Template.ID < owners[j].Template.ID })
	identity, err := contentDigest(struct {
		Wave                 string
		Scope, Policy, Round int
		Digest, Batch        string
		Owners               []templateAssignment
	}{wave.ID, wave.ScopeRevision, wave.Policy.Revision, round, inputDigest, batch, owners})
	if err != nil {
		return receipt, err
	}
	key := "template:" + identity
	err = completionRecord(ctx, tx, "template_expansion", key, &receipt)
	if err == nil {
		if err = kernelRecordTx(ctx, tx, "template_expansion_request", requestKey, wave.ID, receipt); err != nil {
			return receipt, err
		}
		return receipt, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	if wave.FixRounds >= wave.Policy.Limits.FixRounds || round > wave.Policy.Limits.FixRounds {
		return receipt, fmt.Errorf("%w: cumulative fix budget exhausted", ErrTemplateNeedsMain)
	}
	for _, owner := range owners {
		used, err := templateOwnerUsesTx(ctx, tx, wave.ID, owner.Template.ID)
		if err != nil {
			return receipt, err
		}
		if used >= owner.Template.MaxRounds {
			return receipt, fmt.Errorf("%w: template repeat limit exceeded", ErrTemplateNeedsMain)
		}
	}
	// Sequential barriers: every Task up through this phase must have a valid,
	// stopped/applied completed result; all later Tasks must still be unstarted.
	for position, p := range current.Plan.Phases {
		for _, spec := range p.Tasks {
			task, err := loadTask(ctx, tx, structureID("task-", wave.ID, spec.ID))
			if err != nil {
				return receipt, err
			}
			if position <= phasePosition {
				if _, err = k.completedTaskResultTx(ctx, tx, wave, task); err != nil {
					return receipt, fmt.Errorf("%w: required phase is not complete", ErrTemplateNeedsMain)
				}
				var bound string
				if err = tx.QueryRowContext(ctx, "SELECT source_result_id FROM kernel_plan_bindings WHERE wave_id=? AND revision=? AND task_id=?", wave.ID, wave.PlanRevision, task.ID).Scan(&bound); err != nil {
					return receipt, err
				}
				if bound != "" && bound != task.ResultID {
					return receipt, errors.New("completed plan binding conflict")
				}
				inputs, err := k.taskInputDigestTx(ctx, tx, wave, task.Spec)
				if err != nil {
					return receipt, err
				}
				if inputs != task.InputDigest {
					return receipt, errors.New("completed Task inputs changed before expansion")
				}
			} else if task.State != contract.TaskReady || task.Attempts != 0 {
				return receipt, fmt.Errorf("%w: later phase has already progressed", ErrTemplateNeedsMain)
			}
		}
	}
	plan, reviewRoots, err := k.buildKernelTemplatePlan(ctx, tx, wave, current.Plan, phasePosition, key, inputDigest, owners)
	if err != nil {
		return receipt, err
	}
	admission, err := k.adoptPlanModeTx(ctx, tx, wave.Target(), key, plan, "template")
	if err != nil {
		return receipt, err
	}
	// adoptPlanModeTx saved structure counters in this same transaction. Retain
	// those increments when adding the batch's one cumulative fix reservation.
	updated, err := loadWave(ctx, tx, wave.ID)
	if err != nil {
		return receipt, err
	}
	updated.FixRounds++
	if err = saveWave(ctx, tx, updated); err != nil {
		return receipt, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'fix_round',1,0,?)", wave.ID, key, []byte(`{}`)); err != nil {
		return receipt, err
	}
	for local, root := range reviewRoots {
		definitions := []contract.FixReviewTemplate{}
		for _, template := range current.Plan.Templates {
			if template.InputTaskID == root {
				definitions = append(definitions, template)
			}
		}
		digest, err := contentDigest(definitions)
		if err != nil {
			return receipt, err
		}
		taskID := structureID("task-", wave.ID, local)
		var input struct {
			ReviewRoleID string `json:"review_role_id"`
		}
		if err = completionRecord(ctx, tx, "template_task_context", taskID, &input); err != nil {
			return receipt, err
		}
		if err = kernelRecordTx(ctx, tx, "template_review_lineage", taskID, wave.ID, templateReviewLineage{RootReviewLocalID: root, TemplateDigest: digest, Round: round, ExpansionKey: key, ReviewRoleID: input.ReviewRoleID}); err != nil {
			return receipt, err
		}
	}
	receipt = TemplateExpansion{Request: request, Key: key, Round: round, InputDigest: inputDigest, FindingBatchID: batch, SourceResultIDs: results, TemplateIDs: []string{}, Admission: admission}
	for _, owner := range owners {
		receipt.TemplateIDs = append(receipt.TemplateIDs, owner.Template.ID)
	}
	if err = kernelRecordTx(ctx, tx, "template_expansion", key, wave.ID, receipt); err != nil {
		return receipt, err
	}
	if err = kernelRecordTx(ctx, tx, "template_expansion_request", requestKey, wave.ID, receipt); err != nil {
		return receipt, err
	}
	// Any review event in this already batched phase must resolve to the same
	// expansion, even if another review happened to trigger the transaction.
	for _, id := range sourceTaskIDs {
		alias, err := contentDigest(TemplateExpansionRequest{Target: request.Target, ReviewTaskID: id})
		if err != nil {
			return receipt, err
		}
		if err = kernelRecordTx(ctx, tx, "template_expansion_request", alias, wave.ID, receipt); err != nil {
			return receipt, err
		}
	}
	return receipt, tx.Commit()
}

func (k *KernelStore) buildKernelTemplatePlan(ctx context.Context, tx *sql.Tx, wave domain.Wave, current contract.PlanProposal, position int, key, inputDigest string, owners []templateAssignment) (contract.PlanProposal, map[string]string, error) {
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: append([]contract.PhaseSpec{}, current.Phases[:position+1]...), Templates: append([]contract.FixReviewTemplate{}, current.Templates...)}
	reviewRoots := map[string]string{}
	sourceLocals := []string{}
	sourceResults := []string{}
	inputSet := map[string]bool{}
	subjectArtifacts := map[string]bool{}
	subjectProducers := map[string][]string{}
	for _, spec := range reviewSpecsThrough(current, position) {
		if spec.Kind != "review" {
			continue
		}
		task, err := loadTask(ctx, tx, structureID("task-", wave.ID, spec.ID))
		if err != nil {
			return plan, reviewRoots, err
		}
		result, err := k.completedTaskResultTx(ctx, tx, wave, task)
		if err != nil {
			return plan, reviewRoots, err
		}
		if result.Review == nil || result.Review.TargetDigest != inputDigest {
			continue
		}
		sourceLocals = append(sourceLocals, spec.ID)
		sourceResults = append(sourceResults, task.ResultID)
		for _, id := range spec.InputArtifactIDs {
			inputSet[id] = true
			a, err := k.currentArtifactTx(ctx, tx, wave, id)
			if err != nil {
				return plan, reviewRoots, err
			}
			if a.Digest == inputDigest {
				subjectArtifacts[id] = true
			}
		}
		for _, dep := range spec.DependsOn {
			prior, err := loadTask(ctx, tx, structureID("task-", wave.ID, dep.TaskID))
			if err != nil {
				return plan, reviewRoots, err
			}
			produced, err := k.completedTaskResultTx(ctx, tx, wave, prior)
			if err != nil {
				return plan, reviewRoots, err
			}
			allOutputs := resultArtifactReferences(produced)
			producerHasSubject := false
			for _, id := range allOutputs {
				if dep.ArtifactID == "" || dep.ArtifactID == id {
					inputSet[id] = true
					a, err := k.currentArtifactTx(ctx, tx, wave, id)
					if err != nil {
						return plan, reviewRoots, err
					}
					if a.Digest == inputDigest {
						subjectArtifacts[id] = true
						producerHasSubject = true
					}
				}
			}
			if producerHasSubject {
				subjectProducers[dep.TaskID] = allOutputs
			}
		}
	}
	// The reviewer may receive the subject directly by Artifact ID rather than
	// through a producer edge. Find every already completed producer whose result
	// grants that same immutable subject, so later consumers cannot keep a stale
	// grant through an edge the reviewer itself did not need.
	for _, phase := range current.Phases[:position+1] {
		for _, spec := range phase.Tasks {
			task, err := loadTask(ctx, tx, structureID("task-", wave.ID, spec.ID))
			if err != nil {
				return plan, reviewRoots, err
			}
			result, err := k.completedTaskResultTx(ctx, tx, wave, task)
			if err != nil {
				return plan, reviewRoots, err
			}
			outputs := resultArtifactReferences(result)
			matches := []string{}
			for _, id := range outputs {
				a, err := loadArtifact(ctx, tx, id)
				if err != nil {
					return plan, reviewRoots, err
				}
				if a.Digest == inputDigest {
					matches = append(matches, id)
				}
			}
			if len(matches) > 0 {
				subjectProducers[spec.ID] = outputs
				for _, id := range matches {
					subjectArtifacts[id] = true
				}
			}
		}
	}
	sort.Strings(sourceLocals)
	sort.Strings(sourceResults)
	inputs := []string{}
	reviewInputs := []string{}
	for id := range inputSet {
		a, err := k.currentArtifactTx(ctx, tx, wave, id)
		if err != nil {
			return plan, reviewRoots, err
		}
		inputs = append(inputs, id)
		if a.Digest != inputDigest {
			reviewInputs = append(reviewInputs, id)
		}
	}
	sort.Strings(inputs)
	sort.Strings(reviewInputs)
	previousFix := ""
	reviewRoles := map[string]string{}
	roleReviews := map[string]string{}
	rootReviews := map[string][]string{}
	replacements := map[string][]string{}
	reviewPhase := contract.PhaseSpec{ID: structureID("review-phase-", wave.ID, key), Tasks: []contract.TaskSpec{}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, FailurePolicy: "drain_and_block"}
	for _, owner := range owners {
		root, err := loadTask(ctx, tx, structureID("task-", wave.ID, owner.RootReviewLocalID))
		if err != nil {
			return plan, reviewRoots, err
		}
		criteria := append([]string{}, root.Spec.Criteria...)
		fixID := structureID("fix-", wave.ID, key+":"+owner.Template.ID)
		reviewID := structureID("review-", wave.ID, key+":"+owner.Template.ID)
		dependencies := []contract.TaskDependency{}
		for _, local := range sourceLocals {
			dependencies = append(dependencies, contract.TaskDependency{TaskID: local})
		}
		priorFix := previousFix
		if previousFix != "" {
			dependencies = append(dependencies, contract.TaskDependency{TaskID: previousFix})
		}
		fix := contract.TaskSpec{ID: fixID, Kind: "fix", Instruction: owner.Template.FixInstruction, Criteria: criteria, DependsOn: dependencies, InputArtifactIDs: inputs, WorkItemIDs: []string{}}
		phase := contract.PhaseSpec{ID: structureID("fix-phase-", wave.ID, key+":"+owner.Template.ID), Tasks: []contract.TaskSpec{fix}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{fixID}}, FailurePolicy: "drain_and_block"}
		plan.Phases = append(plan.Phases, phase)
		previousFix = fixID
		review := contract.TaskSpec{ID: reviewID, Kind: "review", Instruction: owner.Template.ReviewInstruction, Criteria: criteria, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: reviewInputs, WorkItemIDs: []string{}}
		reviewPhase.Tasks = append(reviewPhase.Tasks, review)
		reviewPhase.Completion.RequiredTaskIDs = append(reviewPhase.Completion.RequiredTaskIDs, reviewID)
		reviewRoles[reviewID] = "template:" + owner.Template.ID
		roleReviews["template:"+owner.Template.ID] = reviewID
		rootReviews[owner.RootReviewLocalID] = append(rootReviews[owner.RootReviewLocalID], reviewID)
		reviewRoots[reviewID] = owner.RootReviewLocalID
		if err := kernelRecordTx(ctx, tx, "template_task_context", structureID("task-", wave.ID, fixID), wave.ID, map[string]any{"expansion_key": key, "template_id": owner.Template.ID, "finding_ids": owner.FindingIDs, "input_digest": inputDigest, "source_result_ids": sourceResults, "previous_fix_local_id": priorFix}); err != nil {
			return plan, reviewRoots, err
		}
	}
	// Preserve every existing review role. Saved owner templates may replace
	// their original root role, but a clean or different required role is not
	// interchangeable with another owner's re-review.
	for _, local := range sourceLocals {
		source, err := loadTask(ctx, tx, structureID("task-", wave.ID, local))
		if err != nil {
			return plan, reviewRoots, err
		}
		result, err := k.completedTaskResultTx(ctx, tx, wave, source)
		if err != nil {
			return plan, reviewRoots, err
		}
		root, role := source.LocalID, "root:"+source.LocalID
		var lineage templateReviewLineage
		err = completionRecord(ctx, tx, "template_review_lineage", source.ID, &lineage)
		if err == nil {
			root = lineage.RootReviewLocalID
			role = lineage.ReviewRoleID
			if role == "" {
				role = "inherited:" + source.LocalID
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return plan, reviewRoots, err
		}
		if next := roleReviews[role]; next != "" {
			replacements[local] = []string{next}
			continue
		}
		if strings.HasPrefix(role, "root:") && result.Review.Verdict == "findings" && len(rootReviews[root]) > 0 {
			replacements[local] = append([]string{}, rootReviews[root]...)
			continue
		}
		id := structureID("role-review-", wave.ID, key+":"+source.ID)
		copied := source.Spec
		copied.ID = id
		copied.DependsOn = []contract.TaskDependency{}
		copied.InputArtifactIDs = reviewInputs
		reviewPhase.Tasks = append(reviewPhase.Tasks, copied)
		reviewPhase.Completion.RequiredTaskIDs = append(reviewPhase.Completion.RequiredTaskIDs, id)
		reviewRoots[id], reviewRoles[id], roleReviews[role] = root, role, id
		replacements[local] = []string{id}
	}
	// Owners fix the same subject in a deterministic sequence, so independent
	// fixes cannot race or produce competing replacements for one candidate.
	origins := map[string][]string{}
	peerOutputs := map[string]bool{}
	replacedReports := map[string]bool{}
	sourceReviewIDs := map[string]bool{}
	for _, local := range sourceLocals {
		sourceReviewIDs[structureID("task-", wave.ID, local)] = true
	}
	sourcePositions := map[string]int{}
	for position, phase := range current.Phases {
		for _, spec := range phase.Tasks {
			sourcePositions[spec.ID] = position
		}
	}
	reviewPositions := map[string]int{}
	for _, local := range sourceLocals {
		source, err := loadTask(ctx, tx, structureID("task-", wave.ID, local))
		if err != nil {
			return plan, reviewRoots, err
		}
		result, err := k.completedTaskResultTx(ctx, tx, wave, source)
		if err != nil {
			return plan, reviewRoots, err
		}
		for _, id := range resultArtifactReferences(result) {
			peerOutputs[id] = true
			a, err := loadArtifact(ctx, tx, id)
			if err != nil {
				return plan, reviewRoots, err
			}
			if sourceReviewIDs[a.TaskID] {
				replacedReports[id] = true
			}
		}
		for _, next := range replacements[local] {
			origins[next] = append(origins[next], local)
		}
	}
	for i := range reviewPhase.Tasks {
		review := &reviewPhase.Tasks[i]
		if len(origins[review.ID]) == 0 {
			for _, local := range sourceLocals {
				source, err := loadTask(ctx, tx, structureID("task-", wave.ID, local))
				if err != nil {
					return plan, reviewRoots, err
				}
				root := source.LocalID
				var lineage templateReviewLineage
				err = completionRecord(ctx, tx, "template_review_lineage", source.ID, &lineage)
				if err == nil {
					root = lineage.RootReviewLocalID
				} else if !errors.Is(err, sql.ErrNoRows) {
					return plan, reviewRoots, err
				}
				if root == reviewRoots[review.ID] {
					origins[review.ID] = append(origins[review.ID], local)
				}
			}
		}
		review.DependsOn = []contract.TaskDependency{{TaskID: previousFix}}
		seen := map[string]bool{previousFix: true}
		explicitInputs := map[string]bool{}
		for _, local := range origins[review.ID] {
			if sourcePositions[local] > reviewPositions[review.ID] {
				reviewPositions[review.ID] = sourcePositions[local]
			}
			source, err := loadTask(ctx, tx, structureID("task-", wave.ID, local))
			if err != nil {
				return plan, reviewRoots, err
			}
			for _, id := range source.Spec.InputArtifactIDs {
				explicitInputs[id] = true
			}
			for _, dep := range source.Spec.DependsOn {
				if next := replacements[dep.TaskID]; next != nil {
					if dep.ArtifactID != "" {
						return plan, reviewRoots, fmt.Errorf("%w: explicit peer review output needs parent remapping", ErrTemplateNeedsMain)
					}
					for _, id := range next {
						if id == review.ID {
							return plan, reviewRoots, fmt.Errorf("%w: peer review roles cannot be merged", ErrTemplateNeedsMain)
						}
						if !seen[id] {
							review.DependsOn = append(review.DependsOn, contract.TaskDependency{TaskID: id})
							seen[id] = true
						}
					}
				}
			}
		}
		filtered := []string{}
		for _, id := range review.InputArtifactIDs {
			if !peerOutputs[id] || explicitInputs[id] {
				filtered = append(filtered, id)
			}
		}
		review.InputArtifactIDs = filtered
		if err := kernelRecordTx(ctx, tx, "template_task_context", structureID("task-", wave.ID, reviewPhase.Tasks[i].ID), wave.ID, map[string]any{"expansion_key": key, "input_task_local_id": previousFix, "source_result_ids": sourceResults, "input_digest": inputDigest, "review_role_id": reviewRoles[reviewPhase.Tasks[i].ID]}); err != nil {
			return plan, reviewRoots, err
		}
	}
	// Separate original phase barriers remain separate even if no explicit data
	// dependency was needed. Ordering must not introduce an extra artifact grant.
	groups := map[int][]contract.TaskSpec{}
	positions := []int{}
	for _, review := range reviewPhase.Tasks {
		position := reviewPositions[review.ID]
		if _, ok := groups[position]; !ok {
			positions = append(positions, position)
		}
		groups[position] = append(groups[position], review)
	}
	sort.Ints(positions)
	for _, position := range positions {
		phase := contract.PhaseSpec{ID: structureID("review-phase-", wave.ID, fmt.Sprintf("%s:%d", key, position)), Tasks: groups[position], Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, FailurePolicy: "drain_and_block"}
		for _, task := range phase.Tasks {
			phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, task.ID)
		}
		plan.Phases = append(plan.Phases, phase)
	}
	cloneIDs := map[string]string{}
	for _, phase := range current.Phases[position+1:] {
		for _, task := range phase.Tasks {
			cloneIDs[task.ID] = structureID("future-", wave.ID, key+":"+task.ID)
		}
	}
	for i := range plan.Templates {
		if next := cloneIDs[plan.Templates[i].InputTaskID]; next != "" {
			plan.Templates[i].InputTaskID = next
		}
	}
	for _, oldPhase := range current.Phases[position+1:] {
		phase := contract.PhaseSpec{ID: structureID("future-phase-", wave.ID, key+":"+oldPhase.ID), Tasks: []contract.TaskSpec{}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, FailurePolicy: oldPhase.FailurePolicy}
		for _, old := range oldPhase.Tasks {
			task := old
			task.ID = cloneIDs[old.ID]
			task.DependsOn = []contract.TaskDependency{}
			inputIDs := map[string]bool{}
			needsFixedSubject := false
			for _, id := range old.InputArtifactIDs {
				if subjectArtifacts[id] {
					needsFixedSubject = true
				} else if replacedReports[id] {
					return plan, reviewRoots, fmt.Errorf("%w: explicit superseded review input needs parent remapping", ErrTemplateNeedsMain)
				} else {
					inputIDs[id] = true
				}
			}
			for _, dep := range old.DependsOn {
				if dep.ArtifactID != "" && subjectArtifacts[dep.ArtifactID] {
					if cloneIDs[dep.TaskID] != "" {
						return plan, reviewRoots, fmt.Errorf("%w: explicit future subject selector needs parent remapping", ErrTemplateNeedsMain)
					}
					needsFixedSubject = true
					continue
				}
				if outputs, affected := subjectProducers[dep.TaskID]; affected && (dep.ArtifactID == "" || subjectArtifacts[dep.ArtifactID]) {
					needsFixedSubject = true
					if dep.ArtifactID == "" {
						for _, id := range outputs {
							if !subjectArtifacts[id] && replacements[dep.TaskID] == nil {
								inputIDs[id] = true
							}
						}
					}
					// A producer edge granting all its old outputs would retain
					// access to the defective subject. Preserve other immutable
					// outputs explicitly and grant the replacement via final fix.
					if replacements[dep.TaskID] == nil {
						continue
					}
				}
				if replacements[dep.TaskID] != nil {
					if dep.ArtifactID != "" {
						return plan, reviewRoots, fmt.Errorf("%w: explicit review-output selector needs parent remapping", ErrTemplateNeedsMain)
					}
					for _, id := range replacements[dep.TaskID] {
						task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: id})
					}
				} else if next := cloneIDs[dep.TaskID]; next != "" {
					task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: next, ArtifactID: dep.ArtifactID})
				} else {
					task.DependsOn = append(task.DependsOn, dep)
				}
			}
			if needsFixedSubject {
				task.DependsOn = append(task.DependsOn, contract.TaskDependency{TaskID: previousFix})
			}
			task.InputArtifactIDs = []string{}
			for id := range inputIDs {
				task.InputArtifactIDs = append(task.InputArtifactIDs, id)
			}
			sort.Strings(task.InputArtifactIDs)
			// Multiple old reviewer edges can converge on the same newly allocated set.
			seen := map[string]bool{}
			unique := []contract.TaskDependency{}
			for _, dep := range task.DependsOn {
				if !seen[dep.TaskID] {
					unique = append(unique, dep)
					seen[dep.TaskID] = true
				}
			}
			task.DependsOn = unique
			phase.Tasks = append(phase.Tasks, task)
			phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, task.ID)
			if err := kernelRecordTx(ctx, tx, "template_future_clone", structureID("task-", wave.ID, task.ID), wave.ID, map[string]any{"source_task_id": structureID("task-", wave.ID, old.ID), "expansion_key": key}); err != nil {
				return plan, reviewRoots, err
			}
			if err := cloneTemplateMetadataTx(ctx, tx, wave, old.ID, task.ID, key, cloneIDs, plan.Templates); err != nil {
				return plan, reviewRoots, err
			}
			if needsFixedSubject && task.Kind == "review" {
				var priorContext templateTaskInput
				contextErr := completionRecord(ctx, tx, "template_task_context", structureID("task-", wave.ID, task.ID), &priorContext)
				if contextErr != nil && !errors.Is(contextErr, sql.ErrNoRows) {
					return plan, reviewRoots, contextErr
				}
				if errors.Is(contextErr, sql.ErrNoRows) {
					for _, id := range old.InputArtifactIDs {
						if !subjectArtifacts[id] {
							return plan, reviewRoots, fmt.Errorf("%w: downstream review has another explicit possible subject", ErrTemplateNeedsMain)
						}
					}
					for _, dep := range old.DependsOn {
						if dep.ArtifactID != "" && !subjectArtifacts[dep.ArtifactID] {
							return plan, reviewRoots, fmt.Errorf("%w: downstream review subject selector is unaffected", ErrTemplateNeedsMain)
						}
						if dep.ArtifactID == "" {
							producer, err := loadTask(ctx, tx, structureID("task-", wave.ID, dep.TaskID))
							if err != nil {
								return plan, reviewRoots, err
							}
							if !producer.State.Terminal() {
								return plan, reviewRoots, fmt.Errorf("%w: downstream review has an unknown future subject", ErrTemplateNeedsMain)
							}
							produced, err := k.completedTaskResultTx(ctx, tx, wave, producer)
							if err != nil {
								return plan, reviewRoots, err
							}
							for _, id := range resultArtifactReferences(produced) {
								if !subjectArtifacts[id] {
									return plan, reviewRoots, fmt.Errorf("%w: downstream review has alternative dependency outputs", ErrTemplateNeedsMain)
								}
							}
						}
					}
				}
				if err := pinClonedReviewSubjectTx(ctx, tx, wave, task.ID, key, inputDigest, previousFix, cloneIDs); err != nil {
					return plan, reviewRoots, err
				}
			}
		}
		plan.Phases = append(plan.Phases, phase)
	}
	if err := domain.ValidateWavePlan(wave, plan); err != nil {
		return plan, reviewRoots, fmt.Errorf("%w: %v", ErrTemplateNeedsMain, err)
	}
	return plan, reviewRoots, nil
}

func pinClonedReviewSubjectTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, local, key, digest, finalFix string, clones map[string]string) error {
	id := structureID("task-", wave.ID, local)
	var input map[string]any
	err := completionRecord(ctx, tx, "template_task_context", id, &input)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if input == nil {
		input = map[string]any{}
	}
	if source, ok := input["input_task_local_id"].(string); ok && source != "" {
		original := source
		for old, next := range clones {
			if next == source {
				original = old
				break
			}
		}
		var prior TemplateSubject
		err = completionRecord(ctx, tx, "template_subject", structureID("task-", wave.ID, original), &prior)
		if err == nil && prior.Digest != digest {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if errors.Is(err, sql.ErrNoRows) {
			if oldKey, ok := input["expansion_key"].(string); ok && oldKey != "" {
				var prior TemplateExpansion
				if err = completionRecord(ctx, tx, "template_expansion", oldKey, &prior); err != nil {
					return err
				}
				if prior.InputDigest != digest {
					return nil
				}
			}
		}
	}
	input["input_task_local_id"], input["input_digest"], input["clone_expansion_key"] = finalFix, digest, key
	if input["expansion_key"] == nil {
		input["expansion_key"] = key
	}
	return kernelRecordTx(ctx, tx, "template_task_context", id, wave.ID, input)
}

func cloneTemplateMetadataTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, oldLocal, newLocal, key string, clones map[string]string, templates []contract.FixReviewTemplate) error {
	oldID, newID := structureID("task-", wave.ID, oldLocal), structureID("task-", wave.ID, newLocal)
	var lineage templateReviewLineage
	err := completionRecord(ctx, tx, "template_review_lineage", oldID, &lineage)
	if err == nil {
		if next := clones[lineage.RootReviewLocalID]; next != "" {
			lineage.RootReviewLocalID = next
		}
		definitions := []contract.FixReviewTemplate{}
		for _, t := range templates {
			if t.InputTaskID == lineage.RootReviewLocalID {
				definitions = append(definitions, t)
			}
		}
		// A clone is a fresh identity for the same authorized pending step,
		// retaining its round and origin rather than resetting repeat limits.
		lineage.TemplateDigest, err = contentDigest(definitions)
		if err != nil {
			return err
		}
		if err = kernelRecordTx(ctx, tx, "template_review_lineage", newID, wave.ID, lineage); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var input map[string]any
	err = completionRecord(ctx, tx, "template_task_context", oldID, &input)
	if err == nil {
		for _, field := range []string{"previous_fix_local_id", "input_task_local_id"} {
			if local, ok := input[field].(string); ok && clones[local] != "" {
				input[field] = clones[local]
			}
		}
		input["clone_expansion_key"] = key
		if err = kernelRecordTx(ctx, tx, "template_task_context", newID, wave.ID, input); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return nil
}

func templateOwnerUsesTx(ctx context.Context, tx *sql.Tx, waveID, templateID string) (int, error) {
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM records WHERE kind='template_expansion' AND work_item_id=?", waveID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	used := 0
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return 0, err
		}
		var prior TemplateExpansion
		if err = json.Unmarshal(raw, &prior); err != nil {
			return 0, err
		}
		for _, id := range prior.TemplateIDs {
			if id == templateID {
				used++
				break
			}
		}
	}
	return used, rows.Err()
}

func reviewSpecsThrough(plan contract.PlanProposal, position int) []contract.TaskSpec {
	reviews := []contract.TaskSpec{}
	for _, phase := range plan.Phases[:position+1] {
		for _, task := range phase.Tasks {
			if task.Kind == "review" {
				reviews = append(reviews, task)
			}
		}
	}
	return reviews
}
