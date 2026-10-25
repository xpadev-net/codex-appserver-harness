package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

// ProductTemplateBinding identifies the old reports and every replacement role.
// The immutable expansion history authenticates this structure with the plan.
type ProductTemplateBinding struct {
	FutureClones  map[string]string                `json:"future_clones,omitempty"`
	WorkItemID    string                           `json:"work_item_id"`
	Candidate     runner.Candidate                 `json:"candidate"`
	SourceReviews map[string]string                `json:"source_reviews"`
	Verification  ProductVerificationPlanReference `json:"verification"`
	MachineDigest string                           `json:"machine_digest"`
	Repairs       []string                         `json:"repairs"`
	Verifier      string                           `json:"verifier"`
	Replacements  map[string][]string              `json:"replacements"`
}

type productTemplateOwner struct {
	Template contract.FixReviewTemplate
	Root     string
	Kind     string
	Findings []string
}

func productTemplateRoot(ctx context.Context, q queryer, task domain.Task) (templateReviewLineage, error) {
	lineage := templateReviewLineage{RootReviewLocalID: task.LocalID, ReviewRoleID: "root:" + task.LocalID}
	err := taskEvidenceRecord(ctx, q, "product_template_review_lineage", task.ID, &lineage)
	if errors.Is(err, sql.ErrNoRows) {
		return templateReviewLineage{RootReviewLocalID: task.LocalID, ReviewRoleID: "root:" + task.LocalID}, nil
	}
	return lineage, err
}

func (k *KernelStore) productTemplateExpansionTx(ctx context.Context, q queryer, key string) (saved TemplateExpansion, err error) {
	var history TemplateExpansion
	if err := taskEvidenceRecord(ctx, q, "template_expansion", key, &saved); err != nil {
		return saved, err
	}
	defer func() {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrTaskEvidence
		}
	}()
	if saved.Product == nil || saved.Key != key || saved.Admission.Mode != "template" || saved.Admission.Key != key || len(saved.Product.Repairs) == 0 || saved.Product.Verifier == "" || len(saved.Product.Replacements) == 0 {
		return saved, ErrTaskEvidence
	}
	digest, err := contentDigest(saved.Admission.Plan)
	if err != nil {
		return saved, err
	}
	if digest != saved.Admission.Digest {
		return saved, ErrTaskEvidence
	}
	if err := taskEvidenceRecord(ctx, q, "product_template_expansion_history", key, &history); err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(saved, history) {
		return saved, ErrTaskEvidence
	}
	for id, digest := range saved.Product.SourceReviews {
		report, err := k.productTaskReviewTx(ctx, q, id)
		if err != nil {
			return saved, err
		}
		if report.Digest != digest || report.Value.Candidate != saved.Product.Candidate || report.Value.Target.WaveID != saved.Request.Target.WaveID || report.Value.Target.ScopeRevision != saved.Request.Target.ScopeRevision || report.Value.Target.PolicyRevision != saved.Request.Target.PolicyRevision {
			return saved, ErrTaskEvidence
		}
		subject, err := loadProductSubject(ctx, q, report.Value.TaskID)
		if err != nil {
			return saved, err
		}
		if subject.Value.WorkItemID != saved.Product.WorkItemID {
			return saved, ErrTaskEvidence
		}
	}
	return saved, nil
}

func (k *KernelStore) expandProductTemplateTx(ctx context.Context, tx *sql.Tx, w domain.Wave, request TemplateExpansionRequest, requestKey string, automatic bool) (TemplateExpansion, error) {
	var receipt TemplateExpansion
	if automatic {
		if err := idleTemplateParentTx(ctx, tx, w); err != nil {
			return receipt, err
		}
	}
	var raw []byte
	var planDigest string
	if err := tx.QueryRowContext(ctx, "SELECT digest,payload FROM kernel_plans WHERE wave_id=? AND revision=?", w.ID, w.PlanRevision).Scan(&planDigest, &raw); err != nil {
		return receipt, err
	}
	var current PlanAdmission
	if err := json.Unmarshal(raw, &current); err != nil {
		return receipt, err
	}
	actual, err := contentDigest(current.Plan)
	if err != nil {
		return receipt, err
	}
	if current.Target != w.Target() || current.Digest != planDigest || actual != planDigest || current.Plan.Validate() != nil {
		return receipt, ErrTaskEvidence
	}
	source, err := loadTask(ctx, tx, request.ReviewTaskID)
	if err != nil {
		return receipt, err
	}
	if !UsesProductReviewOutput(w, source) {
		return receipt, fmt.Errorf("%w: single-WorkItem product reviewer required", ErrTemplateNeedsMain)
	}
	input, err := k.taskResultInputTx(ctx, tx, w, source.ResultID)
	if err != nil {
		return receipt, err
	}
	if input.ProductReview == nil || input.ProductReview.Value.Review.Verdict != "findings" {
		return receipt, fmt.Errorf("%w: no bound product findings", ErrTemplateNeedsMain)
	}
	workID := source.Spec.WorkItemIDs[0]
	p, err := loadProductTargetTx(ctx, tx, w.Target(), workID)
	if err != nil {
		return receipt, err
	}
	var sourcePermit productExecutionPermit
	held, err := k.productWorkItemExecutionPermitTx(ctx, tx, w, workID, &sourcePermit)
	if err != nil {
		return receipt, err
	}
	if held || p.Value.Candidate != input.ProductReview.Value.Candidate {
		return receipt, ErrProductSubjectHeld
	}
	position := -1
	for i, phase := range current.Plan.Phases {
		if structureID("phase-", w.ID, phase.ID) == source.PhaseID {
			position = i
		}
	}
	if position < 0 {
		return receipt, ErrTaskEvidence
	}
	// Prior barriers must be complete; later consumers must be entirely unused.
	for index, phase := range current.Plan.Phases {
		for _, spec := range phase.Tasks {
			task, err := loadTask(ctx, tx, structureID("task-", w.ID, spec.ID))
			if err != nil {
				return receipt, err
			}
			if index > position {
				if err := k.productFutureTaskUnusedTx(ctx, tx, w, task); err != nil {
					return receipt, err
				}
				continue
			}
			if _, err := k.completedTaskResultTx(ctx, tx, w, task); err != nil {
				return receipt, fmt.Errorf("%w: required source Phase incomplete", ErrTemplateNeedsMain)
			}
			if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
				return receipt, err
			}
			digest, err := k.taskInputDigestTx(ctx, tx, w, task.Spec)
			if err != nil {
				return receipt, err
			}
			if digest != task.InputDigest {
				return receipt, ErrTaskEvidence
			}
		}
	}
	subject, err := loadProductSubject(ctx, tx, source.ID)
	if err != nil {
		return receipt, err
	}
	var verificationSource *ProductTaskCandidateSnapshot
	for i := range subject.Value.Sources {
		s := &subject.Value.Sources[i]
		if s.Value.Candidate == p.Value.Candidate && s.Value.VerificationPlan != nil {
			if verificationSource != nil && verificationSource.Value.VerificationPlan.ID != s.Value.VerificationPlan.ID {
				return receipt, ErrTaskEvidence
			}
			verificationSource = s
		}
	}
	if verificationSource == nil {
		return receipt, fmt.Errorf("%w: final canonical verification source required", ErrTemplateNeedsMain)
	}
	machine, err := k.productVerificationEvidenceTx(ctx, tx, verificationSource.Value.VerificationPlan.ID)
	if err != nil {
		return receipt, err
	}
	if !machine.Value.Complete {
		return receipt, ErrProductRunnerHeld
	}
	for _, job := range machine.Value.Jobs {
		if job.Completion.Result.Status != "passed" && job.Completion.Result.Status != "failed" {
			return receipt, fmt.Errorf("%w: terminal runner result %q requires parent decision", ErrTemplateNeedsMain, job.Completion.Result.Status)
		}
	}
	verificationTask, err := loadTask(ctx, tx, verificationSource.Value.TaskID)
	if err != nil {
		return receipt, err
	}
	reviews := []domain.Task{}
	reports := map[string]ProductTaskReviewSnapshot{}
	owners := map[string]*productTemplateOwner{}
	round := 1
	for _, spec := range reviewSpecsThrough(current.Plan, position) {
		if spec.Kind != "review" || !slices.Equal(spec.WorkItemIDs, []string{workID}) {
			continue
		}
		task, err := loadTask(ctx, tx, structureID("task-", w.ID, spec.ID))
		if err != nil {
			return receipt, err
		}
		in, err := k.taskResultInputTx(ctx, tx, w, task.ResultID)
		if err != nil {
			return receipt, err
		}
		if in.ProductReview == nil || in.ProductReview.Value.Candidate != p.Value.Candidate {
			continue
		}
		lineage, err := productTemplateRoot(ctx, tx, task)
		if err != nil {
			return receipt, err
		}
		if lineage.Round+1 > round {
			round = lineage.Round + 1
		}
		reviews = append(reviews, task)
		reports[task.ID] = *in.ProductReview
		if in.Result.Review.Verdict != "findings" {
			continue
		}
		definitions := []contract.FixReviewTemplate{}
		for _, template := range current.Plan.Templates {
			if template.InputTaskID == lineage.RootReviewLocalID {
				definitions = append(definitions, template)
			}
		}
		digest, err := contentDigest(definitions)
		if err != nil {
			return receipt, err
		}
		if lineage.TemplateDigest != "" && lineage.TemplateDigest != digest {
			return receipt, fmt.Errorf("%w: saved template authority changed", ErrTemplateNeedsMain)
		}
		for _, finding := range in.Result.Review.Findings {
			kind := ""
			switch finding.Classification {
			case "implementation_defect":
				kind = "fix"
			case "test_defect":
				kind = "verification"
			default:
				return receipt, fmt.Errorf("%w: product finding needs parent classification", ErrTemplateNeedsMain)
			}
			matches := []contract.FixReviewTemplate{}
			for _, template := range definitions {
				if slices.Contains(template.Classifications, finding.Classification) {
					matches = append(matches, template)
				}
			}
			if len(matches) != 1 {
				return receipt, fmt.Errorf("%w: unsupported or ambiguous saved template", ErrTemplateNeedsMain)
			}
			template := matches[0]
			key := template.ID + ":" + kind
			owner := owners[key]
			if owner == nil {
				owner = &productTemplateOwner{Template: template, Root: lineage.RootReviewLocalID, Kind: kind}
				owners[key] = owner
			}
			owner.Findings = append(owner.Findings, task.ID+":"+finding.ID)
		}
	}
	if len(owners) == 0 {
		return receipt, fmt.Errorf("%w: no actionable saved product template", ErrTemplateNeedsMain)
	}
	if round > w.Policy.Limits.FixRounds || w.FixRounds >= w.Policy.Limits.FixRounds {
		return receipt, fmt.Errorf("%w: cumulative fix limit", ErrTemplateNeedsMain)
	}
	ownerList := []productTemplateOwner{}
	usedTemplates := map[string]bool{}
	for _, owner := range owners {
		uses, err := templateOwnerUsesTx(ctx, tx, w.ID, owner.Template.ID)
		if err != nil {
			return receipt, err
		}
		if uses >= owner.Template.MaxRounds {
			return receipt, fmt.Errorf("%w: saved template repeat limit", ErrTemplateNeedsMain)
		}
		sort.Strings(owner.Findings)
		ownerList = append(ownerList, *owner)
		usedTemplates[owner.Template.ID] = true
	}
	sort.Slice(ownerList, func(i, j int) bool {
		if ownerList[i].Kind != ownerList[j].Kind {
			return ownerList[i].Kind == "fix"
		}
		return ownerList[i].Template.ID < ownerList[j].Template.ID
	})
	sort.Slice(reviews, func(i, j int) bool { return reviews[i].ID < reviews[j].ID })
	reportDigests := map[string]string{}
	sourceResults := []string{}
	for _, task := range reviews {
		report := reports[task.ID]
		reportDigests[task.ResultID] = report.Digest
		sourceResults = append(sourceResults, task.ResultID)
	}
	sort.Strings(sourceResults)
	batch, err := contentDigest(reportDigests)
	if err != nil {
		return receipt, err
	}
	identity, err := contentDigest(struct {
		Wave                 string
		Scope, Policy, Round int
		Candidate            runner.Candidate
		Batch                string
		Owners               []productTemplateOwner
	}{w.ID, w.ScopeRevision, w.Policy.Revision, round, p.Value.Candidate, batch, ownerList})
	if err != nil {
		return receipt, err
	}
	key := "product-template:" + identity
	if saved, err := k.productTemplateExpansionTx(ctx, tx, key); err == nil {
		if err := kernelRecordTx(ctx, tx, "template_expansion_request", requestKey, w.ID, saved); err != nil {
			return receipt, err
		}
		return saved, tx.Commit()
	} else if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	binding := &ProductTemplateBinding{WorkItemID: workID, Candidate: p.Value.Candidate, SourceReviews: reportDigests, Verification: *verificationSource.Value.VerificationPlan, MachineDigest: machine.Digest, Repairs: []string{}, Replacements: map[string][]string{}}
	plan, lineages, err := k.buildProductTemplatePlanTx(ctx, tx, w, current.Plan, position, key, sourceResults, reviews, reports, ownerList, verificationTask, binding)
	if err != nil {
		return receipt, err
	}
	if err := plan.Validate(); err != nil {
		return receipt, fmt.Errorf("%w: generated product plan: %v", ErrTemplateNeedsMain, err)
	}
	admission, err := k.adoptPlanModeTx(ctx, tx, w.Target(), key, plan, "template")
	if err != nil {
		return receipt, err
	}
	// This trusted repair admission explicitly carries the existing product
	// projection into its new plan. Plain parent plan adoption does not do so.
	projected := p.Value
	projected.Target = admission.Target
	if _, err := k.setProductTargetTx(ctx, tx, p.Digest, projected); err != nil {
		return receipt, err
	}
	updated, err := loadWave(ctx, tx, w.ID)
	if err != nil {
		return receipt, err
	}
	updated.FixRounds++
	if err := saveWave(ctx, tx, updated); err != nil {
		return receipt, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'fix_round',1,0,?)", w.ID, key, []byte(`{}`)); err != nil {
		return receipt, err
	}
	for local, lineage := range lineages {
		definitions := []contract.FixReviewTemplate{}
		for _, template := range plan.Templates {
			if template.InputTaskID == lineage.RootReviewLocalID {
				definitions = append(definitions, template)
			}
		}
		lineage.TemplateDigest, err = contentDigest(definitions)
		if err != nil {
			return receipt, err
		}
		if lineage.Round < round {
			lineage.Round = round
		}
		lineage.ExpansionKey = key
		if err := kernelRecordTx(ctx, tx, "product_template_review_lineage", structureID("task-", w.ID, local), w.ID, lineage); err != nil {
			return receipt, err
		}
	}
	templateIDs := []string{}
	for id := range usedTemplates {
		templateIDs = append(templateIDs, id)
	}
	sort.Strings(templateIDs)
	receipt = TemplateExpansion{Product: binding, Request: request, Key: key, Round: round, InputDigest: input.ProductReview.Value.Review.TargetDigest, FindingBatchID: batch, SourceResultIDs: sourceResults, TemplateIDs: templateIDs, Admission: admission}
	for _, kind := range []string{"template_expansion", "product_template_expansion_history"} {
		if err := kernelRecordTx(ctx, tx, kind, key, w.ID, receipt); err != nil {
			return receipt, err
		}
	}
	for _, task := range reviews {
		alias, err := contentDigest(TemplateExpansionRequest{Target: request.Target, ReviewTaskID: task.ID})
		if err != nil {
			return receipt, err
		}
		if err := kernelRecordTx(ctx, tx, "template_expansion_request", alias, w.ID, receipt); err != nil {
			return receipt, err
		}
	}
	if err := k.followTemplateTerminalWaitsTx(ctx, tx, w, receipt); err != nil {
		return receipt, err
	}
	if !sourcePermit.current(k, tx) {
		return receipt, ErrProductSubjectHeld
	}
	return receipt, tx.Commit()
}
