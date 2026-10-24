package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

const maxInputManifest = 32 << 20

var ErrInputManifestLimit = errors.New("session input manifest exceeds the storage bound")

// InputReference identifies immutable controller bytes. A digest/size is never
// an artifact grant or permission to read another session's saved input.
type InputReference struct {
	ID      string          `json:"id"`
	Target  contract.Target `json:"target"`
	Section string          `json:"section"`
	Digest  string          `json:"digest"`
	Bytes   int64           `json:"bytes"`
}
type WaveInput struct {
	Target          contract.Target  `json:"target"`
	State           domain.WaveState `json:"state"`
	Paused          bool             `json:"paused"`
	RequestPreview  string           `json:"request_preview"`
	PreviewComplete bool             `json:"preview_complete"`
	Manifest        InputReference   `json:"manifest"`
	Budget          WaveRunBudget    `json:"budget"`
}
type SessionContext struct {
	Waves        []WaveInput `json:"waves"`
	Instructions string      `json:"instructions"`
}
type savedInputManifest struct {
	SessionID string         `json:"session_id"`
	Reference InputReference `json:"reference"`
	Data      []byte         `json:"data"`
}
type inputReferenceGrant struct {
	SessionID  string           `json:"session_id"`
	References []InputReference `json:"references"`
}
type inputSubscription struct {
	ID         string          `json:"id"`
	Target     contract.Target `json:"target"`
	Resource   string          `json:"resource"`
	Cursor     int64           `json:"cursor"`
	Definition json.RawMessage `json:"definition"`
}
type appliedMainDecision struct {
	RunID    string                `json:"run_id"`
	Decision contract.MainDecision `json:"decision"`
}
type sessionManifest struct {
	ProductSourceRequest                  *ProductSourceRequestAdoption    `json:"product_source_request,omitempty"`
	UnavailableProductSourceRequest       string                           `json:"unavailable_product_source_request,omitempty"`
	ProductContinuations                  []ProductContinuationInput       `json:"product_continuations,omitempty"`
	UnavailableProductContinuations       []UnavailableProductWorkItem     `json:"unavailable_product_continuations,omitempty"`
	ProductMachineVerification            *ProductMachineVerificationInput `json:"product_machine_verification,omitempty"`
	UnavailableProductMachineVerification string                           `json:"unavailable_product_machine_verification,omitempty"`

	ProductFinalVerification            *ProductFinalVerificationEvidence `json:"product_final_verification,omitempty"`
	UnavailableProductFinalVerification string                            `json:"unavailable_product_final_verification,omitempty"`

	ProductMergeCoverage        *ProductCoverageLedger                     `json:"product_merge_coverage,omitempty"`
	UnavailableProductCoverage  string                                     `json:"unavailable_product_coverage,omitempty"`
	ProductVerificationPolicy   *ProductVerificationPolicyInput            `json:"product_verification_policy,omitempty"`
	ProductSubject              *ProductCandidateSubjectSnapshot           `json:"product_subject,omitempty"`
	ProductSubjects             map[string]ProductCandidateSubjectSnapshot `json:"product_subjects,omitempty"`
	ProductWorkspace            *ProductWorkspaceInput                     `json:"product_workspace,omitempty"`
	ProductWorkItems            []ProductWorkItemRegistrationSnapshot      `json:"product_work_items,omitempty"`
	UnavailableProductWorkItems []UnavailableProductWorkItem               `json:"unavailable_product_work_items,omitempty"`
	SchemaVersion               int                                        `json:"schema_version"`
	SessionID                   string                                     `json:"session_id"`
	Kind                        string                                     `json:"kind"`
	Wave                        domain.Wave                                `json:"wave"`
	Plan                        *PlanAdmission                             `json:"plan"`
	Tasks                       []domain.Task                              `json:"tasks"`
	Results                     map[string]contract.TaskResult             `json:"results"`
	SourceResults               map[string]TaskResultInput                 `json:"source_results"`
	UnavailableSourceResults    []unavailableSourceResult                  `json:"unavailable_source_results"`
	UnavailableResults          []unavailableInputResult                   `json:"unavailable_results"`
	TaskApplications            []ResultApplication                        `json:"task_applications"`
	TemplateContexts            map[string]json.RawMessage                 `json:"template_contexts"`
	TemplateSubjects            map[string]TemplateSubject                 `json:"template_subjects"`
	MainDecisions               []appliedMainDecision                      `json:"main_decisions"`
	Artifacts                   []domain.Artifact                          `json:"artifacts"`
	UnavailableArtifacts        []unavailableInputArtifact                 `json:"unavailable_artifacts"`
	UnavailableTaskMetadata     []unavailableTaskMetadata                  `json:"unavailable_task_metadata"`
	Subscriptions               []inputSubscription                        `json:"subscriptions"`
	Direction                   *WaveDirection                             `json:"direction"`
	Budget                      WaveRunBudget                              `json:"budget"`
}

type unavailableInputResult struct {
	TaskID   string `json:"task_id"`
	ResultID string `json:"result_id"`
	RunID    string `json:"run_id"`
	Reason   string `json:"reason"`
}

type unavailableInputArtifact struct {
	ArtifactID string `json:"artifact_id"`
	Reason     string `json:"reason"`
}

type unavailableTaskMetadata struct {
	TaskID string `json:"task_id"`
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

type unavailableSourceResult struct {
	TaskID   string `json:"task_id"`
	ResultID string `json:"result_id"`
	Reason   string `json:"reason"`
}

// BuildSessionInput captures one consistent DB view, with no model/process or
// execution admission. Only the bounded envelope goes to turn/start; the full
// role-filtered state and read grants commit with the immutable snapshot.
func (k *KernelStore) BuildSessionInput(ctx context.Context, sessionID string, targets []contract.Target) (contract.InputSnapshot, error) {
	var snapshot contract.InputSnapshot
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return snapshot, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return snapshot, err
	}
	targets = append([]contract.Target{}, targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].WaveID < targets[j].WaveID })
	state, refs, err := k.sessionContextTx(ctx, tx, b, targets)
	if err != nil {
		return snapshot, err
	}
	raw, err := encodeWakeJSON(struct {
		SchemaVersion int               `json:"schema_version"`
		SessionID     string            `json:"session_id"`
		Targets       []contract.Target `json:"targets"`
		Context       SessionContext    `json:"context"`
	}{1, b.ID, targets, state})
	if err != nil {
		return snapshot, err
	}
	if err = contract.ValidateSessionInput(raw); err != nil {
		return snapshot, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	snapshot = contract.InputSnapshot{ID: "input-" + digest, Digest: digest, Input: raw}
	if err = saveInputSnapshotTx(ctx, tx, snapshot); err != nil {
		return snapshot, err
	}
	if err = saveInputGrantsTx(ctx, tx, b.ID, snapshot.ID, refs); err != nil {
		return snapshot, err
	}
	return snapshot, tx.Commit()
}

func saveInputSnapshotTx(ctx context.Context, tx *sql.Tx, s contract.InputSnapshot) error {
	var digest string
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", s.ID).Scan(&digest, &raw)
	if err == nil {
		if digest != s.Digest || string(raw) != string(s.Input) {
			return errors.New("snapshot identity conflict")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO kernel_snapshots(id,digest,data) VALUES(?,?,?)", s.ID, s.Digest, s.Input)
	return err
}
func saveInputGrantsTx(ctx context.Context, tx *sql.Tx, sessionID, snapshotID string, refs []InputReference) error {
	grant := inputReferenceGrant{SessionID: sessionID, References: refs}
	var prior inputReferenceGrant
	err := completionRecord(ctx, tx, "session_input_references", snapshotID, &prior)
	if err == nil {
		if !reflect.DeepEqual(prior, grant) {
			return errors.New("input reference grant conflict")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return kernelRecordTx(ctx, tx, "session_input_references", snapshotID, sessionID, grant)
}
func (k *KernelStore) sessionContextTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, targets []contract.Target) (SessionContext, []InputReference, error) {
	state := SessionContext{Waves: []WaveInput{}, Instructions: sessionInstructions(b.Kind)}
	refs := []InputReference{}
	if b.Closed || len(targets) == 0 || len(targets) > contract.MaxMainWaves || b.Kind != "orchestrator" && b.Kind != "task" || b.Kind == "task" && (len(targets) != 1 || targets[0] != b.Target) {
		return state, refs, errors.New("invalid session input targets")
	}
	seen := map[string]bool{}
	for _, target := range targets {
		if seen[target.WaveID] {
			return state, refs, errors.New("duplicate input Wave")
		}
		seen[target.WaveID] = true
		_, w, err := artifactAuthorityTx(ctx, tx, b.ID, target)
		if err != nil {
			return state, refs, err
		}
		if err = saveWaveToolScopeTx(ctx, tx, w); err != nil {
			return state, refs, err
		}
		manifest, err := k.sessionManifestTx(ctx, tx, b, w)
		if err != nil {
			return state, refs, err
		}
		if b.Kind == "orchestrator" && w.Policy.Stage == "P" && w.Policy.Product {
			state.Instructions += " A product_base_merge_conflict parent notification contains a stopped conflict reference in product_preparation.conflict. To repair it, plan a fresh fix Task for the same WorkItem with product_base_conflict copied from that reference; never reuse the canceled source Task/session. Preserve budgets and required independent verification/review after the new candidate. The reference alone grants no model, Git or publication permission."
		}
		if manifest.ProductWorkspace != nil {
			if manifest.ProductSubject != nil && manifest.ProductSubject.Value.Spec.Kind == "verification" {
				state.Instructions += " For this independent verification assignment use TaskResult schema_version=3. Modify only the trusted product_verification_policy.verifier_paths. For outcome=result copy product_workspace.reference into product_changes. Set product_verification.subject_id=product_subject.id and product_verification.subject_digest=product_subject.digest, never product_subject.value.candidate_digest. Set plan_artifact_id naming your published verification.Plan against the input Candidate. Publish the owned plan with kind verification_plan and content_type application/json, and declare it in artifacts. Do not claim a new SHA, runner evidence, review or approval. Keep work_item_refs empty and review/verification null; waiting/blocked use empty product_changes and null product_verification."
			} else {
				state.Instructions += " For this product work/fix assignment use TaskResult schema_version=2. For outcome=result copy the product_workspace.reference from this saved manifest into product_changes. For waiting/blocked return an empty product_changes array. Keep work_item_refs empty and review/verification null. The controller owns Git commits, Candidate IDs/SHA and quality/approval gates after normal completion and physical stopping. Publish actual notes/evidence with artifact_publish when needed."
				if manifest.ProductWorkspace.Conflict != nil {
					state.Instructions += " product_workspace.conflict describes the fixed pending base merge. Resolve the files against the original requirements, preserving both intended changes. Git metadata is read-only; the controller stages and commits after your normal stopped result. Do not reset, abort, stage, commit, or treat the conflict summary as verification/approval. If resolution cannot be justified, report waiting/blocked with evidence."
				}
			}
		}
		if manifest.ProductSubject != nil {
			state.Instructions += " product_subject fixes an immutable Candidate from declared applied producer results. Its value.candidate_digest identifies the exact code/base/scope/policy subject. Original requirements and assigned criteria remain the basis for independent evaluation. This subject is data, not a write/dispatch/runner/approval grant."
			if manifest.ProductSubject.Value.Spec.Kind == "review" {
				state.Instructions += " For this product review use TaskResult schema_version=1 and set review.target_digest to product_subject.value.candidate_digest. Keep work_item_refs empty and verification null. product_machine_verification contains authenticated host runner observations for this exact subject; prior Task claims are not current runner status. Report clean/findings/inconclusive only about that fixed subject; the report is not formal approval."
			}
		}
		if manifest.ProductVerificationPolicy != nil {
			state.Instructions += " product_verification_policy fixes trusted test-write paths and registered/required profile IDs. A verification.Plan must refer to the known input Candidate and original assigned criteria; it cannot remove required profiles or supply runner success. Full trusted execution argv stays outside this manifest."
		}
		raw, err := encodeWakeJSON(manifest)
		if err != nil {
			return state, refs, err
		}
		if len(raw) > maxInputManifest {
			return state, refs, ErrInputManifestLimit
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(raw))
		ref := InputReference{ID: structureID("input-manifest-", b.ID, digest), Target: target, Section: "manifest", Digest: digest, Bytes: int64(len(raw))}
		saved := savedInputManifest{SessionID: b.ID, Reference: ref, Data: raw}
		var prior savedInputManifest
		err = completionRecord(ctx, tx, "session_input_manifest", ref.ID, &prior)
		if err == nil {
			if prior.SessionID != b.ID || prior.Reference != ref || string(prior.Data) != string(raw) {
				return state, refs, errors.New("input manifest identity conflict")
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			if err = kernelRecordTx(ctx, tx, "session_input_manifest", ref.ID, b.ID, saved); err != nil {
				return state, refs, err
			}
		} else {
			return state, refs, err
		}
		end := min(len(w.OriginalRequest), 256)
		for end > 0 && end < len(w.OriginalRequest) && !utf8.RuneStart(w.OriginalRequest[end]) {
			end--
		}
		state.Waves = append(state.Waves, WaveInput{Target: target, State: w.State, Paused: w.DispatchPaused(), RequestPreview: w.OriginalRequest[:end], PreviewComplete: end == len(w.OriginalRequest), Manifest: ref, Budget: manifest.Budget})
		refs = append(refs, ref)
	}
	return state, refs, nil
}

func (k *KernelStore) inputManifestTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, r contract.ContextReadRequest) ([]byte, string, contract.Target, error) {
	var snapshotID, runID string
	if err := tx.QueryRowContext(ctx, "SELECT r.snapshot_id,r.run_id FROM kernel_sessions s JOIN kernel_run_bindings r ON r.run_id=s.active_run_id AND r.session_id=s.id WHERE s.id=? AND r.applied=0", b.ID).Scan(&snapshotID, &runID); err != nil {
		return nil, "", contract.Target{}, err
	}
	var grant inputReferenceGrant
	if err := completionRecord(ctx, tx, "session_input_references", snapshotID, &grant); err != nil {
		return nil, "", contract.Target{}, err
	}
	var effective []contract.Target
	var budget RunBudgetBinding
	if b.Kind == "orchestrator" {
		if err := completionRecord(ctx, tx, "run_budget", runID, &budget); err != nil {
			return nil, "", contract.Target{}, err
		}
		if budget.SessionID != b.ID || budget.ThreadID != b.ThreadID || budget.SnapshotID != snapshotID {
			return nil, "", contract.Target{}, errors.New("manifest scope budget conflict")
		}
		var err error
		effective, err = effectiveRunTargetsTx(ctx, tx, b, runID, snapshotID, budget.Targets)
		if err != nil {
			return nil, "", contract.Target{}, err
		}
	}
	var ref InputReference
	found := false
	for _, candidate := range grant.References {
		authorized := candidate.Target == r.Target
		if b.Kind == "orchestrator" && candidate.Target != r.Target {
			original := false
			for _, target := range budget.Targets {
				original = original || target == candidate.Target
			}
			current := false
			for _, target := range effective {
				current = current || target == r.Target && target.WaveID == candidate.Target.WaveID
			}
			authorized = original && current
		}
		if candidate.ID == r.ReferenceID && authorized {
			ref = candidate
			found = true
			break
		}
	}
	if grant.SessionID != b.ID || !found {
		return nil, "", contract.Target{}, errors.New("manifest not granted by the active run snapshot")
	}
	if data, ok := k.inputCache.get(ref); ok {
		return data, ref.Digest, ref.Target, nil
	}
	var saved savedInputManifest
	if err := completionRecord(ctx, tx, "session_input_manifest", ref.ID, &saved); err != nil {
		return nil, "", contract.Target{}, err
	}
	if saved.SessionID != b.ID || saved.Reference != ref || int64(len(saved.Data)) != ref.Bytes || fmt.Sprintf("%x", sha256.Sum256(saved.Data)) != ref.Digest {
		return nil, "", contract.Target{}, errors.New("input manifest integrity conflict")
	}
	k.inputCache.put(ref, saved.Data)
	return saved.Data, ref.Digest, ref.Target, nil
}

func (k *KernelStore) sessionManifestTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, w domain.Wave) (sessionManifest, error) {
	m := sessionManifest{SchemaVersion: 1, SessionID: b.ID, Kind: b.Kind, Wave: w, Tasks: []domain.Task{}, Results: map[string]contract.TaskResult{}, SourceResults: map[string]TaskResultInput{}, UnavailableSourceResults: []unavailableSourceResult{}, UnavailableResults: []unavailableInputResult{}, TaskApplications: []ResultApplication{}, MainDecisions: []appliedMainDecision{}, TemplateContexts: map[string]json.RawMessage{}, TemplateSubjects: map[string]TemplateSubject{}, Artifacts: []domain.Artifact{}, UnavailableArtifacts: []unavailableInputArtifact{}, UnavailableTaskMetadata: []unavailableTaskMetadata{}, Subscriptions: []inputSubscription{}}
	tokens, unknown, saturated, err := waveTokensTx(ctx, tx, w.ID)
	if err != nil {
		return m, err
	}
	m.Budget = WaveRunBudget{WaveID: w.ID, ObservedTokens: tokens, UnobservedRuns: unknown, Saturated: saturated, TokenLimit: w.Policy.Limits.Tokens, MainTurns: w.MainTurns, MainTurnLimit: w.Policy.Limits.MainTurns, Deadline: w.Deadline, DeadlineExceeded: !k.clock().Before(w.Deadline)}
	m.ProductSourceRequest, err = k.productSourceRequestTx(ctx, tx, w)
	if readinessHoldError(err) {
		m.UnavailableProductSourceRequest = "source_request_evidence_unavailable"
	} else if err != nil {
		return m, err
	}
	m.ProductWorkItems, m.UnavailableProductWorkItems, err = k.productWorkItemInputsTx(ctx, tx, b, w)
	if err != nil {
		return m, err
	}
	seenContinuations := map[string]bool{}
	for _, registration := range m.ProductWorkItems {
		lineage, e := k.productContinuationInputsTx(ctx, tx, w, registration.Value.Definition.ID)
		if readinessHoldError(e) || errors.Is(e, ErrProductContinuationHeld) {
			m.UnavailableProductContinuations = append(m.UnavailableProductContinuations, UnavailableProductWorkItem{ID: registration.Value.Definition.ID, Reason: "continuation_evidence_unavailable"})
			continue
		}
		if e != nil {
			return m, e
		}
		for _, node := range lineage {
			if !seenContinuations[node.ID] {
				m.ProductContinuations = append(m.ProductContinuations, productContinuationInput(node))
				seenContinuations[node.ID] = true
			}
		}
	}
	if b.Kind == "orchestrator" && w.Policy.Stage == "P" && w.Policy.Product {
		coverage, err := k.productCoverageTx(ctx, tx, w)
		if errors.Is(err, ErrProductCoverageHeld) {
			m.UnavailableProductCoverage = "coverage_metadata_exceeds_bound"
		} else if err != nil {
			return m, err
		} else {
			m.ProductMergeCoverage = &coverage
		}
		final, err := k.productFinalVerificationEvidenceTx(ctx, tx, w)
		if errors.Is(err, ErrProductFinalEvidenceBound) {
			m.UnavailableProductFinalVerification = "final_verification_metadata_exceeds_bound"
		} else if err != nil {
			return m, err
		} else {
			m.ProductFinalVerification = &final
		}
	}
	m.ProductWorkspace, err = k.productWorkspaceInputTx(ctx, tx, b, w)
	if err != nil {
		return m, err
	}
	allowed := map[string]bool{}
	if b.Kind == "task" {
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return m, err
		}
		allowed[task.ID] = true
		for _, dep := range task.Spec.DependsOn {
			allowed[structureID("task-", w.ID, dep.TaskID)] = true
		}
	} else {
		var raw []byte
		err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_plans WHERE wave_id=? AND revision=?", w.ID, w.PlanRevision).Scan(&raw)
		if err == nil {
			m.Plan = &PlanAdmission{}
			if err = json.Unmarshal(raw, m.Plan); err != nil {
				return m, err
			}
			if m.Plan.Target != w.Target() {
				m.Plan = nil
			} else if digest, digestErr := contentDigest(m.Plan.Plan); digestErr != nil || digest != m.Plan.Digest {
				return m, errors.New("input plan digest conflict")
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return m, err
		}
	}
	ids, err := inputIDsTx(ctx, tx, "SELECT t.id FROM kernel_tasks t JOIN kernel_plan_bindings p ON p.task_id=t.id WHERE p.wave_id=? AND p.revision=? ORDER BY t.id", w.ID, w.PlanRevision)
	if err != nil {
		return m, err
	}
	for _, id := range ids {
		if b.Kind == "task" && !allowed[id] {
			continue
		}
		task, err := loadTask(ctx, tx, id)
		if err != nil {
			return m, err
		}
		if task.ScopeRevision != w.ScopeRevision || task.PolicyRevision != w.Policy.Revision {
			continue
		}
		m.Tasks = append(m.Tasks, task)
		if b.Kind == "task" && b.TaskID == task.ID {
			verificationPolicy, err := k.productVerificationPolicyInputTx(ctx, tx, w, task)
			if err != nil {
				return m, err
			}
			m.ProductVerificationPolicy = verificationPolicy
		}
		productSubject, subjectErr := k.fixedProductSubjectInputTx(ctx, tx, w, task.Spec)
		if subjectErr != nil {
			if b.Kind != "orchestrator" || !readinessHoldError(subjectErr) {
				return m, subjectErr
			}
			m.UnavailableTaskMetadata = append(m.UnavailableTaskMetadata, unavailableTaskMetadata{TaskID: task.ID, Kind: "product_candidate_subject", Reason: "subject_evidence_unavailable"})
		} else if productSubject != nil {
			if m.ProductSubjects == nil {
				m.ProductSubjects = map[string]ProductCandidateSubjectSnapshot{}
			}
			m.ProductSubjects[task.ID] = *productSubject
			if b.Kind == "task" && b.TaskID == task.ID {
				m.ProductSubject = productSubject
				if task.Spec.Kind == "review" {
					summary, unavailable, evidenceErr := k.productReviewMachineInputTx(ctx, tx, *productSubject)
					if evidenceErr != nil {
						return m, evidenceErr
					}
					m.ProductMachineVerification = summary
					if unavailable {
						m.UnavailableProductMachineVerification = "machine_evidence_unavailable"
					}
				}
			}
		}
		var templateOwner string
		var templateContext []byte
		contextErr := tx.QueryRowContext(ctx, "SELECT work_item_id,payload FROM records WHERE kind='template_task_context' AND id=?", task.ID).Scan(&templateOwner, &templateContext)
		if contextErr == nil {
			invalid := templateOwner != w.ID || !json.Valid(templateContext)
			if b.Kind == "orchestrator" && !invalid && (task.Spec.Kind == "fix" || task.Spec.Kind == "review") {
				var input templateTaskInput
				invalid = json.Unmarshal(templateContext, &input) != nil || len(input.InputDigest) != 64 || strings.Trim(input.InputDigest, "0123456789abcdef") != ""
			}
			if invalid {
				if b.Kind != "orchestrator" {
					return m, errors.New("input template context binding conflict")
				}
				m.UnavailableTaskMetadata = append(m.UnavailableTaskMetadata, unavailableTaskMetadata{TaskID: task.ID, Kind: "template_task_context", Reason: "invalid_template_context"})
			} else {
				m.TemplateContexts[task.ID] = json.RawMessage(templateContext)
			}
		} else if !errors.Is(contextErr, sql.ErrNoRows) {
			return m, contextErr
		}
		var subject TemplateSubject
		contextErr = taskEvidenceRecord(ctx, tx, "template_subject", task.ID, &subject)
		if contextErr == nil {
			if subject.TaskID != task.ID || subject.Target != task.Target() {
				if b.Kind != "orchestrator" {
					return m, errors.New("input template subject binding conflict")
				}
				m.UnavailableTaskMetadata = append(m.UnavailableTaskMetadata, unavailableTaskMetadata{TaskID: task.ID, Kind: "template_subject", Reason: "invalid_template_subject_binding"})
			} else {
				m.TemplateSubjects[task.ID] = subject
			}
		} else if b.Kind == "orchestrator" && errors.Is(contextErr, ErrTaskEvidence) {
			m.UnavailableTaskMetadata = append(m.UnavailableTaskMetadata, unavailableTaskMetadata{TaskID: task.ID, Kind: "template_subject", Reason: "invalid_template_subject"})
		} else if !errors.Is(contextErr, sql.ErrNoRows) {
			return m, contextErr
		}

		applicationIDs, err := inputIDsTx(ctx, tx, "SELECT a.id FROM records a JOIN kernel_run_bindings r ON r.run_id=a.id WHERE a.kind='task_result_application' AND a.work_item_id=? AND r.session_id=? AND r.applied=1 ORDER BY a.rowid", task.ID, task.SessionID)
		if err != nil {
			return m, err
		}
		for _, runID := range applicationIDs {
			var application ResultApplication
			if err = completionRecord(ctx, tx, "task_result_application", runID, &application); err != nil {
				return m, err
			}
			if application.RunID != runID || application.TaskID != task.ID || application.Target != task.Target() {
				return m, errors.New("input Task application binding conflict")
			}
			m.TaskApplications = append(m.TaskApplications, application)
			if application.ResultID == "" || application.Archived {
				continue
			}
			var result contract.TaskResult
			if err = taskEvidenceRecord(ctx, tx, "task_result", application.ResultID, &result); err != nil {
				if b.Kind == "orchestrator" && (errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrTaskEvidence)) {
					reason := "invalid_result"
					if errors.Is(err, sql.ErrNoRows) {
						reason = "missing_result"
					}
					m.UnavailableResults = append(m.UnavailableResults, unavailableInputResult{TaskID: task.ID, ResultID: application.ResultID, RunID: runID, Reason: reason})
					continue
				}
				return m, err
			}
			if result.TaskID != task.ID || result.Target != task.Target() {
				if b.Kind == "orchestrator" {
					m.UnavailableResults = append(m.UnavailableResults, unavailableInputResult{TaskID: task.ID, ResultID: application.ResultID, RunID: runID, Reason: "invalid_result_binding"})
					continue
				}
				return m, errors.New("input result binding conflict")
			}
			if err = result.Validate(); err != nil {
				if b.Kind == "orchestrator" {
					m.UnavailableResults = append(m.UnavailableResults, unavailableInputResult{TaskID: task.ID, ResultID: application.ResultID, RunID: runID, Reason: "invalid_result"})
					continue
				}
				return m, fmt.Errorf("%w: %v", ErrTaskEvidence, err)
			}
			m.Results[application.ResultID] = result
		}
	}
	if b.Kind == "orchestrator" {
		applications, err := inputIDsTx(ctx, tx, "SELECT a.id FROM records a JOIN kernel_run_bindings r ON r.run_id=a.id WHERE a.kind='main_result_application' AND r.session_id=? AND r.applied=1 ORDER BY a.rowid", b.ID)
		if err != nil {
			return m, err
		}
		for _, runID := range applications {
			var application MainResultApplication
			if err = completionRecord(ctx, tx, "main_result_application", runID, &application); err != nil {
				return m, err
			}
			if application.SessionID != b.ID || application.RunID != runID {
				return m, errors.New("input main application binding conflict")
			}
			if application.State != "applied" || application.Result == nil {
				continue
			}
			if application.Result.SessionID != b.ID {
				return m, errors.New("input main result owner conflict")
			}
			for _, decision := range application.Result.Decisions {
				if decision.Target.WaveID == w.ID && decision.Target.ScopeRevision == w.ScopeRevision && decision.Target.PolicyRevision == w.Policy.Revision {
					m.MainDecisions = append(m.MainDecisions, appliedMainDecision{RunID: runID, Decision: decision})
				}
			}
		}
	}
	for _, task := range m.Tasks {
		if b.Kind == "task" && task.ID != b.TaskID {
			continue
		}
		for _, resultID := range task.Spec.SourceResultIDs {
			input, err := k.taskResultInputTx(ctx, tx, w, resultID)
			if err != nil {
				if b.Kind == "orchestrator" && readinessHoldError(err) {
					m.UnavailableSourceResults = append(m.UnavailableSourceResults, unavailableSourceResult{TaskID: task.ID, ResultID: resultID, Reason: "source_result_unavailable"})
					continue
				}
				return m, err
			}
			m.SourceResults[resultID] = input
		}
	}
	if b.Kind == "task" && !slices.ContainsFunc(m.Tasks, func(t domain.Task) bool { return t.ID == b.TaskID }) {
		return m, errors.New("Task is outside the current plan")
	}
	ids, err = inputIDsTx(ctx, tx, "SELECT id FROM kernel_artifacts WHERE wave_id=? AND state='committed' ORDER BY id", w.ID)
	if err != nil {
		return m, err
	}
	referenced := map[string]bool{}
	if b.Kind == "orchestrator" {
		selected := map[string]bool{}
		for _, id := range ids {
			selected[id] = true
		}
		for _, result := range m.Results {
			for _, id := range resultArtifactReferences(result) {
				referenced[id] = true
				if !selected[id] {
					ids = append(ids, id)
					selected[id] = true
				}
			}
		}
		for _, task := range m.Tasks {
			for _, id := range task.Spec.InputArtifactIDs {
				referenced[id] = true
				if !selected[id] {
					ids = append(ids, id)
					selected[id] = true
				}
			}
		}
		sort.Strings(ids)
	}
	for _, id := range ids {
		a, err := loadArtifact(ctx, tx, id)
		if err != nil {
			if b.Kind == "orchestrator" && (errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrArtifactIntegrity)) {
				reason := "invalid_artifact_manifest"
				if errors.Is(err, sql.ErrNoRows) {
					reason = "missing_artifact"
				}
				m.UnavailableArtifacts = append(m.UnavailableArtifacts, unavailableInputArtifact{ArtifactID: id, Reason: reason})
				continue
			}
			return m, err
		}
		if a.WaveID != w.ID || a.ScopeRevision != w.ScopeRevision || a.PolicyRevision != w.Policy.Revision {
			if referenced[id] {
				m.UnavailableArtifacts = append(m.UnavailableArtifacts, unavailableInputArtifact{ArtifactID: id, Reason: "outside_scope_artifact"})
			}
			continue
		}
		if a.State != "committed" {
			m.UnavailableArtifacts = append(m.UnavailableArtifacts, unavailableInputArtifact{ArtifactID: id, Reason: "unavailable_artifact"})
			continue
		}
		if b.Kind == "orchestrator" {
			if _, err = k.readArtifactObject(a); err != nil {
				if errors.Is(err, ErrArtifactIntegrity) {
					m.UnavailableArtifacts = append(m.UnavailableArtifacts, unavailableInputArtifact{ArtifactID: id, Reason: "invalid_artifact_object"})
					continue
				}
				return m, err
			}
		}
		if err = artifactReadableTx(ctx, tx, b, w, a); err != nil {
			if errors.Is(err, ErrArtifactGrant) {
				continue
			}
			return m, err
		}
		m.Artifacts = append(m.Artifacts, a)
	}
	ids, err = inputIDsTx(ctx, tx, "SELECT id FROM kernel_subscriptions WHERE session_id=? AND wave_id=? AND scope_revision=? AND plan_revision=? ORDER BY id", b.ID, w.ID, w.ScopeRevision, w.PlanRevision)
	if err != nil {
		return m, err
	}
	for _, id := range ids {
		var raw []byte
		var owner string
		var resource string
		var cursor int64
		if err = tx.QueryRowContext(ctx, "SELECT session_id,resource,cursor,payload FROM kernel_subscriptions WHERE id=?", id).Scan(&owner, &resource, &cursor, &raw); err != nil {
			return m, err
		}
		var header struct {
			ID     string          `json:"id"`
			Target contract.Target `json:"target"`
		}
		if err = json.Unmarshal(raw, &header); err != nil {
			return m, err
		}
		if owner != b.ID || header.ID != id {
			return m, errors.New("input subscription ownership conflict")
		}
		if header.Target == w.Target() {
			m.Subscriptions = append(m.Subscriptions, inputSubscription{ID: id, Target: header.Target, Resource: resource, Cursor: cursor, Definition: raw})
		}
	}
	if w.DirectionID != "" {
		var d WaveDirection
		if err = completionRecord(ctx, tx, "wave_direction", w.DirectionID, &d); err != nil {
			return m, err
		}
		if d.State != "applied" || (d.AppliedTarget.WaveID != w.ID || d.AppliedTarget.ScopeRevision != w.ScopeRevision || d.AppliedTarget.PolicyRevision != w.Policy.Revision) {
			return m, errors.New("input direction binding conflict")
		}
		m.Direction = &d
	}
	// Pending operator instructions are held at the safe boundary, never passed
	// to a model as an implicit steer. The ID itself remains auditable in Wave.
	return m, nil
}
func inputIDsTx(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
