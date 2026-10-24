package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Trusted composition-root configuration, never a model tool or persisted
// readiness Boolean. Every actual process still uses mandatory runtime probes.
type ProductModelRepository struct {
	Name       string                    `json:"name"`
	InitialSHA string                    `json:"initial_sha"`
	BaseBranch string                    `json:"base_branch"`
	Policy     ProductVerificationPolicy `json:"policy"`
	Runner     ProductRunnerRuntime      `json:"runner"`
}
type ProductModelRuntime struct {
	Model         string                   `json:"model"`
	ToolProfile   string                   `json:"tool_profile"`
	RuntimeDigest string                   `json:"runtime_digest"`
	Repositories  []ProductModelRepository `json:"repositories"`
}
type productModelBinding struct {
	isolationPaths  []string
	isolationSealed bool
	runtimeDigest   string
	runtimeModel    string
	runtimeProfile  string
	sync.RWMutex
	generation uint64
	epoch      string
	config     *ProductModelRuntime
}

// BindProductModelRuntime is host-only and scoped to this live store instance.
// Its revocation cannot revoke a later owner, even after an identical rebind.
// Opening the same DB in another process or after restart conveys no grant.
func (k *KernelStore) BindProductModelRuntime(config ProductModelRuntime) (func(), error) {
	if k == nil {
		return nil, errors.New("explicit product store required")
	}
	raw, err := json.Marshal(config)
	var frozen ProductModelRuntime
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &frozen) != nil || !validSourceDigest(frozen.RuntimeDigest) || frozen.ToolProfile != "product_v2" || strings.TrimSpace(frozen.Model) == "" || !utf8.ValidString(frozen.Model) || strings.ContainsAny(frozen.Model, "\x00\r\n") || len(frozen.Model) > contract.MaxInline || len(frozen.Repositories) < 1 || len(frozen.Repositories) > 128 {
		return nil, errors.New("bounded host model, product_v2 runtime and repositories required")
	}
	ids := map[int64]bool{}
	for i := range frozen.Repositories {
		repo := &frozen.Repositories[i]
		parts := strings.Split(repo.Name, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || !fixedSHA(repo.InitialSHA) || repo.BaseBranch == "" || strings.ContainsAny(repo.Name+repo.BaseBranch, "\x00\r\n\t ") || ids[repo.Policy.RepositoryID] {
			return nil, errors.New("unique configured product repository required")
		}
		repo.Policy.WaveID, repo.Policy.PolicyRevision = "bound-product-runtime", repo.Policy.Verification.Revision
		repo.Policy, err = normalizeProductVerificationPolicy(repo.Policy)
		if err != nil {
			return nil, err
		}
		if err := validateProductRunnerRuntime(repo.Runner); err != nil {
			return nil, err
		}
		ids[repo.Policy.RepositoryID] = true
	}
	epoch, err := k.branchOwnerInstanceID()
	if err != nil {
		return nil, err
	}
	state := &k.productModels
	state.Lock()
	defer state.Unlock()
	if state.runtimeDigest != frozen.RuntimeDigest || state.runtimeModel != frozen.Model || state.runtimeProfile != frozen.ToolProfile {
		return nil, errors.New("product readiness requires the registered isolated runtime")
	}
	if state.config != nil {
		return nil, ErrResourceBusy
	}
	if state.generation == ^uint64(0) {
		return nil, errors.New("product runtime generation exhausted")
	}
	state.generation++
	generation := state.generation
	state.config, state.epoch = &frozen, epoch
	return func() {
		state.Lock()
		defer state.Unlock()
		if state.generation == generation {
			state.config = nil
			state.epoch = ""
		}
	}, nil
}
func (k *KernelStore) productModelConfig() *ProductModelRuntime {
	epoch, err := k.branchOwnerInstanceID()
	if err != nil {
		return nil
	}
	k.productModels.RLock()
	defer k.productModels.RUnlock()
	if k.productModels.epoch != epoch {
		return nil
	}
	return k.productModels.config // immutable detached value; revocation is checked again at turn reservation
}
func (k *KernelStore) productTaskReadinessTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) (string, error) {
	config := k.productModelConfig()
	if config == nil {
		return "product_readiness_unavailable", nil
	}
	if !UsesProductChangeOutput(w, task) && !usesProductSubject(w, task.Spec) || len(task.Spec.WorkItemIDs) != 1 {
		return "product_role_unavailable", nil
	}
	held, err := productWorkItemExecutionHeld(ctx, tx, w, task.Spec.WorkItemIDs[0])
	if err != nil {
		return "", err
	}
	if held {
		return "product_work_item_held", nil
	}
	if err := k.productMergeDependenciesTx(ctx, tx, w, task); err != nil {
		if errors.Is(err, ErrProductMergeDependencyHeld) {
			return "product_merge_dependency", nil
		}
		return productReadinessError("product_merge_dependency_unavailable", err)
	}
	saved, err := k.fixedProductWorkspaceInputTx(ctx, tx, w, task.Spec)
	if err != nil {
		return productReadinessError("product_workspace_unavailable", err)
	}
	if saved == nil || saved.State != "ready" || saved.Proof == nil || saved.Proof.Outcome != "prepared" || !saved.Proof.Clean && !(task.Spec.ProductBaseConflict != nil && saved.PreparedBaseConflict()) {
		return "product_workspace_unavailable", nil
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, saved.Value.WorkItemID)
	if err != nil {
		return productReadinessError("product_registration_unavailable", err)
	}
	var host *ProductModelRepository
	for i := range config.Repositories {
		if config.Repositories[i].Policy.RepositoryID == saved.Value.Request.RepositoryID {
			host = &config.Repositories[i]
			break
		}
	}
	if host == nil || host.Name != registration.Value.Definition.Repository || host.Policy.Repository != saved.Value.Request.Repository {
		return "product_repository_unavailable", nil
	}
	if saved.Value.Request.BasePlanID != "" {
		if _, err := k.productModelBaseSelectionTx(ctx, tx, task, *saved, *host); err != nil {
			return productReadinessError("product_base_unavailable", err)
		}
	}
	current, err := loadProductTargetTx(ctx, tx, w.Target(), saved.Value.WorkItemID)
	if err == nil {
		if current.Value.Repository != host.Name || current.Value.BaseBranch != host.BaseBranch {
			return "product_repository_changed", nil
		}
		if !usesProductSubject(w, task.Spec) && saved.Value.Request.BaseSHA != current.Value.Candidate.SHA {
			return "product_candidate_changed", nil
		}
	} else if err != sql.ErrNoRows {
		return productReadinessError("product_candidate_unavailable", err)
	} else if !usesProductSubject(w, task.Spec) && saved.Value.Request.BasePlanID == "" && saved.Value.Request.BaseSHA != host.InitialSHA {
		return "product_base_changed", nil
	}
	policy, err := loadProductVerificationPolicy(ctx, tx, w.ID, w.Policy.Revision, host.Policy.RepositoryID)
	if err != nil {
		return productReadinessError("product_policy_unavailable", err)
	}
	expected := host.Policy
	expected.WaveID, expected.PolicyRevision = w.ID, w.Policy.Revision
	if !reflect.DeepEqual(policy.Value, expected) {
		return "product_policy_changed", nil
	}
	b, err := sessionBindingTx(ctx, tx, task.SessionID)
	if err != nil {
		return "", err
	}
	sandbox := "workspace-write"
	if task.Spec.Kind == "review" {
		sandbox = "read-only"
	}
	if b.Closed || b.Model != config.Model || b.Kind != "task" || b.TaskID != task.ID || b.Target != task.Target() || b.ParentSessionID != w.OrchestratorSessionID || b.CWD != saved.Value.CWD || b.Sandbox != sandbox {
		return "product_session_unavailable", nil
	}
	subject, err := k.productWorkspaceSubjectTx(ctx, tx, w, task, saved.Value)
	if err != nil {
		return productReadinessError("product_subject_unavailable", err)
	}
	if task.Spec.Kind == "review" {
		if subject == nil || len(subject.Value.Sources) == 0 || subject.Value.Sources[0].Value.VerificationPlan == nil {
			return "product_verification_unavailable", nil
		}
		evidence, err := k.productVerificationEvidenceTx(ctx, tx, subject.Value.Sources[0].Value.VerificationPlan.ID)
		if err != nil {
			return productReadinessError("product_verification_unavailable", err)
		}
		if !evidence.Value.Passed {
			return "product_verification_pending", nil
		}
	}
	reservation, err := loadOperationReservationTx(ctx, tx, saved.ID)
	if err != nil {
		return productReadinessError("product_preparation_unavailable", err)
	}
	expectedProof := OperationReleaseProof{OperationID: saved.ID, ProofID: saved.Proof.ProofID, Disposition: "process_stopped", Reason: saved.Proof.Reason}
	if reservation.Kind != "git" || reservation.State != "released" || reservation.SessionID != w.OrchestratorSessionID || reservation.Request.Target != saved.Value.Target || reservation.Request.Repository != saved.Value.Request.Repository || reservation.Request.Worktree != saved.Value.CWD || reservation.Proof == nil || *reservation.Proof != expectedProof {
		return "product_preparation_unavailable", nil
	}
	input, err := k.taskInputDigestTx(ctx, tx, w, task.Spec)
	if err != nil {
		return productReadinessError("product_input_unavailable", err)
	}
	if input != task.InputDigest {
		return "product_input_changed", nil
	}
	return "", nil
}
func productReadinessError(reason string, err error) (string, error) {
	if readinessHoldError(err) || errors.Is(err, ErrProductWorkspaceHeld) || errors.Is(err, ErrProductSubjectHeld) || errors.Is(err, ErrProductRunnerHeld) {
		return reason, nil
	}
	return "", fmt.Errorf("product readiness: %w", err)
}

// ProductModelAdmission records which live host configuration admitted a turn.
// It contains references only; private runtime paths/argv are not model input.
type ProductModelAdmission struct {
	BaseSelection    *ProductBaseSelection `json:"base_selection,omitempty"`
	HostConfigDigest string                `json:"host_config_digest"`
	RunID            string                `json:"run_id"`
	SessionID        string                `json:"session_id"`
	Target           contract.Target       `json:"target"`
	Epoch            string                `json:"epoch"`
	RuntimeDigest    string                `json:"runtime_digest"`
	Model            string                `json:"model"`
	ToolProfile      string                `json:"tool_profile"`
	RepositoryID     int64                 `json:"repository_id"`
	WorkspaceID      string                `json:"workspace_id"`
	WorkspaceDigest  string                `json:"workspace_digest"`
	InputDigest      string                `json:"input_digest"`
	SnapshotID       string                `json:"snapshot_id"`
	SnapshotDigest   string                `json:"snapshot_digest"`
}

func (k *KernelStore) recordProductModelAdmissionTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task, snapshot contract.InputSnapshot, run contract.SessionRun) error {
	if reason, err := k.productTaskReadinessTx(ctx, tx, w, task); err != nil {
		return err
	} else if reason != "" {
		return ErrProductWorkspaceHeld
	}
	config := k.productModelConfig()
	if config == nil {
		return ErrProductWorkspaceHeld
	}
	workspace, err := k.fixedProductWorkspaceInputTx(ctx, tx, w, task.Spec)
	if err != nil {
		return err
	}
	if workspace == nil {
		return ErrProductWorkspaceHeld
	}
	epoch, err := k.branchOwnerInstanceID()
	if err != nil {
		return err
	}
	catalogDigest, err := contentDigest(config)
	if err != nil {
		return err
	}
	value := ProductModelAdmission{HostConfigDigest: catalogDigest, RunID: run.ID, SessionID: task.SessionID, Target: task.Target(), Epoch: epoch, RuntimeDigest: config.RuntimeDigest, Model: config.Model, ToolProfile: config.ToolProfile, RepositoryID: workspace.Value.Request.RepositoryID, WorkspaceID: workspace.ID, WorkspaceDigest: workspace.Digest, InputDigest: task.InputDigest, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest}
	if workspace.Value.Request.BasePlanID != "" {
		var host ProductModelRepository
		for _, repo := range config.Repositories {
			if repo.Policy.RepositoryID == workspace.Value.Request.RepositoryID {
				host = repo
				break
			}
		}
		selected, err := k.productModelBaseSelectionTx(ctx, tx, task, *workspace, host)
		if err != nil {
			return err
		}
		value.BaseSelection = selected
	}
	return kernelRecordTx(ctx, tx, "product_model_admission", run.ID, w.ID, value)
}

// ConfigureProductIsolationPaths carries private authority paths from host
// composition into every worker's actual isolation probe. Runtime construction
// freezes this catalog; changing it requires a new store/controller instance.
func (k *KernelStore) ConfigureProductIsolationPaths(paths []string) error {
	if k == nil || len(paths) > 512 {
		return errors.New("bounded product isolation paths required")
	}
	frozen := slices.Clone(paths)
	for _, path := range frozen {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > contract.MaxInline || strings.ContainsAny(path, "\x00\r\n") {
			return errors.New("absolute canonical private authority path required")
		}
		relative, err := filepath.Rel(filepath.Join(k.Root, "workspaces"), path)
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errors.New("product authority paths cannot be in model workspaces")
		}
	}
	sort.Strings(frozen)
	frozen = slices.Compact(frozen)
	state := &k.productModels
	state.Lock()
	defer state.Unlock()
	if state.isolationSealed && !slices.Equal(state.isolationPaths, frozen) {
		return ErrResourceBusy
	}
	state.isolationPaths = frozen
	return nil
}
func (k *KernelStore) SealProductIsolationPaths() []string {
	state := &k.productModels
	state.Lock()
	defer state.Unlock()
	state.isolationSealed = true
	return slices.Clone(state.isolationPaths)
}

// RegisterIsolatedRuntime is called by the trusted common runtime constructor,
// not by tools. It grants no product readiness or launch permission itself.
func (k *KernelStore) RegisterIsolatedRuntime(digest, model, profile string) error {
	if !validSourceDigest(digest) {
		return errors.New("isolated runtime digest required")
	}
	state := &k.productModels
	state.Lock()
	defer state.Unlock()
	if state.runtimeDigest != "" && (state.runtimeDigest != digest || state.runtimeModel != model || state.runtimeProfile != profile) {
		return ErrResourceBusy
	}
	state.runtimeDigest, state.runtimeModel, state.runtimeProfile = digest, model, profile
	return nil
}
func (k *KernelStore) IsolatedRuntimeMatches(digest string) bool {
	state := &k.productModels
	state.RLock()
	defer state.RUnlock()
	return digest != "" && state.runtimeDigest == digest
}
