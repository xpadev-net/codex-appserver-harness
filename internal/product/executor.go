package product

import (
	"context"
	"errors"
	"os"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// ExecutionRepository is private host configuration. Repository identity, fixed
// initial SHA and base branch are resolved by the host, never Issue/model data.
type ExecutionRepository struct {
	Name       string                            `json:"name"`
	BaseBranch string                            `json:"base_branch"`
	InitialSHA string                            `json:"initial_sha"`
	Policy     storage.ProductVerificationPolicy `json:"policy"`
	Runner     storage.ProductRunnerRuntime      `json:"runner"`
}

// Executor composes synchronous Git owners and detached runner owners. The
// caller must run it outside the model dispatch loop and retain Store until it
// joins. These ports do not enable the global product model-dispatch gate.
type Executor struct {
	BasePlanner  *BasePlanner
	Store        *storage.KernelStore
	Repositories map[int64]ExecutionRepository
	Runner       Runner
}

func (e Executor) repository(ctx context.Context, task domain.Task) (ExecutionRepository, error) {
	if e.Store == nil || len(task.Spec.WorkItemIDs) != 1 {
		return ExecutionRepository{}, storage.ErrProductWorkspaceHeld
	}
	registration, err := e.Store.ReadProductWorkItemRegistration(ctx, task.Spec.WorkItemIDs[0])
	if err != nil {
		return ExecutionRepository{}, err
	}
	definition := registration.Value.Definition
	repo, ok := e.Repositories[definition.RepositoryID]
	if !ok || repo.Name != definition.Repository || repo.Policy.RepositoryID != definition.RepositoryID || repo.BaseBranch == "" {
		return repo, storage.ErrProductWorkspaceHeld
	}
	return repo, nil
}

func (e Executor) Prepare(ctx context.Context, taskID string) (storage.ProductWorkspace, error) {
	if e.Store == nil {
		return storage.ProductWorkspace{}, storage.ErrProductWorkspaceHeld
	}
	input, err := e.Store.ReadProductTaskPreparation(ctx, taskID)
	if err != nil {
		return storage.ProductWorkspace{}, err
	}
	task := input.Task
	repo, err := e.repository(ctx, task)
	if err != nil {
		return storage.ProductWorkspace{}, err
	}
	policy := repo.Policy
	policy.WaveID, policy.PolicyRevision = task.WaveID, task.PolicyRevision
	if _, err := e.Store.RegisterProductVerificationPolicy(ctx, task.Target(), policy); err != nil {
		return storage.ProductWorkspace{}, err
	}
	base := repo.InitialSHA
	if input.Product != nil {
		base = input.Product.Value.Candidate.SHA
	}
	if task.Spec.Kind == "review" || task.Spec.Kind == "verification" {
		subject, err := e.Store.BindProductCandidateSubject(ctx, task.ID)
		if err != nil {
			return storage.ProductWorkspace{}, err
		}
		base = subject.Value.Candidate.SHA
	}
	request := storage.ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: repo.Policy.RepositoryID, Repository: repo.Policy.Repository, BaseSHA: base}
	preparer := WorkspacePreparer{Store: e.Store}
	if e.BasePlanner != nil && e.BasePlanner.Source != nil {
		current := storage.ProductTargetSnapshot{Value: storage.ProductTarget{Target: task.Target(), Repository: repo.Name, BaseBranch: repo.BaseBranch, WorkItem: domain.WorkItem{ID: task.Spec.WorkItemIDs[0], RepositoryID: repo.Policy.RepositoryID}}}
		if input.Product != nil {
			current = *input.Product
		}
		preparer.RefreshSource = func(read context.Context) error { return e.BasePlanner.Source(read, current) }
	}
	if (task.Spec.Kind == "work" || task.Spec.Kind == "fix") && e.BasePlanner != nil {
		if e.BasePlanner.Executor.Store != e.Store {
			return storage.ProductWorkspace{}, errors.New("base planner store owner mismatch")
		}
		plan, err := e.BasePlanner.Prepare(ctx, task.ID)
		if err != nil {
			return storage.ProductWorkspace{}, err
		}
		h := plan.Value.Head
		if h.RepositoryID != repo.Policy.RepositoryID || h.Repository != repo.Name || h.BaseBranch != repo.BaseBranch || h.InitialSHA != repo.InitialSHA || plan.Value.Input.Value.Policy.Value.Repository != repo.Policy.Repository {
			return storage.ProductWorkspace{}, storage.ErrProductWorkspaceHeld
		}
		request.BasePlanID, request.BasePlanDigest = plan.ID, plan.Digest
		if input.Product == nil {
			request.BaseSHA = h.HeadSHA
		}
		preparer.RefreshBase = e.BasePlanner.Prepare
	}
	return preparer.Prepare(ctx, request)
}

// Apply routes only the stored, normally stopped run. A caller cannot supply a
// final result, SHA, verification plan, pass Boolean or replacement command.
func (e Executor) Apply(ctx context.Context, taskID, runID string) (storage.ResultApplication, error) {
	if e.Store == nil {
		return storage.ResultApplication{}, storage.ErrTaskResultReferences
	}
	task, err := e.Store.Task(ctx, taskID)
	if err != nil {
		return storage.ResultApplication{}, err
	}
	w, err := e.Store.Wave(ctx, task.WaveID)
	if err != nil {
		return storage.ResultApplication{}, err
	}
	if !storage.UsesProductChangeOutput(w, task) && !storage.UsesProductVerificationOutput(w, task) && !storage.UsesProductReviewOutput(w, task) {
		return storage.ResultApplication{}, storage.ErrTaskResultReferences
	}
	repo, err := e.repository(ctx, task)
	if err != nil {
		return storage.ResultApplication{}, err
	}
	workspace, err := e.Store.ProductWorkspace(ctx, task.ID)
	if err != nil {
		return storage.ResultApplication{}, err
	}
	if workspace.Value.Request.Repository != repo.Policy.Repository {
		return storage.ResultApplication{}, storage.ErrProductWorkspaceHeld
	}
	if task.State != contract.TaskCompleted {
		state, err := e.Store.SessionDispatchState(ctx, task.SessionID)
		if err != nil {
			return storage.ResultApplication{}, err
		}
		if state.Run == nil || state.Run.ID != runID || state.Run.State != "completed" || !state.ProcessStopped || !state.RunStopped {
			return storage.ResultApplication{}, storage.ErrTaskResultReferences
		}
		var result contract.TaskResult
		if err := contract.DecodeStrict([]byte(state.Run.Final), &result); err != nil {
			return e.Store.ApplyTaskResult(ctx, task.SessionID, runID)
		}
		if err := result.ValidateAssignment(task.Target(), task.ID, task.PhaseID, task.Spec.Criteria); err != nil {
			return e.Store.ApplyTaskResult(ctx, task.SessionID, runID)
		}
		if task.Spec.Kind == "review" || result.Outcome != "result" {
			return e.Store.ApplyTaskResult(ctx, task.SessionID, runID)
		}
		if storage.UsesProductChangeOutput(w, task) && result.SchemaVersion != 2 || storage.UsesProductVerificationOutput(w, task) && result.SchemaVersion != 3 {
			return e.Store.ApplyTaskResult(ctx, task.SessionID, runID)
		}
		if seal, err := (CandidateSealer{Store: e.Store}).Seal(ctx, task.ID, runID); err != nil {
			// Only pre-admission output rejection belongs to result settlement.
			// Preserve all errors once a Git owner has an admitted identity.
			if seal.ID == "" && errors.Is(err, storage.ErrProductChangeReference) {
				return e.Store.ApplyTaskResult(ctx, task.SessionID, runID)
			}
			return storage.ResultApplication{}, err
		}
	}
	if task.Spec.Kind == "verification" {
		return e.Store.AdoptProductVerificationTaskResult(ctx, task.SessionID, runID, repo.BaseBranch)
	}
	if task.Spec.Kind == "review" {
		return e.Store.ApplyTaskResult(ctx, task.SessionID, runID)
	}
	return e.Store.AdoptProductTaskResult(ctx, task.SessionID, runID, repo.BaseBranch)
}

// AdvanceVerification makes one bounded pass over the canonical profile set.
// Files, process identities and persisted intents remain authoritative. Waiting
// consumes no model slot; unknown launch state never grants a second start.
func (e Executor) AdvanceVerification(ctx context.Context, planID string) (storage.ProductVerificationEvidence, error) {
	if e.Store == nil || e.Runner.Store != e.Store {
		return storage.ProductVerificationEvidence{}, storage.ErrProductRunnerHeld
	}
	plan, err := e.Store.ReadProductVerificationPlan(ctx, planID)
	if err != nil {
		return storage.ProductVerificationEvidence{}, err
	}
	task, err := e.Store.Task(ctx, plan.Value.TaskID)
	if err != nil {
		return storage.ProductVerificationEvidence{}, err
	}
	repo, err := e.repository(ctx, task)
	if err != nil {
		return storage.ProductVerificationEvidence{}, err
	}
	evidence, err := e.Store.ReadProductVerificationEvidence(ctx, planID)
	if err != nil {
		return evidence, err
	}
	var failures []error
	for _, observed := range evidence.Value.Jobs {
		var effectErr error
		switch observed.State {
		case "not_run":
			_, effectErr = e.Runner.Start(ctx, planID, observed.ProfileID, repo.Runner)
		case "prepared":
			job, readErr := e.Store.ReadProductRunnerJob(ctx, observed.JobID)
			if readErr != nil {
				effectErr = readErr
			} else if job.Value.RetryOf != "" {
				_, effectErr = e.startRetry(ctx, job.Value.RetryOf, repo.Runner)
			} else {
				_, effectErr = e.Runner.Start(ctx, planID, observed.ProfileID, repo.Runner)
			}
		case "launching":
			_, effectErr = e.Runner.Reconcile(ctx, observed.JobID)
		case "completed":
			if observed.Completion.Result.Status == "infrastructure_failure" {
				_, effectErr = e.startRetry(ctx, observed.JobID, repo.Runner)
			}
		}
		waitingForFiles := observed.State == "launching" && os.IsNotExist(effectErr)
		if effectErr != nil && !errors.Is(effectErr, storage.ErrExecutionCapacity) && !errors.Is(effectErr, runner.ErrJobBusy) && !errors.Is(effectErr, runner.ErrCompletionUnconfirmed) && !waitingForFiles {
			failures = append(failures, effectErr)
		}
	}
	latest, readErr := e.Store.ReadProductVerificationEvidence(ctx, planID)
	return latest, errors.Join(append(failures, readErr)...)
}

func (e Executor) startRetry(ctx context.Context, previousID string, runtime storage.ProductRunnerRuntime) (storage.ProductRunnerJob, error) {
	previous, err := e.Store.ReadProductRunnerJob(ctx, previousID)
	if err != nil {
		return previous, err
	}
	s := previous.Value.Spec
	frozen := storage.ProductRunnerRuntime{Root: s.Root, LauncherContract: s.LauncherContract, Launcher: s.Launcher, ForbiddenPaths: s.ForbiddenPaths}
	if !reflect.DeepEqual(frozen, runtime) {
		return previous, storage.ErrProductRunnerHeld
	}
	return e.Runner.StartRetry(ctx, previousID)
}

// RefreshPreparedBase only refreshes ordinary read evidence for an unused Task.
// A changed HEAD never rewrites the saved workspace or starts another Git owner.
func (e Executor) RefreshPreparedBase(ctx context.Context, taskID string, saved storage.ProductWorkspace) (bool, error) {
	if e.BasePlanner == nil || saved.Value.Request.BasePlanID == "" {
		return false, nil
	}
	if e.BasePlanner.Executor.Store != e.Store {
		return false, errors.New("base planner store owner mismatch")
	}
	current, err := e.Store.CurrentProductBasePlan(ctx, taskID)
	if err == nil {
		if current.ID != saved.Value.Request.BasePlanID || current.Digest != saved.Value.Request.BasePlanDigest {
			return false, storage.ErrProductWorkspaceHeld
		}
		return false, nil
	}
	if !errors.Is(err, storage.ErrProductBasePlanHeld) {
		return false, err
	}
	refreshed, err := e.BasePlanner.Prepare(ctx, taskID)
	if err != nil {
		return false, err
	}
	if refreshed.ID != saved.Value.Request.BasePlanID || refreshed.Digest != saved.Value.Request.BasePlanDigest {
		return true, storage.ErrProductWorkspaceHeld
	}
	return true, nil
}
