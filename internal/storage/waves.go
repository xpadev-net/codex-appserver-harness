package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func opaqueID(prefix string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b), nil
}
func structureID(prefix, wave, local string) string {
	h := sha256.Sum256([]byte(wave + "\x00" + local))
	return fmt.Sprintf("%s%x", prefix, h[:16])
}
func contentDigest(value any) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}
func kernelRecordTx(ctx context.Context, tx *sql.Tx, kind, id, owner string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload,work_item_id=excluded.work_item_id", kind, id, owner, b)
	return err
}
func loadWave(ctx context.Context, q queryer, id string) (domain.Wave, error) {
	var w domain.Wave
	var raw []byte
	err := q.QueryRowContext(ctx, "SELECT payload FROM kernel_waves WHERE id=?", id).Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &w)
	}
	return w, err
}
func loadTask(ctx context.Context, q queryer, id string) (domain.Task, error) {
	var t domain.Task
	var raw []byte
	var wave, phase, state string
	err := q.QueryRowContext(ctx, "SELECT wave_id,phase_id,state,payload FROM kernel_tasks WHERE id=?", id).Scan(&wave, &phase, &state, &raw)
	if err == nil {
		err = json.Unmarshal(raw, &t)
	}
	if err == nil && (t.ID != id || t.WaveID != wave || t.PhaseID != phase || string(t.State) != state) {
		err = errors.New("inconsistent Task identity/state")
	}
	return t, err
}
func saveWave(ctx context.Context, tx *sql.Tx, w domain.Wave) error {
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_waves SET payload=? WHERE id=?", b, w.ID)
	return err
}
func saveTask(ctx context.Context, tx *sql.Tx, t domain.Task) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_tasks SET state=?,payload=? WHERE id=?", t.State, b, t.ID)
	return err
}
func (k *KernelStore) Wave(ctx context.Context, id string) (domain.Wave, error) {
	return loadWave(ctx, k.DB, id)
}
func (k *KernelStore) Task(ctx context.Context, id string) (domain.Task, error) {
	return loadTask(ctx, k.DB, id)
}
func (k *KernelStore) Waves(ctx context.Context) ([]domain.Wave, error) {
	rows, err := k.DB.QueryContext(ctx, "SELECT payload FROM kernel_waves ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Wave{}
	for rows.Next() {
		var raw []byte
		var w domain.Wave
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &w); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
func (k *KernelStore) Tasks(ctx context.Context, wave string) ([]domain.Task, error) {
	rows, err := k.DB.QueryContext(ctx, "SELECT payload FROM kernel_tasks WHERE wave_id=? ORDER BY rowid", wave)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Task{}
	for rows.Next() {
		var raw []byte
		var task domain.Task
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &task); err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}
func (k *KernelStore) CreateOrchestrator(ctx context.Context, id, model, cwd, sandbox string) (contract.SessionBinding, error) {
	if id == "" {
		var err error
		id, err = opaqueID("main-")
		if err != nil {
			return contract.SessionBinding{}, err
		}
	}
	if strings.TrimSpace(model) == "" || !filepath.IsAbs(cwd) || (sandbox != "read-only" && sandbox != "workspace-write") {
		return contract.SessionBinding{}, errors.New("trusted model/workspace/sandbox required")
	}
	canonical, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return contract.SessionBinding{}, err
	}
	root, err := filepath.EvalSymlinks(k.Root)
	if err != nil {
		return contract.SessionBinding{}, err
	}
	relative, err := filepath.Rel(canonical, root)
	if err != nil {
		return contract.SessionBinding{}, err
	}
	if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return contract.SessionBinding{}, errors.New("workspace must not expose the controller root")
	}
	cwd = canonical
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return contract.SessionBinding{}, err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_sessions WHERE id=?", id).Scan(&raw)
	if err == nil {
		var existing contract.SessionBinding
		if err = json.Unmarshal(raw, &existing); err != nil {
			return existing, err
		}
		if existing.Kind != "orchestrator" || existing.Model != model || existing.CWD != cwd || existing.Sandbox != sandbox || existing.Closed {
			return existing, errors.New("orchestrator identity conflict")
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return contract.SessionBinding{}, err
	}
	b := contract.SessionBinding{ID: id, Kind: "orchestrator", CWD: cwd, Model: model, Sandbox: sandbox, Generation: 1}
	raw, err = json.Marshal(b)
	if err != nil {
		return b, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_sessions(id,kind,task_id,parent_id,payload) VALUES(?,'orchestrator',NULL,'',?)", id, raw); err != nil {
		return b, err
	}
	return b, tx.Commit()
}

type WaveRequest struct {
	OrchestratorSessionID string                 `json:"orchestrator_session_id"`
	Source                string                 `json:"source"`
	SourceID              string                 `json:"source_id"`
	OriginalRequest       string                 `json:"original_request"`
	Criteria              []domain.WaveCriterion `json:"criteria"`
	Policy                domain.WavePolicy      `json:"policy"`
}

func (k *KernelStore) CreateWave(ctx context.Context, request WaveRequest) (domain.Wave, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return domain.Wave{}, err
	}
	defer tx.Rollback()
	wave, err := k.createWaveTx(ctx, tx, request)
	if err != nil {
		return wave, err
	}
	return wave, tx.Commit()
}
func (k *KernelStore) createWaveTx(ctx context.Context, tx *sql.Tx, request WaveRequest) (domain.Wave, error) {
	digest, err := contentDigest(request)
	if err != nil {
		return domain.Wave{}, err
	}
	var id, prior string
	err = tx.QueryRowContext(ctx, "SELECT id,input_digest FROM kernel_waves WHERE source=? AND source_id=?", request.Source, request.SourceID).Scan(&id, &prior)
	if err == nil {
		if prior != digest {
			return domain.Wave{}, errors.New("request identity conflict; use an explicit scope revision")
		}
		return loadWave(ctx, tx, id)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.Wave{}, err
	}
	var main contract.SessionBinding
	var mainRaw []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_sessions WHERE id=? AND kind='orchestrator'", request.OrchestratorSessionID).Scan(&mainRaw)
	if err == nil {
		err = json.Unmarshal(mainRaw, &main)
	}
	if err != nil || main.ID != request.OrchestratorSessionID || main.Kind != "orchestrator" || main.Closed {
		return domain.Wave{}, errors.New("active orchestrator session required")
	}
	if err = request.Policy.Limits.Validate(); err != nil {
		return domain.Wave{}, err
	}
	id, err = opaqueID("wave-")
	if err != nil {
		return domain.Wave{}, err
	}
	now := k.clock()
	wave := domain.Wave{ID: id, OrchestratorSessionID: request.OrchestratorSessionID, Source: request.Source, SourceID: request.SourceID, OriginalRequest: request.OriginalRequest, Criteria: request.Criteria, ScopeRevision: 1, PlanRevision: 1, Policy: request.Policy, State: domain.WavePlanning, CreatedAt: now, Deadline: now.Add(time.Duration(request.Policy.Limits.WaveSeconds) * time.Second)}
	if err = wave.Validate(); err != nil {
		return wave, err
	}
	raw, err := json.Marshal(wave)
	if err != nil {
		return wave, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_waves(id,source,source_id,input_digest,payload) VALUES(?,?,?,?,?)", wave.ID, wave.Source, wave.SourceID, digest, raw); err != nil {
		return wave, err
	}
	if err = k.waveEventTx(ctx, tx, wave, "wave_created", "", wave.ID+":created", wave.Target()); err != nil {
		return wave, err
	}
	return wave, nil
}

func (k *KernelStore) waveEventTx(ctx context.Context, tx *sql.Tx, w domain.Wave, kind, taskID, eventID string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "INSERT INTO kernel_events(id,wave_id,task_id,kind,scope_revision,plan_revision,payload) VALUES(?,?,?,?,?,?,?)", eventID, w.ID, taskID, kind, w.ScopeRevision, w.PlanRevision, raw)
	if err != nil {
		return err
	}
	sequence, err := result.LastInsertId()
	if err != nil {
		return err
	}
	// A durable fanout intent is written with every state transition. Task_3
	// consumes it atomically with the corresponding subscriber notifications.
	return kernelRecordTx(ctx, tx, "notification_intent", eventID, w.ID, map[string]any{"event_id": eventID, "sequence": sequence, "target": w.Target(), "task_id": taskID, "kind": kind})
}

type PlanAdmission struct {
	RequestTarget  contract.Target       `json:"request_target"`
	Target         contract.Target       `json:"target"`
	Key            string                `json:"key"`
	Digest         string                `json:"digest"`
	Plan           contract.PlanProposal `json:"plan"`
	CreatedTaskIDs []string              `json:"created_task_ids"`
	CarriedTaskIDs []string              `json:"carried_task_ids"`
}

func (k *KernelStore) AdoptPlan(ctx context.Context, target contract.Target, key string, plan contract.PlanProposal) (PlanAdmission, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return PlanAdmission{}, err
	}
	defer tx.Rollback()
	receipt, err := k.adoptPlanTx(ctx, tx, target, key, plan)
	if err != nil {
		return receipt, err
	}
	return receipt, tx.Commit()
}
func (k *KernelStore) adoptPlanTx(ctx context.Context, tx *sql.Tx, target contract.Target, key string, plan contract.PlanProposal) (PlanAdmission, error) {
	receipt := PlanAdmission{RequestTarget: target, Key: key, Plan: plan, CreatedTaskIDs: []string{}, CarriedTaskIDs: []string{}}
	if strings.TrimSpace(key) == "" || len(key) > 256 || !target.Valid() {
		return receipt, errors.New("versioned adoption key required")
	}
	digest, err := contentDigest(plan)
	if err != nil {
		return receipt, err
	}
	receipt.Digest = digest
	var previous []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_plans WHERE wave_id=? AND adoption_key=?", target.WaveID, key).Scan(&previous)
	if err == nil {
		var prior PlanAdmission
		if err = json.Unmarshal(previous, &prior); err != nil {
			return receipt, err
		}
		if prior.Digest != digest || prior.RequestTarget != target {
			return receipt, errors.New("plan replay identity conflict")
		}
		return prior, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	wave, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return receipt, err
	}
	if wave.Target() != target || wave.Paused || wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted || !k.clock().Before(wave.Deadline) {
		return receipt, errors.New("stale/terminal/paused/expired Wave")
	}
	if err = domain.ValidateWavePlan(wave, plan); err != nil {
		return receipt, err
	}
	var active int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_sessions s JOIN kernel_tasks t ON t.id=s.task_id WHERE t.wave_id=? AND (s.active_run_id<>'' OR s.process_stopped=0 OR s.creating<>0)", wave.ID).Scan(&active); err != nil {
		return receipt, err
	}
	if active > 0 {
		return receipt, errors.New("reconcile and stop active Task processes before replanning")
	}
	var mainRaw []byte
	var main contract.SessionBinding
	if err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_sessions WHERE id=?", wave.OrchestratorSessionID).Scan(&mainRaw); err != nil {
		return receipt, err
	}
	if err = json.Unmarshal(mainRaw, &main); err != nil {
		return receipt, err
	}
	var previousCount int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_plans WHERE wave_id=?", wave.ID).Scan(&previousCount); err != nil {
		return receipt, err
	}
	revision := wave.PlanRevision
	if previousCount > 0 {
		if wave.Replans >= wave.Policy.Limits.Replans {
			return receipt, errors.New("replan budget exhausted")
		}
		var current int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_plans WHERE wave_id=? AND revision=?", wave.ID, revision).Scan(&current); err != nil {
			return receipt, err
		}
		if current > 0 {
			revision++
		}
		wave.Replans++
	}
	receipt.Target = target
	receipt.Target.PlanRevision = revision
	newPhases, newTasks := 0, 0
	included := map[string]bool{}
	for position, phaseSpec := range plan.Phases {
		phaseID := structureID("phase-", wave.ID, phaseSpec.ID)
		phase := domain.WavePhase{ID: phaseID, WaveID: wave.ID, LocalID: phaseSpec.ID, PlanRevision: revision, Position: position, Spec: phaseSpec}
		var existingRaw []byte
		err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_phases WHERE id=?", phaseID).Scan(&existingRaw)
		if err == nil {
			var existing domain.WavePhase
			if err = json.Unmarshal(existingRaw, &existing); err != nil {
				return receipt, err
			}
			if !reflect.DeepEqual(existing.Spec, phaseSpec) || existing.Position != position {
				return receipt, errors.New("phase identities are immutable; use a fresh phase key")
			}
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return receipt, err
		}
		if errors.Is(err, sql.ErrNoRows) {
			raw, err := json.Marshal(phase)
			if err != nil {
				return receipt, err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_phases(id,wave_id,local_id,created_revision,payload) VALUES(?,?,?,?,?)", phaseID, wave.ID, phaseSpec.ID, revision, raw); err != nil {
				return receipt, err
			}
			newPhases++
		}
		for _, spec := range phaseSpec.Tasks {
			taskID := structureID("task-", wave.ID, spec.ID)
			included[taskID] = true
			inputs, err := k.taskInputDigestTx(ctx, tx, wave, spec)
			if err != nil {
				return receipt, err
			}
			existing, err := loadTask(ctx, tx, taskID)
			if err == nil {
				if existing.State != contract.TaskCompleted || existing.ScopeRevision != wave.ScopeRevision || existing.PolicyRevision != wave.Policy.Revision || existing.PhaseID != phaseID || !reflect.DeepEqual(existing.Spec, spec) || existing.ResultID == "" || existing.InputDigest != inputs {
					return receipt, errors.New("only unchanged completed Task results may be explicitly carried; allocate fresh Task/phase keys")
				}
				if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_plan_bindings(wave_id,revision,task_id,source_result_id,input_digest) VALUES(?,?,?,?,?)", wave.ID, revision, taskID, existing.ResultID, inputs); err != nil {
					return receipt, err
				}
				receipt.CarriedTaskIDs = append(receipt.CarriedTaskIDs, taskID)
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return receipt, err
			}
			sessionID := structureID("session-", wave.ID, spec.ID)
			task := domain.Task{ID: taskID, WaveID: wave.ID, PhaseID: phaseID, LocalID: spec.ID, ScopeRevision: wave.ScopeRevision, PlanRevision: revision, PolicyRevision: wave.Policy.Revision, SessionID: sessionID, ParentSessionID: wave.OrchestratorSessionID, State: contract.TaskReady, Spec: spec, InputDigest: inputs}
			raw, err := json.Marshal(task)
			if err != nil {
				return receipt, err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_tasks(id,wave_id,phase_id,local_id,state,payload) VALUES(?,?,?,?,?,?)", taskID, wave.ID, phaseID, spec.ID, task.State, raw); err != nil {
				return receipt, err
			}
			sandbox := main.Sandbox
			if spec.Kind == "review" || wave.Policy.Stage == "K" {
				sandbox = "read-only"
			}
			binding := contract.SessionBinding{ID: sessionID, Kind: "task", TaskID: taskID, PhaseID: phaseID, ParentSessionID: main.ID, TaskState: contract.TaskReady, Target: task.Target(), CWD: filepath.Join(k.Root, "workspaces", sessionID), Model: main.Model, Sandbox: sandbox, Generation: 1}
			bindingRaw, err := json.Marshal(binding)
			if err != nil {
				return receipt, err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_sessions(id,kind,task_id,parent_id,payload) VALUES(?,'task',?,?,?)", sessionID, taskID, main.ID, bindingRaw); err != nil {
				return receipt, err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_plan_bindings(wave_id,revision,task_id,source_result_id,input_digest) VALUES(?,?,?,'',?)", wave.ID, revision, taskID, inputs); err != nil {
				return receipt, err
			}
			for _, artifactID := range spec.InputArtifactIDs {
				if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_artifact_refs(artifact_id,task_id,kind) VALUES(?,?,'input')", artifactID, taskID); err != nil {
					return receipt, err
				}
			}
			receipt.CreatedTaskIDs = append(receipt.CreatedTaskIDs, taskID)
			newTasks++
		}
	}
	if newPhases > wave.Policy.Limits.Phases-wave.PhasesCreated || newTasks > wave.Policy.Limits.Tasks-wave.TasksCreated {
		return receipt, errors.New("cumulative structure budget exhausted")
	}
	// Retiring an omitted, stopped Task is an explicit effect of this adopted plan.
	rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_tasks WHERE wave_id=?", wave.ID)
	if err != nil {
		return receipt, err
	}
	oldIDs := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return receipt, err
		}
		if !included[id] {
			oldIDs = append(oldIDs, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return receipt, err
	}
	for _, id := range oldIDs {
		old, err := loadTask(ctx, tx, id)
		if err != nil {
			return receipt, err
		}
		if !old.State.Terminal() {
			old.State = contract.TaskCanceled
			old.Reason = fmt.Sprintf("superseded by adopted plan %d", revision)
			old.CompletedAt = k.clock()
			if err = saveTask(ctx, tx, old); err != nil {
				return receipt, err
			}
		}
	}
	wave.PlanRevision = revision
	wave.PhasesCreated += newPhases
	wave.TasksCreated += newTasks
	wave.State = domain.WaveRunning
	wave.Reason = ""
	if err = saveWave(ctx, tx, wave); err != nil {
		return receipt, err
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return receipt, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_plans(wave_id,revision,adoption_key,digest,payload) VALUES(?,?,?,?,?)", wave.ID, revision, key, digest, raw); err != nil {
		return receipt, err
	}
	for kind, amount := range map[string]int{"phases": newPhases, "tasks": newTasks} {
		if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,?,?,0,?)", wave.ID, "plan:"+key, kind, amount, []byte(`{}`)); err != nil {
			return receipt, err
		}
	}
	if err = k.waveEventTx(ctx, tx, wave, "plan_adopted", "", wave.ID+":plan:"+key, map[string]any{"target": receipt.Target, "created": receipt.CreatedTaskIDs, "carried": receipt.CarriedTaskIDs}); err != nil {
		return receipt, err
	}
	return receipt, nil
}
func (k *KernelStore) taskInputDigestTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, spec contract.TaskSpec) (string, error) {
	references := map[string]string{}
	for _, id := range spec.InputArtifactIDs {
		var state, digest, owner string
		var payload []byte
		if err := tx.QueryRowContext(ctx, "SELECT state,digest,wave_id,payload FROM kernel_artifacts WHERE id=?", id).Scan(&state, &digest, &owner, &payload); err != nil {
			return "", err
		}
		var metadata struct {
			ScopeRevision int `json:"scope_revision"`
		}
		if err := json.Unmarshal(payload, &metadata); err != nil {
			return "", err
		}
		if state != "committed" || owner != wave.ID || metadata.ScopeRevision != wave.ScopeRevision {
			return "", errors.New("artifact is missing, expired or outside the current scope")
		}
		references[id] = digest
	}
	for _, id := range spec.WorkItemIDs {
		var binding struct {
			Target      contract.Target `json:"target"`
			CandidateID string          `json:"candidate_id"`
		}
		if err := completionRecord(ctx, tx, "wave_work_item", id, &binding); err != nil {
			return "", err
		}
		if binding.Target.WaveID != wave.ID || binding.Target.ScopeRevision != wave.ScopeRevision || binding.Target.PolicyRevision != wave.Policy.Revision {
			return "", errors.New("WorkItem is outside the trusted Wave scope")
		}
		references["work_item:"+id] = binding.CandidateID
	}
	for _, dependency := range spec.DependsOn {
		id := structureID("task-", wave.ID, dependency.TaskID)
		prior, err := loadTask(ctx, tx, id)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		value := ""
		if err == nil && prior.State == contract.TaskCompleted && prior.ScopeRevision == wave.ScopeRevision && prior.PolicyRevision == wave.Policy.Revision {
			value = prior.ResultID
			if dependency.ArtifactID != "" {
				var result contract.TaskResult
				if err = completionRecord(ctx, tx, "task_result", prior.ResultID, &result); err != nil {
					return "", err
				}
				found := false
				for _, id := range result.Artifacts {
					found = found || id == dependency.ArtifactID
				}
				if !found || result.TaskID != prior.ID || result.PhaseID != prior.PhaseID || result.Target != prior.Target() || result.Outcome != "result" {
					return "", errors.New("dependency artifact does not belong to the bound completed result")
				}
				var state, digest, owner string
				var payload []byte
				if err = tx.QueryRowContext(ctx, "SELECT state,digest,wave_id,payload FROM kernel_artifacts WHERE id=?", dependency.ArtifactID).Scan(&state, &digest, &owner, &payload); err != nil {
					return "", err
				}
				var metadata struct {
					ScopeRevision int `json:"scope_revision"`
				}
				if err = json.Unmarshal(payload, &metadata); err != nil {
					return "", err
				}
				if state != "committed" || owner != wave.ID || metadata.ScopeRevision != wave.ScopeRevision {
					return "", errors.New("dependency artifact is missing, expired or outside the current scope")
				}
				references["dependency_artifact:"+dependency.TaskID+":"+dependency.ArtifactID] = digest
			}
		}
		references["dependency:"+dependency.TaskID] = value
	}
	return contentDigest(struct {
		Spec          contract.TaskSpec
		Inputs        map[string]string
		Scope, Policy int
	}{spec, references, wave.ScopeRevision, wave.Policy.Revision})
}
