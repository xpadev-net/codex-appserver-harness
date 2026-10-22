package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"os"
	"time"
)

// CollectArtifacts expires only artifacts past terminal retention, while closing
// over provenance/dependency references of unfinished Tasks and current-plan
// completed results that still prove Phase completion. Deletion intent
// remains charged until the physical file is removed; a crash can resume it.
func (k *KernelStore) CollectArtifacts(ctx context.Context) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_artifacts WHERE state<>'expired'")
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	all := map[string]domain.Artifact{}
	for _, id := range ids {
		a, err := loadArtifact(ctx, tx, id)
		if err != nil {
			return err
		}
		all[id] = a
	}
	protected := map[string]bool{}
	for id, a := range all {
		if a.State == "staged" {
			protected[id] = true
		}
	}
	rows, err = tx.QueryContext(ctx, "SELECT payload FROM kernel_tasks WHERE state NOT IN ('completed','failed','canceled')")
	if err != nil {
		return err
	}
	tasks := []domain.Task{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		var t domain.Task
		if err = json.Unmarshal(raw, &t); err != nil {
			rows.Close()
			return err
		}
		tasks = append(tasks, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, task := range tasks {
		for _, id := range task.Spec.InputArtifactIDs {
			protected[id] = true
		}
		for id, a := range all {
			if a.TaskID == task.ID {
				protected[id] = true
			}
		}
		for _, dep := range task.Spec.DependsOn {
			if dep.ArtifactID != "" {
				protected[dep.ArtifactID] = true
			}
			prior, err := loadTask(ctx, tx, structureID("task-", task.WaveID, dep.TaskID))
			if err != nil {
				return err
			}
			if prior.ResultID != "" {
				var result contract.TaskResult
				if err = completionRecord(ctx, tx, "task_result", prior.ResultID, &result); err != nil {
					return err
				}
				for _, id := range resultArtifactReferences(result) {
					protected[id] = true
				}
			}
		}
	}
	if err = protectPhaseEvidenceTx(ctx, tx, protected); err != nil {
		return err
	}
	// Referenced artifacts retain their own source artifacts recursively.
	var protect func(string)
	expanded := map[string]bool{}
	protect = func(id string) {
		if expanded[id] {
			return
		}
		expanded[id] = true
		protected[id] = true
		for _, input := range all[id].InputArtifactIDs {
			protect(input)
		}
	}
	for id := range protected {
		protect(id)
	}
	deleting := []domain.Artifact{}
	now := k.clock()
	for _, id := range ids {
		a := all[id]
		if a.State == "deleting" {
			deleting = append(deleting, a)
			continue
		}
		if protected[id] || a.State != "committed" {
			continue
		}
		wave, err := loadWave(ctx, tx, a.WaveID)
		if err != nil {
			return err
		}
		terminal := time.Time{}
		if a.TaskID != "" {
			task, err := loadTask(ctx, tx, a.TaskID)
			if err != nil {
				return err
			}
			if task.State.Terminal() {
				terminal = task.CompletedAt
			}
		} else if wave.State == domain.WaveCompleted || wave.State == domain.WaveCanceled {
			terminal = wave.CompletedAt
		}
		if terminal.IsZero() || now.Before(terminal.Add(time.Duration(wave.Policy.Limits.RetentionSeconds)*time.Second)) {
			continue
		}
		// A staged import is deliberately excluded: its partial write and unknown
		// producer outcome need reconciliation rather than optimistic expiration.
		a.State = "deleting"
		if err = saveArtifact(ctx, tx, a); err != nil {
			return err
		}
		deleting = append(deleting, a)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	for _, a := range deleting {
		if err = k.deleteArtifactObject(ctx, a); err != nil {
			return err
		}
	}
	return nil
}

// Phase barriers implicitly reference every required result, even when a later
// Task has no explicit data dependency on it. Keep this evidence through final
// Wave acceptance, including paused/blocked Waves and explicitly carried Tasks.
// Historical results dropped from the current plan may follow normal retention.
func protectPhaseEvidenceTx(ctx context.Context, tx *sql.Tx, protected map[string]bool) error {
	rows, err := tx.QueryContext(ctx, "SELECT b.wave_id,b.revision,b.task_id FROM kernel_plan_bindings b JOIN kernel_tasks t ON t.id=b.task_id WHERE t.state='completed'")
	if err != nil {
		return err
	}
	type binding struct {
		wave     string
		revision int
		task     string
	}
	bindings := []binding{}
	for rows.Next() {
		var b binding
		if err = rows.Scan(&b.wave, &b.revision, &b.task); err != nil {
			rows.Close()
			return err
		}
		bindings = append(bindings, b)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	waves := map[string]domain.Wave{}
	for _, binding := range bindings {
		wave, found := waves[binding.wave]
		if !found {
			wave, err = loadWave(ctx, tx, binding.wave)
			if err != nil {
				return err
			}
			waves[wave.ID] = wave
		}
		if wave.State == domain.WaveCompleted || wave.State == domain.WaveCanceled || binding.revision != wave.PlanRevision {
			continue
		}
		task, err := loadTask(ctx, tx, binding.task)
		if err != nil {
			return err
		}
		if err = currentObservedTaskTx(ctx, tx, wave, task); err != nil {
			return err
		}
		var result contract.TaskResult
		if err = completionRecord(ctx, tx, "task_result", task.ResultID, &result); err != nil {
			return err
		}
		if err = result.Validate(); err != nil {
			return err
		}
		if result.TaskID != task.ID || result.PhaseID != task.PhaseID || result.Target != task.Target() || result.Outcome != "result" {
			return errors.New("Phase evidence result binding conflict")
		}
		for _, id := range task.Spec.InputArtifactIDs {
			protected[id] = true
		}
		for _, id := range resultArtifactReferences(result) {
			protected[id] = true
		}
	}
	return nil
}
func (k *KernelStore) deleteArtifactObject(ctx context.Context, a domain.Artifact) error {
	root, err := k.openArtifactRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	// No new admission can reference deleting artifacts. Resume an already saved
	// delete intent even when a previous process removed the file before crashing.
	if err = root.Remove(a.ID); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir, err := os.Open(root.Name())
	if err != nil {
		return err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadArtifact(ctx, tx, a.ID)
	if err != nil {
		return err
	}
	if current.State == "expired" {
		return nil
	}
	if current.State != "deleting" {
		return errors.New("Artifact deletion disposition changed")
	}
	current.State, current.ExpiredAt = "expired", k.clock()
	if err = saveArtifact(ctx, tx, current); err != nil {
		return err
	}
	return tx.Commit()
}
