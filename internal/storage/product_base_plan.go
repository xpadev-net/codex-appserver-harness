package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

var ErrProductBasePlanHeld = errors.New("product base requires current task inputs and fresh fixed repository head")

type ProductBaseHead struct {
	RepositoryID int64    `json:"repository_id"`
	Repository   string   `json:"repository"`
	BaseBranch   string   `json:"base_branch"`
	InitialSHA   string   `json:"initial_sha"`
	HeadSHA      string   `json:"head_sha"`
	Remote       string   `json:"remote"`
	Ancestors    []string `json:"ancestors"`
}
type ProductBaseObservation struct {
	Head       ProductBaseHead `json:"head"`
	ObservedAt time.Time       `json:"observed_at"`
}
type ProductBasePlanValue struct {
	Input ProductBaseInput `json:"input"`
	Head  ProductBaseHead  `json:"head"`
}
type ProductBasePlan struct {
	ID        string                 `json:"id"`
	Value     ProductBasePlanValue   `json:"value"`
	Digest    string                 `json:"digest"`
	Admission ProductBaseObservation `json:"admission"`
	CreatedAt time.Time              `json:"created_at"`
}
type ProductBaseSelection struct {
	TaskID      string                           `json:"task_id"`
	Plan        ProductVerificationPlanReference `json:"plan"`
	Observation ProductBaseObservation           `json:"observation"`
}

func basePlanIdentity(v ProductBasePlanValue) (string, string, error) {
	digest, err := contentDigest(v)
	return structureID("product-base-plan-", v.Input.Value.Preparation.Task.ID, digest), digest, err
}
func baseSelectionIdentity(s ProductBaseSelection) (string, error) {
	digest, err := contentDigest(s)
	return structureID("product-base-selection-", s.TaskID, digest), err
}
func validateBasePlanValue(v ProductBasePlanValue) error {
	encoded, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(encoded) > 1<<20 {
		return ErrProductBasePlanHeld
	}
	input, h := v.Input, v.Head
	prep := input.Value.Preparation
	if current := prep.Product; current != nil && (current.Value.WorkItem.RepositoryID != h.RepositoryID || current.Value.Repository != h.Repository || current.Value.BaseBranch != h.BaseBranch) {
		return ErrProductBasePlanHeld
	}
	digest, err := contentDigest(input.Value)
	if err != nil {
		return err
	}
	ancestors, err := baseInputAncestors(input, h.InitialSHA)
	if err != nil {
		return err
	}
	if digest != input.Digest || !prep.Task.Target().Valid() || prep.Task.ID == "" || prep.Task.SessionID == "" || prep.Registration.Value.WaveID != prep.Task.WaveID || h.RepositoryID != prep.Registration.Value.Definition.RepositoryID || h.Repository != prep.Registration.Value.Definition.Repository || !fixedSHA(h.HeadSHA) || h.BaseBranch == "" || len(h.BaseBranch) > 1024 || strings.ContainsAny(h.BaseBranch, "\x00\r\n\t ") || len(h.Remote) > 4096 || h.Remote == "" || !slices.Equal(h.Ancestors, ancestors) {
		return ErrTaskEvidence
	}
	raw, err := contentDigest(input.Value.Policy.Value)
	if err != nil {
		return err
	}
	if raw != input.Value.Policy.Digest || input.Value.Policy.Value.RepositoryID != h.RepositoryID {
		return ErrTaskEvidence
	}
	return nil
}
func (k *KernelStore) readProductBasePlanTx(ctx context.Context, q queryer, id string) (plan ProductBasePlan, retErr error) {
	err := taskEvidenceRecord(ctx, q, "product_base_plan", id, &plan)
	if errors.Is(err, sql.ErrNoRows) {
		var known bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_base_plan_history','product_base_plan_admission'))", id).Scan(&known); err != nil {
			return plan, err
		}
		if known {
			return plan, ErrTaskEvidence
		}
		return plan, sql.ErrNoRows
	}
	if err != nil {
		return plan, err
	}
	defer func() {
		if errors.Is(retErr, sql.ErrNoRows) {
			retErr = errors.Join(ErrTaskEvidence, retErr)
		}
	}()
	var history ProductBasePlan
	var admitted string
	if err := taskEvidenceRecord(ctx, q, "product_base_plan_history", id, &history); err != nil {
		return plan, err
	}
	if err := taskEvidenceRecord(ctx, q, "product_base_plan_admission", id, &admitted); err != nil {
		return plan, err
	}
	want, digest, err := basePlanIdentity(plan.Value)
	if err != nil {
		return plan, err
	}
	if id != want || plan.ID != id || plan.Digest != digest || admitted != digest || !reflect.DeepEqual(plan, history) || plan.CreatedAt.IsZero() || plan.CreatedAt.After(k.clock()) || plan.Admission.ObservedAt.IsZero() || plan.Admission.ObservedAt.After(plan.CreatedAt) || plan.CreatedAt.Sub(plan.Admission.ObservedAt) > 30*time.Second || !reflect.DeepEqual(plan.Admission.Head, plan.Value.Head) {
		return plan, ErrTaskEvidence
	}
	return plan, validateBasePlanValue(plan.Value)
}
func (k *KernelStore) ReadProductBasePlan(ctx context.Context, id string) (ProductBasePlan, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductBasePlan{}, err
	}
	defer tx.Rollback()
	return k.readProductBasePlanTx(ctx, tx, id)
}
func (k *KernelStore) readProductBaseSelectionTx(ctx context.Context, q queryer, taskID string) (ProductBaseSelection, ProductBasePlan, error) {
	var selected ProductBaseSelection
	var plan ProductBasePlan
	if err := taskEvidenceRecord(ctx, q, "product_base_current", taskID, &selected); err != nil {
		return selected, plan, err
	}
	id, err := baseSelectionIdentity(selected)
	if err != nil {
		return selected, plan, err
	}
	var history ProductBaseSelection
	if err := taskEvidenceRecord(ctx, q, "product_base_selection_history", id, &history); err != nil {
		return selected, plan, errors.Join(ErrTaskEvidence, err)
	}
	plan, err = k.readProductBasePlanTx(ctx, q, selected.Plan.ID)
	if err != nil {
		return selected, plan, errors.Join(ErrTaskEvidence, err)
	}
	if selected.TaskID != taskID || plan.Value.Input.Value.Preparation.Task.ID != taskID || selected.Plan.Digest != plan.Digest || !reflect.DeepEqual(selected, history) || !reflect.DeepEqual(selected.Observation.Head, plan.Value.Head) || selected.Observation.ObservedAt.Before(plan.Admission.ObservedAt) || selected.Observation.ObservedAt.After(k.clock()) {
		return selected, plan, ErrTaskEvidence
	}
	return selected, plan, nil
}

// PrepareProductBasePlan accepts only a trusted reader's bookended observation.
// It grants no Git/model start and never reconstructs a lost plan or selection.
func (k *KernelStore) PrepareProductBasePlan(ctx context.Context, taskID, inputDigest string, observation ProductBaseObservation) (ProductBasePlan, error) {
	var empty ProductBasePlan
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	input, err := k.productBaseInputTx(ctx, tx, taskID)
	if err != nil {
		return empty, err
	}
	if input.Digest != inputDigest || observation.ObservedAt.IsZero() || observation.ObservedAt.After(k.clock()) || k.clock().Sub(observation.ObservedAt) > 30*time.Second {
		return empty, ErrProductBasePlanHeld
	}
	value := ProductBasePlanValue{Input: input, Head: observation.Head}
	if err := validateBasePlanValue(value); err != nil {
		return empty, err
	}
	if err := workspace.ValidateCommitFetchRemote(value.Head.Remote); err != nil {
		return empty, err
	}
	old, _, err := k.readProductBaseSelectionTx(ctx, tx, taskID)
	if err != nil {
		if err != sql.ErrNoRows {
			return empty, err
		}
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE work_item_id=? AND kind IN ('product_base_selection_history','product_base_plan_history') AND (json_extract(payload,'$.task_id')=? OR json_extract(payload,'$.value.input.value.preparation.task.id')=?))", input.Value.Preparation.Task.WaveID, taskID, taskID).Scan(&known); err != nil {
			return empty, err
		}
		if known {
			return empty, ErrTaskEvidence
		}
	} else if observation.ObservedAt.Before(old.Observation.ObservedAt) {
		return empty, ErrProductBasePlanHeld
	}
	id, digest, err := basePlanIdentity(value)
	if err != nil {
		return empty, err
	}
	plan, err := k.readProductBasePlanTx(ctx, tx, id)
	if err == sql.ErrNoRows {
		plan = ProductBasePlan{ID: id, Value: value, Digest: digest, Admission: observation, CreatedAt: k.clock()}
		for kind, record := range map[string]any{"product_base_plan": plan, "product_base_plan_history": plan, "product_base_plan_admission": digest} {
			if err := kernelRecordTx(ctx, tx, kind, id, input.Value.Preparation.Task.WaveID, record); err != nil {
				return empty, err
			}
		}
	} else if err != nil {
		return empty, err
	} else if !reflect.DeepEqual(plan.Value, value) {
		return empty, ErrTaskEvidence
	}
	selected := ProductBaseSelection{TaskID: taskID, Plan: ProductVerificationPlanReference{ID: plan.ID, Digest: plan.Digest}, Observation: observation}
	sid, err := baseSelectionIdentity(selected)
	if err != nil {
		return empty, err
	}
	var previous ProductBaseSelection
	err = taskEvidenceRecord(ctx, tx, "product_base_selection_history", sid, &previous)
	if err == sql.ErrNoRows {
		if err := kernelRecordTx(ctx, tx, "product_base_selection_history", sid, input.Value.Preparation.Task.WaveID, selected); err != nil {
			return empty, err
		}
	} else if err != nil {
		return empty, err
	} else if !reflect.DeepEqual(previous, selected) {
		return empty, ErrTaskEvidence
	}
	if err := kernelRecordTx(ctx, tx, "product_base_current", taskID, input.Value.Preparation.Task.WaveID, selected); err != nil {
		return empty, err
	}
	return plan, tx.Commit()
}

// CurrentProductBasePlan is an effect-time check of saved observations. The
// host must independently refresh GitHub/source before an actual mutation.
func (k *KernelStore) CurrentProductBasePlan(ctx context.Context, taskID string) (ProductBasePlan, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductBasePlan{}, err
	}
	defer tx.Rollback()
	return k.currentProductBasePlanTx(ctx, tx, taskID)
}
func (k *KernelStore) currentProductBasePlanTx(ctx context.Context, tx *sql.Tx, taskID string) (ProductBasePlan, error) {
	selected, plan, err := k.readProductBaseSelectionTx(ctx, tx, taskID)
	if err != nil {
		return plan, err
	}
	if k.clock().Sub(selected.Observation.ObservedAt) > 30*time.Second {
		return plan, ErrProductBasePlanHeld
	}
	input, err := k.productBaseInputTx(ctx, tx, taskID)
	if err != nil {
		return plan, err
	}
	if input.Digest != plan.Value.Input.Digest {
		return plan, ErrProductBasePlanHeld
	}
	return plan, nil
}
