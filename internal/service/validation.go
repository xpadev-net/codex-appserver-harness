package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"os"
	"path/filepath"
	"time"
)

func (c *Controller) validation(ctx context.Context, w domain.WorkItem, op domain.Operation) error {
	candidate, err := c.candidate(ctx, w)
	if err != nil {
		return err
	}
	interim := op.Kind == "validate_interim"
	var plan verification.Plan
	var profiles []string
	if interim {
		var request InterimRequest
		if err = c.Store.Record(ctx, "interim_request", w.ID, &request); err != nil {
			return err
		}
		profiles, err = c.Config.Verification.ExecutionSet(request.Profiles)
	} else {
		if err = c.Store.Record(ctx, "plan", w.ID, &plan); err != nil {
			return err
		}
		profiles, err = plan.Profiles(c.Config.Verification)
	}
	if err != nil {
		return err
	}
	results := []runner.Result{}
	for _, profile := range profiles {
		suffix := sha256.Sum256([]byte(op.ID))
		key := fmt.Sprintf("%s-%s-%x", candidate.ID, profile, suffix[:4])
		var intent JobIntent
		err = c.Store.Record(ctx, "job_intent", key, &intent)
		if err != nil && !missing(err) {
			return err
		}
		if err != nil {
			busy, err := c.runnerBusy(ctx, "")
			if err != nil {
				return err
			}
			if busy {
				return nil
			}
			latest, err := c.Store.Get(ctx, w.ID)
			if err != nil {
				return err
			}
			if err = c.jobBudget(ctx, w.ID); err != nil {
				return c.reject(ctx, w, op, err.Error())
			}
			if err = c.artifactBudget(); err != nil {
				return c.reject(ctx, w, op, err.Error())
			}
			if latest.Terminal() {
				return c.Store.FinishOperation(ctx, op.ID, "canceled", "no new job after cancellation")
			}
			if !c.Config.IsolationVerified {
				return c.reject(ctx, w, op, "runner isolation is unverified")
			}
			intent = JobIntent{Spec: runner.Spec{JobID: key, Repository: c.Config.RepositoryPath, Root: filepath.Join(c.Config.Root, "jobs"), Candidate: candidate, ProfileID: profile, Profile: c.Config.Verification.Profiles[profile], Launcher: c.Config.Launcher, ForbiddenPaths: forbiddenPaths(c.Config)}, Started: time.Now().UTC()}
			if err = c.Store.Put(ctx, "job_intent", key, w.ID, intent); err != nil {
				return err
			}
			if err = c.Jobs.Start(ctx, intent.Spec); err != nil {
				return err
			}
			intent.Launched = true
			if err = c.Store.Put(context.WithoutCancel(ctx), "job_intent", key, w.ID, intent); err != nil {
				return err
			}
		}
		result, readErr := c.Jobs.Read(intent.Spec)
		if os.IsNotExist(readErr) {
			if time.Since(intent.Started) > time.Duration(intent.Spec.Profile.TimeoutSeconds+300)*time.Second {
				return c.reject(ctx, w, op, "runner result unknown; reconcile orphan job before retry")
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if result.JobID != intent.Spec.JobID || result.Candidate != candidate || (result.TestedSHA != candidate.SHA && !(result.Status == "infrastructure_failure" && result.TestedSHA == "")) || result.ProfileID != profile {
			return c.reject(ctx, w, op, "runner evidence identity mismatch")
		}
		if err = c.Store.Put(ctx, "validation", result.JobID, w.ID, result); err != nil {
			return err
		}
		latest, err := c.Store.Get(ctx, w.ID)
		if err != nil {
			return err
		}
		if latest.Terminal() {
			return c.done(ctx, op, result)
		}
		if result.Status == "infrastructure_failure" && intent.Attempt < c.Config.Limits.RunnerRetries {
			if err = c.jobBudget(ctx, w.ID); err != nil {
				return c.reject(ctx, w, op, err.Error())
			}
			intent.Attempt++
			intent.Spec.JobID = fmt.Sprintf("%s-retry%d", key, intent.Attempt)
			intent.Started = time.Now().UTC()
			intent.Launched = false
			if err = c.Store.Put(ctx, "job_intent", key, w.ID, intent); err != nil {
				return err
			}
			if err = c.Jobs.Start(ctx, intent.Spec); err != nil {
				return err
			}
			intent.Launched = true
			return c.Store.Put(context.WithoutCancel(ctx), "job_intent", key, w.ID, intent)
		}
		results = append(results, result)
		if result.Status != "passed" {
			if interim && result.Status == "failed" {
				return c.finishInterim(ctx, w, op, results)
			}
			if result.Status != "failed" {
				return c.reject(ctx, w, op, "runner "+result.Status+": "+result.Reason)
			}
			correction := Correction{Reason: "classify validation failure", EvidenceIDs: []string{result.JobID}, FeedbackIDs: []string{}, Mode: "assessment"}
			if err = c.Store.Put(ctx, "correction", w.ID, w.ID, correction); err != nil {
				return err
			}

			latest, _ := c.Store.Get(ctx, w.ID)
			return c.complete(ctx, latest, op, result, "repair_tests", latest.Candidate, "validation failure requires classification")
		}
	}
	if interim {
		return c.finishInterim(ctx, w, op, results)
	}
	if !verification.PassedPlan(candidate, c.Config.Verification, plan, results) {
		return c.reject(ctx, w, op, "required profile set is incomplete")
	}

	latest, err := c.Store.Get(ctx, w.ID)
	if err != nil {
		return err
	}
	return c.complete(ctx, latest, op, results, "validation_passed", latest.Candidate, "required external validations passed")
}
func profileIDs(v policy.Verification) []string {
	ids := []string{}
	for id := range v.Profiles {
		ids = append(ids, id)
	}
	return ids
}

func (c *Controller) runnerBusy(ctx context.Context, except string) (bool, error) {
	raw, err := c.Store.Records(ctx, "job_intent", "")
	if err != nil {
		return false, err
	}
	for _, data := range raw {
		var intent JobIntent
		if err = json.Unmarshal(data, &intent); err != nil {
			return false, err
		}
		if intent.Spec.JobID == except {
			continue
		}
		if _, err = c.Jobs.Read(intent.Spec); missing(err) {
			return true, nil
		} else if err != nil {
			return false, err
		}
	}
	return false, nil
}

type InterimRequest struct {
	Profiles []string `json:"profiles"`
	Reason   string   `json:"reason"`
}

func (c *Controller) finishInterim(ctx context.Context, w domain.WorkItem, op domain.Operation, results []runner.Result) error {
	ids := []string{}
	for _, r := range results {
		ids = append(ids, r.JobID)
	}
	correction := Correction{Reason: "interim runner completed; use the recorded evidence to continue implementation", EvidenceIDs: ids, FeedbackIDs: []string{}, Mode: "repair"}
	if err := c.Store.Put(ctx, "correction", w.ID, w.ID, correction); err != nil {
		return err
	}
	return c.complete(ctx, w, op, results, "interim_complete", w.Candidate, "requested interim validation completed")
}
func (c *Controller) jobBudget(ctx context.Context, id string) error {
	raw, err := c.Store.Records(ctx, "job_intent", id)
	if err != nil {
		return err
	}
	attempts := 0
	for _, data := range raw {
		var intent JobIntent
		if err = json.Unmarshal(data, &intent); err != nil {
			return err
		}
		attempts += intent.Attempt + 1
	}
	if attempts >= c.Config.Limits.ValidationJobs {
		return fmt.Errorf("validation job budget exhausted (%d)", attempts)
	}
	return nil
}
