package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

type CIEvidence struct {
	ID               string           `json:"id"`
	Candidate        runner.Candidate `json:"candidate"`
	TestedSHA        string           `json:"tested_sha"`
	Check            github.Check     `json:"check"`
	Reference        string           `json:"reference"`
	Diagnostics      string           `json:"diagnostics"`
	DiagnosticsError string           `json:"diagnostics_error,omitempty"`
	Classification   string           `json:"classification"`
	ObservedAt       time.Time        `json:"observed_at"`
}

func (c *Controller) routeCIFailures(ctx context.Context, w domain.WorkItem, s gate.Snapshot, findings ...review.Finding) (bool, error) {
	target := s.Candidate.SHA
	if s.CI.TestedSHA != "" {
		target = s.CI.TestedSHA
	}
	records := map[string]map[string]any{"ci_evidence": {}, "ci_dispatched": {}}
	ids := []string{}
	feedbackIDs := []string{}
	for _, finding := range findings {
		ids = append(ids, finding.ID)
		feedbackIDs = append(feedbackIDs, finding.ID)
	}
	alreadyAssessed := false
	for _, req := range s.RequiredChecks {
		check := gate.LatestCheck(s.Checks, req, target)
		if check == nil || check.Status != "completed" {
			continue
		}
		allowed := false
		for _, conclusion := range req.AllowedConclusions {
			if check.Conclusion == conclusion {
				allowed = true
			}
		}
		if allowed {
			continue
		}
		if check.ID <= 0 {
			return true, c.block(ctx, w, "CI result lacks check identity; reconcile before repair")
		}
		attempt, _ := json.Marshal([]any{w.Candidate, target, check.ID, check.StartedAt, check.CompletedAt, check.Conclusion})
		hash := sha256.Sum256(attempt)
		id := fmt.Sprintf("ci-%x", hash[:16])
		sanitized := *check
		sanitized.Output.Summary = runner.RedactText(sanitized.Output.Summary)
		sanitized.Output.Text = runner.RedactText(sanitized.Output.Text)
		sanitized.DetailsURL = runner.RedactText(sanitized.DetailsURL)
		check = &sanitized
		classification := "requires_assessment"
		switch check.Conclusion {
		case "timed_out", "cancelled", "startup_failure", "stale":
			classification = "infrastructure_failure"
		case "failure":
		default:
			classification = "policy_or_unknown"
		}
		ref := check.DetailsURL
		if ref == "" {
			ref = fmt.Sprintf("https://api.github.com/repos/%s/check-runs/%d", c.Config.Repository, check.ID)
		}
		evidence := CIEvidence{ID: id, Candidate: s.Candidate, TestedSHA: target, Check: *check, Reference: ref, Classification: classification, ObservedAt: time.Now().UTC()}
		if classification != "requires_assessment" {
			if err := c.Store.Put(ctx, "ci_evidence", id, w.ID, evidence); err != nil {
				return true, err
			}
			return true, c.block(ctx, w, fmt.Sprintf("CI %s: %s (%s); inspect %s, resolve environment/policy and rerun CI before retry", classification, check.Name, check.Conclusion, id))
		}
		diagnostics, diagErr := c.GitHub.CheckDiagnostics(ctx, *check)
		evidence.Diagnostics = diagnostics
		if diagErr != nil {
			evidence.DiagnosticsError = diagErr.Error()
		}
		if strings.TrimSpace(diagnostics+check.Output.Summary+check.Output.Text) == "" {
			evidence.Classification = "diagnostics_unavailable"
			if err := c.Store.Put(ctx, "ci_evidence", id, w.ID, evidence); err != nil {
				return true, err
			}
			return true, c.block(ctx, w, "CI diagnostics unavailable; inspect "+id+" and restore authenticated diagnostic access before retry")
		}
		var dispatched bool
		if err := c.Store.Record(ctx, "ci_dispatched", id, &dispatched); err == nil && dispatched {
			alreadyAssessed = true
			continue
		} else if err != nil && !missing(err) {
			return true, err
		}
		ids = append(ids, id)
		records["ci_evidence"][id] = evidence
		records["ci_dispatched"][id] = true
	}
	if len(ids) == 0 {
		if alreadyAssessed {
			return true, c.block(ctx, w, "previously assessed CI attempt still fails; repair or rerun CI before retry")
		}
		return false, nil
	}
	correction := Correction{Reason: fmt.Sprintf("classify current candidate failures using attached CI diagnostics and review findings: %v", findings), EvidenceIDs: ids, FeedbackIDs: feedbackIDs, Mode: "assessment"}
	records["correction"] = map[string]any{w.ID: correction}
	hash := sha256.Sum256([]byte(fmt.Sprint(ids)))
	e := domain.Event{ID: fmt.Sprintf("ci-batch-%x", hash[:16]), WorkItemID: w.ID, Kind: "repair_tests", Candidate: w.Candidate, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Attempt: w.Attempt, Reason: correction.Reason}
	return true, c.Store.EventRecords(ctx, e, records)
}
