package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

type twoFindingGH struct{ *fakeGH }

func (g twoFindingGH) Reviews(context.Context, int) ([]github.Review, error) {
	return []github.Review{
		{ID: 101, User: github.User{ID: 7}, State: "COMMENTED", CommitID: g.pr.Head.SHA, SubmittedAt: g.requestTime.Add(time.Second), Body: "Finding A: check empty input"},
		{ID: 102, User: github.User{ID: 7}, State: "APPROVED", CommitID: g.pr.Head.SHA, SubmittedAt: g.requestTime.Add(2 * time.Second), Body: "Finding B: check duplicate input"},
	}, nil
}

func TestNoActionCannotResolveUnassessedFeedback(t *testing.T) {
	for _, mode := range []string{"partial", "complete", "duplicate", "unknown", "mixed_ci"} {
		t.Run(mode, func(t *testing.T) {
			c, gh, _, _ := setup(t)
			ctx := context.Background()
			c.GitHub = twoFindingGH{gh}
			if mode == "mixed_ci" {
				c.Config.RequiredChecks = []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}
				if err := c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest()); err != nil {
					t.Fatal(err)
				}
				gh.ciFailFirst = true
			}
			var w domain.WorkItem
			var correction Correction
			for range 15 {
				drive(t, c, 1)
				w, _ = c.Store.Get(ctx, "w")
				if w.Phase == domain.Verifying && c.Store.Record(ctx, "correction", w.ID, &correction) == nil && len(correction.FeedbackIDs) == 2 {
					break
				}
			}
			if len(correction.FeedbackIDs) != 2 {
				t.Fatal("missing review batch", correction)
			}
			ops, err := c.Store.Operations(ctx)
			if err != nil || len(ops) != 1 {
				t.Fatal(ops, err)
			}
			op := ops[0]
			if ok, err := c.Store.BeginDispatch(ctx, op.ID); !ok || err != nil {
				t.Fatal(ok, err)
			}
			candidate, err := c.candidate(ctx, w)
			if err != nil {
				t.Fatal(err)
			}
			ids := append([]string{}, correction.EvidenceIDs...)
			switch mode {
			case "partial":
				ids = ids[:1]
			case "duplicate":
				ids[1] = ids[0]
			case "unknown":
				ids[1] = "unknown"
			case "mixed_ci":
				if len(ids) != 3 {
					t.Fatal("missing CI evidence", correction)
				}
			}
			a := verification.Assessment{SchemaVersion: 1, WorkItemID: w.ID, CandidateID: candidate.ID, ScopeRevision: w.ScopeRevision, Classification: "no_action", EvidenceIDs: ids, Reason: "no defect in assessed evidence", ExpectationBasis: "AC1", TestChanges: []string{}}
			body, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.assessment(ctx, w, op, candidate, correction, body); err != nil {
				t.Fatal(err)
			}
			for _, id := range correction.FeedbackIDs {
				var resolved map[string]string
				err = c.Store.Record(ctx, "resolved_feedback", id, &resolved)
				if mode == "complete" {
					if err != nil || resolved["assessment"] != op.ID {
						t.Fatal("missing resolution evidence", resolved, err)
					}
				} else if !missing(err) {
					t.Fatal("unassessed or mixed evidence resolved", id, resolved, err)
				}
			}
			drive(t, c, 4)
			if mode == "complete" {
				if gh.merges != 1 {
					t.Fatal("complete assessment did not allow merge", gh.merges)
				}
			} else if gh.merges != 0 {
				t.Fatal("unresolved feedback allowed merge", gh.merges)
			}
		})
	}
}
