package review

import (
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"testing"
	"time"
)

func TestApprovalIdentityAndInvalidation(t *testing.T) {
	now := time.Now().UTC()
	c := runner.Candidate{ID: "c", SHA: "head", BaseSHA: "base", ScopeRevision: 1, PolicyRevision: 1}
	round := Round{ID: "r", Candidate: c, RequestedAt: now, RequestCommentID: 1, RequesterID: 2}
	p := Policy{true, true, true, []int64{7}, 1}
	approved := github.Review{ID: 1, User: github.User{ID: 7}, State: "APPROVED", CommitID: "head", SubmittedAt: now.Add(time.Second)}
	for _, v := range []struct {
		name   string
		modify func(*github.Review)
	}{{"comment", func(r *github.Review) { r.State = "COMMENTED" }}, {"old sha", func(r *github.Review) { r.CommitID = "old" }}, {"wrong identity", func(r *github.Review) { r.User.ID = 8 }}, {"old round", func(r *github.Review) { r.SubmittedAt = now.Add(-time.Second) }}, {"dismissed", func(r *github.Review) { r.State = "DISMISSED" }}} {
		t.Run(v.name, func(t *testing.T) {
			r := approved
			v.modify(&r)
			if Evaluate(c, round, p, []github.Review{r}).Approved {
				t.Fatal("invalid approval accepted")
			}
		})
	}
	if !Evaluate(c, round, p, []github.Review{approved}).Approved {
		t.Fatal("valid approval rejected")
	}
	later := approved
	later.ID = 2
	later.State = "CHANGES_REQUESTED"
	later.SubmittedAt = now.Add(2 * time.Second)
	if Evaluate(c, round, p, []github.Review{later, approved}).Approved {
		t.Fatal("out of order change request ignored")
	}
	p.VerifiedNativeApproval = false
	if Evaluate(c, round, p, []github.Review{approved}).Approved {
		t.Fatal("unverified capability accepted")
	}
}
func TestFindingsAggregatedAndDeduplicated(t *testing.T) {
	now := time.Now()
	c := runner.Candidate{ID: "c", SHA: "head"}
	round := Round{Candidate: c, RequestedAt: now}
	p := Policy{TrustedReviewerIDs: []int64{7}}
	reviews := []github.Review{{ID: 1, User: github.User{ID: 7}, State: "COMMENTED", CommitID: "head", SubmittedAt: now}}
	comment := github.Comment{ID: 10, User: github.User{ID: 7}, Body: "defect", CommitID: "head", ReviewID: 1, UpdatedAt: now}
	got := Findings(c, round, p, reviews, []github.Comment{comment, comment})
	if len(got) != 1 {
		t.Fatal(got)
	}
	comment.CommitID = "old"
	if len(Findings(c, round, p, reviews, []github.Comment{comment})) != 0 {
		t.Fatal("old finding accepted")
	}
}
