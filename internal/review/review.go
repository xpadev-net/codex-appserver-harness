// Package review normalizes only observed, identity-bound Cloud review results.
package review

import (
	"crypto/sha256"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"sort"
	"strings"
	"time"
)

type Round struct {
	Number           int              `json:"number"`
	ID               string           `json:"id"`
	Candidate        runner.Candidate `json:"candidate"`
	PR               int              `json:"pr"`
	RequestedAt      time.Time        `json:"requested_at"`
	RequestCommentID int64            `json:"request_comment_id"`
	RequestAttempted bool             `json:"request_attempted"`
	RequesterID      int64            `json:"requester_id"`
}
type Policy struct {
	VerifiedRequest        bool    `json:"verified_request"`
	VerifiedNativeApproval bool    `json:"verified_native_approval"`
	VerifiedSHAMapping     bool    `json:"verified_sha_mapping"`
	TrustedReviewerIDs     []int64 `json:"trusted_reviewer_ids"`
	RequiredApprovals      int     `json:"required_approvals"`
}
type Decision struct {
	Approved  bool    `json:"approved"`
	Blocking  bool    `json:"blocking"`
	Reason    string  `json:"reason"`
	ReviewIDs []int64 `json:"review_ids"`
}

func Evaluate(c runner.Candidate, round Round, p Policy, reviews []github.Review) Decision {
	d := Decision{Reason: "formal approval absent", ReviewIDs: []int64{}}
	if !p.VerifiedRequest || !p.VerifiedNativeApproval || !p.VerifiedSHAMapping || p.RequiredApprovals < 1 || len(p.TrustedReviewerIDs) == 0 || round.Candidate != c || round.RequestedAt.IsZero() || round.RequestCommentID == 0 || round.RequesterID == 0 {
		return Decision{Reason: "unverified review contract or stale round"}
	}
	trusted := map[int64]bool{}
	for _, id := range p.TrustedReviewerIDs {
		if id > 0 {
			trusted[id] = true
		}
	}
	ordered := append([]github.Review{}, reviews...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].SubmittedAt.Equal(ordered[j].SubmittedAt) {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].SubmittedAt.Before(ordered[j].SubmittedAt)
	})
	latest := map[int64]github.Review{}
	for _, r := range ordered {
		switch r.State {
		case "APPROVED", "CHANGES_REQUESTED", "DISMISSED":
			latest[r.User.ID] = r
		}
	}
	count := 0
	for id, r := range latest {
		if r.State == "CHANGES_REQUESTED" {
			d.Blocking = true
		}
		if trusted[id] && r.State == "APPROVED" && r.CommitID == c.SHA && !r.SubmittedAt.Before(round.RequestedAt) {
			count++
			d.ReviewIDs = append(d.ReviewIDs, r.ID)
		}
	}
	d.Approved = count >= p.RequiredApprovals && !d.Blocking
	if d.Approved {
		d.Reason = "current candidate has explicit trusted approval"
	}
	return d
}

type Finding struct {
	ID       string `json:"id"`
	Body     string `json:"body"`
	ReviewID int64  `json:"review_id"`
}

func Findings(c runner.Candidate, round Round, p Policy, reviews []github.Review, comments []github.Comment) []Finding {
	out := []Finding{}
	if round.Candidate != c {
		return out
	}
	trusted := map[int64]bool{}
	for _, id := range p.TrustedReviewerIDs {
		trusted[id] = true
	}
	valid := map[int64]bool{}
	seen := map[string]bool{}
	for _, r := range reviews {
		if trusted[r.User.ID] && r.CommitID == c.SHA && !r.SubmittedAt.Before(round.RequestedAt) && (r.State == "COMMENTED" || r.State == "CHANGES_REQUESTED" || r.State == "APPROVED") {
			valid[r.ID] = true
			if !approvalOnly(r.Body) {
				key := fmt.Sprintf("review:%d:%x", r.ID, sha256.Sum256([]byte(r.Body)))
				if !seen[key] {
					out = append(out, Finding{key, r.Body, r.ID})
					seen[key] = true
				}
			}
		}
	}
	for _, comment := range comments {
		if trusted[comment.User.ID] && valid[comment.ReviewID] && comment.CommitID == c.SHA && comment.Body != "" {
			key := fmt.Sprintf("comment:%d:%s:%x", comment.ID, comment.UpdatedAt.UTC().Format(time.RFC3339Nano), sha256.Sum256([]byte(comment.Body)))
			if !seen[key] {
				out = append(out, Finding{key, comment.Body, comment.ReviewID})
				seen[key] = true
			}
		}
	}
	return out
}

// This narrow filter classifies only exact non-defect boilerplate. It never
// grants approval; COMMENTED still fails Evaluate even when its body is clean.
func approvalOnly(body string) bool {
	switch strings.ToLower(strings.Trim(strings.TrimSpace(body), ".!")) {
	case "", "lgtm", "looks good to me", "no issues found", "no findings", "approved":
		return true
	}
	return false
}
