// Package github uses the authenticated gh CLI, without exposing tokens to workers.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

type Client struct {
	BeforeWrite  func(context.Context, string, string) error
	Repository   string
	RepositoryID int64
}

func (c Client) valid() error {
	if c.RepositoryID <= 0 || !regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`).MatchString(c.Repository) {
		return errors.New("invalid repository")
	}
	return nil
}
func (c Client) API(ctx context.Context, method, endpoint string, body any, out any) error {
	if err := c.valid(); err != nil {
		return err
	}
	if method != "GET" {
		metadata, err := exec.CommandContext(ctx, "gh", "api", "repos/"+c.Repository).Output()
		if err != nil {
			return fmt.Errorf("%w: repository identity unavailable", contract.ErrUnsent)
		}
		var repo struct {
			ID int64 `json:"id"`
		}
		if json.Unmarshal(metadata, &repo) != nil || repo.ID != c.RepositoryID {
			return fmt.Errorf("%w: repository identity changed", contract.ErrUnsent)
		}
		if c.BeforeWrite == nil {
			return fmt.Errorf("%w: operation guard required", contract.ErrUnsent)
		}
		if err = c.BeforeWrite(ctx, method, endpoint); err != nil {
			return fmt.Errorf("%w: %v", contract.ErrUnsent, err)
		}
	}
	args := []string{"api", "--method", method, "repos/" + c.Repository + "/" + endpoint}
	var stdin bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&stdin).Encode(body); err != nil {
			return err
		}
		args = append(args, "--input", "-")
	}
	cmd := exec.CommandContext(ctx, "gh", args...)
	cmd.Stdin = &stdin
	b, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("GitHub %s %s failed or outcome unknown: %w", method, endpoint, err)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}
func (c Client) Pages(ctx context.Context, endpoint string, out any) error {
	if err := c.valid(); err != nil {
		return err
	}
	b, err := exec.CommandContext(ctx, "gh", "api", "--paginate", "--slurp", "repos/"+c.Repository+"/"+endpoint).Output()
	if err != nil {
		return fmt.Errorf("GitHub pagination failed: %w", err)
	}
	var pages [][]json.RawMessage
	if err = json.Unmarshal(b, &pages); err != nil {
		return err
	}
	flat := []json.RawMessage{}
	for _, p := range pages {
		flat = append(flat, p...)
	}
	b, err = json.Marshal(flat)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

type Ref struct {
	SHA  string `json:"sha"`
	Ref  string `json:"ref"`
	Repo struct {
		ID int64 `json:"id"`
	} `json:"repo"`
}
type PR struct {
	MergeCommitSHA string `json:"merge_commit_sha"`
	Number         int    `json:"number"`
	State          string `json:"state"`
	Draft          bool   `json:"draft"`
	Merged         bool   `json:"merged"`
	Head           Ref    `json:"head"`
	Base           Ref    `json:"base"`
	Body           string `json:"body"`
	MergeableState string `json:"mergeable_state"`
}

func (c Client) Pull(ctx context.Context, number int) (PR, error) {
	var p PR
	err := c.API(ctx, "GET", fmt.Sprintf("pulls/%d", number), nil, &p)
	return p, err
}
func Marker(workItem string) string { return "<!-- harness-work-item:" + workItem + " -->" }
func (c Client) FindPR(ctx context.Context, branch, workItem string) (*PR, error) {
	var all []PR
	if err := c.Pages(ctx, "pulls?state=all&per_page=100", &all); err != nil {
		return nil, err
	}
	var found *PR
	for _, p := range all {
		if p.Head.Ref == branch && p.Head.Repo.ID == c.RepositoryID && strings.Contains(p.Body, Marker(workItem)) {
			if found != nil {
				return nil, errors.New("multiple matching PRs require operator reconciliation")
			}
			copy := p
			found = &copy
		}
	}
	return found, nil
}
func (c Client) Publish(ctx context.Context, base, branch, workItem, title, body string) (PR, error) {
	var p PR
	if err := CheckClosingText(title + "\n" + body); err != nil {
		return p, err
	}
	if base == "" || branch == "" || workItem == "" || strings.ContainsAny(workItem, "<>\r\n") {
		return p, errors.New("invalid PR identity")
	}
	found, err := c.FindPR(ctx, branch, workItem)
	if err != nil {
		return p, err
	}
	body = Marker(workItem) + "\n" + body
	if found != nil {
		if found.State != "open" || found.Base.Ref != base {
			return p, errors.New("matching PR is closed or base changed")
		}
		err = c.API(ctx, "PATCH", fmt.Sprintf("pulls/%d", found.Number), map[string]string{"title": title, "body": body}, &p)
		return p, err
	}
	err = c.API(ctx, "POST", "pulls", map[string]string{"base": base, "head": branch, "title": title, "body": body}, &p)
	return p, err
}

type User struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}
type Review struct {
	ID          int64     `json:"id"`
	User        User      `json:"user"`
	State       string    `json:"state"`
	CommitID    string    `json:"commit_id"`
	SubmittedAt time.Time `json:"submitted_at"`
	Body        string    `json:"body"`
}
type Comment struct {
	ID        int64     `json:"id"`
	User      User      `json:"user"`
	Body      string    `json:"body"`
	CommitID  string    `json:"commit_id"`
	ReviewID  int64     `json:"pull_request_review_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (c Client) Reviews(ctx context.Context, number int) ([]Review, error) {
	var r []Review
	err := c.Pages(ctx, fmt.Sprintf("pulls/%d/reviews?per_page=100", number), &r)
	return r, err
}
func (c Client) Comments(ctx context.Context, number int) ([]Comment, error) {
	var r []Comment
	err := c.Pages(ctx, fmt.Sprintf("pulls/%d/comments?per_page=100", number), &r)
	return r, err
}
func (c Client) RequestReview(ctx context.Context, number int, roundID, trigger string, requesterID int64) (int64, error) {
	if requesterID <= 0 || roundID == "" || strings.ContainsAny(roundID, "<>\r\n") || trigger == "" {
		return 0, errors.New("invalid review request")
	}
	marker := "<!-- harness-review:" + roundID + " -->"
	var created Comment
	err := c.API(ctx, "POST", fmt.Sprintf("issues/%d/comments", number), map[string]string{"body": trigger + "\n\n" + marker}, &created)
	if err == nil && created.User.ID != requesterID {
		return 0, errors.New("review requester identity mismatch")
	}
	return created.ID, err
}

// Merge supplies an expected head and uses the ordinary merge endpoint. No admin or
// bypass flag is available. Acknowledgement alone never marks the WorkItem merged.
func (c Client) Merge(ctx context.Context, number int, expectedHead, method string) error {
	if number < 1 || !regexp.MustCompile(`^[0-9a-f]{40,64}$`).MatchString(expectedHead) {
		return errors.New("merge requires PR and exact expected SHA")
	}
	if method != "squash" && method != "merge" && method != "rebase" {
		return errors.New("invalid merge method")
	}
	return c.API(ctx, "PUT", fmt.Sprintf("pulls/%d/merge", number), map[string]string{"sha": expectedHead, "merge_method": method}, nil)
}

type Check struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	HeadSHA    string `json:"head_sha"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	App        struct {
		ID int64 `json:"id"`
	} `json:"app"`
}

func (c Client) Checks(ctx context.Context, ref string) ([]Check, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	if !regexp.MustCompile(`^[0-9a-f]{40,64}$`).MatchString(ref) {
		return nil, errors.New("invalid check ref")
	}
	var checks []Check
	// gh's --jq extracts each page's collection before slurp.
	b, err := exec.CommandContext(ctx, "gh", "api", "--paginate", "--slurp", "repos/"+c.Repository+"/commits/"+ref+"/check-runs?per_page=100").Output()
	if err != nil {
		return nil, err
	}
	var pages []struct {
		Checks []Check `json:"check_runs"`
	}
	if err = json.Unmarshal(b, &pages); err != nil {
		return nil, err
	}
	for _, p := range pages {
		checks = append(checks, p.Checks...)
	}
	return checks, nil
}
func (c Client) Conditions(ctx context.Context, number int) (string, error) {
	if err := c.valid(); err != nil {
		return "", err
	}
	b, err := exec.CommandContext(ctx, "gh", "pr", "view", fmt.Sprint(number), "--repo", c.Repository, "--json", "mergeStateStatus").Output()
	if err != nil {
		return "", err
	}
	var p struct {
		State string `json:"mergeStateStatus"`
	}
	err = json.Unmarshal(b, &p)
	return p.State, err
}

// CheckClosingText rejects GitHub closing-keyword references in partial PR text
// and commit messages; residual work must not close its parent automatically.
func CheckClosingText(text string) error {
	if regexp.MustCompile(`(?i)\b(close[sd]?|fix(e[sd])?|resolve[sd]?)\s*:?[ \t\r\n]*((https://github\.com/[^\s]+/issues/\d+)|([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#\d+)`).MatchString(text) {
		return errors.New("closing-keyword reference requires removal from partial PR")
	}
	return nil
}

type Issue struct {
	Number      int             `json:"number"`
	Title       string          `json:"title"`
	Body        string          `json:"body"`
	State       string          `json:"state"`
	UpdatedAt   time.Time       `json:"updated_at"`
	PullRequest json.RawMessage `json:"pull_request,omitempty"`
	Labels      []struct {
		Name string `json:"name"`
	} `json:"labels"`
}

// Pages invokes gh --paginate --slurp: per_page is a page size, not a total cap.
func (c Client) FindChild(ctx context.Context, marker string) (int, error) {
	var issues []Issue
	if err := c.Pages(ctx, "issues?state=all&per_page=100", &issues); err != nil {
		return 0, err
	}
	found := 0
	for _, issue := range issues {
		if len(issue.PullRequest) == 0 && strings.Contains(issue.Body, "<!-- "+marker+" -->") {
			if found != 0 {
				return 0, errors.New("duplicate split child markers")
			}
			found = issue.Number
		}
	}
	return found, nil
}
func (c Client) CreateChild(ctx context.Context, title, body, marker string) (int, error) {
	if marker == "" || strings.ContainsAny(marker, "<>\r\n") {
		return 0, errors.New("invalid child marker")
	}
	var issue Issue
	err := c.API(ctx, "POST", "issues", map[string]string{"title": title, "body": "<!-- " + marker + " -->\n" + body}, &issue)
	return issue.Number, err
}

func (c Client) GetIssue(ctx context.Context, number int) (Issue, error) {
	var issue Issue
	err := c.API(ctx, "GET", fmt.Sprintf("issues/%d", number), nil, &issue)
	return issue, err
}
func (c Client) Pool(ctx context.Context) ([]Issue, error) {
	var issues []Issue
	err := c.Pages(ctx, "issues?state=open&per_page=100", &issues)
	return issues, err
}
func (c Client) CloseIssue(ctx context.Context, number int) error {
	return c.API(ctx, "PATCH", fmt.Sprintf("issues/%d", number), map[string]string{"state": "closed", "state_reason": "completed"}, nil)
}
func (c Client) FindReviewRequest(ctx context.Context, number int, roundID string, requesterID int64) (int64, error) {
	var comments []Comment
	if err := c.Pages(ctx, fmt.Sprintf("issues/%d/comments?per_page=100", number), &comments); err != nil {
		return 0, err
	}
	marker := "<!-- harness-review:" + roundID + " -->"
	var found int64
	for _, comment := range comments {
		if comment.User.ID == requesterID && strings.Contains(comment.Body, marker) {
			if found != 0 {
				return 0, errors.New("duplicate review request markers")
			}
			found = comment.ID
		}
	}
	return found, nil
}
func (c Client) CommitParents(ctx context.Context, sha string) ([]string, error) {
	if !regexp.MustCompile(`^[0-9a-f]{40,64}$`).MatchString(sha) {
		return nil, errors.New("invalid commit SHA")
	}
	var result struct {
		Parents []struct {
			SHA string `json:"sha"`
		} `json:"parents"`
	}
	if err := c.API(ctx, "GET", "git/commits/"+sha, nil, &result); err != nil {
		return nil, err
	}
	parents := []string{}
	for _, parent := range result.Parents {
		parents = append(parents, parent.SHA)
	}
	return parents, nil
}
