package product

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
)

// IssuePool is an intake worker, never a KernelSource.Ready callback. Existing
// Wave scope synchronization and product execution remain separate controllers.
type IssuePool struct {
	Admitter       IssueAdmitter
	PriorityLabels []string
	Attempts       int
	Every          time.Duration
	OnMutation     func()
	Report         func(error)
}

type IssuePoolScan struct {
	Attempted int      `json:"attempted"`
	Admitted  []string `json:"admitted"`
	Held      []int    `json:"held"`
}
type issuePoolCursor struct {
	PolicyDigest string `json:"policy_digest"`
	LastNumber   int    `json:"last_number"`
}

// Validate checks host selection bounds without API calls or mutations.
func (p IssuePool) Validate() error {
	if p.Admitter.Store == nil || p.Admitter.API == nil || p.Admitter.SessionID == "" || p.Attempts < 1 || p.Attempts > 32 || len(p.PriorityLabels) > 16 {
		return errors.New("configured Issue pool and attempts 1..32 required")
	}
	if err := p.Admitter.Policy.Validate(); err != nil {
		return err
	}
	if err := p.Admitter.Limits.Validate(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, label := range p.PriorityLabels {
		if strings.TrimSpace(label) == "" || len(label) > 256 || !utf8.ValidString(label) || strings.ContainsAny(label, "\x00\r\n") || seen[label] {
			return errors.New("bounded distinct Issue priority labels required")
		}
		seen[label] = true
	}
	return nil
}

func (p IssuePool) Scan(ctx context.Context) (IssuePoolScan, error) {
	result := IssuePoolScan{Admitted: []string{}, Held: []int{}}
	if err := p.Validate(); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: p.Admitter.API}
	if err := sourceRepository(ctx, api, p.Admitter.Policy); err != nil {
		return result, err
	}
	items, err := api.ResourcePages(ctx, "issues?state=open&per_page=100", "")
	if err != nil {
		return result, err
	}
	if len(items) > 2048 {
		return result, errors.New("Issue pool exceeds collection bound")
	}
	var eligible []github.Issue
	seen := map[int]bool{}
	for _, raw := range items {
		var issue github.Issue
		if err := json.Unmarshal(raw, &issue); err != nil {
			return result, err
		}
		if issue.Number < 1 || seen[issue.Number] || issue.UpdatedAt.IsZero() || (issue.State != "open" && issue.State != "closed") {
			return result, errors.New("invalid or duplicate Issue pool identity")
		}
		seen[issue.Number] = true
		ready, hold := false, false
		for _, label := range issue.Labels {
			ready = ready || label.Name == p.Admitter.Policy.ReadyLabel
			hold = hold || label.Name == p.Admitter.Policy.HoldLabel
		}
		if issue.State == "open" && len(issue.PullRequest) == 0 && ready && !hold {
			eligible = append(eligible, issue)
		}
	}
	rank := func(issue github.Issue) int {
		for i, wanted := range p.PriorityLabels {
			for _, label := range issue.Labels {
				if label.Name == wanted {
					return i
				}
			}
		}
		return len(p.PriorityLabels)
	}
	sort.Slice(eligible, func(i, j int) bool {
		if a, b := rank(eligible[i]), rank(eligible[j]); a != b {
			return a < b
		}
		return eligible[i].Number < eligible[j].Number
	})
	policy, err := json.Marshal(struct {
		Source   IssueSourcePolicy
		Priority []string
	}{p.Admitter.Policy, p.PriorityLabels})
	if err != nil {
		return result, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(policy))
	key := strconv.FormatInt(p.Admitter.Policy.RepositoryID, 10)
	var cursor issuePoolCursor
	err = p.Admitter.Store.Record(ctx, "product_issue_pool_cursor", key, &cursor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	if cursor.LastNumber < 0 {
		return result, errors.New("invalid Issue pool cursor")
	}
	start := 0
	if cursor.PolicyDigest == digest {
		for i, issue := range eligible {
			if issue.Number == cursor.LastNumber {
				start = i + 1
				break
			}
		}
	}
	admitter := p.Admitter
	admitter.API = api
	for i := 0; i < len(eligible) && result.Attempted < p.Attempts; i++ {
		issue := eligible[(start+i)%len(eligible)]
		var existing string
		err := p.Admitter.Store.DB.QueryRowContext(ctx, "SELECT id FROM kernel_waves WHERE source='github' AND source_id=?", issueSourceID(p.Admitter.Policy.RepositoryID, issue.Number)).Scan(&existing)
		if err == nil {
			continue // Intake does not revise, reopen or wake an existing Wave.
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Attempted++
		wave, admitErr := admitter.Admit(ctx, issue.Number)
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if admitErr != nil {
			result.Held = append(result.Held, issue.Number)
			if p.Report != nil {
				p.Report(fmt.Errorf("Issue pool repository %d Issue %d: %w", p.Admitter.Policy.RepositoryID, issue.Number, admitErr))
			}
		} else {
			result.Admitted = append(result.Admitted, wave.ID)
			if p.OnMutation != nil {
				p.OnMutation()
			}
		}
		// Persist progress even after a held item so it cannot block later
		// independent sources after a restart. The sorted order rotates fairly.
		cursor = issuePoolCursor{PolicyDigest: digest, LastNumber: issue.Number}
		if err := p.Admitter.Store.Put(ctx, "product_issue_pool_cursor", key, "controller", cursor); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (p IssuePool) Run(ctx context.Context) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.Every <= 0 {
		return errors.New("positive Issue pool interval required")
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := p.Scan(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && p.Report != nil {
			p.Report(err)
		}
		timer := time.NewTimer(p.Every)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
