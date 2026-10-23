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

var ErrIssuePoolScanBudget = errors.New("Issue pool scan budget exhausted; resume from saved cursor")

type issuePoolCursor struct {
	PolicyDigest string `json:"policy_digest"`
	Group        int    `json:"group"`
	After        string `json:"after"`
	LastNumber   int    `json:"last_number"`
}

type IssuePoolMetadataAPI interface {
	IssuePoolMetadata(context.Context, []string, string) (github.IssuePoolPage, error)
}

// Metadata reads may exhaust the scan budget before the candidate Issue is
// fetched. Only a candidate whose Issue read started may advance the cursor.
type poolAdmissionAPI struct {
	*budgetResourceAPI
	issuePath string
	started   bool
}

func (a *poolAdmissionAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if ctx.Err() == nil && path == a.issuePath && a.available(ctx) {
		a.started = true
	}
	return a.budgetResourceAPI.ResourceGET(ctx, path)
}

// Validate checks host selection bounds without API calls or mutations.
func (p IssuePool) Validate() error {
	if p.Admitter.Store == nil || p.Admitter.API == nil || p.Admitter.SessionID == "" || p.Attempts < 1 || p.Attempts > 32 || len(p.PriorityLabels) > 16 {
		return errors.New("configured Issue pool and attempts 1..32 required")
	}
	if _, ok := p.Admitter.API.(IssuePoolMetadataAPI); !ok {
		return errors.New("explicit Issue metadata paging transport required")
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
	return p.scan(ctx, 45*time.Second)
}

func (p IssuePool) rank(labels []string) int {
	for i, wanted := range p.PriorityLabels {
		for _, label := range labels {
			if label == wanted {
				return i
			}
		}
	}
	return len(p.PriorityLabels)
}
func (p IssuePool) eligible(node github.IssuePoolNode, group int) bool {
	if node.State != "OPEN" {
		return false
	}
	ready := false
	for _, label := range node.Labels {
		if label == p.Admitter.Policy.HoldLabel {
			return false
		}
		ready = ready || label == p.Admitter.Policy.ReadyLabel
	}
	rank := p.rank(node.Labels)
	if rank < group {
		return false
	}
	return !node.LabelsComplete || (ready && rank == group)
}
func (p IssuePool) scan(parent context.Context, timeout time.Duration) (IssuePoolScan, error) {
	result := IssuePoolScan{Admitted: []string{}, Held: []int{}}
	if err := p.Validate(); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: p.Admitter.API}
	metadata := p.Admitter.API.(IssuePoolMetadataAPI)
	policy, err := json.Marshal(struct {
		Version  int
		Source   IssueSourcePolicy
		Priority []string
	}{2, p.Admitter.Policy, p.PriorityLabels})
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
	if cursor.PolicyDigest != digest {
		cursor = issuePoolCursor{PolicyDigest: digest}
	}
	if cursor.Group < 0 || cursor.Group > len(p.PriorityLabels) || cursor.LastNumber < 0 || len(cursor.After) > 4096 || !utf8.ValidString(cursor.After) || strings.ContainsAny(cursor.After, "\x00\r\n") {
		return result, errors.New("invalid Issue metadata cursor")
	}
	dirty := false
	save := func(write context.Context) error {
		if !dirty {
			return nil
		}
		if err := p.Admitter.Store.Put(write, "product_issue_pool_cursor", key, "controller", cursor); err != nil {
			return err
		}
		dirty = false
		return nil
	}
	timedOut := func() error {
		if parent.Err() == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && dirty {
			progress, stop := context.WithTimeout(parent, time.Second)
			err := save(progress)
			stop()
			return errors.Join(ctx.Err(), err)
		}
		return ctx.Err()
	}
	exhausted := func() (IssuePoolScan, error) { return result, errors.Join(ErrIssuePoolScanBudget, save(ctx)) }
	seen := 0
	admitter := p.Admitter
	for {
		if ctx.Err() != nil {
			return result, timedOut()
		}
		if !api.available(ctx) || seen >= 2048 {
			return exhausted()
		}
		if result.Attempted >= p.Attempts {
			return result, save(ctx)
		}
		labels := []string{p.Admitter.Policy.ReadyLabel}
		group := cursor.Group
		if group < len(p.PriorityLabels) && p.PriorityLabels[group] != labels[0] {
			labels = append(labels, p.PriorityLabels[group])
		}
		api.calls++
		page, pageErr := metadata.IssuePoolMetadata(ctx, labels, cursor.After)
		if ctx.Err() != nil {
			return result, timedOut()
		}
		if pageErr != nil {
			if !api.available(ctx) {
				return exhausted()
			}
			return result, pageErr
		}
		if err := page.Validate(p.Admitter.Policy.RepositoryID, p.Admitter.Policy.Repository, cursor.After); err != nil {
			return result, err
		}
		size := page.Bytes
		if size == 0 {
			encoded, err := json.Marshal(page)
			if err != nil {
				return result, err
			}
			size = len(encoded)
		}
		api.bytes += size
		if api.bytes > 16<<20 {
			return exhausted()
		}
		sort.Slice(page.Nodes, func(i, j int) bool { return page.Nodes[i].Number < page.Nodes[j].Number })
		for _, node := range page.Nodes {
			if node.Number <= cursor.LastNumber {
				continue
			}
			if ctx.Err() != nil {
				return result, timedOut()
			}
			if !api.available(ctx) || seen >= 2048 {
				return exhausted()
			}
			if result.Attempted >= p.Attempts {
				return result, save(ctx)
			}
			seen++
			advance := func() { cursor.LastNumber = node.Number; dirty = true }
			if !p.eligible(node, group) {
				advance()
				continue
			}
			var existing string
			err := p.Admitter.Store.DB.QueryRowContext(ctx, "SELECT id FROM kernel_waves WHERE source='github' AND source_id=?", issueSourceID(p.Admitter.Policy.RepositoryID, node.Number)).Scan(&existing)
			if ctx.Err() != nil {
				return result, timedOut()
			}
			if err == nil {
				advance()
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return result, err
			}
			attempt := &poolAdmissionAPI{budgetResourceAPI: api, issuePath: "issues/" + strconv.Itoa(node.Number)}
			admitter.API = attempt
			result.Attempted++
			wave, admitErr := admitter.admit(ctx, node.Number, func(issue github.Issue) bool {
				names := make([]string, 0, len(issue.Labels))
				for _, label := range issue.Labels {
					names = append(names, label.Name)
				}
				return p.rank(names) == group
			})
			if ctx.Err() != nil {
				if attempt.started {
					advance()
					if admitErr == nil {
						result.Admitted = append(result.Admitted, wave.ID)
						if parent.Err() == nil && p.OnMutation != nil {
							p.OnMutation()
						}
					} else {
						result.Held = append(result.Held, node.Number)
					}
				}
				return result, timedOut()
			}
			if !api.available(ctx) && !attempt.started {
				return exhausted()
			}
			if admitErr != nil {
				if !errors.Is(admitErr, errIssueNotSelected) {
					result.Held = append(result.Held, node.Number)
					if p.Report != nil {
						p.Report(fmt.Errorf("Issue pool repository %d Issue %d: %w", p.Admitter.Policy.RepositoryID, node.Number, admitErr))
					}
				}
			} else {
				result.Admitted = append(result.Admitted, wave.ID)
				if p.OnMutation != nil {
					p.OnMutation()
				}
			}
			advance()
			if err := save(ctx); err != nil {
				return result, err
			}
		}
		if page.HasNext {
			cursor.After = page.EndCursor
			cursor.LastNumber = 0
		} else {
			cursor.Group++
			cursor.After = ""
			cursor.LastNumber = 0
		}
		complete := cursor.Group > len(p.PriorityLabels)
		if complete {
			cursor.Group = 0
		}
		dirty = true
		if err := save(ctx); err != nil {
			return result, err
		}
		if complete {
			return result, nil
		}
	}
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
