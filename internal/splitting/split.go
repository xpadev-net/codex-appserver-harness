// Package splitting validates lossless partitions and persists child creation intent.
package splitting

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"regexp"
	"strings"
)

type Coverage struct {
	PartID          string `json:"part_id"`
	PartCount       int    `json:"part_count"`
	Basis           string `json:"basis"`
	ParentCriterion string `json:"parent_criterion"`
	ChildCriterion  string `json:"child_criterion"`
}
type Child struct {
	Key         string                   `json:"key"`
	Title       string                   `json:"title"`
	Requirement string                   `json:"requirement"`
	Criteria    []verification.Criterion `json:"criteria"`
	Coverage    []Coverage               `json:"coverage"`
	DependsOn   []string                 `json:"depends_on"`
}
type Proposal struct {
	SchemaVersion int     `json:"schema_version"`
	WorkItemID    string  `json:"work_item_id"`
	ScopeRevision int     `json:"scope_revision"`
	Outcome       string  `json:"outcome"`
	Children      []Child `json:"children"`
}

func Decode(data []byte, id string, parent verification.Scope, depth, maxDepth, maxChildren int) (Proposal, error) {
	var p Proposal
	if err := contract.DecodeStrict(data, &p); err != nil {
		return p, err
	}
	return p, p.Validate(id, parent, depth, maxDepth, maxChildren)
}
func (p Proposal) Validate(id string, parent verification.Scope, depth, maxDepth, maxChildren int) error {
	if err := parent.Validate(); err != nil {
		return err
	}
	if p.SchemaVersion != 1 || p.WorkItemID != id || p.ScopeRevision != parent.Revision || p.Outcome != "split" || depth < 0 || maxDepth < 1 || depth >= maxDepth || len(p.Children) < 2 || len(p.Children) > maxChildren {
		return errors.New("invalid split identity or limits")
	}
	keys := map[string]bool{}
	parents := map[string]bool{}
	for _, id := range parent.Included {
		parents[id] = true
	}
	covered := map[string]bool{}
	full := map[string]bool{}
	parts := map[string]map[string]bool{}
	counts := map[string]int{}
	for _, c := range p.Children {
		if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(c.Key) || keys[c.Key] || strings.TrimSpace(c.Title) == "" || strings.TrimSpace(c.Requirement) == "" || len(c.Criteria) == 0 {
			return errors.New("invalid split child")
		}
		keys[c.Key] = true
		criteria := map[string]bool{}
		for _, criterion := range c.Criteria {
			if criterion.ID == "" || criterion.Requirement == "" || criteria[criterion.ID] {
				return errors.New("invalid child criterion")
			}
			criteria[criterion.ID] = true
		}
		mapped := map[string]bool{}
		for _, mapping := range c.Coverage {
			if !parents[mapping.ParentCriterion] || (covered[mapping.ParentCriterion] && mapping.PartID == "") || !criteria[mapping.ChildCriterion] || mapped[mapping.ChildCriterion] {
				return errors.New("coverage overlaps or references unknown criterion")
			}
			if mapping.PartID == "" {
				if mapping.PartCount != 0 {
					return errors.New("full coverage cannot declare parts")
				}
				full[mapping.ParentCriterion] = true
			} else {
				if full[mapping.ParentCriterion] || mapping.PartCount < 2 || strings.TrimSpace(mapping.Basis) == "" || !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(mapping.PartID) {
					return errors.New("invalid partition coverage")
				}
				if parts[mapping.ParentCriterion] == nil {
					parts[mapping.ParentCriterion] = map[string]bool{}
					counts[mapping.ParentCriterion] = mapping.PartCount
				}
				if counts[mapping.ParentCriterion] != mapping.PartCount || parts[mapping.ParentCriterion][mapping.PartID] {
					return errors.New("partition overlap or count changed")
				}
				parts[mapping.ParentCriterion][mapping.PartID] = true
			}
			covered[mapping.ParentCriterion] = true
			mapped[mapping.ChildCriterion] = true
		}
		if len(mapped) != len(criteria) {
			return errors.New("child criteria missing parent mapping")
		}
	}
	for parent, declared := range counts {
		if len(parts[parent]) != declared {
			return errors.New("partition drops declared requirement parts")
		}
	}
	for id := range parents {
		if !covered[id] {
			return errors.New("split drops parent criterion: " + id)
		}
	}
	for id := range covered {
		if !parents[id] {
			return errors.New("split references unknown parent criterion: " + id)
		}
	}
	edges := map[string][]string{}
	for _, c := range p.Children {
		seen := map[string]bool{}
		for _, dep := range c.DependsOn {
			if !keys[dep] || dep == c.Key || seen[dep] {
				return errors.New("invalid dependency")
			}
			seen[dep] = true
		}
		edges[c.Key] = c.DependsOn
	}
	state := map[string]int{}
	var visit func(string) bool
	visit = func(key string) bool {
		if state[key] == 1 {
			return false
		}
		if state[key] == 2 {
			return true
		}
		state[key] = 1
		for _, dep := range edges[key] {
			if !visit(dep) {
				return false
			}
		}
		state[key] = 2
		return true
	}
	for key := range edges {
		if !visit(key) {
			return errors.New("dependency cycle")
		}
	}
	return nil
}

type Journal interface {
	Put(context.Context, string, string, string, any) error
	Record(context.Context, string, string, any) error
}
type IssueCreator interface {
	FindChild(context.Context, string) (int, error)
	CreateChild(context.Context, string, string, string) (int, error)
}
type ChildState struct {
	Key   string `json:"key"`
	State string `json:"state"`
	Issue int    `json:"issue"`
}
type Progress struct {
	Proposal Proposal     `json:"proposal"`
	Digest   string       `json:"digest"`
	Children []ChildState `json:"children"`
}

// CreateChildren is called under the parent operation lease. A crash leaves a
// durable creating state. If lookup finds nothing, that ambiguous child stays held;
// the operator may retry only after proving no external side effect occurred.
func CreateChildren(ctx context.Context, j Journal, api IssueCreator, p Proposal) (Progress, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return Progress{}, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	var progress Progress
	err = j.Record(ctx, "split_progress", p.WorkItemID, &progress)
	if errors.Is(err, sql.ErrNoRows) {
		progress = Progress{Proposal: p, Digest: digest, Children: []ChildState{}}
		for _, c := range p.Children {
			progress.Children = append(progress.Children, ChildState{Key: c.Key, State: "pending"})
		}
		if err = j.Put(ctx, "split_progress", p.WorkItemID, p.WorkItemID, progress); err != nil {
			return progress, err
		}
	} else if err != nil {
		return progress, err
	}
	if len(progress.Children) != len(p.Children) {
		return progress, errors.New("corrupt split progress")
	}
	if progress.Digest != digest {
		return progress, errors.New("cannot replace an in-progress split")
	}
	for i, child := range p.Children {
		state := &progress.Children[i]
		if state.Key != child.Key {
			return progress, errors.New("split progress identity mismatch")
		}

		marker := "harness-child:" + p.WorkItemID + ":" + child.Key
		existing, err := api.FindChild(ctx, marker)
		if err != nil {
			return progress, err
		}
		if state.State == "created" && (existing != state.Issue || existing <= 0) {
			return progress, errors.New("created child marker or issue identity changed")
		}
		if existing > 0 {
			state.Issue = existing
			state.State = "created"
			if err = j.Put(ctx, "split_progress", p.WorkItemID, p.WorkItemID, progress); err != nil {
				return progress, err
			}
			continue
		}
		if state.State != "pending" {
			return progress, errors.New("ambiguous child creation requires reconciliation")
		}
		state.State = "creating"
		if err = j.Put(ctx, "split_progress", p.WorkItemID, p.WorkItemID, progress); err != nil {
			return progress, err
		}
		body, err := json.Marshal(child)
		if err != nil {
			return progress, err
		}
		number, createErr := api.CreateChild(ctx, child.Title, string(body), marker)
		if errors.Is(createErr, contract.ErrUnsent) {
			state.State = "pending"
			if err = j.Put(context.WithoutCancel(ctx), "split_progress", p.WorkItemID, p.WorkItemID, progress); err != nil {
				return progress, err
			}
			return progress, createErr
		}
		if createErr != nil || number <= 0 {
			return progress, errors.New("child creation outcome unknown; reconcile before retry")
		}
		state.Issue = number
		state.State = "created"
		if err = j.Put(context.WithoutCancel(ctx), "split_progress", p.WorkItemID, p.WorkItemID, progress); err != nil {
			return progress, err
		}
	}
	return progress, nil
}
func ReadyChildren(p Proposal, completed map[string]bool) []string {
	ready := []string{}
	for _, c := range p.Children {
		if completed[c.Key] {
			continue
		}
		ok := true
		for _, dep := range c.DependsOn {
			if !completed[dep] {
				ok = false
			}
		}
		if ok {
			ready = append(ready, c.Key)
		}
	}
	return ready
}

type Residual struct {
	ID               string   `json:"id"`
	ParentWorkItemID string   `json:"parent_work_item_id"`
	ScopeRevision    int      `json:"scope_revision"`
	Criteria         []string `json:"criteria"`
	DependsOn        []string `json:"depends_on"`
	State            string   `json:"state"`
}

func (r Residual) Validate(w domain.WorkItem, s verification.Scope) error {
	if r.ID == "" || r.ParentWorkItemID != w.ID || r.ScopeRevision != s.Revision || r.State != "planned" || len(r.Criteria) == 0 {
		return errors.New("invalid residual work")
	}
	known := map[string]bool{}
	for _, id := range s.Remaining {
		known[id] = true
	}
	seen := map[string]bool{}
	for _, id := range r.Criteria {
		if !known[id] || seen[id] {
			return errors.New("residual coverage invalid")
		}
		seen[id] = true
	}
	if len(seen) != len(known) {
		return errors.New("residual requirements missing")
	}
	return nil
}

// ParentComplete requires coverage from actual merged children at the current
// scope revision, plus separately verified final acceptance of the whole issue.
func ParentComplete(s verification.Scope, children []Completion, finalRevision int, finalPassed bool) bool {
	if s.Validate() != nil || !finalPassed || finalRevision != s.Revision {
		return false
	}
	covered := map[string]bool{}
	known := map[string]bool{}
	for _, criterion := range s.Criteria {
		known[criterion.ID] = true
	}
	for _, c := range children {
		if c.ScopeRevision != s.Revision || c.Phase != domain.Merged {
			return false
		}
		for _, id := range c.ParentCriteria {
			if !known[id] {
				return false
			}
			covered[id] = true
		}
	}
	return len(covered) == len(known)
}

type Completion struct {
	ScopeRevision  int
	Phase          domain.Phase
	ParentCriteria []string
}

// NextScope excludes previously merged criteria from the next partial PR while
// retaining their original requirements and completion correspondence.
func NextScope(current verification.Scope) (verification.Scope, error) {
	if current.Validate() != nil || len(current.Remaining) == 0 {
		return verification.Scope{}, errors.New("no residual scope")
	}
	next := current
	next.Included = append([]string{}, current.Remaining...)
	next.Remaining = []string{}
	next.PreviouslyMerged = append(append([]string{}, current.PreviouslyMerged...), current.Included...)
	return next, next.Validate()
}
