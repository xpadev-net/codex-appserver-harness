// Package service orchestrates persisted operations. Timers reconcile facts, never LLM polling.
package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type GitHub interface {
	GetIssue(context.Context, int) (github.Issue, error)
	Pool(context.Context) ([]github.Issue, error)
	Pull(context.Context, int) (github.PR, error)
	Publish(context.Context, string, string, string, string, string) (github.PR, error)
	FindPR(context.Context, string, string) (*github.PR, error)
	Reviews(context.Context, int) ([]github.Review, error)
	Comments(context.Context, int) ([]github.Comment, error)
	RequestReview(context.Context, int, string, string, int64) (int64, error)
	FindReviewRequest(context.Context, int, string, int64) (int64, error)
	Merge(context.Context, int, string, string) error
	Checks(context.Context, string) ([]github.Check, error)
	CommitParents(context.Context, string) ([]string, error)
	Conditions(context.Context, int) (string, error)
	FindChild(context.Context, string) (int, error)
	CreateChild(context.Context, string, string, string) (int, error)
	CloseIssue(context.Context, int) error
}
type Agents interface {
	Run(context.Context, domain.WorkItem, domain.Operation, string, any) (worker.Run, error)
}
type Jobs interface {
	Start(context.Context, runner.Spec) error
	Read(runner.Spec) (runner.Result, error)
}
type Controller struct {
	Config       Config
	Store        *storage.Store
	GitHub       GitHub
	Agents       Agents
	Jobs         Jobs
	Capabilities contract.Report
}
type Source struct {
	Digest string `json:"digest"`
}
type Correction struct {
	Reason      string   `json:"reason"`
	EvidenceIDs []string `json:"evidence_ids"`
	FeedbackIDs []string `json:"feedback_ids"`
	Mode        string   `json:"mode"`
	Consumed    bool     `json:"consumed"`
}
type JobIntent struct {
	Spec     runner.Spec `json:"spec"`
	Launched bool        `json:"launched"`
	Started  time.Time   `json:"started"`
	Attempt  int         `json:"attempt"`
}

func New(c Config) (*Controller, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.Root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Stat(c.Root)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state root must have private directory permissions (0700)")
	}
	s, err := storage.Open(filepath.Join(c.Root, "state.db"))
	if err != nil {
		return nil, err
	}
	controller := &Controller{Config: c, Store: s}
	controller.GitHub = github.Client{Repository: c.Repository, RepositoryID: c.RepositoryID, BeforeWrite: controller.beforeWrite}
	if c.CapabilitiesPath != "" {
		b, err := os.ReadFile(c.CapabilitiesPath)
		if err != nil {
			s.Close()
			return nil, err
		}
		if err = json.Unmarshal(b, &controller.Capabilities); err != nil {
			s.Close()
			return nil, err
		}
	}
	exe, err := os.Executable()
	if err != nil {
		s.Close()
		return nil, err
	}
	controller.Jobs = FileJobs{Executable: exe}
	controller.Agents = LiveAgents{Config: c, Store: s}
	return controller, nil
}
func (c *Controller) Run(ctx context.Context) error {
	if err := c.VerifyRuntime(ctx); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(c.Config.Root, "controller.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another controller owns this state root")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	ticker := time.NewTicker(time.Duration(c.Config.ReconcileSeconds) * time.Second)
	defer ticker.Stop()
	for {
		if err := c.Tick(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			_ = c.Store.Put(context.WithoutCancel(ctx), "service_error", fmt.Sprint(time.Now().UnixNano()), "", map[string]any{"error": err.Error(), "time": time.Now().UTC()})
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func digest(issue github.Issue) string {
	sum := sha256.Sum256([]byte(issue.Title + "\n" + issue.Body))
	return hex.EncodeToString(sum[:])
}
func (c *Controller) emit(ctx context.Context, w domain.WorkItem, kind, reason string) error {
	return c.Store.Apply(ctx, domain.Event{ID: fmt.Sprintf("%s:%d:%s", w.ID, w.Attempt, kind), WorkItemID: w.ID, Kind: kind, Candidate: w.Candidate, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Attempt: w.Attempt, Reason: reason})
}
func (c *Controller) block(ctx context.Context, w domain.WorkItem, reason string) error {
	return c.Store.Apply(ctx, domain.Event{ID: fmt.Sprintf("%s:block:%d", w.ID, time.Now().UnixNano()), WorkItemID: w.ID, Kind: "block", Reason: reason})
}
func (c *Controller) Tick(ctx context.Context) error {
	batch, err := c.Store.PendingDeliveryIDs(ctx)
	if err != nil {
		return err
	}
	items, err := c.Store.List(ctx)
	if err != nil {
		return err
	}
	// Track sent work even after cancellation. Pending operations are rechecked at dispatch.
	ops, err := c.Store.Operations(ctx)
	if err != nil {
		return err
	}
	for _, op := range ops {
		w, err := c.Store.Get(ctx, op.WorkItemID)
		if err != nil {
			return err
		}
		if op.State == "pending" {
			if w.Terminal() {
				continue
			}
			ok, err := c.Store.BeginDispatch(ctx, op.ID)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			op.State = "sending"
		}
		if err = c.handle(ctx, w, op); err != nil {
			_ = c.Store.Put(context.WithoutCancel(ctx), "operation_error", op.ID, w.ID, map[string]any{"reason": err.Error(), "time": time.Now().UTC()})
			return err
		}
		return nil
	}
	active := false
	busy := false
	for _, item := range items {
		if !item.Terminal() && item.Phase != domain.Ready {
			busy = true
		}
	}
	for _, w := range items {
		if !w.Terminal() {
			active = true
		}
		switch w.Phase {
		case domain.Blocked:
			var intent ReconcileIntent
			if c.Store.Record(ctx, "reconcile_needed", w.ID, &intent) == nil && intent.Active {
				return c.Store.RestoreReconciliation(ctx, w.ID, intent.OperationID, "automatic state reconciliation without new turn")
			}
		case domain.Ready:
			if w.Paused || busy {
				continue
			}
			ready, err := c.Store.DependenciesReady(ctx, w.ID)
			if err != nil {
				return err
			}
			if ready {
				return c.emit(ctx, w, "start", "issue_claimed")
			}
		case domain.Reviewing:
			if err = c.observeReview(ctx, w); err != nil {
				return err
			}
		case domain.Split:
			var scope verification.Scope
			if err = c.Store.Record(ctx, "scope", w.ID, &scope); err != nil {
				return err
			}
			if err = c.finalAcceptance(ctx, w, scope); err != nil {
				return err
			}
		case domain.Merged:
			if err = c.afterMerge(ctx, w); err != nil {
				return err
			}
		}
	}
	fresh, err := c.Store.List(ctx)
	if err != nil {
		return err
	}
	for _, item := range fresh {
		if !item.Terminal() {
			active = true
		}
		if item.Phase == domain.Merged || item.Phase == domain.Split {
			var completion map[string]any
			if missing(c.Store.Record(ctx, "parent_completion", fmt.Sprintf("r%d-i%d", item.RepositoryID, item.Issue), &completion)) {
				active = true
			}
		}
	}
	if !active {
		if err = c.claimPool(ctx); err != nil {
			return err
		}
	}
	return c.Store.AcknowledgeDeliveries(ctx, batch)
}
func (c *Controller) claimPool(ctx context.Context) error {
	issues, err := c.GitHub.Pool(ctx)
	if err != nil {
		return err
	}
	sort.Slice(issues, func(i, j int) bool {
		rank := func(issue github.Issue) int {
			for i, name := range c.Config.PriorityLabels {
				for _, label := range issue.Labels {
					if label.Name == name {
						return i
					}
				}
			}
			return len(c.Config.PriorityLabels)
		}
		a, b := rank(issues[i]), rank(issues[j])
		if a != b {
			return a < b
		}
		return issues[i].Number < issues[j].Number
	})
	for _, issue := range issues {
		if len(issue.PullRequest) > 0 || issue.State != "open" {
			continue
		}
		ready, hold := false, false
		for _, label := range issue.Labels {
			if label.Name == c.Config.ReadyLabel {
				ready = true
			}
			if label.Name == c.Config.HoldLabel {
				hold = true
			}
		}
		if !ready || hold {
			continue
		}
		var claimed string
		err = c.Store.DB.QueryRowContext(ctx, "SELECT work_item_id FROM issue_claims WHERE repository_id=? AND issue=?", c.Config.RepositoryID, issue.Number).Scan(&claimed)
		if err == nil {
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if ok, err := c.poolDependencies(ctx, issue); err != nil {
			return err
		} else if !ok {
			continue
		}
		scope, err := ParseScope(issue.Body)
		if err != nil {
			return err
		}
		id := fmt.Sprintf("r%d-i%d", c.Config.RepositoryID, issue.Number)
		w := domain.WorkItem{ID: id, RepositoryID: c.Config.RepositoryID, Issue: issue.Number, ScopeRevision: scope.Revision, PolicyRevision: c.Config.Verification.Revision, Phase: domain.Ready}
		if err = c.Enqueue(ctx, w, scope, issue); err != nil {
			return err
		}
		return nil
	}
	return nil
}
func ParseScope(body string) (verification.Scope, error) {
	var scope verification.Scope
	const fence = "```harness-scope\n"
	start := strings.Index(body, fence)
	if start < 0 {
		return scope, errors.New("ready Issue requires a fenced harness-scope JSON acceptance contract")
	}
	rest := body[start+len(fence):]
	end := strings.Index(rest, "```")
	if end < 0 {
		return scope, errors.New("unterminated harness-scope")
	}
	if err := contract.DecodeStrict([]byte(rest[:end]), &scope); err != nil {
		return scope, err
	}
	scope.OriginalRequest = body
	return scope, scope.Validate()
}
func (c *Controller) Enqueue(ctx context.Context, w domain.WorkItem, s verification.Scope, issue github.Issue) error {
	if err := c.ensurePolicy(ctx); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if w.ScopeRevision != s.Revision || w.Issue != issue.Number || issue.State != "open" {
		return errors.New("scope/issue mismatch")
	}
	return c.Store.ClaimRecords(ctx, w, map[string]any{"scope": s, "source": Source{digest(issue)}, "policy_snapshot": c.policyDigest()})
}
func (c *Controller) current(ctx context.Context, w domain.WorkItem) (verification.Scope, error) {
	var s verification.Scope
	var savedPolicy string
	if err := c.Store.Record(ctx, "policy_snapshot", w.ID, &savedPolicy); err != nil {
		return s, err
	}
	if savedPolicy != c.policyDigest() {
		return s, errors.New("trusted policy changed; adopt an explicit new revision")
	}
	if err := c.Store.Record(ctx, "scope", w.ID, &s); err != nil {
		return s, err
	}
	issue, err := c.GitHub.GetIssue(ctx, w.Issue)
	if err != nil {
		return s, err
	}
	var source Source
	if err = c.Store.Record(ctx, "source", w.ID, &source); err != nil {
		return s, err
	}
	if issue.State != "open" || digest(issue) != source.Digest || s.Revision != w.ScopeRevision || w.PolicyRevision != c.Config.Verification.Revision {
		return s, errors.New("issue closed, scope changed, or policy revision changed")
	}
	for _, label := range issue.Labels {
		if label.Name == c.Config.HoldLabel {
			return s, errors.New("issue is on hold")
		}
	}
	return s, s.Validate()
}
func (c *Controller) handle(ctx context.Context, w domain.WorkItem, op domain.Operation) error {
	ctx = c.writeContext(ctx, w, op.Kind)
	switch op.Kind {
	case "implement", "verify", "propose_split":
		return c.agent(ctx, w, op)
	case "validate", "validate_interim":
		return c.validation(ctx, w, op)
	case "publish":
		return c.publish(ctx, w, op)
	case "merge":
		return c.merge(ctx, w, op)
	case "split":
		return c.split(ctx, w, op)
	}
	return fmt.Errorf("unknown operation %s", op.Kind)
}
func (c *Controller) done(ctx context.Context, op domain.Operation, result any) error {
	return c.Store.FinishOperation(ctx, op.ID, "done", result)
}
func (c *Controller) candidate(ctx context.Context, w domain.WorkItem) (runner.Candidate, error) {
	var candidate runner.Candidate
	err := c.Store.Record(ctx, "candidate", w.Candidate, &candidate)
	return candidate, err
}
func candidateID(w domain.WorkItem, sha string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%d", w.ID, sha, w.ScopeRevision, w.PolicyRevision)))
	return "c-" + hex.EncodeToString(sum[:16])
}
func (c *Controller) freeze(ctx context.Context, w domain.WorkItem, base string) (runner.Candidate, error) {
	if err := workspace.Verify(ctx, c.Config.RepositoryPath, w.Worktree, w.Branch); err != nil {
		return runner.Candidate{}, err
	}
	var revisionCommit bool
	revisionErr := c.Store.Record(ctx, "requires_revision_commit", w.ID, &revisionCommit)
	if revisionErr != nil && !missing(revisionErr) {
		return runner.Candidate{}, revisionErr
	}
	if revisionCommit {
		if _, err := workspace.Git(ctx, w.Worktree, "commit", "--allow-empty", "-m", fmt.Sprintf("Harness scope %d policy %d", w.ScopeRevision, w.PolicyRevision)); err != nil {
			return runner.Candidate{}, err
		}
		if err := c.Store.Put(ctx, "requires_revision_commit", w.ID, w.ID, false); err != nil {
			return runner.Candidate{}, err
		}
	}
	sha, err := workspace.Freeze(ctx, w.Worktree, "Harness candidate "+w.ID)
	if err != nil {
		return runner.Candidate{}, err
	}
	if _, err = workspace.Git(ctx, w.Worktree, "merge-base", "--is-ancestor", base, sha); err != nil {
		return runner.Candidate{}, errors.New("candidate does not contain its recorded base")
	}
	if err = c.Store.Put(ctx, "workspace_base", w.ID, w.ID, base); err != nil {
		return runner.Candidate{}, err
	}
	candidate := runner.Candidate{ID: candidateID(w, sha), SHA: sha, BaseSHA: base, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision}
	return candidate, c.Store.Put(ctx, "candidate", candidate.ID, w.ID, candidate)
}
func (c *Controller) workspace(ctx context.Context, w domain.WorkItem) (domain.WorkItem, string, error) {
	if w.Worktree == "" {
		hash := sha256.Sum256([]byte(w.ID))
		key := hex.EncodeToString(hash[:16])
		path := filepath.Join(c.Config.Root, "worktrees", key)
		branch := "harness/" + key
		if err := c.Store.SetWorkspace(ctx, w.ID, branch, path); err != nil {
			return w, "", err
		}
		w.Worktree = path
		w.Branch = branch
	}
	var base string
	err := c.Store.Record(ctx, "workspace_base", w.ID, &base)
	if err != nil && !missing(err) {
		return w, "", err
	}
	if missing(err) {
		if _, err = workspace.Git(ctx, c.Config.RepositoryPath, "fetch", "origin", c.Config.BaseBranch); err != nil {
			return w, "", err
		}
		base, err = workspace.Resolve(ctx, c.Config.RepositoryPath, "FETCH_HEAD")
		if err != nil {
			return w, "", err
		}
		if err = c.Store.Put(ctx, "workspace_base", w.ID, w.ID, base); err != nil {
			return w, "", err
		}
	}
	if _, err = os.Stat(w.Worktree); os.IsNotExist(err) {
		if err = os.MkdirAll(filepath.Dir(w.Worktree), 0700); err != nil {
			return w, "", err
		}
		err = workspace.Create(ctx, c.Config.RepositoryPath, w.Worktree, w.Branch, base)
		if err != nil {
			return w, "", err
		}
	} else if err != nil {
		return w, "", err
	}
	branch, err := workspace.Git(ctx, w.Worktree, "branch", "--show-current")
	if err != nil || strings.TrimSpace(branch) != w.Branch {
		return w, "", errors.New("worktree branch mismatch")
	}
	return w, base, nil
}
func (c *Controller) results(ctx context.Context, candidate runner.Candidate, w domain.WorkItem) ([]runner.Result, error) {
	raw, err := c.Store.Records(ctx, "validation", w.ID)
	if err != nil {
		return nil, err
	}
	out := []runner.Result{}
	for _, b := range raw {
		var r runner.Result
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, err
		}
		if r.Candidate == candidate {
			out = append(out, r)
		}
	}
	return out, nil
}

// FileJobs launches a durable _runner subprocess; Read is ordinary filesystem polling.
type FileJobs struct{ Executable string }

func (j FileJobs) Start(ctx context.Context, s runner.Spec) error {
	if err := os.MkdirAll(s.Root, 0700); err != nil {
		return err
	}
	path := filepath.Join(s.Root, s.JobID+".json")
	if err := runner.WriteJSON(path, s); err != nil {
		return err
	}
	_, err := runner.Launch(ctx, j.Executable, path)
	return err
}
func (j FileJobs) Read(s runner.Spec) (runner.Result, error) {
	return runner.LoadResult(s.Root, s.JobID)
}

func (c *Controller) complete(ctx context.Context, w domain.WorkItem, op domain.Operation, result any, kind, candidate, reason string) error {
	return c.Store.Complete(ctx, op.ID, result, domain.Event{ID: op.ID + ":transition:" + kind, WorkItemID: w.ID, Kind: kind, Candidate: candidate, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Attempt: op.Attempt, Reason: reason, Ref: w.SplitPlanID})
}
func missing(err error) bool { return errors.Is(err, sql.ErrNoRows) || errors.Is(err, os.ErrNotExist) }

// Optional task metadata: harness-depends-on: 12, 34. Unmanaged dependencies must
// be closed; managed dependencies additionally need final acceptance evidence.
func (c *Controller) poolDependencies(ctx context.Context, issue github.Issue) (bool, error) {
	matches := regexp.MustCompile(`(?m)^harness-depends-on:\s*([0-9, ]+)\s*$`).FindStringSubmatch(issue.Body)
	if len(matches) == 0 {
		return true, nil
	}
	for _, text := range strings.Split(matches[1], ",") {
		number, err := strconv.Atoi(strings.TrimSpace(text))
		if err != nil || number < 1 || number == issue.Number {
			return false, errors.New("invalid issue dependency")
		}
		dependency, err := c.GitHub.GetIssue(ctx, number)
		if err != nil {
			return false, err
		}
		if dependency.State != "closed" {
			return false, nil
		}
		var claim string
		err = c.Store.DB.QueryRowContext(ctx, "SELECT work_item_id FROM issue_claims WHERE repository_id=? AND issue=?", c.Config.RepositoryID, number).Scan(&claim)
		if err != nil && !missing(err) {
			return false, err
		}
		if err == nil {
			var evidence map[string]any
			err = c.Store.Record(ctx, "parent_completion", fmt.Sprintf("r%d-i%d", c.Config.RepositoryID, number), &evidence)
			if missing(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

type ReconcileIntent struct {
	OperationID string `json:"operation_id"`
	Active      bool   `json:"active"`
}
