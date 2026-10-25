package product

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

type IssueSourcePolicy struct {
	Repository   string `json:"repository"`
	RepositoryID int64  `json:"repository_id"`
	ReadyLabel   string `json:"ready_label"`
	HoldLabel    string `json:"hold_label"`
}

func (p IssueSourcePolicy) Validate() error {
	if p.RepositoryID < 1 || len(p.Repository) > 255 || !regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`).MatchString(p.Repository) || p.ReadyLabel == p.HoldLabel {
		return errors.New("trusted Issue repository and distinct ready/hold labels required")
	}
	for _, label := range []string{p.ReadyLabel, p.HoldLabel} {
		if strings.TrimSpace(label) == "" || len(label) > 256 || strings.ContainsAny(label, "\x00\r\n") {
			return errors.New("bounded Issue selection labels required")
		}
	}
	return nil
}

// IssueAdmitter reads outside the ready loop. It cannot write to GitHub, infer
// approval, or relax policy from Issue text. The host supplies the main session
// and limits; ownership is committed through the same port as direct sources.
type IssueAdmitter struct {
	Store     *storage.KernelStore
	API       ResourceAPI
	Policy    IssueSourcePolicy
	SessionID string
	Limits    domain.KernelLimits
}

func issueSourceID(repositoryID int64, number int) string {
	return fmt.Sprintf("r%d-i%d", repositoryID, number)
}

func issueDependencies(body string, self int) ([]int, error) {
	var numbers []int
	seen := map[int]bool{}
	declarations := 0
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "harness-depends-on:") {
			continue
		}
		declarations++
		if declarations > 1 {
			return nil, errors.New("ambiguous Issue dependencies")
		}
		for _, part := range strings.Split(strings.TrimPrefix(line, "harness-depends-on:"), ",") {
			number, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || number < 1 || number == self || seen[number] || len(numbers) >= 32 {
				return nil, errors.New("invalid or excessive Issue dependencies")
			}
			seen[number] = true
			numbers = append(numbers, number)
		}
	}
	return numbers, nil
}

func readSourceIssue(ctx context.Context, api ResourceAPI, number int) (github.Issue, error) {
	var issue github.Issue
	raw, err := api.ResourceGET(ctx, "issues/"+strconv.Itoa(number))
	if err != nil {
		return issue, err
	}
	if err := json.Unmarshal(raw, &issue); err != nil {
		return issue, err
	}
	if issue.Number != number || issue.UpdatedAt.IsZero() || len(issue.PullRequest) != 0 || (issue.State != "open" && issue.State != "closed") {
		return issue, errors.New("unavailable or invalid Issue identity")
	}
	// Label order alone is not a source edit.
	sort.Slice(issue.Labels, func(i, j int) bool { return issue.Labels[i].Name < issue.Labels[j].Name })
	return issue, nil
}

func sourceRepository(ctx context.Context, api ResourceAPI, policy IssueSourcePolicy) error {
	raw, err := api.ResourceGET(ctx, "")
	if err != nil {
		return err
	}
	var meta struct {
		ID       int64  `json:"id"`
		FullName string `json:"full_name"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return err
	}
	if meta.ID != policy.RepositoryID || !strings.EqualFold(meta.FullName, policy.Repository) {
		return errors.New("Issue repository identity changed")
	}
	return nil
}

func (a IssueAdmitter) Admit(ctx context.Context, number int) (domain.Wave, error) {
	return a.admit(ctx, number, nil)
}

var errIssueNotSelected = errors.New("Issue no longer belongs to this priority group")

func (a IssueAdmitter) admit(ctx context.Context, number int, selected func(github.Issue) bool) (domain.Wave, error) {
	if a.Store == nil || a.API == nil || a.SessionID == "" || number < 1 {
		return domain.Wave{}, errors.New("trusted Issue source and positive number required")
	}
	if err := a.Policy.Validate(); err != nil {
		return domain.Wave{}, err
	}
	if err := a.Limits.Validate(); err != nil {
		return domain.Wave{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: a.API}
	if err := sourceRepository(ctx, api, a.Policy); err != nil {
		return domain.Wave{}, err
	}
	issue, err := readSourceIssue(ctx, api, number)
	if err != nil {
		return domain.Wave{}, err
	}
	ready, hold := false, false
	for _, label := range issue.Labels {
		ready = ready || label.Name == a.Policy.ReadyLabel
		hold = hold || label.Name == a.Policy.HoldLabel
	}
	if issue.State != "open" || !ready || hold {
		return domain.Wave{}, errors.New("Issue is not ready or is held")
	}
	if selected != nil && !selected(issue) {
		return domain.Wave{}, errIssueNotSelected
	}
	// Generated children belong to their existing Wave. Their requirement may
	// itself quote a valid harness-scope block; that is never a fresh root.
	if strings.Contains(issue.Body, "```harness-split-part") || strings.Contains(issue.Body, "<!-- harness-split-child:") {
		return domain.Wave{}, errors.New("split child Issue requires its existing Wave lineage")
	}
	scope, err := verification.ParseScope(issue.Body)
	if err != nil {
		return domain.Wave{}, err
	}
	// Importing an earlier partial/merged lineage requires the versioned
	// continuation path. Do not relabel it as a fresh full-scope product Wave.
	if scope.Revision != 1 || len(scope.PreviouslyMerged) != 0 || len(scope.Remaining) != 0 {
		return domain.Wave{}, errors.New("versioned or partial Issue scope requires explicit lineage import")
	}
	dependencies, err := issueDependencies(issue.Body, number)
	if err != nil {
		return domain.Wave{}, err
	}
	for _, dependency := range dependencies {
		item, err := readSourceIssue(ctx, api, dependency)
		if err != nil {
			return domain.Wave{}, err
		}
		if item.State != "closed" {
			return domain.Wave{}, errors.New("Issue dependency is not closed")
		}
		if strings.Contains(item.Body, "```harness-split-part") || strings.Contains(item.Body, "<!-- harness-split-child:") {
			return domain.Wave{}, storage.ErrProductSplitChildSourceHeld
		}
		if err := a.completedDependency(ctx, dependency); err != nil {
			return domain.Wave{}, err
		}

	}
	current, err := readSourceIssue(ctx, api, number)
	if err != nil {
		return domain.Wave{}, err
	}
	if !reflect.DeepEqual(issue, current) {
		return domain.Wave{}, errors.New("Issue changed during source collection")
	}
	if err := sourceRepository(ctx, api, a.Policy); err != nil {
		return domain.Wave{}, err
	}
	criteria := make([]domain.WaveCriterion, 0, len(scope.Criteria))
	for _, criterion := range scope.Criteria {
		criteria = append(criteria, domain.WaveCriterion{ID: criterion.ID, Requirement: criterion.Requirement})
	}
	id := issueSourceID(a.Policy.RepositoryID, number)
	return a.Store.CreateProductWave(ctx, storage.ProductWaveRequest{
		SourcePolicyDigest: a.Policy.Digest(),
		Wave:               storage.WaveRequest{OrchestratorSessionID: a.SessionID, Source: "github", SourceID: id, OriginalRequest: issue.Title + "\n\n" + issue.Body, Criteria: criteria, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: a.Limits}},
		WorkItems:          []storage.ProductWorkItemDefinition{{ID: id, Repository: a.Policy.Repository, RepositoryID: a.Policy.RepositoryID, Criteria: scope.Included}},
	})
}

// A stable WorkItem may have been admitted from either source. Membership is
// authoritative; the source index only covers older Waves without metadata.
func (a IssueAdmitter) completedDependency(ctx context.Context, number int) error {
	known, err := a.Store.IsKnownProductSplitChildIssue(ctx, a.Policy.RepositoryID, number)
	if err != nil {
		return err
	}
	if known {
		return storage.ErrProductSplitChildSourceHeld
	}
	id := issueSourceID(a.Policy.RepositoryID, number)
	registration, registrationErr := a.Store.ReadProductWorkItemRegistration(ctx, id)
	if registrationErr != nil && !errors.Is(registrationErr, sql.ErrNoRows) {
		return registrationErr
	}
	var membership struct {
		Target contract.Target `json:"target"`
	}
	memberErr := a.Store.Record(ctx, "wave_work_item", id, &membership)
	if memberErr != nil && !errors.Is(memberErr, sql.ErrNoRows) {
		return memberErr
	}
	var waveID string
	if memberErr == nil {
		if !membership.Target.Valid() {
			return errors.New("dependency WorkItem ownership is invalid")
		}
		waveID = membership.Target.WaveID
	} else {
		if registrationErr == nil {
			return errors.New("dependency WorkItem ownership is missing")
		}
		err := a.Store.DB.QueryRowContext(ctx, "SELECT id FROM kernel_waves WHERE source='github' AND source_id=?", id).Scan(&waveID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	wave, err := a.Store.Wave(ctx, waveID)
	if err != nil {
		return err
	}
	if memberErr == nil && (membership.Target.ScopeRevision != wave.ScopeRevision || membership.Target.PolicyRevision != wave.Policy.Revision) {
		return errors.New("dependency WorkItem ownership is stale")
	}
	if registrationErr == nil && (registration.Value.WaveID != wave.ID || registration.Value.Source != wave.Source || registration.Value.SourceID != wave.SourceID || registration.Value.Definition.RepositoryID != a.Policy.RepositoryID || registration.Value.ScopeRevision != wave.ScopeRevision || registration.Value.PolicyRevision != wave.Policy.Revision) {
		return errors.New("dependency source ownership differs")
	}
	if wave.State != domain.WaveCompleted || wave.Policy.Stage != "P" || !wave.Policy.Product || wave.PendingDirectionID != "" {
		return errors.New("claimed Issue dependency has no completed Wave")
	}
	if registrationErr != nil && memberErr == nil {
		projection, err := a.Store.ReadProductTarget(ctx, membership.Target, id)
		if err != nil {
			return err
		}
		if projection.Value.WorkItem.RepositoryID != a.Policy.RepositoryID {
			return errors.New("dependency projection repository differs")
		}
	}
	return nil
}
