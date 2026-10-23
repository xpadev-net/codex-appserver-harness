package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"syscall"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// ProductPublicationGate rechecks the canonical journal rather than accepting
// the kernel's cached projection as validation evidence. It performs reads only;
// the external adapter owns the atomic final send fence and HTTP mutation.
func (c *Controller) ProductPublicationGate(ctx context.Context, saved storage.ProductTargetSnapshot) (product.Publication, error) {
	var publication product.Publication
	if c == nil || c.Store == nil || c.GitHub == nil {
		return publication, errors.New("canonical product journal and GitHub reader required")
	}
	if err := c.Config.Validate(); err != nil {
		return publication, err
	}
	p := saved.Value
	if !p.Target.Valid() {
		return publication, errors.New("versioned product Wave target required")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return publication, err
	}
	if saved.Digest != fmt.Sprintf("%x", sha256.Sum256(raw)) || p.Repository != c.Config.Repository || p.BaseBranch != c.Config.BaseBranch || p.WorkItem.RepositoryID != c.Config.RepositoryID {
		return publication, errors.New("canonical product configuration/projection conflict")
	}
	w, err := c.Store.Get(ctx, p.WorkItem.ID)
	if err != nil {
		return publication, err
	}
	if !reflect.DeepEqual(w, p.WorkItem) || w.Phase != domain.Reviewing || w.Paused || w.MergeHold {
		return publication, errors.New("canonical WorkItem changed or publication held")
	}
	candidate, err := c.candidate(ctx, w)
	if err != nil {
		return publication, err
	}
	if candidate != p.Candidate {
		return publication, errors.New("canonical candidate differs from operation")
	}
	var canonicalPR github.PR
	err = c.Store.Record(ctx, "pull_request", w.ID, &canonicalPR)
	if err != nil && !missing(err) {
		return publication, err
	}
	if (err == nil && canonicalPR.Number < 1) || canonicalPR.Number != p.PRNumber {
		return publication, errors.New("canonical PR differs from projection")
	}
	switch c.GitHub.(type) {
	case github.Client, *github.Client:
		if err := c.VerifyRuntime(ctx); err != nil {
			return publication, err
		}
	}
	scope, err := c.current(ctx, w)
	if err != nil {
		return publication, err
	}
	var plan verification.Plan
	if err := c.Store.Record(ctx, "plan", w.ID, &plan); err != nil {
		return publication, err
	}
	if err := plan.Validate(w.ID, candidate, scope, c.Config.Verification); err != nil {
		return publication, err
	}
	results, err := c.results(ctx, candidate, w)
	if err != nil {
		return publication, err
	}
	if err := plan.ValidateCommit(ctx, w.Worktree, candidate.SHA); err != nil {
		return publication, err
	}
	if !verification.PassedPlan(candidate, c.Config.Verification, plan, results) {
		return publication, errors.New("canonical validation gates are not satisfied")
	}
	if len(scope.Remaining) > 0 {
		var residual splitting.Residual
		if err := c.Store.Record(ctx, "residual", w.ID, &residual); err != nil {
			return publication, err
		}
		if err := residual.Validate(w, scope); err != nil {
			return publication, err
		}
		messages, err := workspace.Git(ctx, w.Worktree, "log", "--format=%B", candidate.BaseSHA+".."+candidate.SHA)
		if err != nil {
			return publication, err
		}
		if err := github.CheckClosingText(messages); err != nil {
			return publication, err
		}
	}
	profiles, err := plan.Profiles(c.Config.Verification)
	if err != nil {
		return publication, err
	}
	publication = product.Publication{Title: "Harness implementation " + w.ID, Body: publicationBody(w, scope, candidate, profiles, results)}
	if err := github.CheckClosingText(publication.Title + "\n" + publication.Body); err != nil {
		return product.Publication{}, err
	}
	latest, err := c.Store.Get(ctx, w.ID)
	if err != nil {
		return product.Publication{}, err
	}
	if !reflect.DeepEqual(latest, w) {
		return product.Publication{}, errors.New("canonical WorkItem changed during gate evaluation")
	}
	return publication, nil
}

// ProductReviewGate checks the proven request capability separately from native
// approval capability. A positive result authorizes only a review request.
func (c *Controller) ProductReviewGate(ctx context.Context, saved storage.ProductTargetSnapshot) (product.ReviewRequest, error) {
	var request product.ReviewRequest
	if _, err := c.ProductPublicationGate(ctx, saved); err != nil {
		return request, err
	}
	p := saved.Value
	if p.PRNumber < 1 {
		return request, errors.New("canonical bound PR required for review")
	}
	pr, err := c.GitHub.Pull(ctx, p.PRNumber)
	if err != nil {
		return request, err
	}
	if !publicationTarget(pr, p.WorkItem, c.Config.BaseBranch) || pr.Head.SHA != p.Candidate.SHA || pr.Base.SHA != p.Candidate.BaseSHA {
		return request, errors.New("canonical review PR/header changed")
	}
	report := c.Capabilities
	if c.Config.CapabilitiesPath != "" {
		f, err := os.OpenFile(c.Config.CapabilitiesPath, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return request, err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return request, err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Size() > 1<<20 {
			return request, errors.New("bounded trusted capability report required")
		}
		raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
		if err != nil {
			return request, err
		}
		if len(raw) > 1<<20 {
			return request, errors.New("capability report exceeds limit")
		}
		report = contract.Report{}
		if err := contract.DecodeStrict(raw, &report); err != nil {
			return request, err
		}
	}
	copy := *c
	copy.Capabilities = report
	if report.Version != 1 || report.ObservedAt.IsZero() || !strings.EqualFold(report.Repository, c.Config.Repository) || report.DefaultBranch != c.Config.BaseBranch || !copy.reviewPolicy().VerifiedRequest || c.Config.RequesterID < 1 || strings.TrimSpace(c.Config.ReviewTrigger) == "" {
		return request, errors.New("canonical Cloud review request route is unverified")
	}
	latest, err := c.Store.Get(ctx, p.WorkItem.ID)
	if err != nil {
		return request, err
	}
	if !reflect.DeepEqual(latest, p.WorkItem) {
		return request, errors.New("canonical WorkItem changed during review gate evaluation")
	}
	return product.ReviewRequest{Trigger: c.Config.ReviewTrigger, RequesterID: c.Config.RequesterID}, nil
}
