package product

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// PublicationGate consumes the new kernel journal, independently of the legacy
// service Store. Publisher calls it again immediately before every HTTP write.
// Local quality and this proof do not constitute native GitHub/Cloud approval.
func (e Executor) PublicationGate(ctx context.Context, saved storage.ProductTargetSnapshot) (Publication, error) {
	var publication Publication
	if e.Store == nil {
		return publication, storage.ErrProductQualityHeld
	}
	p := saved.Value
	ready, err := e.Store.ReadProductPublicationReadiness(ctx, p.Target, p.WorkItem.ID)
	if err != nil {
		return publication, err
	}
	if !reflect.DeepEqual(ready.Value.Product, saved) {
		return publication, storage.ErrProductQualityHeld
	}
	repo, ok := e.Repositories[p.WorkItem.RepositoryID]
	if !ok || repo.Name != p.Repository || repo.BaseBranch != p.BaseBranch || repo.Policy.RepositoryID != p.WorkItem.RepositoryID {
		return publication, storage.ErrProductQualityHeld
	}
	pinned, err := e.Store.ReadProductVerificationPolicy(ctx, p.Target, p.WorkItem.RepositoryID)
	if err != nil {
		return publication, err
	}
	expected := repo.Policy
	expected.WaveID, expected.PolicyRevision = p.Target.WaveID, p.Target.PolicyRevision
	if err := expected.Validate(); err != nil {
		return publication, err
	}
	expected.VerifierPaths = slices.Clone(expected.VerifierPaths)
	slices.Sort(expected.VerifierPaths)
	if !reflect.DeepEqual(expected, pinned.Value) {
		return publication, storage.ErrProductQualityHeld
	}
	wave, err := e.Store.Wave(ctx, p.Target.WaveID)
	if err != nil {
		return publication, err
	}
	registration, err := e.Store.ReadProductWorkItemRegistration(ctx, p.WorkItem.ID)
	if err != nil {
		return publication, err
	}
	// Partial publication needs its own validated residual/parent ledger. Until
	// that port is connected, a narrower WorkItem cannot imply full coverage.
	if wave.Target() != p.Target || len(registration.Value.Definition.Criteria) != len(wave.Criteria) {
		return publication, fmt.Errorf("%w: partial publication requires residual evidence", storage.ErrProductQualityHeld)
	}
	proof := ready.Value.Quality
	var body strings.Builder
	fmt.Fprintf(&body, "Work item `%s` in Wave `%s`.\n\nCandidate: `%s`\n\nComparison base: `%s`\n\nScope revision: %d; policy revision: %d.\n\nLocal independent quality: `%s`.\n", p.WorkItem.ID, p.Target.WaveID, p.Candidate.SHA, p.Candidate.BaseSHA, p.Target.ScopeRevision, p.Target.PolicyRevision, proof.Digest)
	if p.WorkItem.Issue > 0 {
		fmt.Fprintf(&body, "\nRefs #%d\n", p.WorkItem.Issue)
	}
	for _, criterion := range sortedProofKeys(proof.Value.Criteria) {
		fmt.Fprintf(&body, "\n- Evaluated criterion: `%s`", criterion)
	}
	for _, id := range sortedProofKeys(proof.Value.Verifications) {
		fmt.Fprintf(&body, "\n- Machine evidence `%s`: `%s`", id, proof.Value.Verifications[id])
	}
	for _, id := range sortedProofKeys(proof.Value.Reviews) {
		fmt.Fprintf(&body, "\n- Independent review `%s`: `%s`", id, proof.Value.Reviews[id])
	}
	fmt.Fprintln(&body)
	publication = Publication{Title: "Harness implementation " + p.WorkItem.ID, Body: body.String()}
	if err := github.CheckClosingText(publication.Title + "\n" + publication.Body); err != nil {
		return Publication{}, err
	}
	return publication, nil
}

func sortedProofKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
