package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func productGateSnapshot(t *testing.T, p storage.ProductTarget) storage.ProductTargetSnapshot {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return storage.ProductTargetSnapshot{Value: p, Digest: fmt.Sprintf("%x", sha256.Sum256(raw))}
}
func canonicalGateFixture(t *testing.T, reportFile bool) (*Controller, *fakeGH, storage.ProductTargetSnapshot) {
	t.Helper()
	ctx := context.Background()
	c, gh, _, _ := setup(t)
	if reportFile {
		// Host configuration is fixed before starting the fixture's workflow.
		c.Config.CapabilitiesPath = filepath.Join(t.TempDir(), "capabilities.json")
		raw, err := json.Marshal(c.Capabilities)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(c.Config.CapabilitiesPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest()); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		w, err := c.Store.Get(ctx, "w")
		if err != nil {
			t.Fatal(err)
		}
		if w.Phase == domain.Reviewing {
			candidate, err := c.candidate(ctx, w)
			if err != nil {
				t.Fatal(err)
			}
			if gh.posts != 0 {
				t.Fatal("gate fixture published before evaluation")
			}
			return c, gh, productGateSnapshot(t, storage.ProductTarget{Target: contract.Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}, Repository: c.Config.Repository, BaseBranch: c.Config.BaseBranch, WorkItem: w, Candidate: candidate})
		}
		drive(t, c, 1)
	}
	t.Fatal("fixture never reached validated publication")
	return nil, nil, storage.ProductTargetSnapshot{}
}

func TestProductCanonicalPublicationGateUsesActualValidatedJournalWithoutSideEffects(t *testing.T) {
	c, gh, saved := canonicalGateFixture(t, false)
	publication, err := c.ProductPublicationGate(context.Background(), saved)
	if err != nil || publication.Title != "Harness implementation w" || !strings.Contains(publication.Body, saved.Value.Candidate.SHA) || !strings.Contains(publication.Body, "harness-journal:") {
		t.Fatal(publication, err)
	}
	after, err := c.Store.Get(context.Background(), "w")
	if err != nil || after.Version != saved.Value.WorkItem.Version || gh.posts != 0 || gh.merges != 0 {
		t.Fatal("gate changed canonical state or wrote externally", after, err)
	}
}

func TestProductCanonicalPublicationGateRejectsProjectionClaimsAndChangedEvidence(t *testing.T) {
	for _, mode := range []string{"digest", "config", "pause", "issue_edit", "hold_label", "validation_missing", "validation_failed", "candidate", "plan_test", "partial_residual", "canonical_pr"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			c, gh, saved := canonicalGateFixture(t, false)
			switch mode {
			case "digest":
				saved.Digest = "corrupt"
			case "config":
				c.Config.BaseBranch = "changed-default"
			case "pause":
				if err := c.Store.Apply(ctx, domain.Event{ID: "pause", WorkItemID: "w", Kind: "pause"}); err != nil {
					t.Fatal(err)
				}
			case "issue_edit":
				gh.issue.Body += "changed requirements"
			case "hold_label":
				gh.issue.Labels = append(gh.issue.Labels, struct {
					Name string `json:"name"`
				}{Name: c.Config.HoldLabel})
			case "validation_missing":
				if _, err := c.Store.DB.ExecContext(ctx, "DELETE FROM records WHERE kind='validation'"); err != nil {
					t.Fatal(err)
				}
			case "validation_failed":
				results, err := c.results(ctx, saved.Value.Candidate, saved.Value.WorkItem)
				if err != nil || len(results) == 0 {
					t.Fatal(results, err)
				}
				results[0].Status = "failed"
				if err := c.Store.Put(ctx, "validation", results[0].JobID, "w", results[0]); err != nil {
					t.Fatal(err)
				}
			case "candidate":
				candidate := saved.Value.Candidate
				candidate.SHA = strings.Repeat("f", 40)
				if err := c.Store.Put(ctx, "candidate", candidate.ID, "w", candidate); err != nil {
					t.Fatal(err)
				}
			case "plan_test":
				var plan verification.Plan
				if err := c.Store.Record(ctx, "plan", "w", &plan); err != nil {
					t.Fatal(err)
				}
				plan.CriteriaTests[0].TestRef = "tests/not-in-candidate.go"
				if err := c.Store.Put(ctx, "plan", "w", "w", plan); err != nil {
					t.Fatal(err)
				}
			case "partial_residual":
				var scope verification.Scope
				if err := c.Store.Record(ctx, "scope", "w", &scope); err != nil {
					t.Fatal(err)
				}
				scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "remaining behavior"})
				scope.Remaining = []string{"AC2"}
				if err := c.Store.Put(ctx, "scope", "w", "w", scope); err != nil {
					t.Fatal(err)
				}
			case "canonical_pr":
				if err := c.Store.Put(ctx, "pull_request", "w", "w", github.PR{Number: 99}); err != nil {
					t.Fatal(err)
				}
			}
			if publication, err := c.ProductPublicationGate(ctx, saved); err == nil || publication.Title != "" {
				t.Fatal("canonical gate accepted changed source", mode, publication, err)
			}
			if gh.posts != 0 || gh.merges != 0 {
				t.Fatal("gate performed external mutation")
			}
		})
	}
}

func TestProductCanonicalReviewGateReloadsRequestProofWithoutInventingApproval(t *testing.T) {
	ctx := context.Background()
	c, gh, saved := canonicalGateFixture(t, true)
	drive(t, c, 1) // Existing legacy publisher creates the fixture's canonical PR.
	saved.Value.PRNumber = gh.pr.Number
	saved = productGateSnapshot(t, saved.Value)
	report := c.Capabilities
	report.Capabilities = map[string]contract.Capability{}
	for key, value := range c.Capabilities.Capabilities {
		report.Capabilities[key] = value
	}
	report.Capabilities["cloud_native_approval"] = contract.Capability{State: contract.Unknown, Reason: "not observed"}
	save := func() {
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(c.Config.CapabilitiesPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	save()
	request, err := c.ProductReviewGate(ctx, saved)
	if err != nil || request.RequesterID != c.Config.RequesterID || request.Trigger != c.Config.ReviewTrigger || report.AutoMergeReady() {
		t.Fatal(request, err)
	}
	if gh.posts != 1 || gh.requests != 0 || gh.merges != 0 {
		t.Fatal("gate created review/approval/mutation")
	}
	report.Capabilities["cloud_request"] = contract.Capability{State: contract.Unknown, Reason: "request proof revoked"}
	save()
	if _, err := c.ProductReviewGate(ctx, saved); err == nil {
		t.Fatal("cached request proof survived revocation")
	}
	if err := os.WriteFile(c.Config.CapabilitiesPath, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ProductReviewGate(ctx, saved); err == nil {
		t.Fatal("missing report fields inherited verified proof")
	}
}

func TestProductCanonicalReviewGateRejectsChangedPRAndUnsafeReport(t *testing.T) {
	ctx := context.Background()
	c, gh, saved := canonicalGateFixture(t, true)
	drive(t, c, 1)
	saved.Value.PRNumber = gh.pr.Number
	saved = productGateSnapshot(t, saved.Value)
	if _, err := c.ProductReviewGate(ctx, saved); err != nil {
		t.Fatal("valid baseline rejected before negative cases", err)
	}
	pr := gh.pr
	raw, err := json.Marshal(c.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"head_repo", "base_repo", "head_branch", "base_branch", "head_sha", "base_sha", "closed", "draft", "merged", "symlink", "fifo", "group_write", "other_write", "oversized", "foreign_owner"} {
		t.Run(mode, func(t *testing.T) {
			gh.pr = pr
			path := c.Config.CapabilitiesPath
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "head_repo":
				gh.pr.Head.Repo.ID++
			case "base_repo":
				gh.pr.Base.Repo.ID++
			case "head_branch":
				gh.pr.Head.Ref = "another-branch"
			case "base_branch":
				gh.pr.Base.Ref = "another-base"
			case "head_sha":
				gh.pr.Head.SHA = strings.Repeat("e", 40)
			case "base_sha":
				gh.pr.Base.SHA = strings.Repeat("e", 40)
			case "closed":
				gh.pr.State = "closed"
			case "draft":
				gh.pr.Draft = true
			case "merged":
				gh.pr.Merged = true
			case "symlink":
				target := filepath.Join(t.TempDir(), "other-report")
				if err := os.WriteFile(target, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "group_write":
				if err := os.Chmod(path, 0660); err != nil {
					t.Fatal(err)
				}
			case "other_write":
				if err := os.Chmod(path, 0602); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.WriteFile(path, []byte(strings.Repeat(" ", (1<<20)+1)), 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign_owner":
				foreign := 65534
				if os.Geteuid() == foreign {
					foreign = 65533
				}
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
				if err := os.Chown(path, foreign, -1); err != nil {
					if os.IsPermission(err) {
						t.Skip("fixture cannot change report ownership")
					}
					t.Fatal(err)
				}
			}
			if _, err := c.ProductReviewGate(ctx, saved); err == nil {
				t.Fatal("unsafe review source accepted", mode)
			}
			if gh.posts != 1 || gh.requests != 0 || gh.merges != 0 {
				t.Fatal("gate performed a mutation")
			}
		})
	}
}
