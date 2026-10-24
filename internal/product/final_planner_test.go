package product

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type finalReadFixture struct {
	ResourceAPI
	repository        Repository
	head, merge, mode string
	paths             []string
	refs, metadata    int
}

func (a *finalReadFixture) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.paths = append(a.paths, path)
	if a.mode == "read_error" {
		return nil, errors.New("read failed")
	}
	if a.mode == "oversize" {
		return json.RawMessage(strings.Repeat(" ", (4<<20)+1)), nil
	}
	if path == "" {
		a.metadata++
		repo := a.repository
		if a.mode == "repository" || a.mode == "retarget" && a.metadata > 1 {
			repo.ID++
		}
		return json.Marshal(repo)
	}
	if strings.HasPrefix(path, "git/ref/heads/") {
		a.refs++
		sha, kind, ref := a.head, "commit", "refs/heads/"+a.repository.DefaultBranch
		if a.mode == "moved" && a.refs > 1 {
			sha = strings.Repeat("c", 40)
		}
		if a.mode == "bad_sha" {
			sha = "master"
		}
		if a.mode == "tag" {
			kind = "tag"
		}
		if a.mode == "wrong_ref" {
			ref = "refs/heads/other"
		}
		return json.Marshal(map[string]any{"ref": ref, "object": map[string]string{"sha": sha, "type": kind}})
	}
	if strings.HasPrefix(path, "compare/") {
		status, base, ancestor := "ahead", a.merge, a.merge
		if a.mode == "diverged" {
			status = "diverged"
		}
		if a.mode == "wrong_base" {
			base = strings.Repeat("d", 40)
		}
		if a.mode == "wrong_ancestor" {
			ancestor = strings.Repeat("d", 40)
		}
		return json.Marshal(map[string]any{"status": status, "base_commit": map[string]string{"sha": base}, "merge_base_commit": map[string]string{"sha": ancestor}})
	}
	return nil, errors.New("unexpected final-plan GET")
}

// Called by the actual SQLite/Git/runner/independent-review/native-merge
// fixture after the real registered wrapper has stopped and its receipt saved.
func checkFinalPlanner(t *testing.T, k *storage.KernelStore, target contract.Target) {
	t.Helper()
	ctx := context.Background()
	coverage, err := k.ProductCoverage(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ReadProductMergedWorkItem(ctx, coverage.Value.Merged[0].MergeID)
	if err != nil {
		t.Fatal(err)
	}
	p := receipt.Value.Product.Value
	var saved storage.ProductFinalPlan
	for _, mode := range []string{"success", "same_head", "repository", "retarget", "moved", "bad_sha", "tag", "wrong_ref", "diverged", "wrong_base", "wrong_ancestor", "read_error", "oversize", "gate_initial", "gate_final", "cancel", "coverage_changed"} {
		t.Run("final_plan_"+mode, func(t *testing.T) {
			api := &finalReadFixture{repository: Repository{ID: p.WorkItem.RepositoryID, Name: p.Repository, DefaultBranch: p.BaseBranch}, head: strings.Repeat("f", 40), merge: receipt.Value.MergeSHA, mode: mode}
			if mode == "same_head" {
				api.head = api.merge
			}
			gates := 0
			planner := FinalPlanner{Store: k, Gate: func(ctx context.Context, _ contract.Target) error {
				gates++
				if mode == "gate_initial" || mode == "gate_final" && gates == 2 {
					return storage.ErrProductFinalPlanHeld
				}
				if mode == "coverage_changed" && gates == 2 {
					_, err := k.DB.ExecContext(ctx, "DELETE FROM records WHERE kind='product_merged_work_item' AND id=?", receipt.Value.MergeID)
					return err
				}
				return ctx.Err()
			}, API: func(got storage.ProductTarget) ResourceAPI {
				if !reflect.DeepEqual(got, p) {
					t.Fatal("transport received another repository", got)
				}
				return api
			}}
			call := ctx
			if mode == "cancel" {
				c, stop := context.WithCancel(ctx)
				stop()
				call = c
			}
			plan, err := planner.Prepare(call, target)
			if mode == "success" || mode == "same_head" {
				if err != nil || plan.Value.Repositories[0].Candidate.SHA != api.head || gates != 2 {
					t.Fatal(plan, err, gates)
				}
				if mode == "success" {
					saved = plan
					expected := []string{"", "git/ref/heads/" + p.BaseBranch, "compare/" + api.merge + "..." + api.head + "?per_page=1", "git/ref/heads/" + p.BaseBranch, ""}
					if !reflect.DeepEqual(api.paths, expected) {
						t.Fatal("missing fixed-head ancestry/bookends", api.paths)
					}
				} else if len(api.paths) != 4 {
					t.Fatal("same SHA required unnecessary comparison", api.paths)
				}
			} else if err == nil {
				t.Fatal("unproven final plan admitted", mode, plan)
			}
			if mode == "gate_initial" && len(api.paths) != 0 {
				t.Fatal("held source still read GitHub", api.paths)
			}
		})
	}
	// The coverage-loss case must invalidate historical reads through their
	// underlying receipts too; no plan is a substitute for its durable proof.
	if _, err := k.ReadProductFinalPlan(ctx, saved.ID); err == nil {
		t.Fatal("lost receipt still authenticated saved plan")
	}
	if err := k.Put(ctx, "product_merged_work_item", receipt.Value.MergeID, target.WaveID, receipt); err != nil {
		t.Fatal(err)
	}
	checkFinalRunner(t, k, target, receipt)
}
