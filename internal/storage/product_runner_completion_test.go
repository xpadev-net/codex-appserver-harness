package storage

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

// Storage fixtures exercise the host-only proof port, not physical execution.
func productRunnerCompletionFixture(job ProductRunnerJob) runner.Completion {
	code := 0
	s := job.Value.Spec
	return runner.Completion{Result: runner.Result{JobID: job.ID, Candidate: s.Candidate, TestedSHA: s.Candidate.SHA, ProfileID: s.ProfileID, ProfileRevision: s.Profile.Revision, Status: "passed", ExitCode: &code, Started: time.Now().UTC(), Finished: time.Now().UTC(), LogPath: filepath.Join(s.Root, job.ID, "output.log"), Environment: "fixture"}, ResultDigest: strings.Repeat("a", 64), LogDigest: strings.Repeat("b", 64), LogBytes: 5}
}

func TestProductRunnerCompletionPersistsFactsAndReleasesOnlyOwnCapacity(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime := productRunnerFixture(t)
	job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	c := productRunnerCompletionFixture(job)
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c); err == nil {
		t.Fatal("unclaimed completion accepted")
	}
	if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
		t.Fatal(err)
	}
	c = productRunnerCompletionFixture(job)
	finished, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c)
	if err != nil || finished.State != "completed" || finished.Completion == nil || !reflect.DeepEqual(*finished.Completion, c) {
		t.Fatal(finished, err)
	}
	r, err := k.OperationReservation(ctx, job.ID)
	if err != nil || r.State != "released" || r.Proof == nil || r.Proof.Disposition != "process_stopped" {
		t.Fatal(r, err)
	}
	again, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c)
	if err != nil || !reflect.DeepEqual(again, finished) {
		t.Fatal(again, err)
	}
	c.LogBytes++
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c); err == nil {
		t.Fatal("terminal proof changed")
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || p.Value.WorkItem.Phase != domain.Verifying {
		t.Fatal("runner completion bypassed independent quality", p, err)
	}
	if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil || claim.Start {
		t.Fatal(claim, err)
	}
}

func TestProductRunnerCompletionRejectsUnknownAndWrongSource(t *testing.T) {
	for _, mode := range []string{"unknown", "sha", "profile", "job", "missing_exit", "empty_log", "synthetic", "preclaim"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, plan, runtime := productRunnerFixture(t)
			job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
				t.Fatal(err)
			}
			c := productRunnerCompletionFixture(job)
			switch mode {
			case "preclaim":
				c.Result.Started = job.CreatedAt.Add(-time.Second)
			case "unknown":
				c.Result.Status = "unknown"
			case "sha":
				c.Result.TestedSHA = strings.Repeat("e", 40)
			case "profile":
				c.Result.ProfileRevision++
			case "job":
				c.Result.JobID = "foreign"
			case "missing_exit":
				c.Result.ExitCode = nil
			case "empty_log":
				c.LogBytes = 0
			case "synthetic":
				c.Result.ResolutionEvidence = "statement"
			}
			if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c); err == nil {
				t.Fatal("invalid result accepted", mode)
			}
			r, err := k.OperationReservation(ctx, job.ID)
			if err != nil || r.State != "held" {
				t.Fatal("invalid result released capacity", r, err)
			}
		})
	}
}

func TestProductRunnerCompletionRollbackAndTerminalHistory(t *testing.T) {
	ctx := context.Background()
	k, _, plan, runtime := productRunnerFixture(t)
	job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
		t.Fatal(err)
	}
	c := productRunnerCompletionFixture(job)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_runner_terminal BEFORE INSERT ON records WHEN NEW.kind='product_runner_job_terminal' BEGIN SELECT RAISE(FAIL,'terminal rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c); err == nil || !strings.Contains(err.Error(), "terminal rollback") {
		t.Fatal(err)
	}
	r, err := k.OperationReservation(ctx, job.ID)
	if err != nil || r.State != "held" {
		t.Fatal("partial capacity release", r, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_runner_terminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_runner_job_terminal'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductRunnerJob(ctx, job.ID); err == nil {
		t.Fatal("lost terminal history accepted")
	}
}
