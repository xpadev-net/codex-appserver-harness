package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func cloudCompletionFixture(t *testing.T, actors []int64) (*KernelStore, ProductCloudRound, ProductCloudCompletionValue) {
	t.Helper()
	ctx := context.Background()
	k, id, _, proof, o := observedReviewFixture(t, true, actors)
	if _, err := k.BindObservedPRReviewRequest(ctx, id, proof, o); err != nil {
		t.Fatal(err)
	}
	round, err := k.ReadProductCloudRound(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	report := contract.NewReport(round.Product.Value.Repository)
	report.RepositoryID, report.DefaultBranch = round.Resource.RepositoryID, round.Product.Value.BaseBranch
	for _, key := range []string{"cloud_request", "review_sha"} {
		report.Capabilities[key] = contract.Capability{State: contract.Verified, Evidence: "fixture mapped provider response"}
	}
	value := ProductCloudCompletionValue{OperationID: id, RoundDigest: round.Digest, ProofID: "provider-response", Report: report, Reviews: []ProductNativeReviewFact{{ID: 101, ReviewerID: 91, State: "COMMENTED", SHA: round.Product.Value.Candidate.SHA, SubmittedAt: k.clock()}}}
	value.ReadAt = k.clock()
	return k, round, value
}

func TestCloudCompletionReleasesOnlyItsOwnWorkWithoutApprovalAfterPauseOrNewCandidate(t *testing.T) {
	for _, mode := range []string{"normal", "pause", "cancel", "new_candidate"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, round, value := cloudCompletionFixture(t, []int64{91})
			p := round.Product.Value
			if mode == "pause" || mode == "cancel" {
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "control-before-cloud-end", Target: p.Target, Action: mode, Reason: "operator control"}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "new_candidate" {
				next := p
				next.Candidate.ID = "next-candidate"
				next.Candidate.SHA = strings.Repeat("c", 40)
				next.WorkItem.Candidate = next.Candidate.ID
				next.WorkItem.Version++
				if _, err := k.SetProductTarget(ctx, round.Product.Digest, next); err != nil {
					t.Fatal(err)
				}
			}
			before, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil {
				t.Fatal(err)
			}
			completed, err := k.RecordProductCloudReviewCompleted(ctx, value)
			if err != nil || completed.Digest == "" {
				t.Fatal(completed, err)
			}
			seat, err := k.OperationReservation(ctx, round.ReservationID)
			if err != nil || seat.State != "released" || seat.Proof.Disposition != "completed" {
				t.Fatal(seat, err)
			}
			if heldPRSlots(t, k, "cloud_review") != 0 || heldPRSlots(t, k, "pr") != 1 {
				t.Fatal("completion released PR or retained Cloud work")
			}
			after, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("Cloud work completion changed product authority", after, err)
			}
			var approvals int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&approvals); err != nil || approvals != 0 {
				t.Fatal("COMMENTED became approval", approvals, err)
			}
			again, err := k.RecordProductCloudReviewCompleted(ctx, value)
			if err != nil || !reflect.DeepEqual(completed, again) {
				t.Fatal("completion replay changed receipt", again, err)
			}
			root := k.Root
			k.Close()
			k, err = OpenExistingKernel(root + "/state.db")
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			if read, err := k.ReadProductCloudReviewCompletion(ctx, value.OperationID); err != nil || !reflect.DeepEqual(read, completed) {
				t.Fatal("completion lost after reopen", read, err)
			}
		})
	}
}

func TestCloudCompletionUnknownOrUnmappedFactsKeepCapacity(t *testing.T) {
	for _, mode := range []string{"no_actor", "wrong_actor", "wrong_sha", "old_round", "pending", "empty", "duplicate", "future_review", "future_read", "unknown_mapping", "unknown_request", "report_repository", "report_future", "stale_report", "wrong_digest", "wrong_operation"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			actors := []int64{91}
			if mode == "no_actor" {
				actors = nil
			}
			k, round, value := cloudCompletionFixture(t, actors)
			switch mode {
			case "wrong_actor":
				value.Reviews[0].ReviewerID = 92
			case "wrong_sha":
				value.Reviews[0].SHA = round.Product.Value.Candidate.BaseSHA
			case "old_round":
				value.Reviews[0].SubmittedAt = round.RequestedAt.Add(-time.Second)
			case "pending":
				value.Reviews[0].State = "PENDING"
			case "empty":
				value.Reviews = nil
			case "duplicate":
				value.Reviews = append(value.Reviews, value.Reviews[0])
			case "future_review":
				value.Reviews[0].SubmittedAt = value.ReadAt.Add(time.Hour)
			case "future_read":
				value.ReadAt = value.ReadAt.Add(time.Hour)
			case "unknown_mapping":
				value.Report.Capabilities["review_sha"] = contract.Capability{State: contract.Unknown}
			case "unknown_request":
				value.Report.Capabilities["cloud_request"] = contract.Capability{State: contract.Unknown}
			case "report_repository":
				value.Report.RepositoryID++
			case "stale_report":
				value.Report.ObservedAt = round.RequestedAt.Add(-time.Second)
			case "report_future":
				value.Report.ObservedAt = value.ReadAt.Add(time.Hour)
			case "wrong_digest":
				value.RoundDigest = "wrong"
			case "wrong_operation":
				value.OperationID += "other"
			}
			if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err == nil {
				t.Fatal("unconfirmed completion released capacity", mode)
			}
			r, err := k.OperationReservation(ctx, round.ReservationID)
			if err != nil || r.State != "held" {
				t.Fatal(r, err)
			}
		})
	}
}

func TestCloudCompletionRollbackLostHistoryAndActorPinsFailClosed(t *testing.T) {
	for _, mode := range []string{"rollback", "completion_head", "completion_history", "actor_history", "actor_changed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, round, value := cloudCompletionFixture(t, []int64{91})
			if mode == "rollback" {
				if _, err := k.DB.Exec(`CREATE TRIGGER reject_cloud_end BEFORE INSERT ON records WHEN NEW.kind='product_cloud_completion' BEGIN SELECT RAISE(ABORT,'cloud fixture rollback'); END`); err != nil {
					t.Fatal(err)
				}
				if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err == nil {
					t.Fatal("failed receipt committed")
				}
				r, err := k.OperationReservation(ctx, round.ReservationID)
				if err != nil || r.State != "held" {
					t.Fatal("rollback released Cloud work", r, err)
				}
				if _, err := k.DB.Exec("DROP TRIGGER reject_cloud_end"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err != nil {
					t.Fatal("rollback retained false admission", err)
				}
				return
			}
			if mode == "actor_history" || mode == "actor_changed" {
				if mode == "actor_history" {
					if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_cloud_actor_history' AND id=?", value.OperationID); err != nil {
						t.Fatal(err)
					}
				} else {
					packet := round.Packet
					packet.CloudReviewerIDs = []int64{92}
					if err := k.Put(ctx, "product_pr_review_payload", value.OperationID, "fixture", packet); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := k.ReadProductCloudRound(ctx, value.OperationID); !errors.Is(err, ErrTaskEvidence) {
					t.Fatal("actor history loss changed provider", err)
				}
				if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err == nil {
					t.Fatal("actor proof regenerated")
				}
				r, err := k.OperationReservation(ctx, round.ReservationID)
				if err != nil || r.State != "held" {
					t.Fatal(r, err)
				}
				return
			}
			if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err != nil {
				t.Fatal(err)
			}
			kind := "product_cloud_completion"
			if mode == "completion_history" {
				kind = "product_cloud_completion_history"
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, value.OperationID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductCloudReviewCompletion(ctx, value.OperationID); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost completion evidence became absent", err)
			}
			if _, err := k.RecordProductCloudReviewCompleted(ctx, value); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost completion regenerated", err)
			}
		})
	}
}

func TestCloudCompletionReplayDoesNotReleaseNextReviewOwner(t *testing.T) {
	ctx := context.Background()
	k, round, value := cloudCompletionFixture(t, []int64{91})
	if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err != nil {
		t.Fatal(err)
	}
	var intent PROperationIntent
	if err := k.Record(ctx, "product_pr_intent", value.OperationID, &intent); err != nil {
		t.Fatal(err)
	}
	p := round.Product.Value
	args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: p.Target, Candidate: intent.Candidate})
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, contract.ToolInvocation{SessionID: intent.Source.SessionID, RunID: intent.Source.RunID, ThreadID: intent.Source.ThreadID, TurnID: intent.Source.TurnID, CallID: "next-review-after-terminal", Name: "pr_review_request", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	next, err := k.PreparePRDispatch(ctx, reply.OperationID, "next-owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err != nil {
		t.Fatal(err)
	}
	r, err := k.OperationReservation(ctx, next.ReservationID)
	if err != nil || r.State != "held" || heldPRSlots(t, k, "cloud_review") != 1 {
		t.Fatal("old completion released new owner", r, err)
	}
}

func TestCloudActorInputEvidenceCannotBeRegeneratedBeforeSend(t *testing.T) {
	for _, kind := range []string{"product_pr_review_payload", "product_cloud_actor_history"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, id, claim, _, _ := observedReviewFixture(t, false, []int64{91})
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.SavePRReviewRequestPayload(ctx, id, claim.OwnerID, "@review please review", 71, []int64{91}); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost provider pin was regenerated", kind, err)
			}
			var current PRDispatchClaim
			if err := k.Record(ctx, "product_pr_dispatch", id, &current); err != nil || current.State != "prepared" || !current.MayHaveSentAt.IsZero() {
				t.Fatal("lost actor evidence consumed send fence", current, err)
			}
		})
	}
}
