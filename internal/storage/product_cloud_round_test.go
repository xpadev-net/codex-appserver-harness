package storage

import (
	"context"
	"reflect"
	"testing"
)

func TestProductCloudRoundReadsOriginalRequestAfterPauseAndReopen(t *testing.T) {
	ctx := context.Background()
	k, id, claim, proof, o := observedReviewFixture(t, true)
	if _, err := k.ReadProductCloudRound(ctx, id); err == nil {
		t.Fatal("unsettled request became round")
	}
	receipt, err := k.BindObservedPRReviewRequest(ctx, id, proof, o)
	if err != nil {
		t.Fatal(err)
	}
	round, err := k.ReadProductCloudRound(ctx, id)
	if err != nil || round.Packet.RoundID != id || round.Product.Value.Candidate != claim.Product.Value.Candidate || round.RequestCommentID != o.CommentID || !round.RequestedAt.Equal(o.ObservedAt) || round.Product.Digest != claim.Product.Digest || round.Digest == "" {
		t.Fatal(round, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-after-request", Target: claim.Product.Value.Target, Action: "pause", Reason: "operator paused"}); err != nil {
		t.Fatal(err)
	}
	replayed, err := k.ReadProductCloudRound(ctx, id)
	if err != nil || !reflect.DeepEqual(round, replayed) {
		t.Fatal("historical round depended on live dispatch grant", replayed, err)
	}
	var after PRCompletionReceipt
	if err := k.Record(ctx, "product_pr_completion", id, &after); err != nil || !reflect.DeepEqual(receipt, after) {
		t.Fatal("read changed request receipt", after, err)
	}
	r, err := k.OperationReservation(ctx, id)
	if err != nil || r.State != "held" {
		t.Fatal("round read released Cloud work", r, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if again, err := k.ReadProductCloudRound(ctx, id); err != nil || !reflect.DeepEqual(round, again) {
		t.Fatal("round changed after reopen", again, err)
	}
}

func TestProductCloudRoundMissingOrConflictingEvidenceCannotBecomeApproval(t *testing.T) {
	for _, mode := range []string{"claim", "packet", "receipt", "watch", "reservation", "digest", "body", "requester", "candidate", "transport"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, id, _, proof, o := observedReviewFixture(t, true)
			if _, err := k.BindObservedPRReviewRequest(ctx, id, proof, o); err != nil {
				t.Fatal(err)
			}
			kind := ""
			recordID := id
			switch mode {
			case "claim":
				kind = "product_pr_dispatch"
			case "packet":
				kind = "product_pr_review_payload"
			case "receipt":
				kind = "product_pr_completion"
			case "watch":
				var receipt PRCompletionReceipt
				if err := k.Record(ctx, "product_pr_completion", id, &receipt); err != nil {
					t.Fatal(err)
				}
				kind, recordID = "product_pr_subscription_intent", receipt.SubscriptionID
			case "reservation":
				if _, err := k.DB.Exec("DELETE FROM kernel_dispatch_intents WHERE run_id=?", id); err != nil {
					t.Fatal(err)
				}
			default:
				var receipt PRCompletionReceipt
				if err := k.Record(ctx, "product_pr_completion", id, &receipt); err != nil {
					t.Fatal(err)
				}
				if mode == "body" {
					receipt.ReviewRequest.Body += "changed"
				}
				if mode == "requester" {
					receipt.ReviewRequest.RequesterID++
				}
				if mode == "transport" {
					receipt.Transport.Disposition = "not_started"
				}
				if err := k.Put(ctx, "product_pr_completion", id, "fixture", receipt); err != nil {
					t.Fatal(err)
				}
				if mode == "digest" || mode == "candidate" {
					var claim PRDispatchClaim
					if err := k.Record(ctx, "product_pr_dispatch", id, &claim); err != nil {
						t.Fatal(err)
					}
					if mode == "digest" {
						claim.Product.Digest = "changed"
					} else {
						// Keep the Product internally valid and align every digest so only
						// its candidate reference differs from the admitted tool operation.
						claim.Product.Value.Candidate.ID = "changed"
						claim.Product.Value.WorkItem.Candidate = "changed"
						digest, err := contentDigest(claim.Product.Value)
						if err != nil {
							t.Fatal(err)
						}
						claim.Product.Digest = digest
						var packet PRReviewRequestPayload
						if err := k.Record(ctx, "product_pr_review_payload", id, &packet); err != nil {
							t.Fatal(err)
						}
						packet.ProductDigest = digest
						if err := k.Put(ctx, "product_pr_review_payload", id, "fixture", packet); err != nil {
							t.Fatal(err)
						}
						var intent PROperationIntent
						if err := k.Record(ctx, "product_pr_intent", id, &intent); err != nil {
							t.Fatal(err)
						}
						intent.ProductDigest = digest
						if intent.Candidate.CandidateID == claim.Product.Value.Candidate.ID || claim.Product.Value.validate() != nil {
							t.Fatal("candidate fixture did not isolate admitted reference")
						}
						if err := k.Put(ctx, "product_pr_intent", id, "fixture", intent); err != nil {
							t.Fatal(err)
						}
					}
					if err := k.Put(ctx, "product_pr_dispatch", id, "fixture", claim); err != nil {
						t.Fatal(err)
					}
				}
			}
			if kind != "" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, recordID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReadProductCloudRound(ctx, id); err == nil {
				t.Fatal("invalid round admitted", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&count); err != nil || count != 0 {
				t.Fatal("round read fabricated approval", count, err)
			}
		})
	}
}
