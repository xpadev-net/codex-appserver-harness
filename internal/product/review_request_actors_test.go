package product

import (
	"context"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestReviewRequesterRechecksFrozenProviderActorsBeforeSend(t *testing.T) {
	for _, mode := range []string{"changed", "reordered", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, api, _, id := requestFixture(t)
			actors := []int64{93, 91}
			if mode == "legacy" {
				actors = nil
			}
			r.Gate = func(context.Context, storage.ProductTargetSnapshot) (ReviewRequest, error) {
				return ReviewRequest{Trigger: "@review please review", RequesterID: 71, CloudReviewerIDs: actors}, nil
			}
			api.onRequest = func() {
				if mode == "changed" {
					actors[0] = 94
				} else if mode == "reordered" {
					actors[0], actors[1] = actors[1], actors[0]
				}
			}
			receipt, err := r.Request(ctx, id, "actor-owner")
			var packet storage.PRReviewRequestPayload
			if e := r.Store.Record(ctx, "product_pr_review_payload", id, &packet); e != nil {
				t.Fatal(e)
			}
			if mode == "changed" {
				if err == nil || api.requestWrites != 0 || !reflect.DeepEqual(packet.CloudReviewerIDs, []int64{91, 93}) {
					t.Fatal("changed provider mapping sent or changed packet", packet, err)
				}
				var claim storage.PRDispatchClaim
				if e := r.Store.Record(ctx, "product_pr_dispatch", id, &claim); e != nil || claim.State != "prepared" {
					t.Fatal(claim, e)
				}
				actors = []int64{93, 91}
				api.onRequest = nil
				receipt, err = r.Request(ctx, id, "actor-owner")
			}
			if err != nil || receipt.Outcome != "review_requested" || api.requestWrites != 1 {
				t.Fatal(receipt, err)
			}
			round, err := r.Store.ReadProductCloudRound(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "legacy" {
				if round.Packet.CloudReviewerIDs != nil {
					t.Fatal("requester became provider", round)
				}
			} else if !reflect.DeepEqual(round.Packet.CloudReviewerIDs, []int64{91, 93}) {
				t.Fatal(round)
			}
		})
	}
}
