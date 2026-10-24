package storage

import (
	"context"
	"reflect"
	"testing"
)

func TestCloudRequestFreezesCanonicalProviderActorsIndependentlyOfRequester(t *testing.T) {
	ctx := context.Background()
	actors := []int64{93, 91}
	k, id, claim, proof, o := observedReviewFixture(t, false, actors)
	actors[0] = 999
	var packet PRReviewRequestPayload
	if err := k.Record(ctx, "product_pr_review_payload", id, &packet); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(packet.CloudReviewerIDs, []int64{91, 93}) || packet.RequesterID != 71 {
		t.Fatal("provider identities changed or inferred from requester", packet)
	}
	replay, err := k.SavePRReviewRequestPayload(ctx, id, claim.OwnerID, packet.Trigger, packet.RequesterID, []int64{91, 93})
	if err != nil || !reflect.DeepEqual(replay, packet) {
		t.Fatal("equivalent actor set changed packet", replay, err)
	}
	if _, err := k.SavePRReviewRequestPayload(ctx, id, claim.OwnerID, packet.Trigger, packet.RequesterID, []int64{91, 94}); err == nil {
		t.Fatal("saved request retargeted provider")
	}
	if _, err := k.BindObservedPRReviewRequest(ctx, id, proof, o); err != nil {
		t.Fatal(err)
	}
	round, err := k.ReadProductCloudRound(ctx, id)
	if err != nil || !reflect.DeepEqual(round.Packet.CloudReviewerIDs, packet.CloudReviewerIDs) {
		t.Fatal(round, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if again, err := k.ReadProductCloudRound(ctx, id); err != nil || !reflect.DeepEqual(round, again) {
		t.Fatal("actor snapshot lost after reopen", again, err)
	}
}

func TestCloudRequestActorSetsRejectAmbiguousOrUnboundedIdentity(t *testing.T) {
	for _, actors := range [][]int64{{0}, {-1}, {91, 91}, make([]int64, 129)} {
		if _, err := CanonicalCloudReviewerIDs(actors); err == nil {
			t.Fatal("invalid provider identity accepted", actors)
		}
	}
	for _, actors := range [][]int64{nil, {}} {
		canonical, err := CanonicalCloudReviewerIDs(actors)
		if err != nil || canonical != nil {
			t.Fatal("legacy request gained provider identities", canonical, err)
		}
	}
}
