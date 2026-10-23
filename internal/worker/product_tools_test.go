package worker

import (
	"context"
	"errors"
	"testing"
)

type recordingToolProfiles struct {
	*sessionStoreFixture
	profile string
	failure error
}

func (s *recordingToolProfiles) PinSessionTools(_ context.Context, _ SessionBinding, profile string) error {
	s.profile = profile
	return s.failure
}

func TestProductToolsPublishByRoleAndRequirePersistenceBeforeRPC(t *testing.T) {
	for _, kind := range []string{"task", "orchestrator"} {
		d, store, rpc := newSessionDriverFixture(kind)
		d.ProductTools = true
		if _, err := d.Prepare(context.Background(), "session"); err == nil || len(rpc.calls) != 0 || store.creating {
			t.Fatal("product tools allowed without persistence", err)
		}
		profiles := &recordingToolProfiles{sessionStoreFixture: store, failure: errors.New("profile storage unavailable")}
		d.Store = profiles
		if _, err := d.Prepare(context.Background(), "session"); !errors.Is(err, profiles.failure) || len(rpc.calls) != 0 || store.creating {
			t.Fatal("profile save failed after RPC/reservation", err)
		}
		profiles.failure = nil
		if _, err := d.Prepare(context.Background(), "session"); err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, tool := range rpc.preparedTools {
			names[tool.Name] = true
		}
		if profiles.profile != "product_v1" || !names["pr_publish"] || !names["pr_review_request"] || names["wave_create"] != (kind == "orchestrator") {
			t.Fatal(kind, profiles.profile, names)
		}
	}
}

func TestProductToolProfileRejectsUnknownOrImplicitOptInBeforeReservation(t *testing.T) {
	for _, mode := range []struct {
		enabled bool
		profile string
	}{{false, "product_v2"}, {true, "unknown"}} {
		d, store, rpc := newSessionDriverFixture("task")
		d.ProductTools, d.ProductToolProfile = mode.enabled, mode.profile
		profiles := &recordingToolProfiles{sessionStoreFixture: store}
		d.Store = profiles
		if _, err := d.Prepare(context.Background(), "session"); err == nil || store.creating || len(rpc.calls) != 0 || profiles.profile != "" {
			t.Fatal("invalid host profile reserved a thread or called RPC", mode, err)
		}
	}
}
