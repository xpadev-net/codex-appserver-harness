package contract

import "testing"

func TestMergeFailsClosed(t *testing.T) {
	r := NewReport("owner/repo")
	r.RepositoryID = 1
	r.DefaultBranch = "custom"
	if r.AutoMergeReady() {
		t.Fatal("unknown capabilities enabled merge")
	}
	for _, n := range MergeRequirements {
		r.Capabilities[n] = Capability{State: Verified, Evidence: "fixture"}
	}
	if !r.AutoMergeReady() {
		t.Fatal("verified evidence rejected")
	}
	r.Capabilities["cloud_native_approval"] = Capability{State: Unsupported, Evidence: "COMMENTED"}
	if r.AutoMergeReady() {
		t.Fatal("COMMENTED enabled merge")
	}
}
