package contract

import (
	"encoding/json"
	"testing"
)

func TestMainResultContractCannotClaimCompletionOrChangePolicy(t *testing.T) {
	target := Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}
	valid := MainResult{SchemaVersion: 1, SessionID: "main", Decisions: []MainDecision{{Target: target, Action: "report", Summary: "Observed report; controller owns acceptance.", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"complete", "approve", "steer", "cancel", ""} {
		bad := valid
		bad.Decisions = append([]MainDecision{}, valid.Decisions...)
		bad.Decisions[0].Action = action
		if bad.Validate() == nil {
			t.Fatal("unapproved main action accepted", action)
		}
	}
	raw, _ := json.Marshal(valid)
	var data map[string]any
	_ = json.Unmarshal(raw, &data)
	data["policy"] = map[string]any{"revision": 2}
	raw, _ = json.Marshal(data)
	var decoded MainResult
	if DecodeStrict(raw, &decoded) == nil {
		t.Fatal("main result changed policy")
	}
	valid.Decisions = append(valid.Decisions, valid.Decisions[0])
	if valid.Validate() == nil {
		t.Fatal("repeated Wave decision accepted")
	}
}
func TestMainResultWaitNeedsSubscriptionsAndReportCannotSmugglePlan(t *testing.T) {
	d := MainDecision{Target: Target{"wave", 1, 1, 1}, Action: "wait", Summary: "Waiting on saved condition."}
	r := MainResult{SchemaVersion: 1, SessionID: "main", Decisions: []MainDecision{d}}
	if r.Validate() == nil {
		t.Fatal("condition-free waiting accepted")
	}
	r.Decisions[0].SubscriptionIDs = []string{"sub"}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	r.Decisions[0].Action = "report"
	if r.Validate() == nil {
		t.Fatal("report changed waiting conditions")
	}
	r.Decisions[0].SubscriptionIDs = []string{}
	r.Decisions[0].Plan = &PlanProposal{}
	if r.Validate() == nil {
		t.Fatal("report smuggled a plan")
	}
	if MainResultSchema() == nil {
		t.Fatal("schema missing")
	}
}
