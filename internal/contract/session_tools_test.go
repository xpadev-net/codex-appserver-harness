package contract

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func planFixture() PlanProposal {
	return PlanProposal{SchemaVersion: 1, Phases: []PhaseSpec{{ID: "p", FailurePolicy: "drain_and_block", Completion: CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"a", "b"}}, Tasks: []TaskSpec{{ID: "a", Kind: "work", Instruction: "research", Criteria: []string{"AC"}, DependsOn: []TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}, {ID: "b", Kind: "review", Instruction: "review", Criteria: []string{"AC"}, DependsOn: []TaskDependency{{TaskID: "a", ArtifactID: ""}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}}}}, Templates: []FixReviewTemplate{}}
}
func TestPhaseLanguageRejectsUnsupportedPlans(t *testing.T) {
	for _, mode := range []string{"valid", "empty", "quorum", "skip", "cycle", "future", "duplicate", "empty_required", "missing_required", "fanout"} {
		t.Run(mode, func(t *testing.T) {
			p := planFixture()
			switch mode {
			case "empty":
				p.Phases = nil
			case "quorum":
				p.Phases[0].Completion.Kind = "quorum"
			case "skip":
				p.Phases[0].FailurePolicy = "skip"
			case "cycle":
				p.Phases[0].Tasks[0].DependsOn = []TaskDependency{{TaskID: "b"}}
			case "future":
				p.Phases = append(p.Phases, p.Phases[0])
				p.Phases[1].ID = "next"
				p.Phases[1].Tasks = []TaskSpec{{ID: "c", Kind: "work", Instruction: "later", Criteria: []string{"AC"}}}
				p.Phases[1].Completion.RequiredTaskIDs = []string{"c"}
				p.Phases[0].Tasks[0].DependsOn = []TaskDependency{{TaskID: "c"}}
			case "duplicate":
				p.Phases[0].Tasks[1].ID = "a"
			case "empty_required":
				p.Phases[0].Completion.RequiredTaskIDs = nil
			case "missing_required":
				p.Phases[0].Completion.RequiredTaskIDs = []string{"a", "unknown"}
			case "fanout":
				p.Templates = []FixReviewTemplate{{ID: "template", InputTaskID: "b", Classifications: []string{"arbitrary_fanout"}, MaxRounds: 1, FixInstruction: "fix", ReviewInstruction: "review"}}
			}
			if err := p.Validate(); (err == nil) != (mode == "valid") {
				t.Fatal(err)
			}
		})
	}
}

type fakeKernelBackend struct {
	authority SessionAuthority
	effects   int
	omitGuard bool
}

func (b *fakeKernelBackend) Apply(_ context.Context, _ ToolInvocation, _ any, guard func(SessionAuthority) error) (ToolReply, error) {
	if !b.omitGuard {
		if err := guard(b.authority); err != nil {
			return ToolReply{}, err
		}
	}
	b.effects++
	return ToolReply{SchemaVersion: 1, State: "completed", SubscriptionIDs: []string{}, ResourceID: "message"}, nil
}
func TestKernelPublicToolsAndControllerAuthorization(t *testing.T) {
	for _, tool := range KernelTools("task") {
		if parentOnly(tool.Name) {
			t.Fatal("Task sees Wave management", tool.Name)
		}
	}
	target := Target{"wave", 1, 1, 1}
	authority := SessionAuthority{SessionID: "session", ThreadID: "thread", ActiveTurnID: "turn", Kind: "task", TaskID: "task", ParentSessionID: "main", TaskState: "running", Target: target}
	for _, mode := range []string{"valid", "task_wave", "alias", "namespace", "stale_scope", "other_wave", "terminal", "paused", "wrong_turn", "sender_spoof", "duplicate_key", "large_inline", "guard_omitted"} {
		t.Run(mode, func(t *testing.T) {
			backend := &fakeKernelBackend{authority: authority}
			args := SendMessageRequest{SchemaVersion: 1, Target: target, Recipient: "parent", Body: "question", ReplyTo: ""}
			raw, _ := json.Marshal(args)
			call := ToolInvocation{SessionID: "session", ThreadID: "thread", TurnID: "turn", CallID: "call", Name: "send_message", Arguments: raw}
			switch mode {
			case "task_wave":
				call.Name = "wave_create"
				call.Arguments = []byte(`{"schema_version":1,"request_id":"request"}`)
			case "alias":
				call.Name = "execute_wave"
			case "namespace":
				call.Namespace = "other"
			case "stale_scope":
				backend.authority.Target.ScopeRevision = 2
			case "other_wave":
				backend.authority.Target.WaveID = "other"
			case "terminal":
				backend.authority.TaskState = "completed"
			case "paused":
				backend.authority.Paused = true
			case "wrong_turn":
				call.TurnID = "other"
			case "sender_spoof":
				call.Arguments = []byte(strings.TrimSuffix(string(raw), "}") + `,"sender":"main"}`)
			case "duplicate_key":
				call.Arguments = []byte(strings.TrimSuffix(string(raw), "}") + `,"recipient":"sibling"}`)
			case "large_inline":
				args.Body = strings.Repeat("界", MaxInline/3+1)
				call.Arguments, _ = json.Marshal(args)
			case "guard_omitted":
				backend.omitGuard = true
			}
			_, err := (KernelGateway{Backend: backend}).Handle(context.Background(), call)
			if (err == nil) != (mode == "valid") {
				t.Fatal(err)
			}
			if mode != "valid" && mode != "guard_omitted" && backend.effects != 0 {
				t.Fatal("unauthorized effect", mode)
			}
		})
	}
}
func TestGenericTaskResultsAndInputBounds(t *testing.T) {
	result := TaskResult{SchemaVersion: 1, Target: Target{"wave", 1, 1, 1}, PhaseID: "p", TaskID: "a", Outcome: "result", Artifacts: []string{"artifact"}, CriterionResults: []CriterionResult{{CriterionID: "AC", EvidenceIDs: []string{"artifact"}, Status: "claimed", Basis: "request AC"}}, WorkItemRefs: []CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
	raw, _ := json.Marshal(result)
	var decoded TaskResult
	if err := DecodeStrict(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal("generic result requires no WorkItem", err)
	}
	result.Outcome = "waiting"
	if err := result.Validate(); err == nil {
		t.Fatal("non-durable waiting accepted")
	}
	result.SubscriptionIDs = []string{"subscription"}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSessionInput([]byte(`{"wave":"one"}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSessionInput([]byte(`"` + strings.Repeat("x", MaxSessionInput) + `"`)); err == nil {
		t.Fatal("oversized main input")
	}
	if err := ValidateSessionInput([]byte{'"', 0xff, '"'}); err == nil {
		t.Fatal("invalid UTF-8")
	}
	if err := ValidateSessionInput([]byte(`{"request":"` + strings.Repeat("x", MaxInline+1) + `"}`)); err == nil {
		t.Fatal("oversized inline input")
	}
}

func TestTaskClaimRequiresUsableEvidenceIDs(t *testing.T) {
	for _, ids := range [][]string{{""}, {" "}, {"evidence", "evidence"}, {strings.Repeat("x", MaxInline+1)}, {"evidence"}} {
		result := TaskResult{SchemaVersion: 1, Target: Target{"wave", 1, 1, 1}, PhaseID: "p", TaskID: "a", Outcome: "result", Artifacts: []string{"artifact"}, CriterionResults: []CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "request", EvidenceIDs: ids}}}
		err := result.Validate()
		valid := len(ids) == 1 && ids[0] == "evidence"
		if (err == nil) != valid {
			t.Fatal("invalid evidence disposition", ids, err)
		}
	}
}

func TestPlanSourceResultsAreOptionalBoundedAndUnique(t *testing.T) {
	for _, mode := range []string{"legacy", "valid", "duplicate", "too_many", "empty"} {
		t.Run(mode, func(t *testing.T) {
			p := planFixture()
			switch mode {
			case "valid":
				p.Phases[0].Tasks[0].SourceResultIDs = []string{"result"}
			case "duplicate":
				p.Phases[0].Tasks[0].SourceResultIDs = []string{"result", "result"}
			case "too_many":
				for i := 0; i <= MaxTaskSourceResults; i++ {
					p.Phases[0].Tasks[0].SourceResultIDs = append(p.Phases[0].Tasks[0].SourceResultIDs, fmt.Sprint(i))
				}
			case "empty":
				p.Phases[0].Tasks[0].SourceResultIDs = []string{""}
			}
			err := p.Validate()
			if (mode == "legacy" || mode == "valid") != (err == nil) {
				t.Fatal(mode, err)
			}
		})
	}
}

func TestHostJSONLimitDoesNotRaiseModelOutputLimit(t *testing.T) {
	type hostRequest struct {
		Request string `json:"request"`
	}
	raw, err := json.Marshal(hostRequest{Request: strings.Repeat("x", (1<<20)+1)})
	if err != nil {
		t.Fatal(err)
	}
	var decoded hostRequest
	if DecodeStrict(raw, &decoded) == nil {
		t.Fatal("model output envelope limit raised")
	}
	if err = DecodeStrictBounded(raw, &decoded, 2<<20); err != nil {
		t.Fatal("bounded host request rejected", err)
	}
	if DecodeStrictBounded([]byte(`{"request":"first","request":"second"}`), &decoded, 2<<20) == nil {
		t.Fatal("bounded host decoder accepted duplicate keys")
	}
}
