package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const MaxSessionInput = 64 << 10
const MaxInline = 16 << 10

type TaskState string

const (
	TaskReady     TaskState = "ready"
	TaskRunning   TaskState = "running"
	TaskWaiting   TaskState = "waiting"
	TaskBlocked   TaskState = "blocked"
	TaskCompleted TaskState = "completed"
	TaskFailed    TaskState = "failed"
	TaskCanceled  TaskState = "canceled"
)

func (s TaskState) Terminal() bool { return s == TaskCompleted || s == TaskFailed || s == TaskCanceled }

// Target is compared with controller facts, never used to grant authority.
type Target struct {
	WaveID         string `json:"wave_id"`
	ScopeRevision  int    `json:"scope_revision"`
	PlanRevision   int    `json:"plan_revision"`
	PolicyRevision int    `json:"policy_revision"`
}

func (t Target) Valid() bool {
	return t.WaveID != "" && t.ScopeRevision > 0 && t.PlanRevision > 0 && t.PolicyRevision > 0
}

type CompletionCondition struct {
	Kind            string   `json:"kind"`
	RequiredTaskIDs []string `json:"required_task_ids"`
}
type TaskDependency struct {
	TaskID     string `json:"task_id"`
	ArtifactID string `json:"artifact_id"`
}
type TaskSpec struct {
	ID               string           `json:"id"`
	Kind             string           `json:"kind"`
	Instruction      string           `json:"instruction"`
	Criteria         []string         `json:"criteria"`
	DependsOn        []TaskDependency `json:"depends_on"`
	InputArtifactIDs []string         `json:"input_artifact_ids"`
	WorkItemIDs      []string         `json:"work_item_ids"`
}
type PhaseSpec struct {
	ID            string              `json:"id"`
	Tasks         []TaskSpec          `json:"tasks"`
	Completion    CompletionCondition `json:"completion"`
	FailurePolicy string              `json:"failure_policy"`
}
type FixReviewTemplate struct {
	ID                string   `json:"id"`
	InputTaskID       string   `json:"input_task_id"`
	Classifications   []string `json:"classifications"`
	FixInstruction    string   `json:"fix_instruction"`
	ReviewInstruction string   `json:"review_instruction"`
	MaxRounds         int      `json:"max_rounds"`
}
type PlanProposal struct {
	SchemaVersion int                 `json:"schema_version"`
	Phases        []PhaseSpec         `json:"phases"`
	Templates     []FixReviewTemplate `json:"templates"`
}

// Validate checks only the bounded language. Coverage, immutable inputs and
// cumulative structural reservations are checked against persisted policy.
func (p PlanProposal) Validate() error {
	if p.SchemaVersion != 1 || len(p.Phases) == 0 {
		return errors.New("invalid plan version or empty phases")
	}
	phases, tasks := map[string]bool{}, map[string]int{}
	specs := map[string]TaskSpec{}
	for i, phase := range p.Phases {
		if phase.ID == "" || phases[phase.ID] || len(phase.Tasks) == 0 || phase.FailurePolicy != "drain_and_block" || phase.Completion.Kind != "all_success" {
			return errors.New("unsupported phase contract")
		}
		phases[phase.ID] = true
		required := map[string]bool{}
		for _, id := range phase.Completion.RequiredTaskIDs {
			if id == "" || required[id] {
				return errors.New("invalid required task set")
			}
			required[id] = true
		}
		if len(required) != len(phase.Tasks) {
			return errors.New("phase must require all tasks")
		}
		for _, task := range phase.Tasks {
			if task.ID == "" || !required[task.ID] || !inline(task.Instruction) || len(task.Criteria) == 0 || !validIDs(task.Criteria) || !validIDs(task.InputArtifactIDs) || !validIDs(task.WorkItemIDs) {
				return errors.New("invalid task contract")
			}
			if _, ok := tasks[task.ID]; ok {
				return errors.New("duplicate task ID")
			}
			switch task.Kind {
			case "work", "verification", "review", "fix":
			default:
				return errors.New("unsupported task kind")
			}
			tasks[task.ID] = i
			specs[task.ID] = task
		}
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return errors.New("cyclic task dependency")
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		seen := map[string]bool{}
		for _, dep := range specs[id].DependsOn {
			phase, exists := tasks[dep.TaskID]
			if !exists || seen[dep.TaskID] || phase > tasks[id] {
				return errors.New("unknown, repeated or future-phase dependency")
			}
			seen[dep.TaskID] = true
			if err := visit(dep.TaskID); err != nil {
				return err
			}
		}
		delete(visiting, id)
		visited[id] = true
		return nil
	}
	for id := range tasks {
		if err := visit(id); err != nil {
			return err
		}
	}
	templates := map[string]bool{}
	for _, template := range p.Templates {
		_, known := tasks[template.InputTaskID]
		if template.ID == "" || templates[template.ID] || !known || specs[template.InputTaskID].Kind != "review" || template.MaxRounds < 1 || !inline(template.FixInstruction) || !inline(template.ReviewInstruction) || len(template.Classifications) == 0 {
			return errors.New("invalid fix/review template")
		}
		templates[template.ID] = true
		seen := map[string]bool{}
		for _, cls := range template.Classifications {
			if seen[cls] {
				return errors.New("repeated template classification")
			}
			seen[cls] = true
			switch cls {
			case "artifact_defect", "implementation_defect", "test_defect":
			default:
				return errors.New("unsupported template classification")
			}
		}
	}
	return nil
}
func inline(s string) bool {
	return utf8.ValidString(s) && strings.TrimSpace(s) != "" && len(s) <= MaxInline
}
func validIDs(ids []string) bool {
	seen := map[string]bool{}
	for _, id := range ids {
		if strings.TrimSpace(id) == "" || len(id) > MaxInline || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

type CriterionResult struct {
	CriterionID string   `json:"criterion_id"`
	EvidenceIDs []string `json:"evidence_ids"`
	Status      string   `json:"status"`
	Basis       string   `json:"basis"`
}
type CandidateReference struct {
	WorkItemID  string `json:"work_item_id"`
	CandidateID string `json:"candidate_id"`
	SHA         string `json:"sha"`
}
type ReviewFinding struct {
	ID             string `json:"id"`
	Classification string `json:"classification"`
	TargetDigest   string `json:"target_digest"`
	Basis          string `json:"basis"`
	Description    string `json:"description"`
}
type ReviewResult struct {
	TargetDigest string          `json:"target_digest"`
	Verdict      string          `json:"verdict"`
	Findings     []ReviewFinding `json:"findings"`
}
type VerificationReference struct {
	PlanID      string   `json:"plan_id"`
	EvidenceIDs []string `json:"evidence_ids"`
}
type TaskResult struct {
	SchemaVersion    int                    `json:"schema_version"`
	Target           Target                 `json:"target"`
	PhaseID          string                 `json:"phase_id"`
	TaskID           string                 `json:"task_id"`
	Outcome          string                 `json:"outcome"`
	Artifacts        []string               `json:"artifacts"`
	CriterionResults []CriterionResult      `json:"criterion_results"`
	WorkItemRefs     []CandidateReference   `json:"work_item_refs"`
	Remaining        []string               `json:"remaining"`
	Blockers         []string               `json:"blockers"`
	SubscriptionIDs  []string               `json:"subscription_ids"`
	Review           *ReviewResult          `json:"review"`
	Verification     *VerificationReference `json:"verification"`
}

func (r TaskResult) Validate() error {
	if r.SchemaVersion != 1 || !r.Target.Valid() || r.PhaseID == "" || r.TaskID == "" {
		return errors.New("result identity missing")
	}
	if !validIDs(r.Artifacts) || !validIDs(r.SubscriptionIDs) {
		return errors.New("invalid artifact/subscription references")
	}
	for _, items := range [][]string{r.Remaining, r.Blockers} {
		for _, text := range items {
			if !inline(text) {
				return errors.New("invalid inline result text")
			}
		}
	}
	switch r.Outcome {
	case "waiting":
		if len(r.SubscriptionIDs) == 0 {
			return errors.New("waiting needs durable subscriptions")
		}
	case "blocked":
		if len(r.Blockers) == 0 {
			return errors.New("blocked result needs a reason")
		}
	case "result":
		if len(r.Artifacts) == 0 && len(r.WorkItemRefs) == 0 {
			return errors.New("result needs immutable artifacts or candidates")
		}
	default:
		return errors.New("unsupported task outcome")
	}
	seen := map[string]bool{}
	for _, criterion := range r.CriterionResults {
		if criterion.CriterionID == "" || seen[criterion.CriterionID] || !inline(criterion.Basis) || !validIDs(criterion.EvidenceIDs) || (criterion.Status != "claimed" && criterion.Status != "unverified") || (criterion.Status == "claimed" && len(criterion.EvidenceIDs) == 0) {
			return errors.New("invalid criterion evidence")
		}
		seen[criterion.CriterionID] = true
	}
	for _, ref := range r.WorkItemRefs {
		if ref.WorkItemID == "" || ref.CandidateID == "" || len(ref.SHA) != 40 || strings.Trim(ref.SHA, "0123456789abcdef") != "" {
			return errors.New("invalid candidate reference")
		}
	}
	if r.Review != nil {
		review := r.Review
		if review.TargetDigest == "" || (review.Verdict != "clean" && review.Verdict != "findings" && review.Verdict != "inconclusive") || (review.Verdict == "clean" && len(review.Findings) > 0) || (review.Verdict == "findings" && len(review.Findings) == 0) {
			return errors.New("invalid review verdict")
		}
		ids := map[string]bool{}
		for _, finding := range review.Findings {
			if finding.ID == "" || ids[finding.ID] || finding.TargetDigest != review.TargetDigest || !inline(finding.Basis) || !inline(finding.Description) || finding.Classification == "" {
				return errors.New("invalid review finding")
			}
			ids[finding.ID] = true
		}
	}
	if r.Verification != nil && (r.Verification.PlanID == "" || len(r.Verification.EvidenceIDs) == 0 || !validIDs(r.Verification.EvidenceIDs)) {
		return errors.New("invalid verification reference")
	}
	return nil
}

type CreateWaveRequest struct {
	SchemaVersion int    `json:"schema_version"`
	RequestID     string `json:"request_id"`
}
type PlanWaveRequest struct {
	SchemaVersion int          `json:"schema_version"`
	Target        Target       `json:"target"`
	Plan          PlanProposal `json:"plan"`
}
type StateRequest struct {
	SchemaVersion int        `json:"schema_version"`
	Target        Target     `json:"target"`
	Page          *StatePage `json:"page,omitempty"`
}
type StatePage struct {
	Collection string `json:"collection"`
	After      string `json:"after"`
	Limit      int    `json:"limit"`
}
type InstructRequest struct {
	SchemaVersion int    `json:"schema_version"`
	Target        Target `json:"target"`
	TaskID        string `json:"task_id"`
	Body          string `json:"body"`
}
type WaitRequest struct {
	SchemaVersion   int      `json:"schema_version"`
	Target          Target   `json:"target"`
	SubscriptionIDs []string `json:"subscription_ids"`
}
type ReadArtifactRequest struct {
	SchemaVersion int    `json:"schema_version"`
	Target        Target `json:"target"`
	ArtifactID    string `json:"artifact_id"`
	Offset        int64  `json:"offset"`
	Limit         int    `json:"limit"`
}
type PublishArtifactRequest struct {
	SchemaVersion    int      `json:"schema_version"`
	Target           Target   `json:"target"`
	Content          string   `json:"content"`
	ContentType      string   `json:"content_type"`
	Kind             string   `json:"kind"`
	InputArtifactIDs []string `json:"input_artifact_ids"`
}
type SendMessageRequest struct {
	SchemaVersion int    `json:"schema_version"`
	Target        Target `json:"target"`
	Recipient     string `json:"recipient"`
	Body          string `json:"body"`
	ReplyTo       string `json:"reply_to"`
}
type ToolInvocation struct {
	// RunID is supplied by the trusted process owner, never tool arguments.
	SessionID, RunID, ThreadID, TurnID, CallID, Name, Namespace string
	Arguments                                                   json.RawMessage
}
type SessionAuthority struct {
	SessionID, ThreadID, ActiveTurnID, Kind, TaskID, ParentSessionID string
	TaskState                                                        TaskState
	Target                                                           Target
	Closed, Paused, Canceled                                         bool
}
type ToolReply struct {
	SchemaVersion   int      `json:"schema_version"`
	State           string   `json:"state"`
	OperationID     string   `json:"operation_id"`
	SubscriptionIDs []string `json:"subscription_ids"`
	ResourceID      string   `json:"resource_id"`
	Content         string   `json:"content"`
}

// Apply must load authoritative bindings, invoke guard and apply/record the tool
// in the same transaction. It also owns call replay and argument-digest checks.
type KernelToolBackend interface {
	Apply(context.Context, ToolInvocation, any, func(SessionAuthority) error) (ToolReply, error)
}
type KernelGateway struct{ Backend KernelToolBackend }
type ToolDefinition struct {
	Type         string         `json:"type"`
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"inputSchema"`
	DeferLoading bool           `json:"deferLoading"`
}

func toolArgument(name string) any {
	switch name {
	case "wave_create":
		return &CreateWaveRequest{}
	case "wave_plan":
		return &PlanWaveRequest{}
	case "task_instruct":
		return &InstructRequest{}
	case "session_wait":
		return &WaitRequest{}
	case "state_get":
		return &StateRequest{}
	case "artifact_read":
		return &ReadArtifactRequest{}
	case "context_read":
		return &ContextReadRequest{}
	case "artifact_publish":
		return &PublishArtifactRequest{}
	case "send_message":
		return &SendMessageRequest{}
	}
	return nil
}
func parentOnly(name string) bool {
	return name == "wave_create" || name == "wave_plan"
}
func KernelTools(kind string) []ToolDefinition {
	if kind != "orchestrator" && kind != "task" {
		return nil
	}
	out := []ToolDefinition{}
	for _, name := range []string{"wave_create", "wave_plan", "task_instruct", "session_wait", "state_get", "context_read", "artifact_read", "artifact_publish", "send_message"} {
		if kind == "task" && parentOnly(name) {
			continue
		}
		inputSchema := Schema(toolArgument(name))
		if name == "wave_plan" {
			inputSchema["properties"].(map[string]any)["plan"] = PlanSchema()
		}
		description := "Version 1 controller operation: " + name + ". Accepted work is not completed work; wait on the returned subscription."
		if name == "wave_create" {
			description = "Resolve this run's registered request_id to its controller-owned Wave. Use the Wave manifest's source_id as request_id. Scope/policy come from the saved request. Returns accepted Wave ID and a durable terminal subscription; acceptance is not completion. Task sessions cannot call this tool."
		}
		if name == "wave_plan" {
			description = "Submit a bounded plan for this run's current owned Wave target. The controller validates scope, policy, budgets and quiescent Task history, then atomically accepts the plan and returns the resulting target and terminal subscription. Background Tasks may start before this turn ends. Finish with wait/report, or acknowledge the identical accepted plan; never claim Wave completion."
		}
		if name == "context_read" {
			description = "Read a bounded UTF-8 slice of this Wave's immutable original request, current criteria/policy, or applied direction. Sections: request, criteria, policy, direction, manifest. For manifest use the reference_id from this run input; it reads the immutable saved judgment state, including task instructions/results. Start offset 0; limit 1..2048 bytes. Continue with returned next_offset and compare digest/bytes. This exposes no arbitrary host path."
			properties := inputSchema["properties"].(map[string]any)
			properties["section"].(map[string]any)["enum"] = []string{"request", "criteria", "policy", "direction", "manifest"}
			properties["reference_id"].(map[string]any)["maxLength"] = 128
			properties["offset"].(map[string]any)["minimum"] = 0
			properties["limit"].(map[string]any)["minimum"], properties["limit"].(map[string]any)["maximum"] = 1, MaxContextRead
		}
		if name == "state_get" {
			description += " The default overview returns 16 Tasks/subscriptions. Continue each collection with page {collection: tasks or subscriptions, after: its next cursor, limit: 1..64}; an empty next cursor ends that collection."
		}
		out = append(out, ToolDefinition{Type: "function", Name: name, Description: description, InputSchema: inputSchema})
	}
	return out
}

func PlanSchema() map[string]any {
	s := Schema(PlanProposal{})
	phase := s["properties"].(map[string]any)["phases"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	phase["failure_policy"].(map[string]any)["const"] = "drain_and_block"
	phase["completion"].(map[string]any)["properties"].(map[string]any)["kind"].(map[string]any)["const"] = "all_success"
	phase["tasks"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)["kind"].(map[string]any)["enum"] = []string{"work", "verification", "review", "fix"}
	return s
}
func TaskResultSchema() map[string]any {
	s := Schema(TaskResult{})
	properties := s["properties"].(map[string]any)
	properties["outcome"].(map[string]any)["enum"] = []string{"result", "waiting", "blocked"}
	properties["criterion_results"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)["status"].(map[string]any)["enum"] = []string{"claimed", "unverified"}
	properties["review"].(map[string]any)["anyOf"].([]any)[0].(map[string]any)["properties"].(map[string]any)["verdict"].(map[string]any)["enum"] = []string{"clean", "findings", "inconclusive"}
	return s
}
func (g KernelGateway) Handle(ctx context.Context, call ToolInvocation) (ToolReply, error) {
	reject := func(reason string) (ToolReply, error) { return ToolReply{}, errors.New(reason) }
	command := toolArgument(call.Name)
	if g.Backend == nil || command == nil || call.Namespace != "" || call.SessionID == "" || call.ThreadID == "" || call.TurnID == "" || call.CallID == "" || len(call.Arguments) > MaxSessionInput {
		return reject("unknown/unauthenticated kernel tool")
	}
	if err := DecodeStrict(call.Arguments, command); err != nil {
		return ToolReply{}, err
	}
	var target Target
	switch a := command.(type) {
	case *ContextReadRequest:
		target = a.Target
		if !a.Valid() {
			return reject("invalid bounded context read")
		}
	case *CreateWaveRequest:
		if a.SchemaVersion != 1 || !inline(a.RequestID) {
			return reject("invalid wave request")
		}
	case *PlanWaveRequest:
		target = a.Target
		if a.SchemaVersion != 1 {
			return reject("invalid tool version")
		}
		if err := a.Plan.Validate(); err != nil {
			return ToolReply{}, err
		}
	case *StateRequest:
		target = a.Target
		if a.SchemaVersion != 1 {
			return reject("invalid tool version")
		}
		if a.Page != nil && (a.Page.Limit < 1 || a.Page.Limit > 64 || len(a.Page.After) > 256 || (a.Page.Collection != "tasks" && a.Page.Collection != "subscriptions")) {
			return reject("invalid state page")
		}
	case *InstructRequest:
		target = a.Target
		if a.SchemaVersion != 1 || a.TaskID == "" || !inline(a.Body) {
			return reject("invalid instruction")
		}
	case *WaitRequest:
		target = a.Target
		if a.SchemaVersion != 1 || len(a.SubscriptionIDs) == 0 || !validIDs(a.SubscriptionIDs) {
			return reject("invalid wait")
		}
	case *ReadArtifactRequest:
		target = a.Target
		if a.SchemaVersion != 1 || a.ArtifactID == "" || a.Offset < 0 || a.Limit < 1 || a.Limit > MaxInline {
			return reject("invalid artifact read")
		}
	case *PublishArtifactRequest:
		target = a.Target
		if a.SchemaVersion != 1 || !inline(a.Content) || a.ContentType == "" || a.Kind == "" {
			return reject("invalid artifact publication")
		}
	case *SendMessageRequest:
		target = a.Target
		if a.SchemaVersion != 1 || a.Recipient == "" || !inline(a.Body) {
			return reject("invalid message")
		}
	}
	if call.Name != "wave_create" && !target.Valid() {
		return reject("invalid target revisions")
	}
	checked := false
	reply, err := g.Backend.Apply(ctx, call, command, func(a SessionAuthority) error {
		if a.SessionID != call.SessionID || a.ThreadID != call.ThreadID || a.ActiveTurnID != call.TurnID || a.Closed || a.Paused || a.Canceled {
			return errors.New("stale/inactive session")
		}
		if a.Kind != "orchestrator" && a.Kind != "task" {
			return errors.New("unknown session kind")
		}
		if a.Kind == "task" && (parentOnly(call.Name) || a.TaskID == "" || a.ParentSessionID == "" || (a.TaskState != "running")) {
			return errors.New("task cannot invoke Wave management or reuse a terminal session")
		}
		if call.Name != "wave_create" && a.Target != target {
			return errors.New("stale/cross-wave target")
		}
		checked = true
		return nil
	})
	if err != nil {
		return ToolReply{}, err
	}
	if !checked {
		return reject("backend omitted authority guard")
	}
	if reply.SchemaVersion != 1 || len(reply.Content) > MaxInline || !utf8.ValidString(reply.Content) {
		return reject("invalid tool result")
	}
	switch reply.State {
	case "accepted":
		if reply.OperationID == "" || len(reply.SubscriptionIDs) == 0 {
			return reject("accepted operation lacks durable subscription")
		}
	case "unknown":
		if reply.OperationID == "" {
			return reject("unknown operation lacks a reconciliation ID")
		}
	case "completed":
	default:
		return reject("invalid tool operation state")
	}
	return reply, nil
}
func ValidateSessionInput(data []byte) error {
	if len(data) > MaxSessionInput || !utf8.Valid(data) || !json.Valid(data) {
		return fmt.Errorf("session input must be valid UTF-8 JSON of at most %d bytes", MaxSessionInput)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := uniqueJSONKeys(decoder); err != nil {
		return err
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if text, ok := token.(string); ok && len(text) > MaxInline {
			return errors.New("session inline value exceeds byte limit")
		}
	}
}
