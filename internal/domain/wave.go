package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type WaveState string

const (
	WavePlanning  WaveState = "planning"
	WaveRunning   WaveState = "running"
	WaveWaiting   WaveState = "waiting"
	WaveBlocked   WaveState = "blocked"
	WaveCompleted WaveState = "completed"
	WaveCanceled  WaveState = "canceled"
)

type WaveCriterion struct {
	ID          string `json:"id"`
	Requirement string `json:"requirement"`
}
type KernelLimits struct {
	Phases                  int   `json:"phases"`
	Tasks                   int   `json:"tasks"`
	Replans                 int   `json:"replans"`
	MainTurns               int   `json:"main_turns"`
	ThreadTurns             int   `json:"thread_turns"`
	FixRounds               int   `json:"fix_rounds"`
	TaskAttempts            int   `json:"task_attempts"`
	Tokens                  int64 `json:"tokens"`
	WaveSeconds             int64 `json:"wave_seconds"`
	ArtifactBytes           int64 `json:"artifact_bytes"`
	WaveArtifactBytes       int64 `json:"wave_artifact_bytes"`
	ControllerArtifactBytes int64 `json:"controller_artifact_bytes"`
	RetentionSeconds        int64 `json:"retention_seconds"`
}

func DefaultKernelLimits() KernelLimits {
	return KernelLimits{Phases: 16, Tasks: 64, Replans: 8, MainTurns: 16, ThreadTurns: 8, FixRounds: 3, TaskAttempts: 2, Tokens: 1000000, WaveSeconds: 172800, ArtifactBytes: 8 << 20, WaveArtifactBytes: 256 << 20, ControllerArtifactBytes: 1 << 30, RetentionSeconds: 30 * 86400}
}
func (l KernelLimits) Validate() error {
	if l.Phases < 1 || l.Tasks < 1 || l.Replans < 0 || l.MainTurns < 1 || l.ThreadTurns < 1 || l.FixRounds < 0 || l.TaskAttempts < 1 || l.Tokens < 1 || l.WaveSeconds < 1 || l.WaveSeconds > int64((1<<63-1)/time.Second) || l.ArtifactBytes < 1 || l.WaveArtifactBytes < l.ArtifactBytes || l.ControllerArtifactBytes < l.WaveArtifactBytes || l.RetentionSeconds < 1 || l.RetentionSeconds > int64((1<<63-1)/time.Second) {
		return errors.New("invalid kernel limits")
	}
	return nil
}

type WavePolicy struct {
	Revision int          `json:"revision"`
	Stage    string       `json:"stage"`
	Product  bool         `json:"product"`
	Limits   KernelLimits `json:"limits"`
}
type Wave struct {
	SourceHold            *WaveSourceHold `json:"source_hold,omitempty"`
	ID                    string          `json:"id"`
	OrchestratorSessionID string          `json:"orchestrator_session_id"`
	Source                string          `json:"source"`
	SourceID              string          `json:"source_id"`
	OriginalRequest       string          `json:"original_request"`
	Criteria              []WaveCriterion `json:"criteria"`
	ScopeRevision         int             `json:"scope_revision"`
	PlanRevision          int             `json:"plan_revision"`
	Policy                WavePolicy      `json:"policy"`
	State                 WaveState       `json:"state"`
	Paused                bool            `json:"paused"`
	PendingDirectionID    string          `json:"pending_direction_id,omitempty"`
	DirectionID           string          `json:"direction_id,omitempty"`
	Reason                string          `json:"reason"`
	CreatedAt             time.Time       `json:"created_at"`
	Deadline              time.Time       `json:"deadline"`
	CompletedAt           time.Time       `json:"completed_at"`
	PhasesCreated         int             `json:"phases_created"`
	TasksCreated          int             `json:"tasks_created"`
	Replans               int             `json:"replans"`
	MainTurns             int             `json:"main_turns"`
	FixRounds             int             `json:"fix_rounds"`
}

// Source prohibition is independent of manual pause and pending directions.
type WaveSourceHold struct {
	Reason     string    `json:"reason"`
	Digest     string    `json:"digest"`
	ObservedAt time.Time `json:"observed_at"`
}

func (w Wave) Target() contract.Target {
	return contract.Target{WaveID: w.ID, ScopeRevision: w.ScopeRevision, PlanRevision: w.PlanRevision, PolicyRevision: w.Policy.Revision}
}

// A direction request stops new dispatch independently of manual pause. Resume
// cannot remove that barrier before the stopped prior revision is settled.
func (w Wave) DispatchPaused() bool {
	return w.Paused || w.PendingDirectionID != "" || w.SourceHold != nil
}
func (w Wave) Validate() error {
	if w.ID == "" || w.OrchestratorSessionID == "" || (w.Source != "direct" && w.Source != "github") || w.SourceID == "" || strings.TrimSpace(w.OriginalRequest) == "" || !utf8.ValidString(w.OriginalRequest) || len(w.OriginalRequest) > 8<<20 || w.ScopeRevision < 1 || w.PlanRevision < 1 || w.Policy.Revision < 1 || w.CreatedAt.IsZero() || !w.Deadline.After(w.CreatedAt) {
		return errors.New("invalid Wave identity/request")
	}
	if err := w.Policy.Limits.Validate(); err != nil {
		return err
	}
	if h := w.SourceHold; h != nil {
		if w.Policy.Stage != "P" || !w.Policy.Product || w.Source != "github" || h.Reason == "" || len(h.Reason) > 64 || !utf8.ValidString(h.Reason) || strings.ContainsAny(h.Reason, "\x00\r\n") || len(h.Digest) != 64 || strings.Trim(h.Digest, "0123456789abcdef") != "" || h.ObservedAt.IsZero() {
			return errors.New("trusted product source hold required")
		}
	}
	switch w.State {
	case WavePlanning, WaveRunning, WaveWaiting, WaveBlocked, WaveCompleted, WaveCanceled:
	default:
		return errors.New("invalid Wave state")
	}
	if w.PhasesCreated < 0 || w.TasksCreated < 0 || w.Replans < 0 || w.MainTurns < 0 || w.FixRounds < 0 {
		return errors.New("negative cumulative Wave budget")
	}
	if (w.Policy.Stage != "K" && w.Policy.Stage != "P") || (w.Policy.Stage == "K" && (w.Policy.Product || w.Source != "direct")) {
		return errors.New("product/Issue work requires trusted P policy")
	}
	seen := map[string]bool{}
	if len(w.Criteria) > 256 {
		return errors.New("too many Wave criteria")
	}
	for _, c := range w.Criteria {
		if strings.TrimSpace(c.ID) == "" || len(c.ID) > contract.MaxInline || seen[c.ID] || strings.TrimSpace(c.Requirement) == "" || !utf8.ValidString(c.Requirement) || len(c.Requirement) > contract.MaxInline {
			return errors.New("invalid Wave criteria")
		}
		seen[c.ID] = true
	}
	if len(seen) == 0 {
		return errors.New("Wave requires explicit criteria")
	}
	return nil
}

type WavePhase struct {
	ID           string             `json:"id"`
	WaveID       string             `json:"wave_id"`
	LocalID      string             `json:"local_id"`
	PlanRevision int                `json:"plan_revision"`
	Position     int                `json:"position"`
	Spec         contract.PhaseSpec `json:"spec"`
}
type Task struct {
	ID              string             `json:"id"`
	WaveID          string             `json:"wave_id"`
	PhaseID         string             `json:"phase_id"`
	LocalID         string             `json:"local_id"`
	ScopeRevision   int                `json:"scope_revision"`
	PlanRevision    int                `json:"plan_revision"`
	PolicyRevision  int                `json:"policy_revision"`
	SessionID       string             `json:"session_id"`
	ParentSessionID string             `json:"parent_session_id"`
	State           contract.TaskState `json:"state"`
	Spec            contract.TaskSpec  `json:"spec"`
	ResultID        string             `json:"result_id"`
	InputDigest     string             `json:"input_digest"`
	Attempts        int                `json:"attempts"`
	Paused          bool               `json:"paused"`
	Reason          string             `json:"reason"`
	CompletedAt     time.Time          `json:"completed_at"`
}

func (t Task) Target() contract.Target {
	return contract.Target{WaveID: t.WaveID, ScopeRevision: t.ScopeRevision, PlanRevision: t.PlanRevision, PolicyRevision: t.PolicyRevision}
}

// ValidateWavePlan checks criterion coverage in addition to the shared bounded
// language. Model assertions cannot invent or drop the trusted request's scope.
func ValidateWavePlan(w Wave, p contract.PlanProposal) error {
	if err := p.Validate(); err != nil {
		return err
	}
	known, covered := map[string]bool{}, map[string]bool{}
	for _, c := range w.Criteria {
		known[c.ID] = true
	}
	count := 0
	for _, phase := range p.Phases {
		for _, task := range phase.Tasks {
			count++
			for _, id := range task.Criteria {
				if !known[id] {
					return errors.New("plan invents a criterion")
				}
				covered[id] = true
			}
			if w.Policy.Stage == "K" && len(task.WorkItemIDs) > 0 {
				return errors.New("K cannot contain product WorkItems")
			}
		}
	}
	if len(covered) != len(known) {
		return errors.New("plan drops criterion coverage")
	}
	if len(p.Phases) > w.Policy.Limits.Phases || count > w.Policy.Limits.Tasks {
		return errors.New("plan exceeds structural budget")
	}
	for _, template := range p.Templates {
		if template.MaxRounds > w.Policy.Limits.FixRounds {
			return errors.New("template exceeds fixed repeat budget")
		}
	}
	return nil
}
