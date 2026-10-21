package contract

import (
	"encoding/json"
	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"time"
)

// Shared DTOs allow the SQLite implementation to satisfy session ports without
// importing the worker, while retaining the legacy worker API through aliases.
type SessionBinding struct {
	ID              string    `json:"id"`
	Kind            string    `json:"kind"`
	TaskID          string    `json:"task_id"`
	PhaseID         string    `json:"phase_id"`
	ParentSessionID string    `json:"parent_session_id"`
	TaskState       TaskState `json:"task_state"`
	ThreadID        string    `json:"thread_id"`
	CWD             string    `json:"cwd"`
	Model           string    `json:"model"`
	Sandbox         string    `json:"sandbox"`
	Target          Target    `json:"target"`
	Closed          bool      `json:"closed"`
	Generation      int       `json:"generation"`
	PreparationID   string    `json:"preparation_id"`
}
type InputSnapshot struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
	Input  []byte `json:"-"`
}

type SessionRun struct {
	BaselineTurnIDs []string        `json:"baseline_turn_ids"`
	BaselineKnown   bool            `json:"baseline_known"`
	ID              string          `json:"id"`
	ThreadID        string          `json:"thread_id"`
	TurnID          string          `json:"turn_id"`
	State           string          `json:"state"`
	Reason          string          `json:"reason"`
	Started         time.Time       `json:"started"`
	Final           string          `json:"final"`
	Request         *codex.Message  `json:"request,omitempty"`
	Usage           json.RawMessage `json:"usage,omitempty"`
}
