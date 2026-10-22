package contract

import (
	"encoding/json"
	"errors"
	"unicode/utf8"
)

const MaxStatePage = 32

// StateEvent is a controller read surface, never a model decision or wake.
// Large payloads are represented by stable event identity and byte count; they
// remain in the DB rather than making the stream allocate unbounded frames.
// StateEventScope contains only versions actually saved in the event journal.
// It is intentionally not a dispatch Target: historical policy is not inferred.
type StateEventScope struct {
	WaveID        string `json:"wave_id"`
	ScopeRevision int    `json:"scope_revision"`
	PlanRevision  int    `json:"plan_revision"`
}
type StateEvent struct {
	Sequence       int64           `json:"sequence"`
	ID             string          `json:"id"`
	Target         StateEventScope `json:"target"`
	TaskID         string          `json:"task_id"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	PayloadBytes   int64           `json:"payload_bytes"`
	PayloadOmitted bool            `json:"payload_omitted"`
}
type StateEventPage struct {
	Events []StateEvent `json:"events"`
	Cursor int64        `json:"cursor"`
	More   bool         `json:"more"`
}

func (p StateEventPage) Validate(after int64, limit int) error {
	if after < 0 || limit < 1 || limit > MaxStatePage || p.Cursor < after || p.Events == nil || len(p.Events) > limit || p.More && len(p.Events) == 0 {
		return errors.New("invalid state event page")
	}
	cursor := after
	for _, event := range p.Events {
		if event.Sequence <= cursor || event.ID == "" || len(event.ID) > MaxInline || (event.Target.WaveID == "" || event.Target.ScopeRevision < 1 || event.Target.PlanRevision < 1) || len(event.Target.WaveID) > 256 || !utf8.ValidString(event.Target.WaveID) || !utf8.ValidString(event.ID) || event.Kind == "" || len(event.Kind) > 128 || len(event.TaskID) > 256 || !utf8.ValidString(event.TaskID) || !utf8.ValidString(event.Kind) || event.PayloadBytes < 1 {
			return errors.New("invalid state event identity")
		}
		if event.PayloadOmitted {
			if len(event.Payload) != 0 || event.PayloadBytes <= MaxInline {
				return errors.New("invalid state payload reference")
			}
		} else if len(event.Payload) > MaxInline || int64(len(event.Payload)) > event.PayloadBytes || event.PayloadBytes > MaxInline || !json.Valid(event.Payload) {
			return errors.New("invalid inline state payload")
		}
		cursor = event.Sequence
	}
	if p.Cursor != cursor {
		return errors.New("state cursor must match the last delivered event")
	}
	return nil
}
