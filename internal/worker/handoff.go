package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// Handoff is a controller-validated claim, never a claim of test/review success.
type Handoff struct {
	SchemaVersion     int      `json:"schema_version"`
	WorkItemID        string   `json:"work_item_id"`
	ScopeRevision     int      `json:"scope_revision"`
	InputCandidateID  string   `json:"input_candidate_id"`
	Outcome           string   `json:"outcome"`
	Summary           string   `json:"summary"`
	CoveredCriteria   []string `json:"covered_criteria"`
	RemainingWork     []string `json:"remaining_work"`
	RequestedProfiles []string `json:"requested_profiles"`
	Blockers          []string `json:"blockers"`
}

func Decode(data []byte, id string, revision int, candidate string, criteria, profiles []string) (Handoff, error) {
	var h Handoff
	if len(data) == 0 || len(data) > 1<<20 {
		return h, errors.New("invalid handoff size")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return h, err
	}
	for _, key := range []string{"schema_version", "work_item_id", "scope_revision", "input_candidate_id", "outcome", "summary", "covered_criteria", "remaining_work", "requested_profiles", "blockers"} {
		v, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return h, errors.New("required handoff field missing or null: " + key)
		}
	}
	for _, key := range []string{"covered_criteria", "remaining_work", "requested_profiles", "blockers"} {
		var elements []json.RawMessage
		if err := json.Unmarshal(fields[key], &elements); err != nil {
			return h, err
		}
		for _, raw := range elements {
			if len(raw) == 0 || raw[0] != '"' {
				return h, errors.New("array element must be string")
			}
			var item string
			if err := json.Unmarshal(raw, &item); err != nil {
				return h, err
			}
			if len(item) == 0 || utf8.RuneCountInString(item) > 8192 {
				return h, errors.New("array string bounds invalid")
			}
		}
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&h); err != nil {
		return h, err
	}
	if d.Decode(new(any)) != io.EOF {
		return h, errors.New("trailing handoff data")
	}
	if h.CoveredCriteria == nil || h.RemainingWork == nil || h.RequestedProfiles == nil || h.Blockers == nil {
		return h, errors.New("required handoff arrays absent or null")
	}
	if h.SchemaVersion != 1 || h.WorkItemID != id || h.ScopeRevision != revision || h.InputCandidateID != candidate || len(h.Summary) == 0 || utf8.RuneCountInString(h.Summary) > 16384 {
		return h, errors.New("handoff identity or summary invalid")
	}
	switch h.Outcome {
	case "candidate":
		if len(h.CoveredCriteria) == 0 || len(h.Blockers) != 0 {
			return h, errors.New("candidate requires coverage and no blockers")
		}
	case "blocked":
		if len(h.Blockers) == 0 {
			return h, errors.New("blocked handoff requires reason")
		}
	case "validation_needed":
		if len(h.RequestedProfiles) == 0 {
			return h, errors.New("validation request requires profiles")
		}
	default:
		return h, errors.New("unsupported handoff outcome")
	}
	if !subset(h.CoveredCriteria, criteria) || !subset(h.RequestedProfiles, profiles) || len(h.RemainingWork) > 128 || len(h.Blockers) > 32 || len(h.CoveredCriteria) > 128 || len(h.RequestedProfiles) > 128 {
		return h, errors.New("invalid criteria/profile references or bounds")
	}
	return h, nil
}
func subset(items, allowed []string) bool {
	set := map[string]bool{}
	for _, v := range allowed {
		set[v] = true
	}
	seen := map[string]bool{}
	for _, v := range items {
		if v == "" || !set[v] || seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}
