package contract

import "errors"

const MaxMainWaves = 16
const MaxMainListItems = 128

// MainResult proposes actions within the controller's saved Wave input scope.
// Acceptance names evidence for ordinary code to verify; no model claim owns
// completion, approval or policy gates. A report only records an explanation.
type MainResult struct {
	SchemaVersion int            `json:"schema_version"`
	SessionID     string         `json:"session_id"`
	Decisions     []MainDecision `json:"decisions"`
}
type MainDecision struct {
	Target          Target              `json:"target"`
	Action          string              `json:"action"` // plan, wait, report, accept
	Plan            *PlanProposal       `json:"plan"`
	Acceptance      *ArtifactAcceptance `json:"acceptance,omitempty"`
	SubscriptionIDs []string            `json:"subscription_ids"`
	Summary         string              `json:"summary"`
	Remaining       []string            `json:"remaining"`
	Blockers        []string            `json:"blockers"`
}

func (r MainResult) Validate() error {
	if r.SchemaVersion != 1 || !inline(r.SessionID) || len(r.Decisions) == 0 || len(r.Decisions) > MaxMainWaves {
		return errors.New("invalid main result identity or decision bound")
	}
	seen := map[string]bool{}
	for _, d := range r.Decisions {
		if !d.Target.Valid() || seen[d.Target.WaveID] || !inline(d.Summary) || !validIDs(d.SubscriptionIDs) || len(d.SubscriptionIDs) > MaxMainListItems || len(d.Remaining) > MaxMainListItems || len(d.Blockers) > MaxMainListItems {
			return errors.New("invalid or duplicate main decision")
		}
		seen[d.Target.WaveID] = true
		for _, list := range [][]string{d.Remaining, d.Blockers} {
			for _, text := range list {
				if !inline(text) {
					return errors.New("invalid main explanation")
				}
			}
		}
		switch d.Action {
		case "plan":
			if d.Plan == nil || d.Acceptance != nil || len(d.SubscriptionIDs) != 0 {
				return errors.New("plan decision requires only a plan")
			}
			if err := d.Plan.Validate(); err != nil {
				return err
			}
		case "wait":
			if d.Plan != nil || d.Acceptance != nil || len(d.SubscriptionIDs) == 0 {
				return errors.New("waiting requires durable subscriptions")
			}
		case "report":
			if d.Plan != nil || d.Acceptance != nil || len(d.SubscriptionIDs) != 0 {
				return errors.New("report cannot change a plan or waiting conditions")
			}
		case "accept":
			if d.Acceptance == nil || d.Plan != nil || len(d.SubscriptionIDs) != 0 || len(d.Remaining) != 0 || len(d.Blockers) != 0 {
				return errors.New("acceptance requires evidence references and no unresolved work")
			}
			if err := d.Acceptance.Validate(); err != nil {
				return err
			}
		default:
			return errors.New("unsupported main decision")
		}
	}
	return nil
}
func MainResultSchema() map[string]any {
	schema := Schema(MainResult{})
	decisions := schema["properties"].(map[string]any)["decisions"].(map[string]any)
	decisions["minItems"], decisions["maxItems"] = 1, MaxMainWaves
	fields := decisions["items"].(map[string]any)["properties"].(map[string]any)
	for _, name := range []string{"subscription_ids", "remaining", "blockers"} {
		fields[name].(map[string]any)["maxItems"] = MaxMainListItems
	}
	fields["action"].(map[string]any)["enum"] = []string{"plan", "wait", "report", "accept"}
	fields["plan"].(map[string]any)["anyOf"] = []any{PlanSchema(), map[string]any{"type": "null"}}
	fields["acceptance"].(map[string]any)["anyOf"] = []any{ArtifactAcceptanceSchema(), map[string]any{"type": "null"}}
	return schema
}
