package contract

import "errors"

const MaxMainWaves = 16

// MainResult proposes actions within the controller's saved Wave input scope.
// Completion/approval/policy changes are deliberately absent: ordinary code
// owns those gates. A report records an explanation, never Wave acceptance.
type MainResult struct {
	SchemaVersion int            `json:"schema_version"`
	SessionID     string         `json:"session_id"`
	Decisions     []MainDecision `json:"decisions"`
}
type MainDecision struct {
	Target          Target        `json:"target"`
	Action          string        `json:"action"` // plan, wait, report
	Plan            *PlanProposal `json:"plan"`
	SubscriptionIDs []string      `json:"subscription_ids"`
	Summary         string        `json:"summary"`
	Remaining       []string      `json:"remaining"`
	Blockers        []string      `json:"blockers"`
}

func (r MainResult) Validate() error {
	if r.SchemaVersion != 1 || !inline(r.SessionID) || len(r.Decisions) == 0 || len(r.Decisions) > MaxMainWaves {
		return errors.New("invalid main result identity or decision bound")
	}
	seen := map[string]bool{}
	for _, d := range r.Decisions {
		if !d.Target.Valid() || seen[d.Target.WaveID] || !inline(d.Summary) || !validIDs(d.SubscriptionIDs) || len(d.SubscriptionIDs) > 128 || len(d.Remaining) > 128 || len(d.Blockers) > 128 {
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
			if d.Plan == nil || len(d.SubscriptionIDs) != 0 {
				return errors.New("plan decision requires only a plan")
			}
			if err := d.Plan.Validate(); err != nil {
				return err
			}
		case "wait":
			if d.Plan != nil || len(d.SubscriptionIDs) == 0 {
				return errors.New("waiting requires durable subscriptions")
			}
		case "report":
			if d.Plan != nil || len(d.SubscriptionIDs) != 0 {
				return errors.New("report cannot change a plan or waiting conditions")
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
	fields["action"].(map[string]any)["enum"] = []string{"plan", "wait", "report"}
	fields["plan"].(map[string]any)["anyOf"] = []any{PlanSchema(), map[string]any{"type": "null"}}
	return schema
}
