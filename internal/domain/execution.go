package domain

import "errors"

// ExecutionLimits applies to the whole controller. Per-Wave cumulative policy
// remains pinned separately on each Wave.
type ExecutionLimits struct {
	Waves        int `json:"waves"`
	Turns        int `json:"turns"`
	Runners      int `json:"runners"`
	PRs          int `json:"prs"`
	CloudReviews int `json:"cloud_reviews"`
}

func DefaultExecutionLimits() ExecutionLimits {
	return ExecutionLimits{Waves: 4, Turns: 2, Runners: 1, PRs: 1, CloudReviews: 1}
}
func LegacyExecutionLimits() ExecutionLimits {
	return ExecutionLimits{Waves: 1, Turns: 1, Runners: 1, PRs: 1, CloudReviews: 1}
}
func (l ExecutionLimits) Validate() error {
	if l.Waves < 1 || l.Turns < 1 || l.Runners < 1 || l.PRs < 1 || l.CloudReviews < 1 {
		return errors.New("all execution capacities must be positive")
	}
	return nil
}
