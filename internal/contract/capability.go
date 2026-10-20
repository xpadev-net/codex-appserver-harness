// Package contract records observed integration capabilities; absence is never approval.
package contract

import "time"

type State string

const (
	Verified    State = "verified"
	Unsupported State = "unsupported"
	Unknown     State = "unknown"
)

type Capability struct {
	State    State  `json:"state"`
	Reason   string `json:"reason"`
	Evidence string `json:"evidence,omitempty"`
}
type Report struct {
	Version       int                   `json:"version"`
	ObservedAt    time.Time             `json:"observed_at"`
	Repository    string                `json:"repository"`
	RepositoryID  int64                 `json:"repository_id"`
	DefaultBranch string                `json:"default_branch"`
	CodexVersion  string                `json:"codex_version"`
	Capabilities  map[string]Capability `json:"capabilities"`
}

var MergeRequirements = []string{"github_read", "github_write", "app_server", "thread_resume", "cloud_request", "cloud_native_approval", "review_sha", "ci_mapping"}

func (r Report) AutoMergeReady() bool {
	if r.RepositoryID == 0 || r.DefaultBranch == "" {
		return false
	}
	for _, name := range MergeRequirements {
		c := r.Capabilities[name]
		if c.State != Verified || c.Evidence == "" {
			return false
		}
	}
	return true
}
func NewReport(repository string) Report {
	r := Report{Version: 1, ObservedAt: time.Now().UTC(), Repository: repository, Capabilities: map[string]Capability{}}
	for _, name := range MergeRequirements {
		r.Capabilities[name] = Capability{State: Unknown, Reason: "not observed"}
	}
	return r
}
