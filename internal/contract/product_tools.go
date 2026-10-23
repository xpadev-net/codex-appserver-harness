package contract

import "strings"

// PublicationWorkItemMarker is part of the controller/GitHub correlation wire
// format. Sharing it also lets storage bound the exact outgoing request body.
func PublicationWorkItemMarker(workItem string) string {
	return "<!-- harness-work-item:" + workItem + " -->"
}

// Only the controller resolves repository, branch, policy and publication body.
// A model names an already registered immutable product candidate.
type PublishPRRequest struct {
	SchemaVersion int                `json:"schema_version"`
	Target        Target             `json:"target"`
	Candidate     CandidateReference `json:"candidate"`
}
type RequestPRReviewRequest PublishPRRequest

func validProductCandidate(c CandidateReference) bool {
	return inline(c.WorkItemID) && inline(c.CandidateID) && len(c.SHA) == 40 && strings.Trim(c.SHA, "0123456789abcdef") == ""
}

// ProductTools is an explicit opt-in tool set for a configured P adapter.
// KernelTools and the K runtime continue to publish only common kernel tools.
func ProductTools(kind string) []ToolDefinition {
	tools := KernelTools(kind)
	if tools == nil {
		return nil
	}
	for _, name := range []string{"pr_publish", "pr_review_request"} {
		tools = append(tools, ToolDefinition{Type: "function", Name: name, Description: "Request a controller-owned P operation for the exact registered candidate. Acceptance saves operation and automatic resource-subscription intents before external I/O. Accepted is not published, approved, merged, or complete; wait on the returned subscription.", InputSchema: Schema(toolArgument(name))})
	}
	return tools
}
