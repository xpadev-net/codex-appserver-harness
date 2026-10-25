package contract

// The parent explicitly adopts a readable immutable proposal Artifact. Storage
// resolves typed coverage, current registration, repository and all send facts.
type SplitProductRequest struct {
	SchemaVersion      int    `json:"schema_version"`
	Target             Target `json:"target"`
	ProposalArtifactID string `json:"proposal_artifact_id"`
}

// Preparing this reference alone neither adopts a split nor grants execution.
type SplitProductParentScopeRequest struct {
	SchemaVersion    int    `json:"schema_version"`
	Target           Target `json:"target"`
	ParentWorkItemID string `json:"parent_work_item_id"`
}
