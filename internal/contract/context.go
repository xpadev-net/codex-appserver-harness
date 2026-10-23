package contract

// ContextReadRequest reads controller-owned Wave contract bytes, not arbitrary
// host paths. Snapshot references use the returned digest/size and scoped target.
type ContextReadRequest struct {
	SchemaVersion int    `json:"schema_version"`
	Target        Target `json:"target"`
	Section       string `json:"section"` // request, criteria, policy, direction, manifest, external
	ReferenceID   string `json:"reference_id,omitempty"`
	Offset        int64  `json:"offset"`
	Limit         int64  `json:"limit"`
}

const MaxContextRead = 2048

func (r ContextReadRequest) Valid() bool {
	if r.SchemaVersion != 1 || !r.Target.Valid() || r.Offset < 0 || r.Limit < 1 || r.Limit > MaxContextRead {
		return false
	}
	if r.Section == "manifest" || r.Section == "external" {
		return len(r.ReferenceID) > 0 && len(r.ReferenceID) <= 128
	}
	return r.ReferenceID == "" && (r.Section == "request" || r.Section == "criteria" || r.Section == "policy" || r.Section == "direction")
}

type ContextSlice struct {
	SchemaVersion int    `json:"schema_version"`
	Target        Target `json:"target"`
	Section       string `json:"section"`
	Digest        string `json:"digest"`
	Bytes         int64  `json:"bytes"`
	Offset        int64  `json:"offset"`
	NextOffset    *int64 `json:"next_offset"`
	ContentType   string `json:"content_type"`
	Content       string `json:"content"`
}
