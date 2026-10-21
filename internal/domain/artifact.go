package domain

import (
	"errors"
	"mime"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// Artifact describes imported controller-owned bytes, never a worker-owned path.
// The object filename is derived from its opaque ID and is not exposed in tools.
type ArtifactOrigin struct {
	Kind      string `json:"kind"`
	Reference string `json:"reference"`
}
type Artifact struct {
	ID               string         `json:"id"`
	WaveID           string         `json:"wave_id"`
	TaskID           string         `json:"task_id"`
	SessionID        string         `json:"session_id"`
	ScopeRevision    int            `json:"scope_revision"`
	PlanRevision     int            `json:"plan_revision"`
	PolicyRevision   int            `json:"policy_revision"`
	Kind             string         `json:"kind"`
	ContentType      string         `json:"content_type"`
	Digest           string         `json:"digest"`
	Bytes            int64          `json:"bytes"`
	Origin           ArtifactOrigin `json:"origin"`
	InputArtifactIDs []string       `json:"input_artifact_ids"`
	State            string         `json:"state"`
	CreatedAt        time.Time      `json:"created_at"`
	ExpiredAt        time.Time      `json:"expired_at"`
}

func (a Artifact) Target() contract.Target {
	return contract.Target{WaveID: a.WaveID, ScopeRevision: a.ScopeRevision, PlanRevision: a.PlanRevision, PolicyRevision: a.PolicyRevision}
}
func (a Artifact) Validate() error {
	if !strings.HasPrefix(a.ID, "artifact-") || len(a.ID) != 41 || strings.Trim(a.ID[9:], "0123456789abcdef") != "" || !a.Target().Valid() || a.SessionID == "" || a.Bytes < 0 || len(a.Digest) != 64 || strings.Trim(a.Digest, "0123456789abcdef") != "" || a.CreatedAt.IsZero() {
		return errors.New("invalid Artifact identity/content manifest")
	}
	for _, s := range []string{a.Kind, a.Origin.Reference} {
		if strings.TrimSpace(s) == "" || len(s) > contract.MaxInline || !utf8.ValidString(s) {
			return errors.New("invalid Artifact provenance")
		}
	}
	if len(a.ContentType) > contract.MaxInline {
		return errors.New("invalid Artifact content type")
	}
	if _, _, err := mime.ParseMediaType(a.ContentType); err != nil {
		return err
	}
	switch a.Origin.Kind {
	case "task", "repository", "external":
	default:
		return errors.New("unsupported Artifact origin")
	}
	if a.Origin.Kind == "task" && (a.TaskID == "" || a.Origin.Reference != a.TaskID) {
		return errors.New("Task Artifact origin mismatch")
	}
	switch a.State {
	case "staged", "committed", "deleting", "expired":
	default:
		return errors.New("invalid Artifact retention state")
	}
	seen := map[string]bool{}
	for _, id := range a.InputArtifactIDs {
		if id == "" || seen[id] {
			return errors.New("invalid Artifact input references")
		}
		seen[id] = true
	}
	return nil
}
