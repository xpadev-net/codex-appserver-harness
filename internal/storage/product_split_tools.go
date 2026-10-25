package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
)

type ProductSplitToolAdoption struct {
	Preparation       ProductSplitPreparation `json:"preparation"`
	Source            kernelToolOperation     `json:"source"`
	ArtifactID        string                  `json:"artifact_id"`
	ArtifactDigest    string                  `json:"artifact_digest"`
	ParentHeadID      string                  `json:"parent_head_id"`
	PreparationID     string                  `json:"preparation_id"`
	PreparationDigest string                  `json:"preparation_digest"`
}

func requireProductSplitSessionTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, request contract.SplitProductRequest) error {
	if b.Kind != "orchestrator" || request.SchemaVersion != 1 || !request.Target.Valid() || strings.TrimSpace(request.ProposalArtifactID) == "" || len(request.ProposalArtifactID) > 128 {
		return errors.New("product split requires an explicit parent request")
	}
	var profile SessionToolProfile
	id := structureID("session-tools-", b.ID, fmt.Sprint(b.Generation))
	if err := taskEvidenceRecord(ctx, tx, "session_tool_profile", id, &profile); err != nil {
		return err
	}
	if profile.SessionID != b.ID || profile.Generation != b.Generation || profile.Profile != "product_v3" || profile.CreatedAt.IsZero() {
		return errors.New("product split requires a pinned product_v3 parent session")
	}
	return nil
}

func (k *KernelStore) acceptProductSplitTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, b contract.SessionBinding, request contract.SplitProductRequest) (contract.ToolReply, error) {
	var empty contract.ToolReply
	_, wave, err := artifactAuthorityTx(ctx, tx, b.ID, op.Target)
	if err != nil {
		return empty, err
	}
	artifact, err := loadArtifact(ctx, tx, request.ProposalArtifactID)
	if err != nil {
		return empty, err
	}
	if err := artifactReadableTx(ctx, tx, b, wave, artifact); err != nil {
		return empty, err
	}
	if artifact.Kind != "split_proposal" || artifact.ContentType != "application/json" || artifact.Bytes > 1<<20 {
		return empty, errors.New("bounded split_proposal JSON Artifact required")
	}
	raw, err := k.readArtifactObject(artifact)
	if err != nil {
		return empty, err
	}
	var proposal splitting.Proposal
	if err := contract.DecodeStrict(raw, &proposal); err != nil {
		return empty, err
	}
	registration, err := publicationScopeRegistrationTx(ctx, tx, wave, proposal.WorkItemID)
	if err != nil {
		return empty, err
	}
	saved, err := k.prepareProductSplitExecutionTx(ctx, tx, ProductSplitProposalRequest{Target: op.Target, Key: op.ID, ParentWorkItemID: proposal.WorkItemID, RegistrationDigest: registration.Digest, Proposal: proposal})
	if err != nil {
		return empty, err
	}
	adoption := ProductSplitToolAdoption{Preparation: saved, Source: op, ArtifactID: artifact.ID, ArtifactDigest: artifact.Digest, ParentHeadID: saved.Value.ParentHeadID, PreparationID: saved.ID, PreparationDigest: saved.Digest}
	if err := kernelRecordTx(ctx, tx, "product_split_tool_adoption", op.ID, op.Target.WaveID, adoption); err != nil {
		return empty, err
	}
	// This reply deliberately contains no private product projection/workspace.
	content, err := json.Marshal(struct {
		PreparationID string `json:"preparation_id"`
		Digest        string `json:"digest"`
		Children      int    `json:"children"`
		Stage         string `json:"stage"`
	}{saved.ID, saved.Digest, len(saved.Value.Creations), "prepared"})
	if err != nil {
		return empty, err
	}
	return contract.ToolReply{SchemaVersion: 1, State: "completed", OperationID: op.ID, SubscriptionIDs: []string{}, ResourceID: saved.ID, Content: string(content)}, nil
}

// Cached completion is an audited preparation response, never a repair of lost
// admission evidence or a new send permission.
func verifyProductSplitReplayTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, request contract.SplitProductRequest, reply contract.ToolReply) error {
	var adoption ProductSplitToolAdoption
	if err := taskEvidenceRecord(ctx, tx, "product_split_tool_adoption", op.ID, &adoption); err != nil {
		return splitEvidenceErr(err)
	}
	if adoption.Source != op || adoption.ArtifactID != request.ProposalArtifactID || !validSourceDigest(adoption.ArtifactDigest) || adoption.PreparationID != reply.ResourceID {
		return ErrProductSplitHeld
	}
	artifact, err := loadArtifact(ctx, tx, adoption.ArtifactID)
	if err != nil {
		return err
	}
	if artifact.Digest != adoption.ArtifactDigest || artifact.Target() != op.Target {
		return ErrProductSplitHeld
	}
	saved, err := loadProductSplitPreparationTx(ctx, tx, adoption.ParentHeadID)
	if err != nil {
		return splitEvidenceErr(err)
	}
	if saved.ID != adoption.PreparationID || saved.Digest != adoption.PreparationDigest || !reflect.DeepEqual(saved, adoption.Preparation) {
		return ErrProductSplitHeld
	}
	return nil
}
