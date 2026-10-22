package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrTurnAssociationHeld = errors.New("explicit turn association requires a stopped unknown run and an observed nonbaseline candidate")

type TurnAssociationRequest struct {
	Key           string                  `json:"key"`
	Expected      contract.SessionBinding `json:"expected"`
	RunID         string                  `json:"run_id"`
	ObservationID string                  `json:"observation_id"`
	TurnID        string                  `json:"turn_id"`
	Reason        string                  `json:"reason"`
}
type TurnAssociation struct {
	ID               string                 `json:"id"`
	Request          TurnAssociationRequest `json:"request"`
	RequestDigest    string                 `json:"request_digest"`
	ObservingOwnerID string                 `json:"observing_owner_id"`
	CreatedAt        time.Time              `json:"created_at"`
}

// AssociateObservedTurn is trusted operator input. It resolves only which
// already observed candidate belongs to an unknown start. The run remains
// unknown until ordinary read-only reconciliation reads its actual outcome.
func (k *KernelStore) AssociateObservedTurn(ctx context.Context, r TurnAssociationRequest) (TurnAssociation, error) {
	var out TurnAssociation
	for _, value := range []string{r.Key, r.RunID, r.ObservationID, r.TurnID, r.Reason} {
		if strings.TrimSpace(value) == "" || len(value) > contract.MaxInline || !utf8.ValidString(value) {
			return out, errors.New("bounded explicit association identity and reason required")
		}
	}
	if len(r.Key) > 256 {
		return out, errors.New("association key exceeds limit")
	}
	digest, err := contentDigest(r)
	if err != nil {
		return out, err
	}
	id := structureID("turn-association-", r.Expected.ID, r.Key)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "session_turn_association", id, &out)
	if err == nil {
		if out.ID != id || out.RequestDigest != digest {
			return out, errors.New("turn association replay conflict")
		}
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	b, err := sessionBindingTx(ctx, tx, r.Expected.ID)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(b, r.Expected) || b.ThreadID == "" {
		return out, ErrTurnAssociationHeld
	}
	state, err := sessionDispatchStateTx(ctx, tx, b.ID)
	if err != nil {
		return out, err
	}
	if state.Creating || !state.ProcessStopped || state.Run == nil || !state.RunStopped || state.Run.ID != r.RunID || state.Run.State != "unknown" || state.Run.TurnID != "" || !state.Run.BaselineKnown {
		return out, ErrTurnAssociationHeld
	}
	var unresolved int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='managed_process' AND json_extract(payload,'$.request.binding.id')=? AND json_extract(payload,'$.state')<>'stopped'", b.ID).Scan(&unresolved); err != nil {
		return out, err
	}
	if unresolved != 0 {
		return out, ErrTurnAssociationHeld
	}
	run := *state.Run
	var observation HistoryObservation
	var ownerSession string
	if err = tx.QueryRowContext(ctx, "SELECT work_item_id FROM records WHERE kind='session_history_observation' AND id=?", r.ObservationID).Scan(&ownerSession); err != nil {
		return out, err
	}
	if err = completionRecord(ctx, tx, "session_history_observation", r.ObservationID, &observation); err != nil {
		return out, err
	}
	observationID, err := historyObservationID(observation)
	if err != nil {
		return out, err
	}
	if ownerSession != b.ID || observation.ID != r.ObservationID || observationID != r.ObservationID || observation.SessionID != b.ID || observation.RunID != run.ID || observation.ThreadID != b.ThreadID || observation.SavedTurnID != "" || !observation.BaselineKnown || !slices.Equal(observation.BaselineTurnIDs, run.BaselineTurnIDs) || observation.Status != "ambiguous_turn" || observation.ResponseDigest == "" || observation.ErrorDigest != "" || !slices.Contains(observation.CandidateTurnIDs, r.TurnID) || slices.Contains(run.BaselineTurnIDs, r.TurnID) {
		return out, ErrTurnAssociationHeld
	}
	found := false
	for _, turn := range observation.Turns {
		found = found || turn.ID == r.TurnID
	}
	if !found {
		return out, ErrTurnAssociationHeld
	}
	observer, err := managedProcessTx(ctx, tx, observation.OwnerID)
	if err != nil {
		return out, err
	}
	if observer.Request.Mode != "recovery" || observer.State != "stopped" || observer.Request.RunID != run.ID || observer.Request.Binding.ID != b.ID || observer.Request.Binding.ThreadID != b.ThreadID || observer.PreparationID != observation.PreparationID {
		return out, ErrTurnAssociationHeld
	}
	var proof ManagedStopProof
	if err = completionRecord(ctx, tx, "managed_stop_proof", observation.OwnerID, &proof); err != nil {
		return out, err
	}
	if proof.OwnerID != observation.OwnerID || proof.Disposition != "group_stopped" || proof.Identity != observer.Identity {
		return out, ErrTurnAssociationHeld
	}
	var other bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='agent_run' AND id<>? AND work_item_id=? AND json_extract(payload,'$.thread_id')=? AND json_extract(payload,'$.turn_id')=?)`, run.ID, b.ID, b.ThreadID, r.TurnID).Scan(&other); err != nil {
		return out, err
	}
	if other {
		return out, ErrTurnAssociationHeld
	}
	// This transaction changes only association, never outcome, input, stop facts
	// or usage. Unknown/held states cannot gain a model-send capability here.
	run.TurnID = r.TurnID
	if err = kernelRecordTx(ctx, tx, "agent_run", run.ID, b.ID, run); err != nil {
		return out, err
	}
	out = TurnAssociation{ID: id, Request: r, RequestDigest: digest, ObservingOwnerID: observation.OwnerID, CreatedAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "session_turn_association", id, b.ID, out); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return TurnAssociation{}, err
	}
	return out, nil
}
