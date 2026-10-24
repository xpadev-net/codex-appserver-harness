package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestSourceRequestEvidenceReadKeepsCancellationAndSQLFailureOperational(t *testing.T) {
	k, w, _ := productSourceFixture(t)
	// This deliberately incomplete record reaches the direction read, never
	// adoption. Its saved checksum is valid so a SQL/context failure is the cause.
	w.DirectionID = "missing-direction"
	saved := ProductSourceRequestAdoption{Value: ProductSourceRequestAdoptionValue{Request: ProductSourceRequestAdoptionRequest{Target: w.Target(), DirectionID: w.DirectionID}, RequestDigest: ProductSourceRequestDigest("new request"), OriginalRequestDigest: ProductSourceRequestDigest(w.OriginalRequest), CreatedAt: k.clock()}}
	var err error
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		t.Fatal(err)
	}
	saved.ID = structureID("source-request-", sourceRequestEpoch(w.Target()), saved.Digest)
	tx, err := k.kernelTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := k.checkProductSourceRequestTx(ctx, tx, w, saved); !errors.Is(err, context.Canceled) || errors.Is(err, ErrTaskEvidence) {
		t.Fatal("cancellation became corrupt evidence", err)
	}
	if err := k.checkProductSourceRequestTx(context.Background(), tx, w, saved); !errors.Is(err, sql.ErrNoRows) || !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("missing evidence lost its hold classification", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := k.checkProductSourceRequestTx(context.Background(), tx, w, saved); !errors.Is(err, sql.ErrTxDone) || errors.Is(err, ErrTaskEvidence) {
		t.Fatal("SQL failure became corrupt evidence", err)
	}
}
