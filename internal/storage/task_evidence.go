package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrTaskEvidence = errors.New("Task evidence is unavailable or inconsistent")
var ErrTaskBinding = errors.New("Task is not bound to the current plan")

// Classify only decoding of saved evidence. SQL, cancellation and other I/O
// errors must remain operational errors rather than successful readiness holds.
func taskEvidenceRecord(ctx context.Context, q queryer, kind, id string, out any) error {
	err := completionRecord(ctx, q, kind, id, out)
	var syntax *json.SyntaxError
	var shape *json.UnmarshalTypeError
	if errors.As(err, &syntax) || errors.As(err, &shape) {
		return fmt.Errorf("%w: %v", ErrTaskEvidence, err)
	}
	return err
}
