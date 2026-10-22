package kernelcli

import (
	"context"
	"errors"
	"time"

	"github.com/mattn/go-sqlite3"
)

// Deferred SQLite transactions can lose a read-to-write upgrade despite busy
// timeout. Retry only the entire idempotent mutation, after its rollback, never
// an individual statement or a non-busy storage/domain failure.
func retryMutation(ctx context.Context, apply func(context.Context) error) error {
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	delay := 5 * time.Millisecond
	for {
		if err := bounded.Err(); err != nil {
			return err
		}
		err := apply(bounded)
		if err == nil {
			return nil
		}
		var sqliteErr sqlite3.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code != sqlite3.ErrBusy && sqliteErr.Code != sqlite3.ErrLocked {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-bounded.Done():
			timer.Stop()
			return errors.Join(bounded.Err(), err)
		case <-timer.C:
		}
		if delay < 100*time.Millisecond {
			delay *= 2
		}
	}
}
