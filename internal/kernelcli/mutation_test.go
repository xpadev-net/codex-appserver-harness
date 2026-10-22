package kernelcli

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestKernelMutationRetriesWholeSnapshotTransaction(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "snapshot.db") + "?_journal_mode=WAL&_busy_timeout=50"
	first, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err = first.ExecContext(ctx, "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)"); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	err = retryMutation(ctx, func(ctx context.Context) error {
		attempts++
		tx, err := first.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var observed int
		if err = tx.QueryRowContext(ctx, "SELECT value FROM counter").Scan(&observed); err != nil {
			return err
		}
		if attempts == 1 {
			if _, err = second.ExecContext(ctx, "UPDATE counter SET value=value+1"); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, "UPDATE counter SET value=?", observed+1); err != nil {
			return err
		}
		return tx.Commit()
	})
	var value int
	if err != nil {
		t.Fatal(err)
	}
	if err = first.QueryRowContext(ctx, "SELECT value FROM counter").Scan(&value); err != nil || value != 2 || attempts != 2 {
		t.Fatal(value, attempts, err)
	}
}
func TestKernelMutationPreservesIOErrorsAndCancellation(t *testing.T) {
	calls := 0
	failure := sqlite3.Error{Code: sqlite3.ErrIoErr}
	err := retryMutation(context.Background(), func(context.Context) error { calls++; return failure })
	var actual sqlite3.Error
	if !errors.As(err, &actual) || actual.Code != sqlite3.ErrIoErr || calls != 1 {
		t.Fatal("I/O error retried or erased", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = retryMutation(ctx, func(context.Context) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("canceled mutation executed", calls, err)
	}
}
