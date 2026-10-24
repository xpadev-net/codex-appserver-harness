package control

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mattn/go-sqlite3"
)

var capacityDriverSequence atomic.Uint64

// Complete an actual SQLite capacity-read transaction by rolling it back, as
// database/sql can do on cancellation, then expose its deterministic race result.
type capacityCommitDriver struct {
	cancel context.CancelFunc
	result error
}

func (d capacityCommitDriver) Open(name string) (driver.Conn, error) {
	c, err := (&sqlite3.SQLiteDriver{}).Open(name)
	if err != nil {
		return nil, err
	}
	return capacityCommitConn{Conn: c, driver: d}, nil
}

type capacityCommitConn struct {
	driver.Conn
	driver capacityCommitDriver
}

func (c capacityCommitConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return capacityCommitTx{Tx: tx, driver: c.driver}, nil
}

type capacityCommitTx struct {
	driver.Tx
	driver capacityCommitDriver
}

func (t capacityCommitTx) Commit() error {
	err := t.Tx.Rollback()
	if t.driver.cancel != nil {
		t.driver.cancel()
	}
	if err != nil {
		return errors.Join(t.driver.result, err)
	}
	return t.driver.result
}

func TestLoopCapacityStartupCancellationKeepsOtherErrorsVisible(t *testing.T) {
	for _, mode := range []string{"canceled_transaction", "live_transaction_error", "canceled_database_error", "canceled_joined_error"} {
		t.Run(mode, func(t *testing.T) {
			k, b := taskFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := error(sql.ErrTxDone)
			var commitCancel context.CancelFunc
			if mode != "live_transaction_error" {
				commitCancel = cancel
			}
			if mode == "canceled_joined_error" {
				result = errors.Join(sql.ErrTxDone, errors.New("fixture joined database failure"))
			}
			if mode == "canceled_database_error" {
				result = errors.New("fixture real database error")
			}
			name := fmt.Sprintf("capacity-cancel-%d", capacityDriverSequence.Add(1))
			sql.Register(name, capacityCommitDriver{cancel: commitCancel, result: result})
			if err := k.DB.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open(name, filepath.Join(k.Root, "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			k.DB = db
			source := newSource(workRequest(b))
			calls := 0
			loop := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { calls++; return nil, errors.New("unexpected launch") })}, source)
			err = loop.Run(ctx)
			if mode == "canceled_transaction" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("capacity rollback hid cancellation", err)
				}
			} else if !errors.Is(err, result) || errors.Is(err, context.Canceled) {
				t.Fatal("database error was hidden", err)
			}
			if calls != 0 {
				t.Fatal("startup failure launched process", calls)
			}
			select {
			case <-source.scans:
				t.Fatal("startup failure admitted work")
			default:
			}
		})
	}
}
