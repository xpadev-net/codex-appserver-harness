// Package storage persists state and operation intent in one SQLite transaction.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

type Store struct{ DB *sql.DB }

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db}
	_, err = db.Exec(`PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;
 CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY);
 CREATE TABLE IF NOT EXISTS work_items(id TEXT PRIMARY KEY,repository_id INTEGER NOT NULL,issue INTEGER NOT NULL,data BLOB NOT NULL,UNIQUE(repository_id,issue));
 CREATE TABLE IF NOT EXISTS event_inbox(id TEXT PRIMARY KEY,work_item_id TEXT NOT NULL,payload BLOB NOT NULL,processed_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS operation_outbox(id TEXT PRIMARY KEY,work_item_id TEXT NOT NULL REFERENCES work_items(id),payload BLOB NOT NULL,state TEXT NOT NULL,result BLOB);
 CREATE TABLE IF NOT EXISTS resource_leases(resource TEXT PRIMARY KEY,owner TEXT NOT NULL,expires INTEGER NOT NULL,generation INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS records(kind TEXT NOT NULL,id TEXT NOT NULL,work_item_id TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(kind,id));
 INSERT OR IGNORE INTO schema_migrations VALUES(1);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) Claim(ctx context.Context, w domain.WorkItem) error {
	if w.ID == "" || w.RepositoryID <= 0 || w.Issue <= 0 || w.ScopeRevision < 1 || w.PolicyRevision < 1 || w.Phase != domain.Ready {
		return errors.New("invalid work item")
	}
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, "INSERT INTO work_items VALUES(?,?,?,?)", w.ID, w.RepositoryID, w.Issue, b)
	return err
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func read(ctx context.Context, q queryer, id string) (domain.WorkItem, error) {
	var b []byte
	var w domain.WorkItem
	err := q.QueryRowContext(ctx, "SELECT data FROM work_items WHERE id=?", id).Scan(&b)
	if err == nil {
		err = json.Unmarshal(b, &w)
	}
	return w, err
}
func (s *Store) Get(ctx context.Context, id string) (domain.WorkItem, error) {
	return read(ctx, s.DB, id)
}
func (s *Store) List(ctx context.Context) ([]domain.WorkItem, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT data FROM work_items ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.WorkItem{}
	for rows.Next() {
		var b []byte
		var w domain.WorkItem
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &w); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
func (s *Store) Apply(ctx context.Context, e domain.Event) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO event_inbox VALUES(?,?,?,?)", e.ID, e.WorkItemID, b, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return tx.Commit()
	}
	w, err := read(ctx, tx, e.WorkItemID)
	if err != nil {
		return err
	}
	w, ops, err := domain.Apply(w, e)
	if err != nil {
		return err
	}
	b, err = json.Marshal(w)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE work_items SET data=? WHERE id=?", b, w.ID); err != nil {
		return err
	}
	for _, o := range ops {
		b, err = json.Marshal(o)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO operation_outbox(id,work_item_id,payload,state) VALUES(?,?,?,'pending')", o.ID, o.WorkItemID, b); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Operations includes unknown and sending states: a dispatcher must reconcile those, never resend them blindly.
func (s *Store) Operations(ctx context.Context) ([]domain.Operation, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT payload,state FROM operation_outbox WHERE state NOT IN ('done','canceled') ORDER BY rowid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Operation{}
	for rows.Next() {
		var b []byte
		var state string
		var o domain.Operation
		if err = rows.Scan(&b, &state); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &o); err != nil {
			return nil, err
		}
		o.State = state
		out = append(out, o)
	}
	return out, rows.Err()
}

// BeginDispatch checks current operator and candidate state transactionally immediately before sending.
func (s *Store) BeginDispatch(ctx context.Context, id string) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var b []byte
	var state string
	if err = tx.QueryRowContext(ctx, "SELECT payload,state FROM operation_outbox WHERE id=?", id).Scan(&b, &state); err != nil {
		return false, err
	}
	if state != "pending" {
		return false, nil
	}
	var o domain.Operation
	if err = json.Unmarshal(b, &o); err != nil {
		return false, err
	}
	w, err := read(ctx, tx, o.WorkItemID)
	if err != nil {
		return false, err
	}
	if !domain.CanDispatch(w, o) {
		return false, nil
	}
	if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state='sending' WHERE id=?", id); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
func (s *Store) FinishOperation(ctx context.Context, id, state string, result any) error {
	if state != "done" && state != "unknown" && state != "canceled" {
		return errors.New("invalid operation result state")
	}
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	r, err := s.DB.ExecContext(ctx, "UPDATE operation_outbox SET state=?,result=? WHERE id=? AND state IN ('sending','unknown')", state, b, id)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return errors.New("operation is not dispatched")
	}
	return nil
}

// Acquire never steals expired leases: the old owner must first be reconciled and explicitly released.
func (s *Store) Acquire(ctx context.Context, resource, owner string, ttl time.Duration) (int64, error) {
	if resource == "" || owner == "" || ttl <= 0 {
		return 0, errors.New("invalid lease")
	}
	var generation int64
	err := s.DB.QueryRowContext(ctx, `INSERT INTO resource_leases VALUES(?,?,?,1) ON CONFLICT(resource) DO UPDATE SET expires=excluded.expires,generation=resource_leases.generation+1 WHERE resource_leases.owner=excluded.owner AND resource_leases.expires>? RETURNING generation`, resource, owner, time.Now().Add(ttl).UnixNano(), time.Now().UnixNano()).Scan(&generation)
	return generation, err
}
func (s *Store) Release(ctx context.Context, resource, owner string, generation int64) error {
	r, err := s.DB.ExecContext(ctx, "DELETE FROM resource_leases WHERE resource=? AND owner=? AND generation=?", resource, owner, generation)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return fmt.Errorf("lease owner or generation mismatch")
	}
	return nil
}
func (s *Store) Put(ctx context.Context, kind, id, workItem string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload", kind, id, workItem, b)
	return err
}
func (s *Store) Record(ctx context.Context, kind, id string, value any) error {
	var b []byte
	if err := s.DB.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind=? AND id=?", kind, id).Scan(&b); err != nil {
		return err
	}
	return json.Unmarshal(b, value)
}
