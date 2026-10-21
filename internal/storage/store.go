// Package storage persists state and operation intent in one SQLite transaction.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"os"
	"reflect"
	"time"
)

type Store struct{ DB *sql.DB }

func Open(path string) (*Store, error) {
	if path != ":memory:" {
		version, err := inspectDatabaseFile(path)
		if err != nil {
			return nil, err
		}
		if version != 0 && version != 1 {
			return nil, fmt.Errorf("legacy runtime does not support schema version %d", version)
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = file.Close(); err != nil {
			return nil, err
		}
	}
	dsn, err := sqliteFileDSN(path, "rwc")
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	version, err := databaseVersion(db)
	if err != nil || (version != 0 && version != 1) {
		db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("legacy runtime does not support schema version %d", version)
	}
	if path != ":memory:" {
		if err = os.Chmod(path, 0600); err != nil {
			db.Close()
			return nil, err
		}
	}
	s := &Store{db}
	_, err = db.Exec(`PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;
 CREATE TABLE IF NOT EXISTS raw_deliveries(id TEXT PRIMARY KEY,event TEXT NOT NULL,payload BLOB NOT NULL,received_at TEXT NOT NULL,processed INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY);
 CREATE TABLE IF NOT EXISTS work_items(id TEXT PRIMARY KEY,repository_id INTEGER NOT NULL,issue INTEGER NOT NULL,data BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS issue_claims(repository_id INTEGER NOT NULL,issue INTEGER NOT NULL,work_item_id TEXT NOT NULL,PRIMARY KEY(repository_id,issue));
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
	return s.ClaimRecords(ctx, w, nil)
}
func (s *Store) ClaimRecords(ctx context.Context, w domain.WorkItem, records map[string]any) error {
	if w.ID == "" || w.RepositoryID <= 0 || w.Issue <= 0 || w.ScopeRevision < 1 || w.PolicyRevision < 1 || w.Phase != domain.Ready {
		return errors.New("invalid work item")
	}
	initializeClock(&w)
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO issue_claims VALUES(?,?,?)", w.RepositoryID, w.Issue, w.ID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO work_items VALUES(?,?,?,?)", w.ID, w.RepositoryID, w.Issue, b); err != nil {
		return err
	}
	for kind, value := range records {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?)", kind, w.ID, w.ID, data); err != nil {
			return err
		}
	}
	return tx.Commit()
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
	if err = s.applyTx(ctx, tx, e); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) applyTx(ctx context.Context, tx *sql.Tx, e domain.Event) error {
	if e.ObservedAt.IsZero() {
		e.ObservedAt = time.Now().UTC()
	}
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
		return nil
	}
	w, err := read(ctx, tx, e.WorkItemID)
	if err != nil {
		return err
	}
	previousPhase := w.Phase
	w, ops, err := domain.Apply(w, e)
	if err != nil {
		return err
	}
	advanceClock(&w, previousPhase, e.ObservedAt)
	if len(ops) > 0 {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM operation_outbox WHERE work_item_id=? AND state IN ('sending','unknown')", w.ID).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return errors.New("previous operation requires reconciliation")
		}
		if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state='canceled' WHERE work_item_id=? AND state='pending'", w.ID); err != nil {
			return err
		}
	}
	if w.Phase == domain.Canceled {
		if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state='canceled' WHERE work_item_id=? AND state='pending'", w.ID); err != nil {
			return err
		}
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
	return nil
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
	if o.Kind == "implement" || o.Kind == "verify" {
		ready, err := dependenciesReady(ctx, tx, w.ID)
		if err != nil {
			return false, err
		}
		if !ready {
			return false, nil
		}
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
	err := s.DB.QueryRowContext(ctx, `INSERT INTO resource_leases VALUES(?,?,?,1) ON CONFLICT(resource) DO UPDATE SET owner=excluded.owner,expires=excluded.expires,generation=resource_leases.generation+1 WHERE resource_leases.owner='' OR (resource_leases.owner=excluded.owner AND resource_leases.expires>?) RETURNING generation`, resource, owner, time.Now().Add(ttl).UnixNano(), time.Now().UnixNano()).Scan(&generation)
	return generation, err
}
func (s *Store) Release(ctx context.Context, resource, owner string, generation int64) error {
	r, err := s.DB.ExecContext(ctx, "UPDATE resource_leases SET owner='',expires=0 WHERE resource=? AND owner=? AND generation=?", resource, owner, generation)
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

// ClaimNext transfers an issue claim only after its preceding partial work merged.
// This explicit path prevents the pool from reclaiming canceled or split parents.
func (s *Store) ClaimNext(ctx context.Context, previous string, w domain.WorkItem) error {
	return s.ClaimNextRecords(ctx, previous, w, nil)
}
func (s *Store) ClaimNextRecords(ctx context.Context, previous string, w domain.WorkItem, records map[string]any) error {
	return s.claimSuccessor(ctx, previous, w, records, "", nil, nil)
}

// ClaimFinalRepair atomically transfers a terminal issue claim and its separate
// final-acceptance lifecycle. It cannot reopen or alter the merged PR history.
func (s *Store) ClaimFinalRepair(ctx context.Context, previous string, w domain.WorkItem, records map[string]any, key string, expected, next any) error {
	if key == "" {
		return errors.New("final acceptance key required")
	}
	return s.claimSuccessor(ctx, previous, w, records, key, expected, next)
}
func (s *Store) claimSuccessor(ctx context.Context, previous string, w domain.WorkItem, records map[string]any, finalKey string, expected, next any) error {
	if w.ID == "" || w.ID == previous || w.Phase != domain.Ready || w.ScopeRevision < 1 || w.PolicyRevision < 1 {
		return errors.New("invalid next work item")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	prev, err := read(ctx, tx, previous)
	if err != nil {
		return err
	}
	if (prev.Phase != domain.Merged && !(finalKey != "" && prev.Phase == domain.Split)) || w.RepositoryID != prev.RepositoryID || w.Issue != prev.Issue {
		return errors.New("predecessor must be merged, or explicitly recovering a split, on same issue")
	}
	if finalKey != "" {
		if w.ScopeRevision != prev.ScopeRevision || w.PolicyRevision != prev.PolicyRevision {
			return errors.New("final repair revision changed")
		}
		var actual any
		if err = completionRecord(ctx, tx, "parent_final_state", finalKey, &actual); err != nil {
			return err
		}
		data, err := json.Marshal(expected)
		if err != nil {
			return err
		}
		var want any
		if err = json.Unmarshal(data, &want); err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, want) {
			return errors.New("final acceptance state changed")
		}
		data, err = json.Marshal(next)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE records SET payload=?,work_item_id=? WHERE kind='parent_final_state' AND id=?", data, w.ID, finalKey); err != nil {
			return err
		}
	}
	if finalKey == "" {
		if err = transferFinal(ctx, tx, prev, w); err != nil {
			return err
		}
	}
	initializeClock(&w)
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, "UPDATE issue_claims SET work_item_id=? WHERE repository_id=? AND issue=? AND work_item_id=?", w.ID, w.RepositoryID, w.Issue, previous)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return errors.New("issue claim already transferred")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO work_items VALUES(?,?,?,?)", w.ID, w.RepositoryID, w.Issue, b); err != nil {
		return err
	}
	for kind, value := range records {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?)", kind, w.ID, w.ID, data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RetryUnexecuted is only for trusted adapter reconciliation proving no side effect.
// A timeout alone is not such evidence. The original intent and attempt are retained.
func (s *Store) RetryUnexecuted(ctx context.Context, id, evidence string) error {
	return s.RetryUnexecutedBounded(ctx, id, evidence, 3)
}
func (s *Store) RetryUnexecutedBounded(ctx context.Context, id, evidence string, maximum int) error {
	return s.retryUnexecutedBounded(ctx, id, "", evidence, maximum)
}
func (s *Store) RetrySplitChildBounded(ctx context.Context, id, child, evidence string, maximum int) error {
	if child == "" {
		return errors.New("split child key is required")
	}
	return s.retryUnexecutedBounded(ctx, id, child, evidence, maximum)
}
func (s *Store) retryUnexecutedBounded(ctx context.Context, id, child, evidence string, maximum int) error {
	if evidence == "" {
		return errors.New("reconciliation evidence required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var b []byte
	var state string
	if err = tx.QueryRowContext(ctx, "SELECT payload,state FROM operation_outbox WHERE id=?", id).Scan(&b, &state); err != nil {
		return err
	}
	if state != "unknown" && state != "sending" {
		return errors.New("operation does not require reconciliation")
	}
	var op domain.Operation
	if err = json.Unmarshal(b, &op); err != nil {
		return err
	}
	w, err := read(ctx, tx, op.WorkItemID)
	if err != nil {
		return err
	}
	if op.Kind != "split" && w.Phase == domain.Blocked && w.RecoveryPhase != "" {
		previous := w.Phase
		w.Phase = w.RecoveryPhase
		w.RecoveryPhase = ""
		w.Reason = ""
		advanceClock(&w, previous, time.Now().UTC())
	}
	if !domain.CanDispatch(w, op) {
		return errors.New("operation is no longer permitted")
	}
	if op.Kind == "split" {
		if child == "" {
			return errors.New("split retry requires a confirmed uncreated child key")
		}
		var raw []byte
		if err = tx.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='split_progress' AND id=?", w.ID).Scan(&raw); err != nil {
			return err
		}
		var progress map[string]json.RawMessage
		if err = json.Unmarshal(raw, &progress); err != nil {
			return err
		}
		var children []map[string]json.RawMessage
		if err = json.Unmarshal(progress["children"], &children); err != nil {
			return err
		}
		found := false
		for _, entry := range children {
			var key, status string
			var issue int
			if err = json.Unmarshal(entry["key"], &key); err != nil {
				return err
			}
			if key != child {
				continue
			}
			if found {
				return errors.New("duplicate child key")
			}
			found = true
			if err = json.Unmarshal(entry["state"], &status); err != nil {
				return err
			}
			if err = json.Unmarshal(entry["issue"], &issue); err != nil {
				return err
			}
			if status != "creating" || issue != 0 {
				return errors.New("only an ambiguous, uncreated child can be retried")
			}
			entry["state"] = json.RawMessage(`"pending"`)
		}
		if !found {
			return errors.New("unknown split child")
		}
		progress["children"], err = json.Marshal(children)
		if err != nil {
			return err
		}
		updated, err := json.Marshal(progress)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE records SET payload=? WHERE kind='split_progress' AND id=?", updated, w.ID); err != nil {
			return err
		}
	} else if child != "" {
		return errors.New("child key supplied for a non-split operation")
	}
	if maximum < 1 {
		return errors.New("network retry budget is disabled")
	}
	var count int
	var previous []byte
	retryErr := tx.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='network_retry_count' AND id=?", w.ID).Scan(&previous)
	if retryErr == nil {
		if err = json.Unmarshal(previous, &count); err != nil {
			return err
		}
	} else if !errors.Is(retryErr, sql.ErrNoRows) {
		return retryErr
	}
	if count >= maximum {
		return errors.New("network retry budget exhausted")
	}
	count++
	counter, err := json.Marshal(count)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES('network_retry_count',?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload", w.ID, w.ID, counter); err != nil {
		return err
	}
	proof, err := json.Marshal(map[string]any{"unexecuted_evidence": evidence, "child_key": child, "observed_at": time.Now().UTC()})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state='pending',result=? WHERE id=?", proof, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE id=? AND kind IN ('publication_intent','merge_intent')", id); err != nil {
		return err
	}
	data, err := json.Marshal(w)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE work_items SET data=? WHERE id=?", data, w.ID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?)", "operator_audit", fmt.Sprintf("%s:retry:%d", id, time.Now().UnixNano()), w.ID, proof); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Receive(ctx context.Context, id, event string, payload []byte) error {
	if id == "" || len(id) > 256 || event == "" || len(event) > 128 || len(payload) > 1<<20 || !json.Valid(payload) {
		return errors.New("invalid delivery")
	}
	_, err := s.DB.ExecContext(ctx, "INSERT OR IGNORE INTO raw_deliveries(id,event,payload,received_at) VALUES(?,?,?,?)", id, event, payload, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
func (s *Store) DeliveriesPending(ctx context.Context) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, "SELECT count(*) FROM raw_deliveries WHERE processed=0").Scan(&n)
	return n > 0, err
}
func (s *Store) PendingDeliveryIDs(ctx context.Context) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT id FROM raw_deliveries WHERE processed=0 ORDER BY rowid LIMIT 1000")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (s *Store) AcknowledgeDeliveries(ctx context.Context, ids []string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range ids {
		if _, err = tx.ExecContext(ctx, "UPDATE raw_deliveries SET processed=1 WHERE id=?", id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type ChildRegistration struct {
	Depth          int
	Item           domain.WorkItem
	Dependencies   []string
	Scope          any
	Source         any
	PolicySnapshot any
}

// RegisterChildren makes the local split durable only after every external child
// is reconciled. Replays verify identities rather than duplicating work items.
func (s *Store) RegisterChildren(ctx context.Context, parentID string, children []ChildRegistration) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	parent, err := read(ctx, tx, parentID)
	if err != nil {
		return err
	}
	if parent.Phase != domain.Blocked || parent.Reason != "split_pending" {
		return errors.New("parent is not splitting")
	}
	allIDs := map[string]bool{}
	for _, child := range children {
		allIDs[child.Item.ID] = true
	}
	seenChildren := map[string]bool{}
	seenIssues := map[int]bool{}
	for _, child := range children {
		w := child.Item
		if seenChildren[w.ID] || seenIssues[w.Issue] {
			return errors.New("duplicate child registration")
		}
		for _, dep := range child.Dependencies {
			if dep == w.ID || !allIDs[dep] {
				return errors.New("invalid child dependency reference")
			}
		}
		seenChildren[w.ID] = true
		seenIssues[w.Issue] = true
		if w.ID == "" || w.ID == parentID || w.RepositoryID != parent.RepositoryID || w.Issue < 1 || w.Phase != domain.Ready || w.ScopeRevision < 1 || w.PolicyRevision != parent.PolicyRevision {
			return errors.New("invalid child registration")
		}
		existing, err := read(ctx, tx, w.ID)
		if err == nil {
			w.CreatedAt = existing.CreatedAt
			w.PhaseStartedAt = existing.PhaseStartedAt
			w.MergeObservedAt = existing.MergeObservedAt
			w.PhaseDurations = existing.PhaseDurations
			if !reflect.DeepEqual(existing, w) {
				return errors.New("child identity conflict")
			}
			for kind, value := range map[string]any{"dependencies": child.Dependencies, "scope": child.Scope, "split_parent": parentID, "source": child.Source, "policy_snapshot": child.PolicySnapshot, "split_depth": child.Depth} {
				var actual []byte
				if err = tx.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind=? AND id=?", kind, w.ID).Scan(&actual); err != nil {
					return err
				}
				expected, err := json.Marshal(value)
				if err != nil {
					return err
				}
				var a, b any
				if json.Unmarshal(actual, &a) != nil || json.Unmarshal(expected, &b) != nil || !reflect.DeepEqual(a, b) {
					return errors.New("child registration data changed")
				}
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		initializeClock(&w)
		data, err := json.Marshal(w)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO issue_claims VALUES(?,?,?)", w.RepositoryID, w.Issue, w.ID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO work_items VALUES(?,?,?,?)", w.ID, w.RepositoryID, w.Issue, data); err != nil {
			return err
		}
		for kind, value := range map[string]any{"dependencies": child.Dependencies, "scope": child.Scope, "split_parent": parentID, "source": child.Source, "policy_snapshot": child.PolicySnapshot, "split_depth": child.Depth} {
			data, err = json.Marshal(value)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO records VALUES(?,?,?,?)", kind, w.ID, w.ID, data); err != nil {
				return err
			}
			var prior []byte
			if err = tx.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind=? AND id=?", kind, w.ID).Scan(&prior); err != nil {
				return err
			}
			var before, after any
			if json.Unmarshal(prior, &before) != nil || json.Unmarshal(data, &after) != nil || !reflect.DeepEqual(before, after) {
				return errors.New("child metadata conflicts with prior intent")
			}
		}
	}
	return tx.Commit()
}

func dependenciesReady(ctx context.Context, q queryer, id string) (bool, error) {
	var data []byte
	err := q.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='dependencies' AND id=?", id).Scan(&data)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err == nil {
		var ids []string
		if err = json.Unmarshal(data, &ids); err != nil {
			return false, err
		}
		for _, dep := range ids {
			w, err := read(ctx, q, dep)
			if err != nil {
				return false, err
			}
			ok, err := dependencyComplete(ctx, q, w, map[string]bool{})
			if err != nil || !ok {
				return ok, err
			}
		}
	}
	err = q.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='split_parent' AND id=?", id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var parentID string
	if err = json.Unmarshal(data, &parentID); err != nil {
		return false, err
	}
	parent, err := read(ctx, q, parentID)
	if err != nil {
		return false, err
	}
	return parent.Phase == domain.Split, nil
}
func (s *Store) DependenciesReady(ctx context.Context, id string) (bool, error) {
	return dependenciesReady(ctx, s.DB, id)
}

func (s *Store) SetWorkspace(ctx context.Context, id, branch, path string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	w, err := read(ctx, tx, id)
	if err != nil {
		return err
	}
	if w.Worktree != "" && (w.Worktree != path || w.Branch != branch) {
		return errors.New("workspace already assigned")
	}
	w.Worktree = path
	w.Branch = branch
	data, err := json.Marshal(w)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE work_items SET data=? WHERE id=?", data, id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) Records(ctx context.Context, kind, workItem string) ([]json.RawMessage, error) {
	query := "SELECT payload FROM records WHERE kind=? ORDER BY rowid"
	args := []any{kind}
	if workItem != "" {
		query = "SELECT payload FROM records WHERE kind=? AND work_item_id=? ORDER BY rowid"
		args = append(args, workItem)
	}
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		out = append(out, json.RawMessage(raw))
	}
	return out, rows.Err()
}
func (s *Store) GetInto(ctx context.Context, id string, out *domain.WorkItem) error {
	w, err := s.Get(ctx, id)
	if err == nil {
		*out = w
	}
	return err
}

// Complete commits the observed operation result, state transition, and next intent
// together. A crash cannot leave a done operation with its next event missing.
func (s *Store) Complete(ctx context.Context, id string, result any, e domain.Event) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owner, state string
	if err = tx.QueryRowContext(ctx, "SELECT work_item_id,state FROM operation_outbox WHERE id=?", id).Scan(&owner, &state); err != nil {
		return err
	}
	if owner != e.WorkItemID || (state != "sending" && state != "unknown" && state != "done") {
		return errors.New("operation completion identity/state invalid")
	}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state='done',result=? WHERE id=?", data, id); err != nil {
		return err
	}
	if err = s.applyTx(ctx, tx, e); err != nil {
		return err
	}
	return tx.Commit()
}

// Adopt is an explicit operator revision change. Running/unknown effects must be
// reconciled first, and historical scope, counters, and evidence remain intact.
func (s *Store) Adopt(ctx context.Context, id string, scopeRevision, policyRevision int, records map[string]any) error {
	if records == nil {
		return errors.New("adoption requires scope records")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	w, err := read(ctx, tx, id)
	if err != nil {
		return err
	}
	if w.Terminal() || scopeRevision != w.ScopeRevision+1 || policyRevision < w.PolicyRevision {
		return errors.New("invalid adoption revision/state")
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM operation_outbox WHERE work_item_id=? AND state IN ('sending','unknown')", id).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return errors.New("reconcile sent operations before adoption")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records(kind,id,work_item_id,payload) SELECT 'scope_history',?,work_item_id,payload FROM records WHERE kind='scope' AND id=?", fmt.Sprintf("%s:%d", id, w.ScopeRevision), id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO records SELECT 'correction_history',?,work_item_id,payload FROM records WHERE kind='correction' AND id=?", fmt.Sprintf("%s:scope%d", id, w.ScopeRevision), id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='correction' AND id=?", id); err != nil {
		return err
	}
	next := w
	next.ScopeRevision = scopeRevision
	next.PolicyRevision = policyRevision
	if err = transferFinal(ctx, tx, w, next); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO records SELECT 'final_failure_history',?,work_item_id,payload FROM records WHERE kind='final_failure' AND id=?", fmt.Sprintf("%s:scope%d", id, w.ScopeRevision), id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='final_failure' AND id=?", id); err != nil {
		return err
	}
	w.ScopeRevision = scopeRevision
	w.PolicyRevision = policyRevision
	previousPhase := w.Phase
	w.Phase = domain.Ready
	advanceClock(&w, previousPhase, time.Now().UTC())
	w.Candidate = ""
	w.Reason = ""
	w.RecoveryPhase = ""
	w.Version++
	data, err := json.Marshal(w)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE work_items SET data=? WHERE id=?", data, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state='canceled' WHERE work_item_id=? AND state='pending'", id); err != nil {
		return err
	}
	records["requires_revision_commit"] = true
	for kind, value := range records {
		data, err = json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload", kind, id, id, data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// EventRecords commits operator intent and its associated state transition together.
var ErrEventPreconditions = errors.New("event record preconditions changed")

func (s *Store) EventRecords(ctx context.Context, e domain.Event, records map[string]map[string]any) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	w, err := read(ctx, tx, e.WorkItemID)
	if err != nil {
		return err
	}
	if (e.Candidate != "" || e.Kind == "start_assessment") && (w.Candidate != e.Candidate || w.ScopeRevision != e.ScopeRevision || w.PolicyRevision != e.PolicyRevision || w.Attempt != e.Attempt) {
		return ErrEventPreconditions
	}

	for kind, entries := range records {
		for id, value := range entries {
			data, err := json.Marshal(value)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload", kind, id, e.WorkItemID, data); err != nil {
				return err
			}
		}
	}
	if err = s.applyTx(ctx, tx, e); err != nil {
		return err
	}
	return tx.Commit()
}

// RestoreReconciliation reopens only an existing agent operation with a persisted
// run. The adapter must read that run's history; Execute refuses its duplicate ID.
func (s *Store) RestoreReconciliation(ctx context.Context, id, operation, reason string) error {
	if reason == "" {
		return errors.New("reconciliation reason required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	w, err := read(ctx, tx, id)
	if err != nil {
		return err
	}
	if w.Phase != domain.Blocked || (w.RecoveryPhase != domain.Implementing && w.RecoveryPhase != domain.Verifying) {
		return errors.New("work item is not awaiting agent reconciliation")
	}
	var data []byte
	var state string
	if err = tx.QueryRowContext(ctx, "SELECT payload,state FROM operation_outbox WHERE id=?", operation).Scan(&data, &state); err != nil {
		return err
	}
	var op domain.Operation
	if err = json.Unmarshal(data, &op); err != nil {
		return err
	}
	if op.WorkItemID != id || op.Attempt != w.Attempt || op.ScopeRevision != w.ScopeRevision || op.PolicyRevision != w.PolicyRevision || op.Candidate != w.Candidate || (op.Kind != "implement" && op.Kind != "verify" && op.Kind != "propose_split") {
		return errors.New("operation no longer matches work item")
	}
	if state != "done" && state != "unknown" {
		return errors.New("operation is not awaiting reconciliation")
	}
	if err = tx.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='agent_run' AND id=?", operation).Scan(&data); err != nil {
		return err
	}
	var run struct {
		ID       string `json:"id"`
		ThreadID string `json:"thread_id"`
	}
	if err = json.Unmarshal(data, &run); err != nil {
		return err
	}
	if run.ID != operation || run.ThreadID == "" {
		return errors.New("saved run identity missing")
	}
	previousPhase := w.Phase
	w.Phase = w.RecoveryPhase
	advanceClock(&w, previousPhase, time.Now().UTC())
	w.RecoveryPhase = ""
	w.Reason = reason
	w.Version++
	data, err = json.Marshal(w)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE work_items SET data=? WHERE id=?", data, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state='sending' WHERE id=?", operation); err != nil {
		return err
	}
	return tx.Commit()
}
func initializeClock(w *domain.WorkItem) {
	if w.CreatedAt.IsZero() {
		w.CreatedAt = time.Now().UTC()
	}
	if w.PhaseStartedAt.IsZero() {
		w.PhaseStartedAt = w.CreatedAt
	}
}
func advanceClock(w *domain.WorkItem, previous domain.Phase, now time.Time) {
	if w.Phase == previous {
		return
	}
	if !w.PhaseStartedAt.IsZero() && !now.Before(w.PhaseStartedAt) {
		if w.PhaseDurations == nil {
			w.PhaseDurations = map[string]int64{}
		}
		w.PhaseDurations[string(previous)] += now.Sub(w.PhaseStartedAt).Nanoseconds()
	}
	w.PhaseStartedAt = now
	if w.Phase == domain.Merged && w.MergeObservedAt.IsZero() {
		w.MergeObservedAt = now
	}
}

// EnsureRecord pins an immutable policy/evidence snapshot without permitting a
// second writer to reuse its identifier for different content.
func (s *Store) EnsureRecord(ctx context.Context, kind, id string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO records VALUES(?,?,'',?)", kind, id, data); err != nil {
		return err
	}
	var previous []byte
	if err = tx.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind=? AND id=?", kind, id).Scan(&previous); err != nil {
		return err
	}
	var a, b any
	if json.Unmarshal(previous, &a) != nil || json.Unmarshal(data, &b) != nil || !reflect.DeepEqual(a, b) {
		return errors.New("immutable record identifier reused for different content")
	}
	return tx.Commit()
}

// ReturnUnsent preserves an intent held before any external request. It is not an
// API-timeout recovery path; only trusted pre-send guard failures may call it.
func (s *Store) ReturnUnsent(ctx context.Context, id, reason string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var data []byte
	var state string
	if err = tx.QueryRowContext(ctx, "SELECT payload,state FROM operation_outbox WHERE id=?", id).Scan(&data, &state); err != nil {
		return err
	}
	if state != "sending" && state != "unknown" {
		return errors.New("operation not dispatched")
	}
	var op domain.Operation
	if err = json.Unmarshal(data, &op); err != nil {
		return err
	}
	w, err := read(ctx, tx, op.WorkItemID)
	if err != nil {
		return err
	}
	next := "pending"
	if w.Terminal() || w.Candidate != op.Candidate || w.ScopeRevision != op.ScopeRevision || w.PolicyRevision != op.PolicyRevision {
		next = "canceled"
	}
	result, err := json.Marshal(map[string]string{"unsent_reason": reason})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE operation_outbox SET state=?,result=? WHERE id=?", next, result, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE id=? AND kind IN ('publication_intent','merge_intent')", id); err != nil {
		return err
	}
	return tx.Commit()
}
