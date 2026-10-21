package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

const KernelSchemaVersion = 2

// KernelStore is opened explicitly on a private, new DB. It never initializes a
// legacy WorkItem database as if it were empty. Legacy migration is offline M.
type KernelStore struct {
	*Store
	Root                    string
	Now                     func() time.Time
	ControllerArtifactBytes int64
}

func OpenKernel(path string) (*KernelStore, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("kernel DB requires an absolute private path")
	}
	root := filepath.Dir(path)
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("kernel root must be private")
	}
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, errors.New("kernel DB must be a regular file")
	}
	inspected, err := inspectDatabaseFileWith(path, func(db *sql.DB, version int) error {
		if version == KernelSchemaVersion {
			return validateKernelSchema(db)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if inspected != 0 && inspected != KernelSchemaVersion {
		return nil, fmt.Errorf("kernel refuses schema %d; explicit offline migration required", inspected)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	success := false
	defer func() {
		if !success {
			_ = db.Close()
		}
	}()
	version, err := databaseVersion(db)
	if err != nil {
		return nil, err
	}
	if version != 0 && version != KernelSchemaVersion {
		return nil, fmt.Errorf("kernel refuses schema %d; explicit offline migration required", version)
	}
	if version == 0 {
		tx, err := db.Begin()
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(kernelSchema); err != nil {
			tx.Rollback()
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	} else {
		if err = validateKernelSchema(db); err != nil {
			return nil, err
		}
	}
	if err = os.Chmod(path, 0600); err != nil {
		return nil, err
	}
	if _, err = db.Exec("PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;"); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Join(root, "objects"), 0700); err != nil {
		return nil, err
	}
	success = true
	return &KernelStore{Store: &Store{DB: db}, Root: root, Now: time.Now, ControllerArtifactBytes: domain.DefaultKernelLimits().ControllerArtifactBytes}, nil
}

type schemaObject struct{ kind, name, table, definition string }

var kernelShapeOnce sync.Once
var kernelShape []schemaObject
var kernelShapeErr error

func schemaObjects(db *sql.DB) ([]schemaObject, error) {
	rows, err := db.Query("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []schemaObject{}
	for rows.Next() {
		var o schemaObject
		var definition sql.NullString
		if err = rows.Scan(&o.kind, &o.name, &o.table, &definition); err != nil {
			return nil, err
		}
		o.definition = normalizeSchemaSQL(definition.String)
		out = append(out, o)
	}
	return out, rows.Err()
}

// Compare the complete declared DDL, including columns, PK/unique constraints,
// indexes and triggers. Unrelated legacy tables remain permissible for M.
func validateKernelSchema(db *sql.DB) error {
	kernelShapeOnce.Do(func() {
		memory, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			kernelShapeErr = err
			return
		}
		defer memory.Close()
		if _, err = memory.Exec(kernelSchema); err != nil {
			kernelShapeErr = err
			return
		}
		kernelShape, kernelShapeErr = schemaObjects(memory)
	})
	if kernelShapeErr != nil {
		return kernelShapeErr
	}
	actual, err := schemaObjects(db)
	if err != nil {
		return err
	}
	tables := map[string]bool{}
	expected := map[string]schemaObject{}
	for _, o := range kernelShape {
		if o.kind == "table" {
			tables[o.name] = true
		}
		expected[o.kind+":"+o.name] = o
	}
	seen := map[string]bool{}
	for _, o := range actual {
		if !tables[o.table] && !strings.HasPrefix(o.table, "kernel_") {
			continue
		}
		key := o.kind + ":" + o.name
		if want, ok := expected[key]; !ok || want != o {
			return errors.New("kernel schema structure differs; restore/reconcile before dispatch")
		}
		seen[key] = true
	}
	if len(seen) != len(expected) {
		return errors.New("incomplete kernel schema; restore/reconcile before dispatch")
	}
	return nil
}
func normalizeSchemaSQL(s string) string {
	var out strings.Builder
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			out.WriteByte(c)
			if c == quote {
				if i+1 < len(s) && s[i+1] == quote {
					out.WriteByte(s[i+1])
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '[':
			quote = ']'
		case ' ', '\t', '\r', '\n':
			continue
		}
		if quote == 0 && c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out.WriteByte(c)
	}
	return out.String()
}
func (k *KernelStore) clock() time.Time {
	if k.Now == nil {
		return time.Now().UTC()
	}
	return k.Now().UTC()
}
func (k *KernelStore) kernelTx(ctx context.Context) (*sql.Tx, error) { return k.DB.BeginTx(ctx, nil) }

const kernelSchema = `
CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
INSERT INTO schema_migrations VALUES(2);
PRAGMA user_version=2;
CREATE TABLE records(kind TEXT NOT NULL,id TEXT NOT NULL,work_item_id TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(kind,id));
CREATE TABLE kernel_waves(id TEXT PRIMARY KEY,source TEXT NOT NULL,source_id TEXT NOT NULL,input_digest TEXT NOT NULL,payload BLOB NOT NULL,UNIQUE(source,source_id));
CREATE TABLE kernel_plans(wave_id TEXT NOT NULL,revision INTEGER NOT NULL,adoption_key TEXT NOT NULL,digest TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(wave_id,revision),UNIQUE(wave_id,adoption_key));
CREATE TABLE kernel_phases(id TEXT PRIMARY KEY,wave_id TEXT NOT NULL,local_id TEXT NOT NULL,created_revision INTEGER NOT NULL,payload BLOB NOT NULL,UNIQUE(wave_id,local_id));
CREATE TABLE kernel_tasks(id TEXT PRIMARY KEY,wave_id TEXT NOT NULL,phase_id TEXT NOT NULL,local_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,UNIQUE(wave_id,local_id));
CREATE TABLE kernel_plan_bindings(wave_id TEXT NOT NULL,revision INTEGER NOT NULL,task_id TEXT NOT NULL,source_result_id TEXT NOT NULL,input_digest TEXT NOT NULL,PRIMARY KEY(wave_id,revision,task_id));
CREATE TABLE kernel_sessions(id TEXT PRIMARY KEY,kind TEXT NOT NULL,task_id TEXT UNIQUE,parent_id TEXT NOT NULL,thread_id TEXT UNIQUE,creating INTEGER NOT NULL DEFAULT 0,active_run_id TEXT NOT NULL DEFAULT '',process_stopped INTEGER NOT NULL DEFAULT 1,payload BLOB NOT NULL);
CREATE TABLE kernel_snapshots(id TEXT PRIMARY KEY,digest TEXT NOT NULL,data BLOB NOT NULL);
CREATE TABLE kernel_run_bindings(run_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,snapshot_id TEXT NOT NULL,thread_id TEXT NOT NULL,state TEXT NOT NULL,process_stopped INTEGER NOT NULL DEFAULT 0,applied INTEGER NOT NULL DEFAULT 0);
CREATE TABLE kernel_rebindings(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL,generation INTEGER NOT NULL,old_thread TEXT NOT NULL,new_thread TEXT NOT NULL,snapshot_id TEXT NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(session_id,generation));
CREATE TABLE kernel_artifacts(id TEXT PRIMARY KEY,reservation_key TEXT NOT NULL UNIQUE,wave_id TEXT NOT NULL,task_id TEXT NOT NULL,state TEXT NOT NULL,bytes INTEGER NOT NULL,digest TEXT NOT NULL,payload BLOB NOT NULL);
CREATE TABLE kernel_artifact_refs(artifact_id TEXT NOT NULL,task_id TEXT NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(artifact_id,task_id,kind));
CREATE TABLE kernel_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,wave_id TEXT NOT NULL,task_id TEXT NOT NULL,kind TEXT NOT NULL,scope_revision INTEGER NOT NULL,plan_revision INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE TABLE kernel_subscriptions(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,wave_id TEXT NOT NULL,scope_revision INTEGER NOT NULL,plan_revision INTEGER NOT NULL,resource TEXT NOT NULL,cursor INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE TABLE kernel_notifications(id TEXT PRIMARY KEY,subscription_id TEXT NOT NULL,event_id TEXT NOT NULL,session_id TEXT NOT NULL,state TEXT NOT NULL,run_id TEXT NOT NULL DEFAULT '',payload BLOB NOT NULL,UNIQUE(subscription_id,event_id));
CREATE TABLE kernel_messages(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,recipient_id TEXT NOT NULL,wave_id TEXT NOT NULL,call_key TEXT NOT NULL UNIQUE,payload BLOB NOT NULL);
CREATE TABLE kernel_mailbox(message_id TEXT NOT NULL,session_id TEXT NOT NULL,state TEXT NOT NULL,run_id TEXT NOT NULL DEFAULT '',PRIMARY KEY(message_id,session_id));
CREATE TABLE kernel_wake_intents(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,run_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,payload BLOB NOT NULL);
CREATE TABLE kernel_dispatch_intents(run_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,wave_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL);
CREATE TABLE kernel_resource_leases(resource TEXT PRIMARY KEY,run_id TEXT NOT NULL,wave_id TEXT NOT NULL,state TEXT NOT NULL,generation INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE TABLE kernel_budget_ledger(wave_id TEXT NOT NULL,operation_key TEXT NOT NULL,kind TEXT NOT NULL,amount INTEGER NOT NULL,unknown INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(wave_id,operation_key,kind));
CREATE TABLE kernel_tool_operations(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,thread_id TEXT NOT NULL,turn_id TEXT NOT NULL,call_id TEXT NOT NULL,name TEXT NOT NULL,argument_digest TEXT NOT NULL,state TEXT NOT NULL,result BLOB,payload BLOB NOT NULL,UNIQUE(session_id,turn_id,call_id));
`
