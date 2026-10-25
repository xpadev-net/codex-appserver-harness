package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func legacyBackupFixture(t *testing.T) (*Store, string, string) {
	t.Helper()
	source := kernelPath(t)
	s, err := Open(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err := s.DB.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Claim(ctx, work()); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, domain.Event{ID: "start", WorkItemID: "wi", Kind: "start", ScopeRevision: 1, PolicyRevision: 1}); err != nil {
		t.Fatal(err)
	}
	if started, err := s.BeginDispatch(ctx, "start:implement"); err != nil || !started {
		t.Fatal("fixture dispatch denied", err)
	}
	if err := s.FinishOperation(ctx, "start:implement", "unknown", nil); err != nil {
		t.Fatal(err)
	}
	// Opaque historical payloads must survive byte-for-byte. These fixtures do
	// not claim that the corresponding workflows have been migrated/executed.
	for _, kind := range []string{"role_thread", "agent_run", "split_plan", "continuation", "repair", "budget", "parent_final_acceptance"} {
		if err := s.Put(ctx, kind, kind+"-history", "wi", map[string]any{"id": kind + "-original", "thread_id": "retained-thread", "attempt": 3, "unknown": true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.DB.Exec("CREATE TABLE future_audit(id TEXT PRIMARY KEY,payload BLOB NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("INSERT INTO future_audit VALUES(?,?)", "unrecognized-history", []byte{0, 1, 2, 255}); err != nil {
		t.Fatal(err)
	}
	return s, source, kernelPath(t)
}

func backupFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func legacyBackupRecords(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query("SELECT kind,id,work_item_id,payload FROM records ORDER BY kind,id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := map[string]string{}
	for rows.Next() {
		var kind, id, owner string
		var payload []byte
		if err := rows.Scan(&kind, &id, &owner, &payload); err != nil {
			t.Fatal(err)
		}
		values[kind+":"+id] = owner + ":" + string(payload)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return values
}

func TestLegacyBackupIncludesWALAndPreservesUnknownOperationsAndOpaqueHistory(t *testing.T) {
	s, source, destination := legacyBackupFixture(t)
	ctx := context.Background()
	mainBefore := backupFileBytes(t, source)
	walBefore := backupFileBytes(t, source+"-wal")
	if len(walBefore) == 0 {
		t.Fatal("fixture did not leave committed data in WAL")
	}
	result, err := BackupLegacyDatabase(ctx, source, destination)
	if err != nil {
		t.Fatal(err)
	}
	raw := backupFileBytes(t, destination)
	hash := sha256.Sum256(raw)
	if result.SchemaVersion != 1 || result.Bytes != int64(len(raw)) || result.SHA256 != hex.EncodeToString(hash[:]) {
		t.Fatal("backup descriptor does not authenticate its file", result)
	}
	if !bytes.Equal(mainBefore, backupFileBytes(t, source)) || !bytes.Equal(walBefore, backupFileBytes(t, source+"-wal")) {
		t.Fatal("backup changed source main/WAL bytes")
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("backup permissions changed", err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(destination + suffix); !os.IsNotExist(err) {
			t.Fatal("backup needs a SQLite companion", suffix, err)
		}
	}
	dsn, err := sqliteFileDSN(destination, "ro")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	copy := &Store{DB: db}
	want, err := s.Get(ctx, "wi")
	if err != nil {
		t.Fatal(err)
	}
	got, err := copy.Get(ctx, "wi")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("WorkItem identity/state/history changed", err)
	}
	wantOps, err := s.Operations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotOps, err := copy.Operations(ctx)
	if err != nil || !reflect.DeepEqual(gotOps, wantOps) || len(gotOps) != 1 || gotOps[0].State != "unknown" {
		t.Fatal("unknown outbox changed", err)
	}
	if started, err := copy.BeginDispatch(ctx, "start:implement"); err != nil || started {
		t.Fatal("backup made an unknown operation resendable", err)
	}
	if !reflect.DeepEqual(legacyBackupRecords(t, s.DB), legacyBackupRecords(t, db)) {
		t.Fatal("opaque lineage or thread payloads changed")
	}
	var future []byte
	if err := db.QueryRow("SELECT payload FROM future_audit WHERE id=?", "unrecognized-history").Scan(&future); err != nil || !bytes.Equal(future, []byte{0, 1, 2, 255}) {
		t.Fatal("unrecognized table was omitted", err)
	}
	// A snapshot keeps its original state after the source subsequently changes.
	if err := s.Put(ctx, "later", "after-snapshot", "wi", true); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM records WHERE kind='later'").Scan(&count); err != nil || count != 0 {
		t.Fatal("backup remained attached to source", err)
	}
}

func TestLegacyBackupRejectsUnsupportedOrUnsafeInputsWithoutOverwriting(t *testing.T) {
	for _, mode := range []string{"unknown_version", "empty", "missing", "existing_destination", "source_symlink", "destination_symlink", "source_hardlink", "public_source", "public_directory", "destination_companion", "source_companion", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			s, source, destination := legacyBackupFixture(t)
			ctx := context.Background()
			switch mode {
			case "unknown_version":
				if _, err := s.DB.Exec("UPDATE schema_migrations SET version=99; PRAGMA user_version=99"); err != nil {
					t.Fatal(err)
				}
			case "empty", "missing":
				source = filepath.Join(filepath.Dir(source), "other.db")
				if mode == "empty" {
					if err := os.WriteFile(source, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "existing_destination":
				if err := os.WriteFile(destination, []byte("original backup"), 0600); err != nil {
					t.Fatal(err)
				}
			case "source_symlink":
				link := filepath.Join(filepath.Dir(source), "linked.db")
				if err := os.Symlink(source, link); err != nil {
					t.Fatal(err)
				}
				source = link
			case "destination_symlink":
				if err := os.Symlink(source, destination); err != nil {
					t.Fatal(err)
				}
			case "source_hardlink":
				if err := os.Link(source, filepath.Join(filepath.Dir(source), "alias.db")); err != nil {
					t.Fatal(err)
				}
			case "public_source":
				if err := os.Chmod(source, 0644); err != nil {
					t.Fatal(err)
				}
			case "public_directory":
				if err := os.Chmod(filepath.Dir(destination), 0755); err != nil {
					t.Fatal(err)
				}
			case "destination_companion":
				if err := os.WriteFile(destination+"-wal", []byte("retained companion"), 0600); err != nil {
					t.Fatal(err)
				}
			case "source_companion":
				destination = source + "-wal"
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			before, _ := os.ReadFile(source)
			oldDestination, oldErr := os.ReadFile(destination)
			if _, err := BackupLegacyDatabase(ctx, source, destination); err == nil {
				t.Fatal("unsafe backup admitted")
			}
			after, _ := os.ReadFile(source)
			if !bytes.Equal(before, after) {
				t.Fatal("rejected backup modified source")
			}
			if oldErr == nil {
				if !bytes.Equal(oldDestination, backupFileBytes(t, destination)) {
					t.Fatal("existing destination overwritten")
				}
			} else if _, err := os.Lstat(destination); !os.IsNotExist(err) {
				t.Fatal("rejected backup left a destination", err)
			}
		})
	}
}

func TestLegacyBackupCancellationAfterWritingRemovesPartialAndAllowsFreshRetry(t *testing.T) {
	s, source, destination := legacyBackupFixture(t)
	if _, err := s.DB.Exec("INSERT INTO future_audit VALUES('large',zeroblob(16777216))"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := BackupLegacyDatabase(ctx, source, destination); done <- err }()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
waiting:
	for {
		select {
		case err := <-done:
			t.Fatal("backup completed before mid-copy cancellation", err)
		case <-deadline.C:
			t.Fatal("backup never started writing")
		case <-tick.C:
			if info, err := os.Stat(destination); err == nil && info.Size() > 0 {
				cancel()
				break waiting
			}
		}
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("copy cancellation lost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled backup did not stop")
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(destination + suffix); !os.IsNotExist(err) {
			t.Fatal("partial backup retained", suffix, err)
		}
	}
	if _, err := BackupLegacyDatabase(context.Background(), source, destination); err != nil {
		t.Fatal("fresh retry could not use cleaned destination", err)
	}
}
