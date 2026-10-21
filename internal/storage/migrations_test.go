package storage

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func kernelPath(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "state.db")
}
func TestKernelNewDBAndVersionBoundaries(t *testing.T) {
	path := kernelPath(t)
	kernel, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = kernel.Put(context.Background(), "fixture", "id", "session", map[string]string{"value": "retained"}); err != nil {
		t.Fatal(err)
	}
	if err = kernel.Close(); err != nil {
		t.Fatal(err)
	}
	kernel, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]string
	if err = kernel.Record(context.Background(), "fixture", "id", &record); err != nil || record["value"] != "retained" {
		t.Fatal(record, err)
	}
	kernel.Close()
	if legacy, err := Open(path); err == nil {
		legacy.Close()
		t.Fatal("legacy runtime opened new kernel schema")
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	var tables int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='work_items'").Scan(&tables); err != nil || tables != 0 {
		t.Fatal("legacy DDL touched kernel database", tables, err)
	}
	db.Close()
}
func TestKernelDoesNotInitializeLegacyOrUnknownDB(t *testing.T) {
	for _, kind := range []string{"legacy", "unknown", "unversioned", "incomplete"} {
		t.Run(kind, func(t *testing.T) {
			path := kernelPath(t)
			if kind == "legacy" {
				s, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Put(context.Background(), "legacy", "evidence", "old", map[string]string{"state": "unknown external operation"}); err != nil {
					t.Fatal(err)
				}
				s.Close()
			} else {
				db, err := sql.Open("sqlite3", path)
				if err != nil {
					t.Fatal(err)
				}
				ddl := "CREATE TABLE unrecognized(payload BLOB); INSERT INTO unrecognized VALUES('retain');"
				if kind == "unknown" {
					ddl += "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY); INSERT INTO schema_migrations VALUES(999); PRAGMA user_version=999;"
				}
				if kind == "incomplete" {
					ddl += "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY); INSERT INTO schema_migrations VALUES(2); PRAGMA user_version=2;"
				}
				if _, err = db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if s, err := OpenKernel(path); err == nil {
				s.Close()
				t.Fatal("unsupported DB accepted", kind)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("existing DB contents changed", err)
			}
			if _, err = os.Stat(filepath.Join(filepath.Dir(path), "objects")); !os.IsNotExist(err) {
				t.Fatal("failed open created artifact state", err)
			}
		})
	}
}
func TestLegacyRuntimeRejectsUnknownSchemaBeforeDDL(t *testing.T) {
	path := kernelPath(t)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY); INSERT INTO schema_migrations VALUES(99);"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if s, err := Open(path); err == nil {
		s.Close()
		t.Fatal("unknown schema accepted")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("unknown schema modified")
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0644 {
		t.Fatal("rejected DB permissions changed", info, err)
	}
}

func TestKernelRejectsDamagedOtherwiseValidSchemas(t *testing.T) {
	for _, ddl := range []string{"DROP TABLE kernel_snapshots", "ALTER TABLE records DROP COLUMN payload", "CREATE TRIGGER unexpected AFTER INSERT ON kernel_waves BEGIN DELETE FROM kernel_tasks; END"} {
		t.Run(ddl, func(t *testing.T) {
			path := kernelPath(t)
			store, err := OpenKernel(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.DB.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			store.Close()
			if err = os.Chmod(path, 0644); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if reopened, err := OpenKernel(path); err == nil {
				reopened.Close()
				t.Fatal("damaged schema accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("damaged schema modified", err)
			}
			if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0644 {
				t.Fatal("damaged DB permissions changed", info, err)
			}
		})
	}
}
func TestMigrationRegistryRejectsConflictsAndDoesNotRunOnOpen(t *testing.T) {
	ran := false
	apply := func(context.Context, *sql.Tx) error { ran = true; return nil }
	var registry MigrationRegistry
	if err := registry.Register(Migration{From: 1, To: 2, Key: "legacy-to-wave", Apply: apply}); err != nil {
		t.Fatal(err)
	}
	for _, m := range []Migration{{From: 1, To: 2, Key: "duplicate-version", Apply: apply}, {From: 2, To: 3, Key: "legacy-to-wave", Apply: apply}, {From: 1, To: 3, Key: "jump", Apply: apply}} {
		if err := registry.Register(m); err == nil {
			t.Fatal("migration conflict accepted")
		}
	}
	if steps, err := registry.Steps(1, 2); err != nil || len(steps) != 1 || ran {
		t.Fatal(steps, ran, err)
	}
	if _, err := registry.Steps(1, 3); err == nil {
		t.Fatal("unknown migration planned")
	}
	s, err := OpenKernel(kernelPath(t))
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if ran {
		t.Fatal("opening a DB ran a legacy migration")
	}
}
