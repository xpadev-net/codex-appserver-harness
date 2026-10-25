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
	"strings"
	"testing"
)

func legacyKernelImportFixture(t *testing.T) (*Store, LegacyKernelImportRequest) {
	t.Helper()
	s, source, snapshot := legacyBackupFixture(t)
	backup, err := BackupLegacyDatabase(context.Background(), source, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	if err := os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	return s, LegacyKernelImportRequest{Key: "offline-schema-fixture", Source: snapshot, DestinationRoot: filepath.Join(parent, "imported"), Backup: backup}
}

func openLegacyImportDB(t *testing.T, root, mode string) *sql.DB {
	t.Helper()
	dsn, err := sqliteFileDSN(filepath.Join(root, "state.db"), mode)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLegacyKernelImportPreservesSourceAndHistoryButNeverAdmitsRuntime(t *testing.T) {
	s, request := legacyKernelImportFixture(t)
	ctx := context.Background()
	sourceBytes := backupFileBytes(t, request.Source)
	saved, err := PrepareLegacyKernelImport(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Value.From != 1 || saved.Value.To != 2 || saved.Value.State != "schema_prepared" || saved.Value.Request != request {
		t.Fatal("schema preparation is not bound to the original request")
	}
	if !bytes.Equal(sourceBytes, backupFileBytes(t, request.Source)) || !bytes.Equal(sourceBytes, backupFileBytes(t, filepath.Join(request.DestinationRoot, "legacy-source.db"))) {
		t.Fatal("original snapshot/archive bytes changed")
	}
	inspected, err := InspectLegacyKernelImport(ctx, request.DestinationRoot)
	if err != nil || !reflect.DeepEqual(inspected, saved) {
		t.Fatal("offline inspection failed", err)
	}
	db := openLegacyImportDB(t, request.DestinationRoot, "ro")
	copy := &Store{DB: db}
	want, err := s.Get(ctx, "wi")
	if err != nil {
		t.Fatal(err)
	}
	got, err := copy.Get(ctx, "wi")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("legacy WorkItem changed", err)
	}
	wantOps, err := s.Operations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gotOps, err := copy.Operations(ctx)
	if err != nil || !reflect.DeepEqual(gotOps, wantOps) || len(gotOps) != 1 || gotOps[0].State != "unknown" {
		t.Fatal("legacy unknown outbox changed", err)
	}
	gotRecords := legacyBackupRecords(t, db)
	for key := range gotRecords {
		if strings.HasPrefix(key, "legacy_kernel_import:") || strings.HasPrefix(key, "legacy_kernel_import_history:") {
			delete(gotRecords, key)
		}
	}
	if !reflect.DeepEqual(gotRecords, legacyBackupRecords(t, s.DB)) {
		t.Fatal("opaque thread/lineage/budget/final history changed")
	}
	var payload []byte
	if err := db.QueryRow("SELECT payload FROM future_audit WHERE id='unrecognized-history'").Scan(&payload); err != nil || !bytes.Equal(payload, []byte{0, 1, 2, 255}) {
		t.Fatal("unrecognized legacy table lost", err)
	}
	var versions string
	if err := db.QueryRow("SELECT group_concat(version,',') FROM (SELECT version FROM schema_migrations ORDER BY version)").Scan(&versions); err != nil || versions != "1,2" {
		t.Fatal("version history was replaced", versions, err)
	}
	for _, table := range []string{"kernel_waves", "kernel_phases", "kernel_tasks", "kernel_sessions", "kernel_dispatch_intents", "kernel_budget_ledger"} {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("schema import inferred runtime work or budget", table, count, err)
		}
	}
	before := backupFileBytes(t, filepath.Join(request.DestinationRoot, "state.db"))
	for _, opener := range []func(string) (*KernelStore, error){OpenKernel, OpenExistingKernel} {
		if k, err := opener(filepath.Join(request.DestinationRoot, "state.db")); !errors.Is(err, ErrLegacyMigrationPending) {
			if k != nil {
				k.Close()
			}
			t.Fatal("schema-only import granted runtime admission", err)
		}
	}
	if !bytes.Equal(before, backupFileBytes(t, filepath.Join(request.DestinationRoot, "state.db"))) {
		t.Fatal("rejected runtime opener modified imported data")
	}
}

func TestLegacyKernelImportReplayUsesStableReceiptAndRetainedArchive(t *testing.T) {
	_, request := legacyKernelImportFixture(t)
	ctx := context.Background()
	first, err := PrepareLegacyKernelImport(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	before := backupFileBytes(t, filepath.Join(request.DestinationRoot, "state.db"))
	if err := os.Remove(request.Source); err != nil {
		t.Fatal(err)
	}
	again, err := PrepareLegacyKernelImport(ctx, request)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("replay regenerated the migration or required its old external path", err)
	}
	if !bytes.Equal(before, backupFileBytes(t, filepath.Join(request.DestinationRoot, "state.db"))) {
		t.Fatal("replay changed database/history")
	}
	wrong := request
	wrong.Key = "another-migration"
	if _, err := PrepareLegacyKernelImport(ctx, wrong); err == nil {
		t.Fatal("another key adopted an existing root")
	}
	archive := filepath.Join(request.DestinationRoot, "legacy-source.db")
	if err := os.WriteFile(archive, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareLegacyKernelImport(ctx, request); err == nil {
		t.Fatal("lost original archive was silently repaired")
	}
}

func TestLegacyKernelImportReplayRejectsReplacedRootType(t *testing.T) {
	for _, mode := range []string{"file", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			_, request := legacyKernelImportFixture(t)
			if _, err := PrepareLegacyKernelImport(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			retained := request.DestinationRoot + ".retained"
			if err := os.Rename(request.DestinationRoot, retained); err != nil {
				t.Fatal(err)
			}
			before := backupFileBytes(t, filepath.Join(retained, "state.db"))
			if mode == "file" {
				if err := os.WriteFile(request.DestinationRoot, []byte("other host file"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Symlink(retained, request.DestinationRoot); err != nil {
				t.Fatal(err)
			}
			if _, err := PrepareLegacyKernelImport(context.Background(), request); err == nil {
				t.Fatal("replaced root adopted as replay")
			}
			if !bytes.Equal(before, backupFileBytes(t, filepath.Join(retained, "state.db"))) {
				t.Fatal("rejected replay changed retained database")
			}
			if mode == "file" && string(backupFileBytes(t, request.DestinationRoot)) != "other host file" {
				t.Fatal("conflicting file replaced")
			}
		})
	}
}

func TestLegacyKernelImportFailuresDoNotPublishPartialRootAndRetryIsSafe(t *testing.T) {
	for _, point := range []string{"before_schema", "schema_applied", "before_root_publish"} {
		t.Run(point, func(t *testing.T) {
			_, request := legacyKernelImportFixture(t)
			ctx := context.Background()
			before := backupFileBytes(t, request.Source)
			sentinel := errors.New("injected import failure")
			_, err := prepareLegacyKernelImport(ctx, request, func(at string) error {
				if at == point {
					return sentinel
				}
				return nil
			})
			if !errors.Is(err, sentinel) {
				t.Fatal("failure injection was not reached", err)
			}
			if _, err := os.Lstat(request.DestinationRoot); !os.IsNotExist(err) {
				t.Fatal("partial root published", err)
			}
			stages, err := filepath.Glob(filepath.Join(filepath.Dir(request.DestinationRoot), ".legacy-kernel-import-*"))
			if err != nil || len(stages) != 0 {
				t.Fatal("scratch root retained", stages, err)
			}
			if !bytes.Equal(before, backupFileBytes(t, request.Source)) {
				t.Fatal("failure changed original backup")
			}
			if _, err := PrepareLegacyKernelImport(ctx, request); err != nil {
				t.Fatal("fresh retry failed", err)
			}
		})
	}
}

func TestLegacyKernelImportUncertainPublicationReconcilesWithoutRepeatingMigration(t *testing.T) {
	_, request := legacyKernelImportFixture(t)
	sentinel := errors.New("directory durability unconfirmed")
	_, err := prepareLegacyKernelImport(context.Background(), request, func(point string) error {
		if point == "root_published" {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	before, err := InspectLegacyKernelImport(context.Background(), request.DestinationRoot)
	if err != nil {
		t.Fatal("completed uncertain root was deleted", err)
	}
	after, err := PrepareLegacyKernelImport(context.Background(), request)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("uncertain publication reran conversion", err)
	}
}

func TestLegacyKernelSchemaStepRollsBackVersionDDLAndDataTogether(t *testing.T) {
	_, request := legacyKernelImportFixture(t)
	copy := kernelPath(t)
	if _, err := BackupLegacyDatabase(context.Background(), request.Source, copy); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("schema transaction interrupted")
	_, err := applyLegacyKernelImport(context.Background(), copy, request, func(point string) error {
		if point == "schema_applied" {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	version, err := inspectDatabaseFileWith(copy, func(db *sql.DB, _ int) error {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE 'kernel_%'").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("rolled-back kernel tables remain")
		}
		return nil
	})
	if err != nil || version != 1 {
		t.Fatal("schema/version rollback failed", version, err)
	}
}

func TestLegacyKernelImportMissingMarkersCannotBecomeOrdinaryNewDB(t *testing.T) {
	_, request := legacyKernelImportFixture(t)
	if _, err := PrepareLegacyKernelImport(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	db := openLegacyImportDB(t, request.DestinationRoot, "rw")
	if _, err := db.Exec("DELETE FROM records WHERE kind IN ('legacy_kernel_import','legacy_kernel_import_history')"); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectLegacyKernelImport(context.Background(), request.DestinationRoot); err == nil {
		t.Fatal("missing migration authority repaired")
	}
	if _, err := PrepareLegacyKernelImport(context.Background(), request); err == nil {
		t.Fatal("existing damaged root reconverted")
	}
	if k, err := OpenExistingKernel(filepath.Join(request.DestinationRoot, "state.db")); !errors.Is(err, ErrLegacyMigrationPending) {
		if k != nil {
			k.Close()
		}
		t.Fatal("lost markers granted runtime", err)
	}
	if err := os.Remove(filepath.Join(request.DestinationRoot, "legacy-source.db")); err != nil {
		t.Fatal(err)
	}
	if k, err := OpenExistingKernel(filepath.Join(request.DestinationRoot, "state.db")); !errors.Is(err, ErrLegacyMigrationPending) {
		if k != nil {
			k.Close()
		}
		t.Fatal("legacy tables alone became a new empty runtime", err)
	}
}

func TestLegacyKernelImportArchiveAlonePreventsFreshInitialization(t *testing.T) {
	_, request := legacyKernelImportFixture(t)
	if _, err := PrepareLegacyKernelImport(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(request.DestinationRoot, "state.db")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if k, err := OpenKernel(path); !errors.Is(err, ErrLegacyMigrationPending) {
		if k != nil {
			k.Close()
		}
		t.Fatal("retained migration archive allowed a new empty runtime", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("rejected initializer recreated the lost database", err)
	}
}

func TestLegacyKernelImportRejectsWrongSnapshotAndCompetingRoot(t *testing.T) {
	for _, mode := range []string{"digest", "size", "companions", "reserved_authority", "existing_root", "publish_race"} {
		t.Run(mode, func(t *testing.T) {
			_, request := legacyKernelImportFixture(t)
			var hook func(string) error
			switch mode {
			case "digest":
				request.Backup.SHA256 = strings.Repeat("b", 64)
			case "size":
				request.Backup.Bytes++
			case "companions":
				if err := os.WriteFile(request.Source+"-wal", []byte("unexpected"), 0600); err != nil {
					t.Fatal(err)
				}
			case "reserved_authority":
				dsn, err := sqliteFileDSN(request.Source, "rw")
				if err != nil {
					t.Fatal(err)
				}
				db, err := sql.Open("sqlite3", dsn)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("INSERT INTO records VALUES('legacy_kernel_import','forged','other','{}')"); err != nil {
					db.Close()
					t.Fatal(err)
				}
				db.Close()
				raw := backupFileBytes(t, request.Source)
				hash := sha256.Sum256(raw)
				request.Backup.Bytes, request.Backup.SHA256 = int64(len(raw)), hex.EncodeToString(hash[:])
			case "existing_root", "publish_race":
				reserve := func() error {
					if err := os.Mkdir(request.DestinationRoot, 0700); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(request.DestinationRoot, "other-host"), []byte("preserved"), 0600)
				}
				if mode == "existing_root" {
					if err := reserve(); err != nil {
						t.Fatal(err)
					}
				} else {
					hook = func(point string) error {
						if point == "before_root_publish" {
							return reserve()
						}
						return nil
					}
				}
			}
			if _, err := prepareLegacyKernelImport(context.Background(), request, hook); err == nil {
				t.Fatal("invalid import accepted")
			}
			if mode == "existing_root" || mode == "publish_race" {
				if string(backupFileBytes(t, filepath.Join(request.DestinationRoot, "other-host"))) != "preserved" {
					t.Fatal("competing root overwritten")
				}
			} else if _, err := os.Lstat(request.DestinationRoot); !os.IsNotExist(err) {
				t.Fatal("invalid input published root", err)
			}
		})
	}
}
