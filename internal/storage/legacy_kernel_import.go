package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"golang.org/x/sys/unix"
)

var ErrLegacyMigrationPending = errors.New("legacy family, session and configuration migration must finish before runtime admission")

type LegacyKernelImportRequest struct {
	Key             string       `json:"key"`
	Source          string       `json:"source"`
	DestinationRoot string       `json:"destination_root"`
	Backup          LegacyBackup `json:"backup"`
}

type LegacyKernelImportValue struct {
	Request    LegacyKernelImportRequest `json:"request"`
	From       int                       `json:"from"`
	To         int                       `json:"to"`
	State      string                    `json:"state"`
	PreparedAt time.Time                 `json:"prepared_at"`
}

type LegacyKernelImport struct {
	ID     string                  `json:"id"`
	Value  LegacyKernelImportValue `json:"value"`
	Digest string                  `json:"digest"`
}

func validateLegacyKernelRuntimeAdmission(db *sql.DB, path string) error {
	if _, err := os.Lstat(filepath.Join(filepath.Dir(path), "legacy-source.db")); err == nil {
		return ErrLegacyMigrationPending
	} else if !os.IsNotExist(err) {
		return err
	}
	var pending bool
	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('legacy_kernel_import','legacy_kernel_import_history')) OR EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name IN ('work_items','operation_outbox','raw_deliveries','issue_claims','event_inbox','resource_leases'))").Scan(&pending); err != nil {
		return err
	}
	if pending {
		return ErrLegacyMigrationPending
	}
	return nil
}

// PrepareLegacyKernelImport preserves the complete frozen v1 snapshot and its
// tables, then applies the registered schema step in an unpublished private
// root. This is schema preparation only: no Wave/Task/session or execution
// permission is inferred from legacy state. Normal runtime opening stays held.
// The destination must not exist; its parent must be private and same-user.
func PrepareLegacyKernelImport(ctx context.Context, request LegacyKernelImportRequest) (LegacyKernelImport, error) {
	return prepareLegacyKernelImport(ctx, request, nil)
}

func prepareLegacyKernelImport(ctx context.Context, request LegacyKernelImportRequest, checkpoint func(string) error) (LegacyKernelImport, error) {
	var empty LegacyKernelImport
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if request.Key == "" || len(request.Key) > 128 || request.Backup.SchemaVersion != 1 || !validSourceDigest(request.Backup.SHA256) || request.Backup.Bytes < 1 || !filepath.IsAbs(request.Source) || filepath.Clean(request.Source) != request.Source || !filepath.IsAbs(request.DestinationRoot) || filepath.Clean(request.DestinationRoot) != request.DestinationRoot {
		return empty, errors.New("bounded key, canonical paths and authenticated v1 backup descriptor required")
	}
	parent := filepath.Dir(request.DestinationRoot)
	if err := privateBackupPath(filepath.Join(parent, ".import-parent-check")); err != nil {
		return empty, err
	}
	if _, err := os.Lstat(request.DestinationRoot); err == nil {
		saved, err := InspectLegacyKernelImport(ctx, request.DestinationRoot)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(saved.Value.Request, request) {
			return empty, errors.New("existing migration root belongs to another request")
		}
		if err := syncLegacyImportDirectory(parent); err != nil {
			return empty, err
		}
		return saved, nil
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	stage, err := os.MkdirTemp(parent, ".legacy-kernel-import-")
	if err != nil {
		return empty, err
	}
	defer os.RemoveAll(stage)
	archive := filepath.Join(stage, "legacy-source.db")
	_, err = backupLegacyDatabase(ctx, request.Source, filepath.Join(stage, "state.db"), func(point string) error {
		if point == "source_schema" {
			// Only frozen, self-contained snapshots are import inputs. Comparing
			// main-file hashes alone would ignore a newly attached WAL.
			for _, suffix := range []string{"-wal", "-shm", "-journal"} {
				if _, err := os.Lstat(request.Source + suffix); !os.IsNotExist(err) {
					return errors.New("import requires a frozen self-contained backup without companions")
				}
			}
			if err := copyLegacyImportArchive(ctx, request.Source, archive, request.Backup); err != nil {
				return err
			}
		}
		if checkpoint != nil {
			return checkpoint(point)
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	if checkpoint != nil {
		if err := checkpoint("before_schema"); err != nil {
			return empty, err
		}
	}
	saved, err := applyLegacyKernelImport(ctx, filepath.Join(stage, "state.db"), request, checkpoint)
	if err != nil {
		return empty, err
	}
	if err := os.Mkdir(filepath.Join(stage, "objects"), 0700); err != nil {
		return empty, err
	}
	verified, err := inspectLegacyKernelImport(ctx, stage, request.DestinationRoot)
	if err != nil || !reflect.DeepEqual(saved, verified) {
		if err != nil {
			return empty, err
		}
		return empty, errors.New("prepared import failed independent validation")
	}
	stateFile, err := os.OpenFile(filepath.Join(stage, "state.db"), os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return empty, err
	}
	err = errors.Join(stateFile.Sync(), stateFile.Close())
	if err != nil {
		return empty, err
	}
	if err := syncLegacyImportDirectory(stage); err != nil {
		return empty, err
	}
	if checkpoint != nil {
		if err := checkpoint("before_root_publish"); err != nil {
			return empty, err
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, stage, unix.AT_FDCWD, request.DestinationRoot, unix.RENAME_NOREPLACE); err != nil {
		return empty, err
	}
	if checkpoint != nil {
		if err := checkpoint("root_published"); err != nil {
			return empty, err
		}
	}
	if err := syncLegacyImportDirectory(parent); err != nil {
		// A fully validated published root remains for key-bound reconciliation.
		return empty, err
	}
	return saved, nil
}

func copyLegacyImportArchive(ctx context.Context, source, target string, expected LegacyBackup) error {
	input, err := os.OpenFile(source, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil || !privateBackupFile(info) || info.Size() != expected.Bytes {
		return errors.New("snapshot identity or size differs from backup descriptor")
	}
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return err
	}
	defer output.Close()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(output, hash), legacyBackupReader{ctx, input})
	if err != nil {
		return err
	}
	if size != expected.Bytes || hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
		return errors.New("snapshot digest differs from backup descriptor")
	}
	return output.Sync()
}

func legacyKernelSchemaStep() Migration {
	return Migration{From: 1, To: KernelSchemaVersion, Key: "legacy-v1-kernel-v2-schema", Apply: func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, kernelCoreSchema); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations VALUES(2); PRAGMA user_version=2")
		return err
	}}
}

func applyLegacyKernelImport(ctx context.Context, path string, request LegacyKernelImportRequest, checkpoint func(string) error) (LegacyKernelImport, error) {
	var empty LegacyKernelImport
	dsn, err := sqliteFileDSN(path, "rw")
	if err != nil {
		return empty, err
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return empty, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	var reserved bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('legacy_kernel_import','legacy_kernel_import_history'))").Scan(&reserved); err != nil {
		return empty, err
	}
	if reserved {
		return empty, errors.New("source contains reserved migration authority")
	}
	var registry MigrationRegistry
	if err := registry.Register(legacyKernelSchemaStep()); err != nil {
		return empty, err
	}
	steps, err := registry.Steps(1, KernelSchemaVersion)
	if err != nil {
		return empty, err
	}
	for _, step := range steps {
		if err := step.Apply(ctx, tx); err != nil {
			return empty, err
		}
	}
	if checkpoint != nil {
		if err := checkpoint("schema_applied"); err != nil {
			return empty, err
		}
	}
	value := LegacyKernelImportValue{request, 1, KernelSchemaVersion, "schema_prepared", time.Now().UTC()}
	digest, err := contentDigest(value)
	if err != nil {
		return empty, err
	}
	saved := LegacyKernelImport{structureID("legacy-kernel-import-", request.Key, digest), value, digest}
	for _, item := range []struct{ kind, id string }{{"legacy_kernel_import", "schema"}, {"legacy_kernel_import_history", saved.ID}} {
		if err := kernelRecordTx(ctx, tx, item.kind, item.id, request.Key, saved); err != nil {
			return empty, err
		}
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return saved, nil
}

// InspectLegacyKernelImport is an explicit read-only offline port. It does not
// lift the normal runtime hold or change any imported legacy operation state.
func InspectLegacyKernelImport(ctx context.Context, root string) (LegacyKernelImport, error) {
	return inspectLegacyKernelImport(ctx, root, root)
}

func inspectLegacyKernelImport(ctx context.Context, root, expectedRoot string) (LegacyKernelImport, error) {
	var saved LegacyKernelImport
	path := filepath.Join(root, "state.db")
	if err := privateBackupPath(path); err != nil {
		return saved, err
	}
	info, err := os.Lstat(path)
	if err != nil || !privateBackupFile(info) {
		return saved, errors.New("private imported database required")
	}
	version, err := inspectDatabaseFileWith(path, func(db *sql.DB, version int) error {
		if version != KernelSchemaVersion {
			return errors.New("imported kernel schema required")
		}
		if err := validateKernelSchema(db); err != nil {
			return err
		}
		if err := taskEvidenceRecord(ctx, db, "legacy_kernel_import", "schema", &saved); err != nil {
			return err
		}
		digest, err := contentDigest(saved.Value)
		if err != nil {
			return err
		}
		v := saved.Value
		if v.Request.DestinationRoot != expectedRoot || v.Request.Key == "" || len(v.Request.Key) > 128 || v.Request.Backup.SchemaVersion != 1 || v.Request.Backup.Bytes < 1 || saved.Digest != digest || saved.ID != structureID("legacy-kernel-import-", v.Request.Key, digest) || v.From != 1 || v.To != KernelSchemaVersion || v.State != "schema_prepared" || v.PreparedAt.IsZero() || !validSourceDigest(v.Request.Backup.SHA256) {
			return errors.New("import receipt is invalid")
		}
		var history LegacyKernelImport
		if err := taskEvidenceRecord(ctx, db, "legacy_kernel_import_history", saved.ID, &history); err != nil {
			return err
		}
		for _, item := range []struct{ kind, id string }{{"legacy_kernel_import", "schema"}, {"legacy_kernel_import_history", saved.ID}} {
			var owner string
			if err := db.QueryRowContext(ctx, "SELECT work_item_id FROM records WHERE kind=? AND id=?", item.kind, item.id).Scan(&owner); err != nil {
				return err
			}
			if owner != v.Request.Key {
				return errors.New("import authority owner differs")
			}
		}
		if !reflect.DeepEqual(saved, history) {
			return errors.New("import receipt history differs")
		}
		return nil
	})
	if err != nil {
		return saved, err
	}
	if version != KernelSchemaVersion {
		return saved, errors.New("import schema version differs")
	}
	archive := filepath.Join(root, "legacy-source.db")
	file, err := os.OpenFile(archive, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return saved, err
	}
	defer file.Close()
	archiveInfo, err := file.Stat()
	if err != nil || !privateBackupFile(archiveInfo) {
		return saved, errors.New("private source archive required")
	}
	hash := sha256.New()
	size, err := io.Copy(hash, legacyBackupReader{ctx, file})
	if err != nil {
		return saved, err
	}
	if size != saved.Value.Request.Backup.Bytes || hex.EncodeToString(hash.Sum(nil)) != saved.Value.Request.Backup.SHA256 {
		return saved, errors.New("retained source archive differs from original backup")
	}
	return saved, nil
}

func syncLegacyImportDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
