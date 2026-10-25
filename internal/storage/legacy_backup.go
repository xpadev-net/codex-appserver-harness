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
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3"
	"golang.org/x/sys/unix"
)

// LegacyBackup describes a self-contained, consistent v1 snapshot. It is not a
// migration, a process-quiescence receipt, or permission to resume dispatch.
// The offline migration host must separately stop/reconcile the controller,
// model/runner processes and unknown external operations before cutover.
type LegacyBackup struct {
	SchemaVersion int    `json:"schema_version"`
	SHA256        string `json:"sha256"`
	Bytes         int64  `json:"bytes"`
}

// BackupLegacyDatabase copies committed SQLite state, including live WAL
// contents, through SQLite's backup API. It never opens the source for writes,
// initializes it, or overwrites an existing destination or companion file.
// The caller supplies cancellation and the deadline for its database size.
func BackupLegacyDatabase(ctx context.Context, source, destination string) (LegacyBackup, error) {
	return backupLegacyDatabase(ctx, source, destination, nil)
}

// A synchronous checkpoint is used by fault-injection tests, never stored or
// exposed to model tools. Production passes nil.
func backupLegacyDatabase(ctx context.Context, source, destination string, checkpoint func(string) error) (LegacyBackup, error) {
	var empty LegacyBackup
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := privateBackupPath(source); err != nil {
		return empty, err
	}
	if err := privateBackupPath(destination); err != nil {
		return empty, err
	}
	unlock, err := lockLegacyBackupSource(source)
	if err != nil {
		return empty, err
	}
	defer unlock()
	if filepath.Dir(destination) != filepath.Dir(source) {
		release, err := lockLegacyBackupSource(destination)
		if err != nil {
			return empty, err
		}
		defer release()
	}
	sourceInfo, err := os.Lstat(source)
	if err != nil {
		return empty, err
	}
	if !privateBackupFile(sourceInfo) || source == destination {
		return empty, errors.New("private same-user regular source and separate destination required")
	}
	if err := vacantLegacyBackupDestination(source, destination); err != nil {
		return empty, err
	}
	// Inspect and copy through one retained read-only connection. Host path
	// replacement is serialized by controller.lock; bookends also reject
	// unexpected/non-cooperating one-way replacement before reading schema.
	if checkpoint != nil {
		if err := checkpoint("source_open"); err != nil {
			return empty, err
		}
	}
	if err := checkLegacyBackupSource(source, sourceInfo); err != nil {
		return empty, err
	}
	sourceDSN, err := sqliteFileDSN(source, "ro")
	if err != nil {
		return empty, err
	}
	sourceDB, err := sql.Open("sqlite3", sourceDSN)
	if err != nil {
		return empty, err
	}
	defer sourceDB.Close()
	sourceConn, err := sourceDB.Conn(ctx)
	if err != nil {
		return empty, err
	}
	defer sourceConn.Close()
	if err := checkLegacyBackupSource(source, sourceInfo); err != nil {
		return empty, err
	}
	if checkpoint != nil {
		if err := checkpoint("source_schema"); err != nil {
			return empty, err
		}
	}
	if err := checkLegacyBackupSource(source, sourceInfo); err != nil {
		return empty, err
	}
	var header, count, minimum, maximum int
	if err := sourceConn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&header); err != nil {
		return empty, err
	}
	if err := sourceConn.QueryRowContext(ctx, "SELECT count(*),coalesce(min(version),0),coalesce(max(version),0) FROM schema_migrations").Scan(&count, &minimum, &maximum); err != nil {
		return empty, err
	}
	if err := checkLegacyBackupSource(source, sourceInfo); err != nil {
		return empty, err
	}
	if count != 1 || minimum != 1 || maximum != 1 || header != 0 && header != 1 {
		return empty, errors.New("existing version-one database required for legacy backup")
	}

	// SQLite opens only this unique private staging name. Another host operation
	// may reserve the public destination at any point without being overwritten.
	stage, err := os.MkdirTemp(filepath.Dir(destination), ".legacy-backup-")
	if err != nil {
		return empty, err
	}
	defer os.RemoveAll(stage)
	staged := filepath.Join(stage, "snapshot.db")
	file, err := os.OpenFile(staged, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return empty, err
	}
	defer file.Close()
	owned, err := file.Stat()
	if err != nil {
		return empty, err
	}
	copyCtx := ctx
	if err := copyLegacySQLite(copyCtx, sourceConn, staged, checkpoint); err != nil {
		return empty, err
	}
	current, err := os.Lstat(source)
	if err != nil || !os.SameFile(current, sourceInfo) || !privateBackupFile(current) {
		return empty, errors.New("source identity changed during backup")
	}
	current, err = os.Lstat(staged)
	if err != nil || !os.SameFile(current, owned) || !privateBackupFile(current) {
		return empty, errors.New("staged backup identity changed")
	}
	version, err := inspectDatabaseFileWith(staged, func(db *sql.DB, _ int) error {
		var result string
		if err := db.QueryRowContext(copyCtx, "PRAGMA integrity_check").Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			return errors.New("backup integrity check failed")
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	if version != 1 {
		return empty, errors.New("source schema changed during backup")
	}
	if err := file.Sync(); err != nil {
		return empty, err
	}
	hash := sha256.New()
	size, err := io.Copy(hash, legacyBackupReader{ctx: copyCtx, reader: file})
	if err != nil {
		return empty, err
	}
	if err := copyCtx.Err(); err != nil {
		return empty, err
	}
	if err := vacantLegacyBackupDestination(source, destination); err != nil {
		return empty, err
	}
	if checkpoint != nil {
		if err := checkpoint("publish"); err != nil {
			return empty, err
		}
	}
	// Atomically publish without replacing another host's destination. SQLite
	// never opens the public path, even when publication loses a race.
	if err := unix.Renameat2(unix.AT_FDCWD, staged, unix.AT_FDCWD, destination, unix.RENAME_NOREPLACE); err != nil {
		return empty, err
	}
	directory, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return empty, err
	}
	err = directory.Sync()
	err = errors.Join(err, directory.Close())
	if err != nil {
		// Retain a fully verified snapshot after uncertain publication durability;
		// a retry must inspect it explicitly rather than deleting or overwriting it.
		return empty, err
	}
	return LegacyBackup{SchemaVersion: version, SHA256: hex.EncodeToString(hash.Sum(nil)), Bytes: size}, nil
}

func vacantLegacyBackupDestination(source, destination string) error {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if suffix != "" && destination == source+suffix {
			return errors.New("backup cannot replace a source companion")
		}
		if _, err := os.Lstat(destination + suffix); !os.IsNotExist(err) {
			if err != nil {
				return err
			}
			return errors.New("backup destination or SQLite companion already exists")
		}
	}
	return nil
}

// All offline host operations that replace a legacy source path must hold the
// same controller.lock as the legacy controller. The lock does not prove that
// its model/runner processes or unknown remote operations have been reconciled.
func lockLegacyBackupSource(source string) (func(), error) {
	lock, err := os.OpenFile(filepath.Join(filepath.Dir(source), "controller.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := lock.Stat()
	if err != nil {
		lock.Close()
		return nil, err
	}
	if !privateBackupFile(info) {
		lock.Close()
		return nil, errors.New("private same-user controller lock required")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("stop the legacy controller before taking an offline snapshot")
	}
	return func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); _ = lock.Close() }, nil
}

type legacyBackupReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r legacyBackupReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func privateBackupPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("canonical absolute database path required")
	}
	parent := filepath.Dir(path)
	actual, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	info, err := os.Stat(parent)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if actual != parent || !info.IsDir() || info.Mode().Perm()&0077 != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("private canonical same-user database directory required")
	}
	return nil
}

func privateBackupFile(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return info.Mode().IsRegular() && info.Mode().Perm()&0077 == 0 && ok && stat.Uid == uint32(os.Geteuid()) && stat.Nlink == 1
}

func checkLegacyBackupSource(path string, expected os.FileInfo) error {
	current, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !os.SameFile(current, expected) || !privateBackupFile(current) {
		return errors.New("source identity changed during backup")
	}
	return nil
}

func copyLegacySQLite(ctx context.Context, sourceConn *sql.Conn, destination string, checkpoint func(string) error) error {
	destinationDSN, err := sqliteFileDSN(destination, "rw")
	if err != nil {
		return err
	}
	dst, err := sql.Open("sqlite3", destinationDSN)
	if err != nil {
		return err
	}
	defer dst.Close()
	destinationConn, err := dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer destinationConn.Close()
	err = destinationConn.Raw(func(destinationDriver any) error {
		return sourceConn.Raw(func(sourceDriver any) (result error) {
			backup, err := destinationDriver.(*sqlite3.SQLiteConn).Backup("main", sourceDriver.(*sqlite3.SQLiteConn), "main")
			if err != nil {
				return err
			}
			defer func() { result = errors.Join(result, backup.Finish()) }()
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				done, err := backup.Step(64)
				if err == nil && checkpoint != nil {
					if err := checkpoint("copy"); err != nil {
						return err
					}
				}

				if err != nil || done {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond):
				}
			}
		})
	})
	if err != nil {
		return err
	}
	// Produce one independent file rather than another WAL-dependent backup.
	var mode string
	if err := destinationConn.QueryRowContext(ctx, "PRAGMA journal_mode=DELETE").Scan(&mode); err != nil {
		return err
	}
	if mode != "delete" {
		return errors.New("self-contained backup journal mode required")
	}
	return nil
}
