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
func BackupLegacyDatabase(ctx context.Context, source, destination string) (LegacyBackup, error) {
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
	sourceInfo, err := os.Lstat(source)
	if err != nil {
		return empty, err
	}
	if !privateBackupFile(sourceInfo) || source == destination {
		return empty, errors.New("private same-user regular source and separate destination required")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if destination == source+suffix {
			return empty, errors.New("backup cannot replace a source companion")
		}
		if _, err := os.Lstat(destination + suffix); !os.IsNotExist(err) {
			if err != nil {
				return empty, err
			}
			return empty, errors.New("backup destination has existing SQLite companions")
		}
	}
	version, err := inspectDatabaseFile(source)
	if err != nil {
		return empty, err
	}
	if version != 1 {
		return empty, errors.New("existing version-one database required for legacy backup")
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return empty, err
	}
	defer file.Close()
	owned, err := file.Stat()
	if err != nil {
		return empty, err
	}
	complete := false
	defer func() {
		if !complete {
			// Never remove a replacement installed by another host operation.
			if current, err := os.Lstat(destination); err == nil && os.SameFile(current, owned) {
				_ = os.Remove(destination)
			}
		}
	}()
	copyCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := copyLegacySQLite(copyCtx, source, destination); err != nil {
		return empty, err
	}
	current, err := os.Lstat(source)
	if err != nil || !os.SameFile(current, sourceInfo) {
		return empty, errors.New("source identity changed during backup")
	}
	current, err = os.Lstat(destination)
	if err != nil || !os.SameFile(current, owned) || !privateBackupFile(current) {
		return empty, errors.New("backup destination identity changed")
	}
	version, err = inspectDatabaseFileWith(destination, func(db *sql.DB, _ int) error {
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
	if err := copyCtx.Err(); err != nil {
		return empty, err
	}
	if err := file.Sync(); err != nil {
		return empty, err
	}
	hash := sha256.New()
	bytes, err := io.Copy(hash, legacyBackupReader{ctx: copyCtx, reader: file})
	if err != nil {
		return empty, err
	}
	directory, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return empty, err
	}
	err = directory.Sync()
	err = errors.Join(err, directory.Close())
	if err != nil {
		return empty, err
	}
	if err := copyCtx.Err(); err != nil {
		return empty, err
	}
	complete = true
	return LegacyBackup{SchemaVersion: version, SHA256: hex.EncodeToString(hash.Sum(nil)), Bytes: bytes}, nil
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

func copyLegacySQLite(ctx context.Context, source, destination string) error {
	sourceDSN, err := sqliteFileDSN(source, "ro")
	if err != nil {
		return err
	}
	destinationDSN, err := sqliteFileDSN(destination, "rw")
	if err != nil {
		return err
	}
	src, err := sql.Open("sqlite3", sourceDSN)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := sql.Open("sqlite3", destinationDSN)
	if err != nil {
		return err
	}
	defer dst.Close()
	sourceConn, err := src.Conn(ctx)
	if err != nil {
		return err
	}
	defer sourceConn.Close()
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
