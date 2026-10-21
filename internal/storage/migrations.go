package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// databaseVersion inspects before any DDL or journal-mode changes. Version zero
// denotes a truly empty SQLite database, never an unrecognized existing schema.
func databaseVersion(db *sql.DB) (int, error) {
	var count, header int
	if err := db.QueryRow("PRAGMA user_version").Scan(&header); err != nil {
		return 0, err
	}
	if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'").Scan(&count); err != nil {
		return 0, err
	}
	if count == 0 {
		if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").Scan(&count); err != nil {
			return 0, err
		}
		if count > 0 || header != 0 {
			return 0, errors.New("existing database has an unsupported schema")
		}
		return 0, nil
	}
	rows, err := db.Query("SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	version := 0
	for rows.Next() {
		var v int
		if err = rows.Scan(&v); err != nil {
			return 0, err
		}
		if v < 1 || v <= version {
			return 0, errors.New("invalid schema version history")
		}
		version = v
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if version == 0 || (header != 0 && header != version) {
		return 0, errors.New("missing/conflicting database schema version")
	}
	return version, nil
}

type Migration struct {
	From, To int
	Key      string
	Apply    func(context.Context, *sql.Tx) error
}
type MigrationRegistry struct {
	steps map[int]Migration
	keys  map[string]bool
}

func (r *MigrationRegistry) Register(m Migration) error {
	if m.From < 1 || m.To != m.From+1 || m.Key == "" || m.Apply == nil {
		return errors.New("migration requires consecutive versions, key and implementation")
	}
	if r.steps == nil {
		r.steps = map[int]Migration{}
		r.keys = map[string]bool{}
	}
	if _, exists := r.steps[m.To]; exists || r.keys[m.Key] {
		return errors.New("duplicate migration version/key")
	}
	r.steps[m.To] = m
	r.keys[m.Key] = true
	return nil
}

// Steps only plans explicit offline migration. Opening a DB never runs these.
// Task_12 owns controller/process reconciliation, consistent backup and apply.
func (r *MigrationRegistry) Steps(from, to int) ([]Migration, error) {
	if from < 1 || to < from {
		return nil, errors.New("invalid migration range")
	}
	out := []Migration{}
	for v := from + 1; v <= to; v++ {
		m, ok := r.steps[v]
		if !ok || m.From != v-1 {
			return nil, fmt.Errorf("unsupported migration to %d", v)
		}
		out = append(out, m)
	}
	return out, nil
}
