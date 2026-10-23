package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

const workspacePreparationPrefix = "workspace_prepare:"

func workspacePathsOverlap(a, b string) bool {
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// Git discovery changes identity when a worktree is created. The preparation
// lease pins its canonical destination path before mutation and survives that
// change, restart, and an unknown outcome until an explicit stopping proof.
func checkWorkspacePreparationTx(ctx context.Context, tx *sql.Tx, path string) error {
	canonical, err := resourcePath(path)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT resource FROM kernel_resource_leases WHERE state='held' AND resource GLOB ?", workspacePreparationPrefix+"*")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var resource string
		if err := rows.Scan(&resource); err != nil {
			return err
		}
		pinned := strings.TrimPrefix(resource, workspacePreparationPrefix)
		if !filepath.IsAbs(pinned) || filepath.Clean(pinned) != pinned {
			return ErrResourceBusy
		}
		if workspacePathsOverlap(pinned, canonical) {
			return ErrResourceBusy
		}
	}
	return rows.Err()
}

// Before creating/replacing a checkout, also exclude live processes and local
// operations in the destination or a containing directory. A plain directory
// need not yet have a shared Git identity, so path containment matters here.
func checkWorkspacePreparationUsersTx(ctx context.Context, tx *sql.Tx, path string) error {
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM kernel_sessions WHERE process_stopped=0")
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw []byte
		var binding contract.SessionBinding
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal(raw, &binding); err != nil {
			rows.Close()
			return err
		}
		other, err := resourcePath(binding.CWD)
		if err != nil {
			rows.Close()
			return err
		}
		if workspacePathsOverlap(path, other) {
			rows.Close()
			return ErrResourceBusy
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	operations, err := tx.QueryContext(ctx, "SELECT run_id,session_id,wave_id,state,payload FROM kernel_dispatch_intents WHERE state='held'")
	if err != nil {
		return err
	}
	defer operations.Close()
	for operations.Next() {
		var id, session, wave, state string
		var raw []byte
		if err := operations.Scan(&id, &session, &wave, &state, &raw); err != nil {
			return err
		}
		var header struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &header); err != nil {
			return err
		}
		if header.Kind != "runner" && header.Kind != "git" {
			continue
		}
		reservation, err := decodeOperationReservation(raw, id, session, wave, state)
		if err != nil {
			return err
		}
		for _, usedPath := range []string{reservation.Request.Worktree, reservation.Request.Repository} {
			if usedPath == "" {
				continue
			}
			other, err := resourcePath(usedPath)
			if err != nil {
				return err
			}
			if workspacePathsOverlap(path, other) {
				return ErrResourceBusy
			}
		}
	}
	return operations.Err()
}
