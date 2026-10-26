package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func TestLegacyInventoryCLIReadsPreparedImportWithoutRuntimeAdmission(t *testing.T) {
	ctx := context.Background()
	parent := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(parent, "legacy.db")
	legacy, err := storage.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	item := domain.WorkItem{ID: "r17-i42", RepositoryID: 17, Issue: 42, ScopeRevision: 1, PolicyRevision: 1, Version: 1, Phase: domain.Ready}
	scope := verification.Scope{Revision: 1, OriginalRequest: "preserve existing request", Criteria: []verification.Criterion{{ID: "AC", Requirement: "preserve behavior"}}, Included: []string{"AC"}, PreviouslyMerged: []string{}, Remaining: []string{}}
	if err = legacy.ClaimRecords(ctx, item, map[string]any{"scope": scope, "policy_snapshot": strings.Repeat("a", 64)}); err != nil {
		t.Fatal(err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := filepath.Join(parent, "snapshot.db")
	backup, err := storage.BackupLegacyDatabase(ctx, source, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "prepared")
	request := storage.LegacyKernelImportRequest{Key: "cli-fixture", Source: snapshot, DestinationRoot: root, Backup: backup}
	if _, err = storage.PrepareLegacyKernelImport(ctx, request); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "state.db")
	stateBefore, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(root, "legacy-source.db")
	archiveBefore, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err = Run(ctx, []string{"legacy-inventory", "--root", root, "--limit", "1"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var page storage.LegacyInventoryPage
	if err = json.Unmarshal(out.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Value.ImportID == "" || len(page.Value.WorkItems) != 1 || page.Value.WorkItems[0].WorkItem.ID != item.ID || page.Value.WorkItems[0].ScopeDigest == "" || page.Value.WorkItems[0].PolicyDigest == "" || page.Value.NextAfter != "" {
		t.Fatal("CLI did not expose the authenticated legacy inventory", page)
	}
	stateAfter, err := os.ReadFile(statePath)
	if err != nil || !bytes.Equal(stateBefore, stateAfter) {
		t.Fatal("inventory read modified the prepared runtime database", err)
	}
	archiveAfter, err := os.ReadFile(archivePath)
	if err != nil || !bytes.Equal(archiveBefore, archiveAfter) {
		t.Fatal("inventory read modified the retained legacy archive", err)
	}
	if opened, err := storage.OpenExistingKernel(statePath); !errors.Is(err, storage.ErrLegacyMigrationPending) {
		if opened != nil {
			opened.Close()
		}
		t.Fatal("read-only inventory accidentally released runtime admission", err)
	}
}
