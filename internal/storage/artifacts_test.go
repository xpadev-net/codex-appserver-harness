package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func artifactFixture(t *testing.T) (*KernelStore, domain.Wave, ArtifactImport) {
	t.Helper()
	store, r := waveStoreFixture(t)
	w, err := store.CreateWave(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return store, w, ArtifactImport{SessionID: r.OrchestratorSessionID, Target: w.Target(), Key: "import", Kind: "note", ContentType: "text/plain; charset=utf-8", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "direct request"}, InputArtifactIDs: []string{}}
}
func mustImport(t *testing.T, k *KernelStore, r ArtifactImport, data string) domain.Artifact {
	t.Helper()
	a, err := k.ImportArtifact(context.Background(), r, []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func TestArtifactImportIsImmutableReplayableAndVerifiesActualBytes(t *testing.T) {
	ctx := context.Background()
	store, w, r := artifactFixture(t)
	a := mustImport(t, store, r, "immutable evidence")
	if a.Bytes != 18 || a.Digest != fmt.Sprintf("%x", sha256.Sum256([]byte("immutable evidence"))) || a.State != "committed" || a.ScopeRevision != 1 || a.TaskID != "" {
		t.Fatal(a)
	}
	replay := mustImport(t, store, r, "immutable evidence")
	if replay.ID != a.ID {
		t.Fatal("duplicate import", a, replay)
	}
	if _, err := store.ImportArtifact(ctx, r, []byte("changed")); err == nil {
		t.Fatal("import key reused for different bytes")
	}
	_, excerpt, err := store.ReadArtifact(ctx, r.SessionID, w.Target(), a.ID, 10, 8)
	if err != nil || string(excerpt) != "evidence" {
		t.Fatal(string(excerpt), err)
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, w.Target(), a.ID, 19, 1); err == nil {
		t.Fatal("out of bounds excerpt")
	}
	var count int
	if err = store.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='artifact_committed'").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	// Same-size corruption must fail both reads and replay, without being repaired
	// behind a committed immutable manifest.
	if err = os.WriteFile(filepath.Join(store.Root, "objects", a.ID), []byte("IMMUTABLE EVIDENCE"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, w.Target(), a.ID, 0, 16); err == nil {
		t.Fatal("digest corruption accepted")
	}
	if _, err = store.ImportArtifact(ctx, r, []byte("immutable evidence")); err == nil {
		t.Fatal("committed bytes silently repaired")
	}
}
func TestArtifactFileImportConfinesSourceAndChecksAccess(t *testing.T) {
	ctx := context.Background()
	store, _, r := artifactFixture(t)
	b, err := store.SessionBinding(ctx, r.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(b.CWD, "note.txt"), []byte("source contents"), 0600); err != nil {
		t.Fatal(err)
	}
	a, err := store.ImportArtifactFile(ctx, r, "note.txt")
	if err != nil || a.Bytes != 15 {
		t.Fatal(a, err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err = os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(b.CWD, "escape")); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Mkfifo(filepath.Join(b.CWD, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Link(outside, filepath.Join(b.CWD, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(b.CWD, "unreadable"), []byte("private"), 0000); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{outside, "../secret", "nested/../note.txt", "escape", "fifo", "hardlink", "unreadable", "."} {
		t.Run(path, func(t *testing.T) {
			request := r
			request.Key = path
			if _, err := store.ImportArtifactFile(ctx, request, path); err == nil {
				t.Fatal("invalid source admitted", path)
			}
		})
	}
	if err = os.Chmod(filepath.Join(store.Root, "objects", a.ID), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, r.Target, a.ID, 0, 16); err == nil {
		t.Fatal("public controller object read")
	}
	if err = os.Chmod(filepath.Join(store.Root, "objects", a.ID), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(store.Root, "objects", a.ID)); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(store.Root, "objects", a.ID)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, r.Target, a.ID, 0, 16); err == nil {
		t.Fatal("object symlink accepted")
	}
}
func TestArtifactReadRequiresDeclaredTaskGrantAndCurrentScope(t *testing.T) {
	ctx := context.Background()
	store, w, r := artifactFixture(t)
	a := mustImport(t, store, r, "input")
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].InputArtifactIDs = []string{a.ID}
	receipt, err := store.AdoptPlan(ctx, w.Target(), "plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Task(ctx, receipt.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Task(ctx, receipt.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, data, err := store.ReadArtifact(ctx, first.SessionID, w.Target(), a.ID, 0, 16); err != nil || string(data) != "input" {
		t.Fatal(string(data), err)
	}
	if _, _, err = store.ReadArtifact(ctx, second.SessionID, w.Target(), a.ID, 0, 16); err == nil {
		t.Fatal("undeclared sibling input read")
	}
	other := WaveRequest{OrchestratorSessionID: w.OrchestratorSessionID, Source: "direct", SourceID: "other", OriginalRequest: w.OriginalRequest, Criteria: w.Criteria, Policy: w.Policy}
	another, err := store.CreateWave(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, another.Target(), a.ID, 0, 16); err == nil {
		t.Fatal("cross-Wave read")
	}
	request := r
	request.SessionID = first.SessionID
	request.Key = "task"
	request.Origin = domain.ArtifactOrigin{Kind: "external", Reference: "forged outside source"}
	if _, err = store.ImportArtifact(ctx, request, []byte("forged")); err == nil {
		t.Fatal("Task invented external provenance")
	}
	request.Origin = domain.ArtifactOrigin{Kind: "task", Reference: first.ID}
	own := mustImport(t, store, request, "owned")
	if own.TaskID != first.ID {
		t.Fatal(own)
	}
	tx, err := store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.ScopeRevision++
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, w.Target(), a.ID, 0, 16); err == nil {
		t.Fatal("old scope artifact accepted")
	}
	if _, _, err = store.ReadArtifact(ctx, first.SessionID, first.Target(), a.ID, 0, 16); err == nil {
		t.Fatal("stale Task read")
	}
}
func TestArtifactStagedCrashReplaysAndReservesCapacity(t *testing.T) {
	ctx := context.Background()
	store, w, r := artifactFixture(t)
	// Fail after the bytes are fsynced but before manifest+notification commit.
	if _, err := store.DB.Exec("CREATE TRIGGER fail_artifact_event BEFORE INSERT ON kernel_events WHEN NEW.kind='artifact_committed' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ImportArtifact(ctx, r, []byte("pending"))
	if err == nil {
		t.Fatal("injected commit failure ignored")
	}
	saved, err := store.Artifact(ctx, pending.ID)
	if err != nil || saved.State != "staged" {
		t.Fatal(saved, err)
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, w.Target(), saved.ID, 0, 16); err == nil {
		t.Fatal("uncommitted object exposed")
	}
	if _, err = store.DB.Exec("DROP TRIGGER fail_artifact_event"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.Root, "state.db")
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, err := reopened.ReconcileArtifactImports(ctx)
	if err != nil || len(report.Committed) != 1 || len(report.Pending) != 0 {
		t.Fatal(report, err)
	}
	replay := mustImport(t, reopened, r, "pending")
	if replay.ID != pending.ID || replay.State != "committed" {
		t.Fatal(replay)
	}
	r.Key = "partial"
	partial, err := reopened.reserveArtifact(ctx, r, []byte("complete"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(reopened.Root, "objects", partial.ID), []byte("part"), 0600); err != nil {
		t.Fatal(err)
	}
	report, err = reopened.ReconcileArtifactImports(ctx)
	if err != nil || len(report.Pending) != 1 || report.Pending[0] != partial.ID {
		t.Fatal(report, err)
	}
	repaired := mustImport(t, reopened, r, "complete")
	if repaired.ID != partial.ID {
		t.Fatal("partial replay changed ID")
	}
	var charged int64
	if err = reopened.DB.QueryRow("SELECT sum(amount) FROM kernel_budget_ledger WHERE kind='artifact_bytes'").Scan(&charged); err != nil || charged != 15 {
		t.Fatal(charged, err)
	}
}
func TestArtifactCapacityIncludesConcurrentAndPendingImports(t *testing.T) {
	ctx := context.Background()
	store, w, r := artifactFixture(t)
	store.ControllerArtifactBytes = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	successful := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			request := r
			request.Key = fmt.Sprint(i)
			if _, err := store.ImportArtifact(ctx, request, []byte("0123456789abcdef")); err == nil {
				mu.Lock()
				successful++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if successful != 4 {
		t.Fatal("concurrent capacity admitted", successful)
	}
	var bytes int64
	if err := store.DB.QueryRow("SELECT sum(bytes) FROM kernel_artifacts WHERE state<>'expired'").Scan(&bytes); err != nil || bytes != 64 {
		t.Fatal(bytes, err)
	}
	store.ControllerArtifactBytes = 80
	r.Key = "pending"
	if _, err := store.reserveArtifact(ctx, r, []byte("0123456789abcdef")); err != nil {
		t.Fatal(err)
	}
	r.Key = "overflow"
	if _, err := store.ImportArtifact(ctx, r, []byte("x")); err == nil {
		t.Fatal("staged capacity ignored")
	}
	tx, err := store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.Policy.Limits.ArtifactBytes = 2
	w.Policy.Limits.WaveArtifactBytes = 80
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	r.Key = "oversized"
	if _, err = store.ImportArtifact(ctx, r, []byte("123")); err == nil {
		t.Fatal("per-object limit ignored")
	}
}

func TestArtifactRetentionProtectsUnfinishedInputsAndTheirProvenance(t *testing.T) {
	ctx := context.Background()
	store, w, r := artifactFixture(t)
	base := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	store.Now = func() time.Time { return base }
	// Use a future fixed fixture clock consistently with the Wave deadline.
	w.Deadline = base.Add(time.Hour)
	w.Policy.Limits.RetentionSeconds = 10
	tx, err := store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	source := mustImport(t, store, r, "source")
	r.Key = "derived"
	r.InputArtifactIDs = []string{source.ID}
	derived := mustImport(t, store, r, "derived")
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].InputArtifactIDs = []string{derived.ID}
	if _, err = store.AdoptPlan(ctx, w.Target(), "plan", plan); err != nil {
		t.Fatal(err)
	}
	tx, err = store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.State, w.CompletedAt = domain.WaveCanceled, base
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	store.Now = func() time.Time { return base.Add(11 * time.Second) }
	if err = store.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{source.ID, derived.ID} {
		a, err := store.Artifact(ctx, id)
		if err != nil || a.State != "committed" {
			t.Fatal("unfinished Task input collected", a, err)
		}
	}
	tasks, err := store.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err = store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		task.State, task.CompletedAt = contract.TaskCanceled, base
		if err = saveTask(ctx, tx, task); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	store.Now = func() time.Time { return base.Add(9 * time.Second) }
	if err = store.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := store.Artifact(ctx, derived.ID)
	if err != nil || a.State != "committed" {
		t.Fatal("early collection", a, err)
	}
	store.Now = func() time.Time { return base.Add(11 * time.Second) }
	if err = store.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{source.ID, derived.ID} {
		a, err := store.Artifact(ctx, id)
		if err != nil || a.State != "expired" || a.ExpiredAt.IsZero() {
			t.Fatal(a, err)
		}
		if _, err = os.Stat(filepath.Join(store.Root, "objects", id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("expired bytes retained", err)
		}
	}
	if _, _, err = store.ReadArtifact(ctx, r.SessionID, w.Target(), derived.ID, 0, 16); err == nil {
		t.Fatal("expired artifact read")
	}
	if _, err = store.ImportArtifact(ctx, r, []byte("derived")); err == nil {
		t.Fatal("expired import replay silently republished")
	}
}
func TestArtifactDeletionCrashRemainsChargedUntilDisposition(t *testing.T) {
	ctx := context.Background()
	store, w, r := artifactFixture(t)
	a := mustImport(t, store, r, "data")
	tx, err := store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.State, w.CompletedAt = domain.WaveCompleted, time.Now().Add(-31*24*time.Hour)
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("CREATE TRIGGER fail_expiration BEFORE UPDATE ON kernel_artifacts WHEN NEW.state='expired' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if err = store.CollectArtifacts(ctx); err == nil {
		t.Fatal("injected disposition failure ignored")
	}
	saved, err := store.Artifact(ctx, a.ID)
	if err != nil || saved.State != "deleting" {
		t.Fatal(saved, err)
	}
	var charged int64
	if err = store.DB.QueryRow("SELECT sum(bytes) FROM kernel_artifacts WHERE state<>'expired'").Scan(&charged); err != nil || charged != 4 {
		t.Fatal(charged, err)
	}
	if _, err = store.DB.Exec("DROP TRIGGER fail_expiration"); err != nil {
		t.Fatal(err)
	}
	if err = store.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	saved, err = store.Artifact(ctx, a.ID)
	if err != nil || saved.State != "expired" {
		t.Fatal(saved, err)
	}
}
func TestArtifactMissingContentBlocksPlanAdmission(t *testing.T) {
	ctx := context.Background()
	store, w, r := artifactFixture(t)
	a := mustImport(t, store, r, "bound")
	if err := os.Remove(filepath.Join(store.Root, "objects", a.ID)); err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].InputArtifactIDs = []string{a.ID}
	if _, err := store.AdoptPlan(ctx, w.Target(), "plan", plan); err == nil {
		t.Fatal("plan admitted missing input bytes")
	}
	tasks, err := store.Tasks(ctx, w.ID)
	if err != nil || len(tasks) != 0 {
		t.Fatal(tasks, err)
	}
}

func TestArtifactConcurrentReplayCommitsOneManifest(t *testing.T) {
	ctx := context.Background()
	store, _, r := artifactFixture(t)
	var wg sync.WaitGroup
	results := make(chan domain.Artifact, 8)
	failures := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			a, err := store.ImportArtifact(ctx, r, []byte("same"))
			if err != nil {
				failures <- err
			} else {
				results <- a
			}
		})
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	id := ""
	count := 0
	for a := range results {
		count++
		if id == "" {
			id = a.ID
		}
		if a.ID != id || a.State != "committed" {
			t.Error(a)
		}
	}
	if count != 8 {
		t.Fatal("replay count", count)
	}
	var manifests, events int
	if err := store.DB.QueryRow("SELECT count(*) FROM kernel_artifacts").Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='artifact_committed'").Scan(&events); err != nil || manifests != 1 || events != 1 {
		t.Fatal(manifests, events, err)
	}
}
