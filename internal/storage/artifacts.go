package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ArtifactImport is trusted controller input. Task callers can only publish their
// own Task origin; repository/external imports belong to the owning main. Tool
// authentication and call replay are additional Task_3 gateway responsibilities.
type ArtifactImport struct {
	SessionID        string                `json:"session_id"`
	Target           contract.Target       `json:"target"`
	Key              string                `json:"key"`
	Kind             string                `json:"kind"`
	ContentType      string                `json:"content_type"`
	Origin           domain.ArtifactOrigin `json:"origin"`
	InputArtifactIDs []string              `json:"input_artifact_ids"`
}

var ErrArtifactGrant = errors.New("Artifact was not granted to this Task")
var ErrArtifactScope = errors.New("Artifact is outside the current scope/policy")
var ErrArtifactUnavailable = errors.New("Artifact is not committed")
var ErrArtifactIntegrity = errors.New("Artifact content or manifest is inconsistent")

func loadArtifact(ctx context.Context, q queryer, id string) (domain.Artifact, error) {
	var a domain.Artifact
	var raw []byte
	var wave, task, state, digest string
	var size int64
	err := q.QueryRowContext(ctx, "SELECT wave_id,task_id,state,bytes,digest,payload FROM kernel_artifacts WHERE id=?", id).Scan(&wave, &task, &state, &size, &digest, &raw)
	if err != nil {
		return a, err
	}
	if err = json.Unmarshal(raw, &a); err != nil {
		return a, fmt.Errorf("%w: %v", ErrArtifactIntegrity, err)
	}
	if err = a.Validate(); err != nil {
		return a, fmt.Errorf("%w: %v", ErrArtifactIntegrity, err)
	}
	if a.ID != id || a.WaveID != wave || a.TaskID != task || a.State != state || a.Bytes != size || a.Digest != digest {
		return a, fmt.Errorf("%w: manifest fields differ", ErrArtifactIntegrity)
	}
	return a, nil
}
func (k *KernelStore) Artifact(ctx context.Context, id string) (domain.Artifact, error) {
	return loadArtifact(ctx, k.DB, id)
}
func saveArtifact(ctx context.Context, tx *sql.Tx, a domain.Artifact) error {
	if err := a.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_artifacts SET state=?,payload=? WHERE id=?", a.State, raw, a.ID)
	return err
}
func artifactAuthorityTx(ctx context.Context, tx *sql.Tx, sessionID string, target contract.Target) (contract.SessionBinding, domain.Wave, error) {
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return b, domain.Wave{}, err
	}
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return b, w, err
	}
	if !target.Valid() || w.Target() != target {
		return b, w, errors.New("stale Artifact target")
	}
	if b.Kind == "orchestrator" {
		if w.OrchestratorSessionID != b.ID || b.Closed {
			return b, w, errors.New("Artifact main ownership conflict")
		}
	} else if b.Kind != "task" || b.Target != target {
		return b, w, errors.New("Artifact Task ownership conflict")
	}
	return b, w, nil
}
func artifactReadableTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, w domain.Wave, a domain.Artifact) error {
	if a.State != "committed" {
		return ErrArtifactUnavailable
	}
	if a.WaveID != w.ID || a.ScopeRevision != w.ScopeRevision || a.PolicyRevision != w.Policy.Revision {
		return ErrArtifactScope
	}
	if b.Kind == "orchestrator" && w.OrchestratorSessionID == b.ID {
		return nil
	}
	if b.Kind != "task" || b.Target != w.Target() {
		return errors.New("Artifact reader ownership conflict")
	}
	task, err := loadTask(ctx, tx, b.TaskID)
	if err != nil {
		return err
	}
	if a.TaskID == task.ID {
		return nil
	}
	for _, id := range task.Spec.InputArtifactIDs {
		if id == a.ID {
			return nil
		}
	}
	for _, dep := range task.Spec.DependsOn {
		prior, err := loadTask(ctx, tx, structureID("task-", w.ID, dep.TaskID))
		if err != nil {
			return err
		}
		if prior.State != contract.TaskCompleted || prior.ResultID == "" || prior.ScopeRevision != w.ScopeRevision || prior.PolicyRevision != w.Policy.Revision {
			continue
		}
		if dep.ArtifactID != "" && dep.ArtifactID != a.ID {
			continue
		}
		var result contract.TaskResult
		if err = completionRecord(ctx, tx, "task_result", prior.ResultID, &result); err != nil {
			return err
		}
		if result.TaskID != prior.ID || result.Target != prior.Target() {
			return errors.New("dependency result binding conflict")
		}
		for _, id := range resultArtifactReferences(result) {
			if id == a.ID {
				return nil
			}
		}
	}
	return ErrArtifactGrant
}

// Evidence may be carried separately from top-level output artifacts. Read
// grants, dependency bindings and retention must all follow the same closure.
func resultArtifactReferences(result contract.TaskResult) []string {
	ids := append([]string{}, result.Artifacts...)
	for _, criterion := range result.CriterionResults {
		ids = append(ids, criterion.EvidenceIDs...)
	}
	if result.Verification != nil {
		ids = append(ids, result.Verification.EvidenceIDs...)
	}
	seen := map[string]bool{}
	unique := []string{}
	for _, id := range ids {
		if !seen[id] {
			unique = append(unique, id)
			seen[id] = true
		}
	}
	return unique
}
func (k *KernelStore) reserveArtifact(ctx context.Context, request ArtifactImport, data []byte) (domain.Artifact, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return domain.Artifact{}, err
	}
	defer tx.Rollback()
	a, err := k.reserveArtifactTx(ctx, tx, request, data)
	if err != nil {
		return a, err
	}
	return a, tx.Commit()
}
func (k *KernelStore) reserveArtifactTx(ctx context.Context, tx *sql.Tx, request ArtifactImport, data []byte) (domain.Artifact, error) {
	var a domain.Artifact
	if strings.TrimSpace(request.Key) == "" || len(request.Key) > 256 {
		return a, errors.New("Artifact import key required")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	identity, err := contentDigest(struct {
		Request ArtifactImport
		Digest  string
		Bytes   int
	}{request, digest, len(data)})
	if err != nil {
		return a, err
	}
	reservation, err := contentDigest([]string{request.SessionID, request.Key})
	if err != nil {
		return a, err
	}
	var id string
	err = tx.QueryRowContext(ctx, "SELECT id FROM kernel_artifacts WHERE reservation_key=?", reservation).Scan(&id)
	if err == nil {
		var saved string
		if err = completionRecord(ctx, tx, "artifact_import_identity", id, &saved); err != nil {
			return a, err
		}
		if saved != identity {
			return a, errors.New("Artifact import replay identity conflict")
		}
		return loadArtifact(ctx, tx, id)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return a, err
	}
	b, w, err := artifactAuthorityTx(ctx, tx, request.SessionID, request.Target)
	if err != nil {
		return a, err
	}
	if b.Closed || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
		return a, errors.New("Artifact publication is closed, paused or expired")
	}
	if b.Kind == "task" {
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return a, err
		}
		if task.Paused || task.State.Terminal() || request.Origin.Kind != "task" || request.Origin.Reference != task.ID {
			return a, errors.New("Task cannot invent Artifact provenance")
		}
	} else if request.Origin.Kind == "session" {
		if request.Origin.Reference != b.ID {
			return a, errors.New("main cannot invent session provenance")
		}
	} else if request.Origin.Kind != "repository" && request.Origin.Kind != "external" {
		return a, errors.New("main imports require explicit repository/external/session provenance")
	}
	if int64(len(data)) > w.Policy.Limits.ArtifactBytes {
		return a, errors.New("Artifact byte limit exceeded")
	}
	for _, input := range request.InputArtifactIDs {
		prior, err := loadArtifact(ctx, tx, input)
		if err != nil {
			return a, err
		}
		if err = artifactReadableTx(ctx, tx, b, w, prior); err != nil {
			return a, err
		}
		// Revalidate the imported bytes, not merely the caller's digest claim.
		if _, err = k.readArtifactObject(prior); err != nil {
			return a, err
		}
	}
	var waveBytes, totalBytes int64
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(sum(bytes),0) FROM kernel_artifacts WHERE wave_id=? AND state<>'expired'", w.ID).Scan(&waveBytes); err != nil {
		return a, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(sum(bytes),0) FROM kernel_artifacts WHERE state<>'expired'").Scan(&totalBytes); err != nil {
		return a, err
	}
	cap := k.ControllerArtifactBytes
	if cap <= 0 {
		return a, errors.New("controller Artifact capacity is not configured")
	}
	if w.Policy.Limits.ControllerArtifactBytes < cap {
		cap = w.Policy.Limits.ControllerArtifactBytes
	}
	if waveBytes > w.Policy.Limits.WaveArtifactBytes || int64(len(data)) > w.Policy.Limits.WaveArtifactBytes-waveBytes || totalBytes > cap || int64(len(data)) > cap-totalBytes {
		return a, errors.New("Artifact capacity exhausted (including staged imports)")
	}
	id, err = opaqueID("artifact-")
	if err != nil {
		return a, err
	}
	a = domain.Artifact{ID: id, WaveID: w.ID, TaskID: b.TaskID, SessionID: b.ID, ScopeRevision: w.ScopeRevision, PlanRevision: w.PlanRevision, PolicyRevision: w.Policy.Revision, Kind: request.Kind, ContentType: request.ContentType, Digest: digest, Bytes: int64(len(data)), Origin: request.Origin, InputArtifactIDs: request.InputArtifactIDs, State: "staged", CreatedAt: k.clock()}
	if err = a.Validate(); err != nil {
		return a, err
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return a, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_artifacts(id,reservation_key,wave_id,task_id,state,bytes,digest,payload) VALUES(?,?,?,?,?,?,?,?)", a.ID, reservation, a.WaveID, a.TaskID, a.State, a.Bytes, a.Digest, raw); err != nil {
		return a, err
	}
	if err = kernelRecordTx(ctx, tx, "artifact_import_identity", a.ID, a.WaveID, identity); err != nil {
		return a, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'artifact_bytes',?,0,?)", w.ID, a.ID, a.Bytes, []byte(`{}`)); err != nil {
		return a, err
	}
	return a, nil
}

// ImportArtifact first commits a capacity reservation, then saves and fsyncs the
// bytes, then commits the immutable manifest/event. Crash retries use the same
// ID; staged imports continue to consume capacity until resolved.
func (k *KernelStore) ImportArtifact(ctx context.Context, request ArtifactImport, data []byte) (domain.Artifact, error) {
	return k.importArtifact(ctx, request, data, false)
}
func (k *KernelStore) importArtifact(ctx context.Context, request ArtifactImport, data []byte, nonblocking bool) (domain.Artifact, error) {
	a, err := k.reserveArtifact(ctx, request, data)
	if err != nil {
		return a, err
	}
	if a.State == "committed" {
		_, err = k.readArtifactObject(a)
		return a, err
	}
	if a.State != "staged" {
		return a, errors.New("Artifact import is expired/deleting")
	}
	root, err := k.openArtifactRoot()
	if err != nil {
		return a, err
	}
	defer root.Close()
	file, err := root.OpenFile(a.ID, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return a, err
	}
	defer file.Close()
	if err = lockArtifactWriter(ctx, file, nonblocking); err != nil {
		return a, err
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	if err = privateArtifactFile(file); err != nil {
		return a, err
	}
	current, err := k.Artifact(ctx, a.ID)
	if err != nil {
		return a, err
	}
	if current.State == "committed" {
		_, err = k.readArtifactObject(current)
		return current, err
	}
	if current.State != "staged" || !reflect.DeepEqual(current, a) {
		return a, errors.New("Artifact reservation changed")
	}
	// Only a staged object may be repaired after a partial file write. A committed
	// object's contents are never overwritten, even by an identical import call.
	if err = file.Truncate(0); err != nil {
		return a, err
	}
	if _, err = file.WriteAt(data, 0); err != nil {
		return a, err
	}
	if err = file.Sync(); err != nil {
		return a, err
	}
	dir, err := os.Open(root.Name())
	if err != nil {
		return a, err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return a, err
	}
	return k.commitArtifact(ctx, a)
}
func lockArtifactWriter(ctx context.Context, file *os.File, nonblocking bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if nonblocking || (!errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN)) {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func privateArtifactFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%w: object must be a private regular controller-owned file", ErrArtifactIntegrity)
	}
	return nil
}
func (k *KernelStore) readArtifactObject(a domain.Artifact) ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrArtifactIntegrity, err)
	}
	root, err := k.openArtifactRoot()
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.OpenFile(a.ID, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err = privateArtifactFile(file); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != a.Bytes {
		return nil, fmt.Errorf("%w: byte count differs from manifest", ErrArtifactIntegrity)
	}
	if a.Bytes > k.ControllerArtifactBytes || a.Bytes == int64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: object exceeds controller capacity", ErrArtifactIntegrity)
	}
	data, err := io.ReadAll(io.LimitReader(file, a.Bytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != a.Bytes || fmt.Sprintf("%x", sha256.Sum256(data)) != a.Digest {
		return nil, fmt.Errorf("%w: digest mismatch", ErrArtifactIntegrity)
	}
	return data, nil
}
func (k *KernelStore) commitArtifact(ctx context.Context, a domain.Artifact) (domain.Artifact, error) {
	if _, err := k.readArtifactObject(a); err != nil {
		return a, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	current, err := loadArtifact(ctx, tx, a.ID)
	if err != nil {
		return a, err
	}
	if current.State == "committed" {
		if err = k.finishKernelPublicationTx(ctx, tx, current); err != nil {
			return current, err
		}
		return current, tx.Commit()
	}
	if !reflect.DeepEqual(current, a) || a.State != "staged" {
		return a, errors.New("Artifact changed before manifest commit")
	}
	a.State = "committed"
	if err = saveArtifact(ctx, tx, a); err != nil {
		return a, err
	}
	w, err := loadWave(ctx, tx, a.WaveID)
	if err != nil {
		return a, err
	}
	// Late storage facts retain their original target and cannot masquerade as a
	// publication in a newer direction/replan.
	w.ScopeRevision, w.PlanRevision, w.Policy.Revision = a.ScopeRevision, a.PlanRevision, a.PolicyRevision
	if err = k.waveEventTx(ctx, tx, w, "artifact_committed", a.TaskID, a.ID+":committed", a); err != nil {
		return a, err
	}
	if err = k.finishKernelPublicationTx(ctx, tx, a); err != nil {
		return a, err
	}
	return a, tx.Commit()
}
func (k *KernelStore) ReadArtifact(ctx context.Context, sessionID string, target contract.Target, id string, offset int64, limit int) (domain.Artifact, []byte, error) {
	var a domain.Artifact
	if offset < 0 || limit < 1 || limit > contract.MaxInline {
		return a, nil, errors.New("invalid Artifact excerpt bounds")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return a, nil, err
	}
	defer tx.Rollback()
	b, w, err := artifactAuthorityTx(ctx, tx, sessionID, target)
	if err != nil {
		return a, nil, err
	}
	a, err = loadArtifact(ctx, tx, id)
	if err != nil {
		return a, nil, err
	}
	if err = artifactReadableTx(ctx, tx, b, w, a); err != nil {
		return a, nil, err
	}
	data, err := k.readArtifactObject(a)
	if err != nil {
		return a, nil, err
	}
	if offset > int64(len(data)) {
		return a, nil, errors.New("Artifact excerpt begins beyond content")
	}
	end := int64(len(data))
	if int64(limit) < end-offset {
		end = offset + int64(limit)
	}
	return a, data[offset:end], nil
}

// ImportArtifactFile reads only the stored session workspace. Root confines
// symlink traversal; O_NONBLOCK prevents special files from hanging admission.
func (k *KernelStore) ImportArtifactFile(ctx context.Context, request ArtifactImport, relative string) (domain.Artifact, error) {
	var a domain.Artifact
	if relative == "." || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return a, errors.New("Artifact source must be a normalized workspace-relative file")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return a, err
	}
	b, w, err := artifactAuthorityTx(ctx, tx, request.SessionID, request.Target)
	rollbackErr := tx.Rollback()
	if err != nil {
		return a, err
	}
	if rollbackErr != nil {
		return a, rollbackErr
	}
	if b.Closed || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || !k.clock().Before(w.Deadline) {
		return a, errors.New("Artifact source session is closed, paused or expired")
	}
	root, err := os.OpenRoot(b.CWD)
	if err != nil {
		return a, err
	}
	defer root.Close()
	file, err := root.OpenFile(relative, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return a, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return a, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0444 == 0 || info.Size() > w.Policy.Limits.ArtifactBytes {
		return a, errors.New("Artifact source is unreadable, special or oversized")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		return a, errors.New("Artifact source hard links are not permitted")
	}
	maximum := w.Policy.Limits.ArtifactBytes
	if maximum >= k.ControllerArtifactBytes {
		maximum = k.ControllerArtifactBytes
	}
	if maximum < 0 || maximum == int64(^uint64(0)>>1) {
		return a, errors.New("invalid Artifact source bound")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return a, err
	}
	if int64(len(data)) > maximum {
		return a, errors.New("Artifact source grew beyond byte limit")
	}
	return k.ImportArtifact(ctx, request, data)
}

// ReconcileArtifactImports completes fsynced staged objects after a crash. A
// missing/partial file stays staged and charged; replay with original bytes can
// repair it. No empty content or expired manifest is substituted.
type ArtifactReconciliation struct {
	Committed []string `json:"committed"`
	Pending   []string `json:"pending"`
}

func (k *KernelStore) ReconcileArtifactImports(ctx context.Context) (ArtifactReconciliation, error) {
	report := ArtifactReconciliation{Committed: []string{}, Pending: []string{}}
	rows, err := k.DB.QueryContext(ctx, "SELECT id FROM kernel_artifacts WHERE state='staged'")
	if err != nil {
		return report, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return report, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, err
	}
	for _, id := range ids {
		committed, err := k.reconcileArtifactImport(ctx, id)
		if err != nil {
			return report, err
		}
		if committed {
			report.Committed = append(report.Committed, id)
		} else {
			report.Pending = append(report.Pending, id)
		}
	}
	return report, nil
}
func (k *KernelStore) reconcileArtifactImport(ctx context.Context, id string) (bool, error) {
	a, err := k.Artifact(ctx, id)
	if err != nil {
		return false, err
	}
	if a.State == "committed" {
		return true, nil
	}
	if a.State != "staged" {
		return false, nil
	}
	root, err := k.openArtifactRoot()
	if err != nil {
		return false, err
	}
	defer root.Close()
	file, err := root.OpenFile(a.ID, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	if err = privateArtifactFile(file); err != nil {
		return false, err
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return false, nil
		}
		return false, err
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	if _, err = k.readArtifactObject(a); err != nil {
		return false, nil
	}
	if err = file.Sync(); err != nil {
		return false, err
	}
	dir, err := os.Open(root.Name())
	if err != nil {
		return false, err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return false, err
	}
	if _, err = k.commitArtifact(ctx, a); err != nil {
		return false, err
	}
	return true, nil
}

func (k *KernelStore) openArtifactRoot() (*os.Root, error) {
	path := filepath.Join(k.Root, "objects")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("Artifact directory must be private and controller-owned")
	}
	return os.OpenRoot(path)
}
