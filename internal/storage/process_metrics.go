package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Only trusted process owners submit these facts. Unknown measurements stay nil;
// recording an exit does not itself authorize release of a resource generation.
type ProcessObservation struct {
	ReservationID    string     `json:"reservation_id"`
	SessionID        string     `json:"session_id"`
	PID              *int64     `json:"pid,omitempty"`
	SpawnedAt        *time.Time `json:"spawned_at,omitempty"`
	ReadyAt          *time.Time `json:"ready_at,omitempty"`
	StopRequestedAt  *time.Time `json:"stop_requested_at,omitempty"`
	StoppedAt        *time.Time `json:"stopped_at,omitempty"`
	PeakRSSBytes     *int64     `json:"peak_rss_bytes,omitempty"`
	RSSMethod        string     `json:"rss_method,omitempty"`
	RSSIntervalNanos int64      `json:"rss_interval_nanos,omitempty"`
}

// RSSProcTree is a sampled sum, including shared pages once per process and
// only descendants visible during the scan. It is not a memory limit guarantee.
const RSSProcTree = "linux_proc_tree_rss_sampled_sum"

type ProcessMetrics struct {
	ProcessObservation
	Kind                 string     `json:"kind"`
	ReservationCreatedAt time.Time  `json:"reservation_created_at"`
	ObservedAt           *time.Time `json:"observed_at"`
	StartupNanos         *int64     `json:"startup_nanos"`
	StopNanos            *int64     `json:"stop_nanos"`
}

func processMetricsBaseTx(ctx context.Context, q queryer, id string) (ProcessMetrics, error) {
	var result ProcessMetrics
	var raw []byte
	var kind struct {
		Kind string `json:"kind"`
	}
	if err := q.QueryRowContext(ctx, "SELECT payload FROM kernel_dispatch_intents WHERE run_id=?", id).Scan(&raw); err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &kind); err != nil {
		return result, err
	}
	result.ReservationID, result.Kind = id, kind.Kind
	if kind.Kind == "app_server" {
		r, err := loadProcessReservationTx(ctx, q, id)
		if err != nil {
			return result, err
		}
		result.SessionID, result.ReservationCreatedAt = r.SessionID, r.CreatedAt
	} else if kind.Kind == "runner" || kind.Kind == "git" {
		r, err := loadOperationReservationTx(ctx, q, id)
		if err != nil {
			return result, err
		}
		result.SessionID, result.ReservationCreatedAt = r.SessionID, r.CreatedAt
	} else {
		return result, errors.New("external reservation is not an owned process")
	}
	return result, nil
}
func processMetricsTx(ctx context.Context, q queryer, id string) (ProcessMetrics, error) {
	base, err := processMetricsBaseTx(ctx, q, id)
	if err != nil {
		return base, err
	}
	var result ProcessMetrics
	err = completionRecord(ctx, q, "process_metrics", id, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return base, nil
	}
	if err != nil {
		return base, err
	}
	if result.ReservationID != id || result.SessionID != base.SessionID || result.Kind != base.Kind || !result.ReservationCreatedAt.Equal(base.ReservationCreatedAt) {
		return base, errors.New("process metric source conflict")
	}
	result.StartupNanos, result.StopNanos = nil, nil
	if result.SpawnedAt != nil && result.ReadyAt != nil {
		duration := result.ReadyAt.Sub(*result.SpawnedAt).Nanoseconds()
		result.StartupNanos = &duration
	}
	if result.StopRequestedAt != nil && result.StoppedAt != nil {
		duration := result.StoppedAt.Sub(*result.StopRequestedAt).Nanoseconds()
		result.StopNanos = &duration
	}
	return result, nil
}
func (k *KernelStore) ProcessMetrics(ctx context.Context, id string) (ProcessMetrics, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProcessMetrics{}, err
	}
	defer tx.Rollback()
	result, err := processMetricsTx(ctx, tx, id)
	if err != nil {
		return result, err
	}
	return result, tx.Commit()
}
func mergeProcessTime(old **time.Time, next *time.Time) error {
	if next == nil {
		return nil
	}
	if *old != nil && !(*old).Equal(*next) {
		return errors.New("conflicting process lifecycle timestamp")
	}
	value := next.UTC()
	*old = &value
	return nil
}
func (k *KernelStore) ObserveProcess(ctx context.Context, observation ProcessObservation) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	m, err := processMetricsTx(ctx, tx, observation.ReservationID)
	if err != nil {
		return err
	}
	if observation.SessionID != m.SessionID {
		return errors.New("process observation belongs to another session")
	}
	if observation.PID != nil {
		if *observation.PID < 1 || m.PID != nil && *m.PID != *observation.PID {
			return errors.New("conflicting process PID")
		}
		value := *observation.PID
		m.PID = &value
	}
	for _, pair := range []struct {
		old  **time.Time
		next *time.Time
	}{{&m.SpawnedAt, observation.SpawnedAt}, {&m.ReadyAt, observation.ReadyAt}, {&m.StopRequestedAt, observation.StopRequestedAt}, {&m.StoppedAt, observation.StoppedAt}} {
		if err = mergeProcessTime(pair.old, pair.next); err != nil {
			return err
		}
	}
	now := k.clock()
	for _, at := range []*time.Time{m.SpawnedAt, m.ReadyAt, m.StopRequestedAt, m.StoppedAt} {
		if at != nil && (at.IsZero() || at.Before(m.ReservationCreatedAt) || at.After(now)) {
			return errors.New("process timestamp is outside the reservation observation interval")
		}
	}
	if m.SpawnedAt != nil && m.ReadyAt != nil && m.ReadyAt.Before(*m.SpawnedAt) {
		return errors.New("process ready before spawn")
	}
	for _, at := range []*time.Time{m.SpawnedAt, m.ReadyAt, m.StopRequestedAt} {
		if at != nil && m.StoppedAt != nil && m.StoppedAt.Before(*at) {
			return errors.New("process stopped before an earlier lifecycle event")
		}
	}
	if observation.PeakRSSBytes != nil {
		if m.PeakRSSBytes != nil && m.RSSMethod == "" && observation.RSSMethod != "" {
			return errors.New("cannot assign sampling provenance to an earlier RSS measurement")
		}
		if m.RSSMethod != "" && observation.RSSMethod != m.RSSMethod {
			return errors.New("RSS measurement requires its existing method")
		}
		if *observation.PeakRSSBytes < 0 {
			return errors.New("negative process RSS")
		}
		if m.PeakRSSBytes == nil || *observation.PeakRSSBytes > *m.PeakRSSBytes {
			value := *observation.PeakRSSBytes
			m.PeakRSSBytes = &value
		}
	}
	if observation.RSSMethod != "" || observation.RSSIntervalNanos != 0 {
		if observation.RSSMethod != RSSProcTree || observation.RSSIntervalNanos <= 0 || observation.PeakRSSBytes == nil {
			return errors.New("RSS sampling requires a known method, interval and measurement")
		}
		if m.RSSMethod != "" && (m.RSSMethod != observation.RSSMethod || m.RSSIntervalNanos != observation.RSSIntervalNanos) {
			return errors.New("conflicting RSS sampling method")
		}
		m.RSSMethod, m.RSSIntervalNanos = observation.RSSMethod, observation.RSSIntervalNanos
	}
	if m.ObservedAt == nil || now.After(*m.ObservedAt) {
		m.ObservedAt = &now
	}
	// Derived durations are reconstructed from facts, never accepted from callers.
	m.StartupNanos, m.StopNanos = nil, nil
	if err = kernelRecordTx(ctx, tx, "process_metrics", m.ReservationID, m.SessionID, m); err != nil {
		return err
	}
	return tx.Commit()
}

type ProcessMetricsPage struct {
	Processes []ProcessMetrics `json:"processes"`
	NextAfter string           `json:"next_after"`
}

// The cursor bounds scanned identities, including external reservations, so
// sparse measurements and completed histories cannot hide later processes.
func (k *KernelStore) ProcessMetricsPage(ctx context.Context, after string, limit int) (ProcessMetricsPage, error) {
	page := ProcessMetricsPage{Processes: []ProcessMetrics{}}
	if limit < 1 || limit > 1000 {
		return page, errors.New("process metrics page limit must be 1..1000")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT run_id,payload FROM kernel_dispatch_intents WHERE run_id>? ORDER BY run_id LIMIT ?", after, limit)
	if err != nil {
		return page, err
	}
	ids := []string{}
	scanned := 0
	last := ""
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return page, err
		}
		scanned++
		last = id
		var h struct {
			Kind string `json:"kind"`
		}
		if err = json.Unmarshal(raw, &h); err != nil {
			rows.Close()
			return page, err
		}
		if h.Kind == "app_server" || h.Kind == "runner" || h.Kind == "git" {
			ids = append(ids, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return page, err
	}
	for _, id := range ids {
		m, err := processMetricsTx(ctx, tx, id)
		if err != nil {
			return page, err
		}
		page.Processes = append(page.Processes, m)
	}
	if scanned == limit {
		page.NextAfter = last
	}
	return page, tx.Commit()
}
