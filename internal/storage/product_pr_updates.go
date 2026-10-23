package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// These are controller observations from authenticated, read-only API scans.
// Webhooks are retained as hints/history; their order must not set current state.
// Untrusted bodies are opaque data and never grant policy or approval.
type PRResourceEntity struct {
	ID           string          `json:"id"`
	CandidateSHA string          `json:"candidate_sha"`
	Data         json.RawMessage `json:"data"`
}
type PRResourceSection struct {
	Kind         string             `json:"kind"`
	Availability string             `json:"availability"` // complete or unavailable
	Reason       string             `json:"reason"`
	Entities     []PRResourceEntity `json:"entities"`
}
type PRResourceScan struct {
	ID           string              `json:"id"`
	RepositoryID int64               `json:"repository_id"`
	PRNumber     int                 `json:"pr_number"`
	HeadSHA      string              `json:"head_sha"`
	StartedAt    time.Time           `json:"started_at"`
	Sections     []PRResourceSection `json:"sections"`
}
type PRResourceState struct {
	Resource        PRResourceBinding   `json:"resource"`
	Digest          string              `json:"digest"`
	HeadSHA         string              `json:"head_sha"`
	Sections        []PRResourceSection `json:"sections"`
	LastScanStarted time.Time           `json:"last_scan_started"`
}
type PRResourceChange struct {
	Kind         string `json:"kind"`
	EntityID     string `json:"entity_id"`
	Action       string `json:"action"` // added, updated, deleted, availability, head_changed
	BeforeDigest string `json:"before_digest"`
	AfterDigest  string `json:"after_digest"`
	CandidateSHA string `json:"candidate_sha"`
}
type PRResourceUpdateReceipt struct {
	ScanID            string             `json:"scan_id"`
	InputDigest       string             `json:"input_digest"`
	ResourceKey       string             `json:"resource_key"`
	PreviousDigest    string             `json:"previous_digest"`
	Digest            string             `json:"digest"`
	Changes           []PRResourceChange `json:"changes"`
	NotificationState string             `json:"notification_state"` // pending outbox or unchanged
}

var prSectionKinds = []string{"pr", "issue_comment", "inline_comment", "reaction", "review", "check", "status", "history"}

func resourceFactDigest(head string, sections []PRResourceSection) (string, error) {
	raw, err := resourceFactBytes(head, sections)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}
func resourceFactBytes(head string, sections []PRResourceSection) ([]byte, error) {
	return json.Marshal(struct {
		Head     string
		Sections []PRResourceSection
	}{head, sections})
}
func canonicalResourceSections(sections []PRResourceSection) ([]PRResourceSection, error) {
	if len(sections) != len(prSectionKinds) {
		return nil, errors.New("all resource sections must report completeness or unavailability")
	}
	known := map[string]bool{}
	for _, kind := range prSectionKinds {
		known[kind] = true
	}
	result := make([]PRResourceSection, 0, len(sections))
	count, total := 0, 0
	for _, s := range sections {
		if !known[s.Kind] {
			return nil, errors.New("duplicate or unknown PR resource section")
		}
		delete(known, s.Kind)
		if (s.Availability != "complete" && s.Availability != "unavailable") || !utf8.ValidString(s.Reason) || len(s.Reason) > 512 || strings.ContainsRune(s.Reason, '\x00') || (s.Availability == "complete" && s.Reason != "") || (s.Availability == "unavailable" && (strings.TrimSpace(s.Reason) == "" || len(s.Entities) > 0)) {
			return nil, errors.New("explicit resource completeness required")
		}
		copy := PRResourceSection{Kind: s.Kind, Availability: s.Availability, Reason: s.Reason, Entities: []PRResourceEntity{}}
		seen := map[string]bool{}
		for _, e := range s.Entities {
			count++
			total += len(e.Data)
			if count > 2048 || total > 2<<20 || !validPRProofID(e.ID) || seen[e.ID] || len(e.Data) > 512<<10 || (e.CandidateSHA != "" && !fixedSHA(e.CandidateSHA)) || !json.Valid(e.Data) || !utf8.Valid(e.Data) {
				return nil, errors.New("bounded, unique PR resource entities required")
			}
			seen[e.ID] = true
			var value any
			decoder := json.NewDecoder(bytes.NewReader(e.Data))
			decoder.UseNumber()
			if err := decoder.Decode(&value); err != nil {
				return nil, err
			}
			data, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			copy.Entities = append(copy.Entities, PRResourceEntity{ID: e.ID, CandidateSHA: e.CandidateSHA, Data: data})
		}
		sort.Slice(copy.Entities, func(i, j int) bool { return copy.Entities[i].ID < copy.Entities[j].ID })
		result = append(result, copy)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Kind < result[j].Kind })
	return result, nil
}

// RecordPRResourceScan requires a fresh read after every CAS conflict. Retrying
// an old snapshot with a new expected digest would defeat the ordering fence.
// Failed/partial sections retain their previous entities; only complete sections
// can prove absence/deletion. No API result here becomes a review/merge gate.
func (k *KernelStore) RecordPRResourceScan(ctx context.Context, expected string, scan PRResourceScan) (PRResourceUpdateReceipt, error) {
	var receipt PRResourceUpdateReceipt
	if !validPRProofID(scan.ID) || scan.RepositoryID <= 0 || scan.PRNumber < 1 || !fixedSHA(scan.HeadSHA) || scan.StartedAt.IsZero() || scan.StartedAt.After(k.clock()) {
		return receipt, errors.New("versioned PR resource scan identity required")
	}
	sections, err := canonicalResourceSections(scan.Sections)
	if err != nil {
		return receipt, err
	}
	scan.Sections, scan.StartedAt = sections, scan.StartedAt.UTC()
	inputDigest, err := contentDigest(struct {
		Expected string
		Scan     PRResourceScan
	}{expected, scan})
	if err != nil {
		return receipt, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "product_pr_scan", scan.ID, &receipt)
	if err == nil {
		if receipt.InputDigest != inputDigest {
			return receipt, errors.New("PR scan ID replay conflict")
		}
		return receipt, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	key := fmt.Sprintf("github:%d:pull:%d", scan.RepositoryID, scan.PRNumber)
	var binding PRResourceBinding
	if err = completionRecord(ctx, tx, "product_pr_resource", key, &binding); err != nil {
		return receipt, err
	}
	if binding.ResourceKey != key || binding.RepositoryID != scan.RepositoryID || binding.Number != scan.PRNumber {
		return receipt, errors.New("PR scan resource identity conflict")
	}
	var old PRResourceState
	err = completionRecord(ctx, tx, "product_pr_state", key, &old)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	if err == nil {
		digest, e := resourceFactDigest(old.HeadSHA, old.Sections)
		if e != nil {
			return receipt, e
		}
		if old.Resource != binding || digest != old.Digest {
			return receipt, errors.New("saved PR resource state identity conflict")
		}
	}
	if old.Digest != expected || scan.StartedAt.Before(old.LastScanStarted) {
		return receipt, errors.New("PR scan conflict; fetch a fresh resource snapshot")
	}
	previous := map[string]PRResourceSection{}
	for _, s := range old.Sections {
		previous[s.Kind] = s
	}
	changes := []PRResourceChange{}
	if old.HeadSHA != scan.HeadSHA {
		changes = append(changes, PRResourceChange{Kind: "pr", EntityID: fmt.Sprint(scan.PRNumber), Action: "head_changed", BeforeDigest: old.HeadSHA, AfterDigest: scan.HeadSHA, CandidateSHA: scan.HeadSHA})
	}
	for i, s := range sections {
		before := previous[s.Kind]
		if s.Availability != before.Availability || s.Reason != before.Reason {
			changes = append(changes, PRResourceChange{Kind: s.Kind, Action: "availability"})
		}
		if s.Availability == "unavailable" {
			sections[i].Entities = before.Entities
			continue
		}
		prior := map[string]PRResourceEntity{}
		for _, e := range before.Entities {
			prior[e.ID] = e
		}
		for _, e := range s.Entities {
			was, exists := prior[e.ID]
			delete(prior, e.ID)
			if exists && reflect.DeepEqual(was, e) {
				continue
			}
			afterDigest, err := contentDigest(e)
			if err != nil {
				return receipt, err
			}
			change := PRResourceChange{Kind: s.Kind, EntityID: e.ID, Action: "added", AfterDigest: afterDigest, CandidateSHA: e.CandidateSHA}
			if exists {
				change.Action = "updated"
				change.BeforeDigest, err = contentDigest(was)
				if err != nil {
					return receipt, err
				}
			}
			changes = append(changes, change)
		}
		for _, e := range prior {
			digest, err := contentDigest(e)
			if err != nil {
				return receipt, err
			}
			changes = append(changes, PRResourceChange{Kind: s.Kind, EntityID: e.ID, Action: "deleted", BeforeDigest: digest, CandidateSHA: e.CandidateSHA})
		}
	}
	sort.Slice(changes, func(i, j int) bool {
		a, b := changes[i], changes[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.EntityID != b.EntityID {
			return a.EntityID < b.EntityID
		}
		return a.Action < b.Action
	})
	state := PRResourceState{Resource: binding, HeadSHA: scan.HeadSHA, Sections: sections, LastScanStarted: scan.StartedAt}
	retained := 0
	for _, s := range sections {
		retained += len(s.Entities)
	}
	if retained > 2048 {
		return receipt, errors.New("retained PR resource entities exceed budget")
	}
	state.Digest, err = resourceFactDigest(state.HeadSHA, state.Sections)
	if err != nil {
		return receipt, err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return receipt, err
	}
	if len(raw) > 4<<20 {
		return receipt, errors.New("retained PR resource snapshot exceeds budget")
	}
	versionID := structureID("pr-state-", key, state.Digest)
	var priorVersion PRResourceState
	err = completionRecord(ctx, tx, "product_pr_state_version", versionID, &priorVersion)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		if err = kernelRecordTx(ctx, tx, "product_pr_state_version", versionID, binding.WaveID, state); err != nil {
			return receipt, err
		}
	} else if priorVersion.Resource != binding || priorVersion.Digest != state.Digest || priorVersion.HeadSHA != state.HeadSHA || !reflect.DeepEqual(priorVersion.Sections, state.Sections) {
		return receipt, errors.New("PR resource version conflict")
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_state", key, binding.WaveID, state); err != nil {
		return receipt, err
	}
	notice := "unchanged"
	if len(changes) > 0 {
		notice = "pending"
	}
	receipt = PRResourceUpdateReceipt{ScanID: scan.ID, InputDigest: inputDigest, ResourceKey: key, PreviousDigest: old.Digest, Digest: state.Digest, Changes: changes, NotificationState: notice}
	if err = kernelRecordTx(ctx, tx, "product_pr_scan", scan.ID, binding.WaveID, receipt); err != nil {
		return receipt, err
	}
	return receipt, tx.Commit()
}

func (k *KernelStore) PRResourceState(ctx context.Context, key string) (PRResourceState, error) {
	var state PRResourceState
	err := k.Record(ctx, "product_pr_state", key, &state)
	if err != nil {
		return state, err
	}
	digest, err := resourceFactDigest(state.HeadSHA, state.Sections)
	if err == nil && (state.Resource.ResourceKey != key || digest != state.Digest) {
		err = errors.New("PR resource state digest mismatch")
	}
	return state, err
}
