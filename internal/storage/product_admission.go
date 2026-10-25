package storage

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ProductWaveRequest is authored by the trusted source controller. External
// Issue text and model tools cannot supply policy or repository ownership.
type ProductWaveRequest struct {
	Wave               WaveRequest                 `json:"wave"`
	WorkItems          []ProductWorkItemDefinition `json:"work_items"`
	SourcePolicyDigest string                      `json:"source_policy_digest,omitempty"`
}

type productAdmissionValue struct {
	WaveID        string            `json:"wave_id"`
	RequestDigest string            `json:"request_digest"`
	Source        string            `json:"source"`
	SourceID      string            `json:"source_id"`
	Registrations map[string]string `json:"registrations"`
}
type productAdmissionReceipt struct {
	Value  productAdmissionValue `json:"value"`
	Digest string                `json:"digest"`
}

// CreateProductWave publishes the initial Wave and all ownership metadata in
// one transaction. Exact source retries return the current Wave without
// restoring old definitions, emitting events or resetting control state.
func (k *KernelStore) CreateProductWave(ctx context.Context, request ProductWaveRequest) (domain.Wave, error) {
	if request.SourcePolicyDigest != "" && !validSourceDigest(request.SourcePolicyDigest) {
		return domain.Wave{}, errors.New("valid source policy digest required")
	}
	if request.Wave.Policy.Stage != "P" || !request.Wave.Policy.Product || len(request.WorkItems) < 1 || len(request.WorkItems) > maxProductWorkItems {
		return domain.Wave{}, errors.New("trusted P request and bounded WorkItem list required")
	}
	if request.Wave.SourceID == "" || len(request.Wave.SourceID) > 4096 || !utf8.ValidString(request.Wave.SourceID) || strings.ContainsAny(request.Wave.SourceID, "\x00\r\n") {
		return domain.Wave{}, errors.New("bounded stable product source identity required")
	}
	request.WorkItems = append([]ProductWorkItemDefinition{}, request.WorkItems...)
	for i := range request.WorkItems {
		request.WorkItems[i].Criteria = append([]string{}, request.WorkItems[i].Criteria...)
		sort.Strings(request.WorkItems[i].Criteria)
	}
	sort.Slice(request.WorkItems, func(i, j int) bool { return request.WorkItems[i].ID < request.WorkItems[j].ID })
	for i := 1; i < len(request.WorkItems); i++ {
		if request.WorkItems[i-1].ID == request.WorkItems[i].ID {
			return domain.Wave{}, errors.New("duplicate product WorkItem identity")
		}
	}
	digest, err := contentDigest(request)
	if err != nil {
		return domain.Wave{}, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return domain.Wave{}, err
	}
	defer tx.Rollback()
	if request.Wave.Source == "github" {
		if err := rejectProductSplitChildRoot(ctx, tx, request.Wave.SourceID); err != nil {
			return domain.Wave{}, err
		}
	}
	for _, definition := range request.WorkItems {
		if err := rejectProductSplitChildRoot(ctx, tx, definition.ID); err != nil {
			return domain.Wave{}, err
		}
	}
	receiptID := structureID("product-admission-", request.Wave.Source, request.Wave.SourceID)
	var receipt productAdmissionReceipt
	err = taskEvidenceRecord(ctx, tx, "product_wave_admission", receiptID, &receipt)
	if err == nil {
		receiptDigest, err := contentDigest(receipt.Value)
		if err != nil {
			return domain.Wave{}, err
		}
		if receipt.Digest != receiptDigest || receipt.Value.RequestDigest != digest || receipt.Value.Source != request.Wave.Source || receipt.Value.SourceID != request.Wave.SourceID || len(receipt.Value.Registrations) != len(request.WorkItems) {
			return domain.Wave{}, errors.New("product source admission identity conflict")
		}
		wave, err := loadWave(ctx, tx, receipt.Value.WaveID)
		if err != nil {
			return wave, err
		}
		if wave.Source != receipt.Value.Source || wave.SourceID != receipt.Value.SourceID || wave.OrchestratorSessionID != request.Wave.OrchestratorSessionID {
			return domain.Wave{}, errors.New("product source admission Wave conflict")
		}
		return wave, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.Wave{}, err
	}
	// An already visible Wave cannot be silently retrofitted with a new initial
	// source snapshot; its first input may already have been reserved.
	var existing string
	err = tx.QueryRowContext(ctx, "SELECT id FROM kernel_waves WHERE source=? AND source_id=?", request.Wave.Source, request.Wave.SourceID).Scan(&existing)
	if err == nil {
		return domain.Wave{}, errors.New("existing Wave requires explicit source revision, not initial product admission")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.Wave{}, err
	}
	wave, err := k.createWaveTx(ctx, tx, request.Wave)
	if err != nil {
		return wave, err
	}
	if request.SourcePolicyDigest != "" {
		if err := saveProductSourcePolicyTx(ctx, tx, wave, request.SourcePolicyDigest); err != nil {
			return domain.Wave{}, err
		}
	}
	receipt.Value = productAdmissionValue{WaveID: wave.ID, RequestDigest: digest, Source: wave.Source, SourceID: wave.SourceID, Registrations: map[string]string{}}
	for _, definition := range request.WorkItems {
		saved, err := k.registerProductWorkItemTx(ctx, tx, wave.Target(), "", definition)
		if err != nil {
			return domain.Wave{}, err
		}
		receipt.Value.Registrations[definition.ID] = saved.Digest
	}
	receipt.Digest, err = contentDigest(receipt.Value)
	if err != nil {
		return domain.Wave{}, err
	}
	if err := kernelRecordTx(ctx, tx, "product_wave_admission", receiptID, wave.ID, receipt); err != nil {
		return domain.Wave{}, err
	}
	return wave, tx.Commit()
}
