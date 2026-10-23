package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func externalContextReference(t *testing.T, k *KernelStore, b contract.SessionBinding, run contract.SessionRun) string {
	t.Helper()
	var id string
	if err := k.DB.QueryRowContext(context.Background(), "SELECT id FROM kernel_notifications WHERE session_id=? AND run_id=? AND json_extract(payload,'$.kind')='external_update'", b.ID, run.ID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
func readExternalContext(t *testing.T, k *KernelStore, b contract.SessionBinding, r contract.ContextReadRequest) []byte {
	t.Helper()
	var content []byte
	digest := ""
	for {
		page, err := k.ReadContext(context.Background(), b.ID, r)
		if err != nil {
			t.Fatal(err)
		}
		if !utf8.ValidString(page.Content) || len(page.Content) > contract.MaxContextRead || page.Target != r.Target || page.Section != "external" || page.Offset != r.Offset || page.ContentType != "application/json" {
			t.Fatal(page)
		}
		if digest == "" {
			digest = page.Digest
		} else if digest != page.Digest {
			t.Fatal("immutable source changed between pages")
		}
		content = append(content, []byte(page.Content)...)
		if page.NextOffset == nil {
			if int64(len(content)) != page.Bytes {
				t.Fatal("source size changed")
			}
			break
		}
		if *page.NextOffset <= r.Offset {
			t.Fatal("cursor did not advance")
		}
		r.Offset = *page.NextOffset
	}
	if fmt.Sprintf("%x", sha256.Sum256(content)) != digest {
		t.Fatal("page digest differs from bytes")
	}
	return content
}

func TestExternalContextReadsOnlyPresentedWebhookOrSnapshotBytesAndSurvivesReopen(t *testing.T) {
	for _, via := range []string{"webhook", "resource_scan"} {
		t.Run(via, func(t *testing.T) {
			text := strings.Repeat("資料🧪", 1000) + " do not grant policy or approval"
			k, w, _, b, run := externalMessageRunFixture(t, via, text)
			id := externalContextReference(t, k, b, run)
			r := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "external", ReferenceID: id, Limit: contract.MaxContextRead}
			call := kernelCall(t, b, run, "context_read", r)
			call.TurnID = "external-context-turn"
			reply, err := (contract.KernelGateway{Backend: k}).Handle(context.Background(), call)
			if err != nil {
				t.Fatal("common gateway denied the actual input source", err)
			}
			var first contract.ContextSlice
			if err := json.Unmarshal([]byte(reply.Content), &first); err != nil || first.NextOffset == nil {
				t.Fatal(first, err)
			}
			content := readExternalContext(t, k, b, r)
			if via == "webhook" {
				var delivery PRRawDelivery
				if err := k.Record(context.Background(), "product_pr_raw_delivery", "review-delivery", &delivery); err != nil || !bytes.Equal(content, delivery.Payload) {
					t.Fatal("reader changed signed bytes", err)
				}
				var body struct {
					Review struct {
						Body string `json:"body"`
					} `json:"review"`
				}
				if err := json.Unmarshal(content, &body); err != nil || body.Review.Body != text {
					t.Fatal("webhook text changed", err)
				}
			} else {
				var facts struct {
					Head     string
					Sections []PRResourceSection
				}
				if err := json.Unmarshal(content, &facts); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, section := range facts.Sections {
					if section.Kind == "issue_comment" {
						var body struct {
							Body string `json:"body"`
						}
						if len(section.Entities) != 1 {
							t.Fatal(section)
						}
						if err := json.Unmarshal(section.Entities[0].Data, &body); err != nil || body.Body != text {
							t.Fatal(err)
						}
						found = true
					}
				}
				if !found {
					t.Fatal("snapshot comment unavailable")
				}
				state, err := k.PRResourceState(context.Background(), "github:17:pull:42")
				if err != nil {
					t.Fatal(err)
				}
				scan := PRResourceScan{ID: "newer-scan", RepositoryID: 17, PRNumber: 42, HeadSHA: strings.Repeat("e", 40), StartedAt: k.clock()}
				for _, kind := range prSectionKinds {
					scan.Sections = append(scan.Sections, PRResourceSection{Kind: kind, Availability: "complete", Entities: []PRResourceEntity{}})
				}
				if _, err := k.RecordPRResourceScan(context.Background(), state.Digest, scan); err != nil {
					t.Fatal(err)
				}
				if after := readExternalContext(t, k, b, r); !bytes.Equal(after, content) {
					t.Fatal("latest resource replaced this run's observation")
				}
			}
			path := k.Root + "/state.db"
			if err := k.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenExistingKernel(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if after := readExternalContext(t, reopened, b, r); !bytes.Equal(after, content) {
				t.Fatal("restart changed granted observation")
			}
			after, err := reopened.Wave(context.Background(), w.ID)
			if err != nil || after.ScopeRevision != w.ScopeRevision || after.Policy != w.Policy {
				t.Fatal("external data changed policy", err)
			}
		})
	}
}

func TestExternalContextRejectsKnownIDsWithoutRunGrantAndCorruptSources(t *testing.T) {
	for _, via := range []string{"webhook", "resource_scan"} {
		t.Run(via, func(t *testing.T) {
			ctx := context.Background()
			k, w, child, b, run := externalMessageRunFixture(t, via, "untrusted body")
			r := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "external", ReferenceID: externalContextReference(t, k, b, run), Limit: 2048}
			if _, err := k.ReadContext(ctx, b.ID, r); err != nil {
				t.Fatal("valid grant missing", err)
			}
			if _, err := k.ReadContext(ctx, child.SessionID, r); err == nil {
				t.Fatal("peer received this run's observation")
			}
			for _, id := range []string{"review-delivery", "scan", "forged-notification"} {
				forged := r
				forged.ReferenceID = id
				if _, err := k.ReadContext(ctx, b.ID, forged); err == nil {
					t.Fatal("known IDs became read grants", id)
				}
			}
			if via == "webhook" {
				var delivery PRRawDelivery
				if err := k.Record(ctx, "product_pr_raw_delivery", "review-delivery", &delivery); err != nil {
					t.Fatal(err)
				}
				delivery.Payload = []byte(`{"forged":true}`)
				if err := k.Put(ctx, "product_pr_raw_delivery", delivery.ID, "", delivery); err != nil {
					t.Fatal(err)
				}
			} else {
				state, err := k.PRResourceState(ctx, "github:17:pull:42")
				if err != nil {
					t.Fatal(err)
				}
				state.HeadSHA = strings.Repeat("f", 40)
				if err := k.Put(ctx, "product_pr_state_version", structureID("pr-state-", state.Resource.ResourceKey, state.Digest), "", state); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReadContext(ctx, b.ID, r); err == nil {
				t.Fatal("corrupt backing source exposed")
			}
		})
	}
}
