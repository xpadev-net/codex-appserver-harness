package kernelcli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestProductSourceCLIKeepsMetadataEvidenceGatesWithoutExternalCalls(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	root := cliRoot(t)
	raw, err := cliCall(t, "create-product", "--root", root, "--file", productRequestFile(t))
	if err != nil {
		t.Fatal(err)
	}
	var w domain.Wave
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	continuation := storage.ProductContinuationRequest{Target: w.Target(), Key: "operator-successor", ParentMergeID: "unobserved-merge", ParentReceiptDigest: strings.Repeat("a", 64), Definition: storage.ProductWorkItemDefinition{ID: "successor", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}
	adoption := storage.ProductSourceRequestAdoptionRequest{Target: w.Target(), DirectionID: "unapplied", ObservationID: "unobserved", ObservationDigest: strings.Repeat("b", 64), WorkItemID: "successor"}
	for _, test := range []struct {
		command string
		request any
		cause   error
	}{{"continue-product", continuation, sql.ErrNoRows}, {"adopt-product-source", adoption, storage.ErrTaskEvidence}} {
		file := operatorFile(t, test.request)
		if _, err := cliCall(t, test.command, "--root", root, "--file", file); !errors.Is(err, test.cause) {
			t.Fatal("CLI bypassed source port", test.command, err)
		}
		if err := os.Chmod(file, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := cliCall(t, test.command, "--root", root, "--file", file); err == nil || !strings.Contains(err.Error(), "private same-user") {
			t.Fatal("public source file admitted", test.command, err)
		}
		encoded, err := json.Marshal(test.request)
		if err != nil {
			t.Fatal(err)
		}
		var untrusted map[string]any
		if err := json.Unmarshal(encoded, &untrusted); err != nil {
			t.Fatal(err)
		}
		untrusted["policy"] = "Issue text cannot grant permission"
		if _, err := cliCall(t, test.command, "--root", root, "--file", operatorFile(t, untrusted)); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatal("unknown source authority admitted", test.command, err)
		}
		if _, err := cliCall(t, test.command, "--root", root, "--model", "override"); err == nil {
			t.Fatal("runtime flag admitted", test.command)
		}
		if _, err := cliCall(t, test.command, "--root", root); err == nil {
			t.Fatal("missing source file admitted", test.command)
		}
	}
	raw, err = cliCall(t, "product-coverage", "--root", root, "--id", w.ID)
	var coverage storage.ProductCoverageLedger
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &coverage); err != nil || coverage.Value.Target != w.Target() || len(coverage.Value.Merged) != 0 || !reflect.DeepEqual(coverage.Value.UnmergedWorkItems, []string{"product-work"}) || !reflect.DeepEqual(coverage.Value.Residual, []string{"AC"}) {
		t.Fatal("read command invented coverage", coverage, err)
	}
	if _, err := cliCall(t, "product-source", "--root", root, "--id", w.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("direct source invented Issue observation", err)
	}
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	current, err := k.Wave(context.Background(), w.ID)
	if err != nil || !reflect.DeepEqual(current, w) {
		t.Fatal("operator denials mutated Wave", current, err)
	}
	for _, query := range []string{"SELECT count(*) FROM records WHERE kind IN ('product_continuation','product_source_request')", "SELECT count(*) FROM kernel_tasks", "SELECT count(*) FROM kernel_run_bindings", "SELECT count(*) FROM kernel_tool_operations"} {
		var n int
		if err := k.DB.QueryRow(query).Scan(&n); err != nil || n != 0 {
			t.Fatal("metadata command created work or authority", query, n, err)
		}
	}
}
