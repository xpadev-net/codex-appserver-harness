package kernelcli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelCLIRunsAndOutputPreserveArchivedPhysicalEvidence(t *testing.T) {
	ctx := context.Background()
	root, k, b, run, snapshot := recoveryMainFixture(t, "completed")
	r := storage.MainRunRejectionRequest{Key: "reject-for-inspection", Expected: b, RunID: run.ID, Snapshot: snapshot, Reason: "explicit invalid output rejection"}
	if _, err := cliCall(t, "reject-main", "--root", root, "--file", operatorFile(t, r)); err != nil {
		t.Fatal(err)
	}
	before, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cliCall(t, "runs", "--root", root, "--limit", "1")
	if err != nil {
		t.Fatal(err)
	}
	var page storage.RunHistoryPage
	if err = json.Unmarshal(raw, &page); err != nil || len(page.Entries) != 1 || !page.Entries[0].Disposed || page.Entries[0].Active || page.Entries[0].Disposition == nil || page.Entries[0].Disposition.State != "archived" || bytes.Contains(raw, []byte(run.Final)) {
		t.Fatal(string(raw), err)
	}
	raw, err = cliCall(t, "output", "--root", root, "--id", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var excerpt RunOutputExcerpt
	if err = json.Unmarshal(raw, &excerpt); err != nil || !excerpt.Done || excerpt.Data != run.Final || excerpt.Entry.Run.State != run.State || excerpt.Entry.Disposition.State != "archived" {
		t.Fatal(string(raw), err)
	}
	next, err := cliCall(t, "runs", "--root", root, "--limit", "1", "--cursor", fmt.Sprint(page.Cursor))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(next, &page); err != nil || len(page.Entries) != 0 || page.More {
		t.Fatal(string(next), err)
	}
	after, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("inspection changed rotation/run state", after, err)
	}
}
func TestKernelCLIOutputBytePagingPreservesUnknownUnicode(t *testing.T) {
	ctx := context.Background()
	root, k, b, run, _ := recoveryMainFixture(t, "unknown")
	run.Final = "保存済み結果"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	actual := []byte{}
	for offset := int64(0); offset < int64(len(run.Final)); {
		raw, err := cliCall(t, "output", "--root", root, "--id", run.ID, "--offset", fmt.Sprint(offset), "--bytes", "1")
		if err != nil {
			t.Fatal(err)
		}
		var page RunOutputExcerpt
		if err = json.Unmarshal(raw, &page); err != nil || page.Entry.Run.State != "unknown" || page.Entry.Disposed || page.NextOffset != offset+1 {
			t.Fatal(string(raw), err)
		}
		chunk := []byte(page.Data)
		if page.Encoding == "base64" {
			chunk, err = base64.StdEncoding.DecodeString(page.Data)
		}
		if err != nil {
			t.Fatal(err)
		}
		actual = append(actual, chunk...)
		offset = page.NextOffset
	}
	if !bytes.Equal(actual, []byte(run.Final)) {
		t.Fatal("byte boundaries changed saved Unicode", actual)
	}
	for _, args := range [][]string{{"--offset", "-1"}, {"--offset", "999"}, {"--bytes", "16385"}, {"--session", b.ID}} {
		if _, err := cliCall(t, append([]string{"output", "--root", root, "--id", run.ID}, args...)...); err == nil {
			t.Fatal("invalid output flags accepted", args)
		}
	}
}
