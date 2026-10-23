package kernelcli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelCLIArtifactMetadataAndVerifiedTextBinaryExcerpts(t *testing.T) {
	root, w := controlWave(t)
	ctx := context.Background()
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for i, data := range [][]byte{[]byte("report: 根拠と結果"), {0xff, 0, 0xfe, 1}} {
		a, err := k.ImportArtifact(ctx, storage.ArtifactImport{SessionID: "main", Target: w.Target(), Key: fmt.Sprint("report-", i), Kind: "report", ContentType: "application/octet-stream", Origin: domain.ArtifactOrigin{Kind: "session", Reference: "main"}}, data)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := cliCall(t, "artifact", "--root", root, "--id", a.ID)
		if err != nil {
			t.Fatal(err)
		}
		var metadata domain.Artifact
		if err = json.Unmarshal(raw, &metadata); err != nil || !reflect.DeepEqual(a, metadata) {
			t.Fatal(string(raw), err)
		}
		raw, err = cliCall(t, "artifact", "--root", root, "--id", a.ID, "--content")
		if err != nil {
			t.Fatal(err)
		}
		var excerpt ArtifactExcerpt
		if err = json.Unmarshal(raw, &excerpt); err != nil || !excerpt.Done || excerpt.NextOffset != int64(len(data)) {
			t.Fatal(string(raw), err)
		}
		actual := []byte(excerpt.Data)
		if excerpt.Encoding == "base64" {
			actual, err = base64.StdEncoding.DecodeString(excerpt.Data)
		}
		if err != nil || !bytes.Equal(actual, data) {
			t.Fatal(excerpt, err)
		}
		for _, args := range [][]string{{"--offset", "1"}, {"--content", "--bytes", "16385"}, {"--session", "main"}} {
			if _, err = cliCall(t, append([]string{"artifact", "--root", root, "--id", a.ID}, args...)...); err == nil {
				t.Fatal("invalid Artifact flags accepted", args)
			}
		}
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.MainTurns != 0 {
		t.Fatal("Artifact read started main", current, err)
	}
}
func TestKernelCLIMailboxAndRepliesRemainUnacknowledged(t *testing.T) {
	root, w := controlWave(t)
	ctx := context.Background()
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	specs := []contract.TaskSpec{}
	for _, id := range []string{"first", "sibling"} {
		specs = append(specs, contract.TaskSpec{ID: id, Kind: "work", Instruction: "research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}})
	}
	plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{{ID: "work", Tasks: specs, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"first", "sibling"}}, FailurePolicy: "drain_and_block"}}}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	child, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	sibling, err := k.Task(ctx, admitted.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	question, err := k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: "main", Target: child.Target(), Key: "question", Recipient: child.ID, Body: "which evidence supports the report?"})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: child.SessionID, Target: child.Target(), Key: "reply", Recipient: "parent", Body: "the immutable report records the evidence", ReplyTo: question.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: child.SessionID, Target: child.Target(), Key: "second", Recipient: "parent", Body: "one additional observation"}); err != nil {
		t.Fatal(err)
	}
	before, err := k.Mailbox(ctx, child.SessionID, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cliCall(t, "mailbox", "--root", root, "--session", child.SessionID, "--limit", "1")
	if err != nil {
		t.Fatal(err)
	}
	var page MailboxPage
	if err = json.Unmarshal(raw, &page); err != nil || len(page.Entries) != 1 || !page.Entries[0].HasReply || page.Entries[0].DeliveryAcknowledged {
		t.Fatal(string(raw), err)
	}
	raw, err = cliCall(t, "mailbox", "--root", root, "--session", "main", "--limit", "1")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &page); err != nil || len(page.Entries) != 1 || page.Entries[0].Message.ID != reply.ID || !page.More {
		t.Fatal(string(raw), err)
	}
	raw, err = cliCall(t, "mailbox", "--root", root, "--session", "main", "--limit", "1", "--cursor", fmt.Sprint(page.Cursor))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &page); err != nil || len(page.Entries) != 1 || page.More || page.Entries[0].Message.ID == reply.ID {
		t.Fatal(string(raw), err)
	}
	raw, err = cliCall(t, "message", "--root", root, "--session", child.SessionID, "--id", reply.ID)
	if err != nil {
		t.Fatal(err)
	}
	var displayed storage.Message
	if err = json.Unmarshal(raw, &displayed); err != nil || displayed.Request.ReplyTo != question.ID {
		t.Fatal(displayed, err)
	}
	if _, err = cliCall(t, "message", "--root", root, "--session", sibling.SessionID, "--id", question.ID); err == nil {
		t.Fatal("sibling conversation read accepted")
	}
	after, err := k.Mailbox(ctx, child.SessionID, 0, 64)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("inspection acknowledged or applied message", after, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.MainTurns != 0 {
		t.Fatal("mailbox inspection started main", current, err)
	}
}

func TestKernelObservationDefaultsToConfiguredOrchestrator(t *testing.T) {
	root := filepath.Join(t.TempDir(), "kernel")
	if _, err := cliCall(t, "init", "--root", root, "--session", "coordinator", "--model", "unavailable-model"); err != nil {
		t.Fatal(err)
	}
	raw, err := cliCall(t, "session", "--root", root)
	if err != nil {
		t.Fatal(err)
	}
	var view storage.SessionRecoveryInspection
	if err = json.Unmarshal(raw, &view); err != nil || view.Session.Binding.ID != "coordinator" {
		t.Fatal(string(raw), err)
	}
	if _, err = cliCall(t, "mailbox", "--root", root); err != nil {
		t.Fatal(err)
	}
	if _, err = cliCall(t, "mailbox", "--root", root, "--session", "main"); err == nil {
		t.Fatal("explicit nonexistent session silently replaced")
	}
}
