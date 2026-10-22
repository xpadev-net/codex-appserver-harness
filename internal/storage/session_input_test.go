package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func inputContext(t *testing.T, snapshot contract.InputSnapshot) SessionContext {
	t.Helper()
	var envelope struct {
		Context SessionContext `json:"context"`
	}
	if err := json.Unmarshal(snapshot.Input, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Context
}
func readSavedInput(t *testing.T, k *KernelStore, session string, ref InputReference) sessionManifest {
	t.Helper()
	r := contract.ContextReadRequest{SchemaVersion: 1, Target: ref.Target, Section: "manifest", ReferenceID: ref.ID, Limit: contract.MaxContextRead}
	var raw strings.Builder
	for {
		slice, err := k.ReadContext(context.Background(), session, r)
		if err != nil {
			t.Fatal(err)
		}
		if slice.Digest != ref.Digest || slice.Bytes != ref.Bytes {
			t.Fatal("manifest reference mismatch")
		}
		raw.WriteString(slice.Content)
		if slice.NextOffset == nil {
			break
		}
		r.Offset = *slice.NextOffset
	}
	if fmt.Sprintf("%x", sha256.Sum256([]byte(raw.String()))) != ref.Digest {
		t.Fatal("saved input bytes changed")
	}
	var m sessionManifest
	if err := json.Unmarshal([]byte(raw.String()), &m); err != nil {
		t.Fatal(err)
	}
	return m
}
func reserveInputRun(t *testing.T, k *KernelStore, session string, snapshot contract.InputSnapshot) (contract.SessionBinding, contract.SessionRun) {
	t.Helper()
	b := prepareWakeSession(t, k, session)
	run := contract.SessionRun{ID: "input-run-" + session, ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err := k.ReserveSessionRun(context.Background(), b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	return b, run
}
func TestSessionInputPersistsLargeExactFactsAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.OriginalRequest = strings.Repeat("元要求 <tag>\n\x00 😀", 5000)
	request.Criteria[0].Requirement = strings.Repeat("受入条件", 1000)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if err = contract.ValidateSessionInput(snapshot.Input); err != nil || len(snapshot.Input) >= contract.MaxSessionInput {
		t.Fatal(len(snapshot.Input), err)
	}
	state := inputContext(t, snapshot)
	if len(state.Waves) != 1 || state.Waves[0].PreviewComplete || len(state.Waves[0].RequestPreview) > 256 || state.Waves[0].Budget.TokenLimit != w.Policy.Limits.Tokens {
		t.Fatal(state)
	}
	ref := state.Waves[0].Manifest
	if ref.Bytes <= contract.MaxSessionInput {
		t.Fatal("full input was silently truncated", ref)
	}
	var runs, attempts int
	k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs)
	k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind IN ('task_attempt','main_turn','tokens')").Scan(&attempts)
	if runs != 0 || attempts != 0 {
		t.Fatal("building input launched or charged work", runs, attempts)
	}
	replay, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil || !reflect.DeepEqual(snapshot, replay) {
		t.Fatal("same facts changed input", err)
	}
	if _, err = k.ReadContext(ctx, "main", contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "manifest", ReferenceID: ref.ID, Limit: 2048}); err == nil {
		t.Fatal("unbound manifest read")
	}
	b, run := reserveInputRun(t, k, "main", snapshot)
	before := readSavedInput(t, k, "main", ref)
	if before.Wave.OriginalRequest != request.OriginalRequest || !reflect.DeepEqual(before.Wave.Criteria, request.Criteria) || before.Plan == nil || len(before.Tasks) != 2 || len(before.Subscriptions) != 2 {
		t.Fatal("missing authoritative input facts")
	}
	// A subsequent DB event changes current facts without rewriting judgment input.
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: before.Tasks[0].SessionID, Target: w.Target(), Key: "later", Recipient: "parent", Body: "new question"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	after := readSavedInput(t, k, "main", ref)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("restart changed saved judgment facts")
	}
	call := kernelCall(t, b, run, "context_read", contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "manifest", ReferenceID: ref.ID, Limit: 2048})
	if _, err = (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
		t.Fatal("saved manifest not available to actual tool source", err)
	}
	// Tampering must be rejected before content is exposed.
	var saved savedInputManifest
	if err = k.Record(ctx, "session_input_manifest", ref.ID, &saved); err != nil {
		t.Fatal(err)
	}
	saved.Data[0] = '!'
	if err = k.Put(ctx, "session_input_manifest", ref.ID, b.ID, saved); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReadContext(ctx, "main", contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "manifest", ReferenceID: ref.ID, Limit: 2048}); err == nil {
		t.Fatal("corrupt manifest read")
	}
}

func TestTaskInputFiltersPeersArtifactsAndCrossSessionReferences(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := mailboxFixture(t)
	// Only the owner sees the ungranted external artifact. A Task's mere awareness
	// of a digest/reference never becomes an artifact or manifest grant.
	a := mustImport(t, k, ArtifactImport{SessionID: "main", Target: w.Target(), Key: "private-artifact", Kind: "note", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "source"}}, "private report")
	mainInput, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	mainRef := inputContext(t, mainInput).Waves[0].Manifest
	taskInput, err := k.BuildSessionInput(ctx, tasks[0].SessionID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	ref := inputContext(t, taskInput).Waves[0].Manifest
	b, run := reserveInputRun(t, k, tasks[0].SessionID, taskInput)
	m := readSavedInput(t, k, b.ID, ref)
	if m.Plan != nil || len(m.Tasks) != 1 || m.Tasks[0].ID != b.TaskID || len(m.Artifacts) != 0 || len(m.Results) != 0 || len(m.Subscriptions) != 1 {
		t.Fatal("Task saw ungranted peer state", m)
	}
	call := kernelCall(t, b, run, "context_read", contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "manifest", ReferenceID: mainRef.ID, Limit: 2048})
	if _, err = (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("Task read owning main manifest")
	}
	if _, _, err = k.ReadArtifact(ctx, b.ID, w.Target(), a.ID, 0, 16); err == nil {
		t.Fatal("snapshot changed artifact grants")
	}
	for _, targets := range [][]contract.Target{nil, {w.Target(), w.Target()}, {{WaveID: w.ID, ScopeRevision: 2, PlanRevision: 1, PolicyRevision: 1}}} {
		if _, err = k.BuildSessionInput(ctx, "main", targets); err == nil {
			t.Fatal("invalid input targets", targets)
		}
	}
}

func TestSessionInputManifestAndWakeAreAtomic(t *testing.T) {
	ctx := context.Background()
	k, w, task, _, request := wakeFixture(t)
	request.IncludeSessionContext = true
	if _, err := k.DB.Exec("CREATE TRIGGER reject_input_grant BEFORE INSERT ON records WHEN NEW.kind='session_input_references' BEGIN SELECT RAISE(ABORT,'input grant failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveWake(ctx, request); err == nil {
		t.Fatal("partial input accepted")
	}
	for _, table := range []string{"kernel_snapshots", "kernel_wake_intents"} {
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal(table, count, err)
		}
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('session_input_manifest','session_input_references')").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER reject_input_grant"); err != nil {
		t.Fatal(err)
	}
	// A valid supplemental context can overflow the final envelope. The caller
	// must choose a smaller batch/context; no notification or manifest is lost.
	request.Context, _ = json.Marshal([]string{strings.Repeat("x", 16384), strings.Repeat("x", 16384), strings.Repeat("x", 16384), strings.Repeat("x", 16000)})
	if err := contract.ValidateSessionInput(request.Context); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveWake(ctx, request); err == nil {
		t.Fatal("oversized composed input accepted")
	}
	var state string
	if err := k.DB.QueryRow("SELECT state FROM kernel_notifications WHERE id=?", request.NotificationIDs[0]).Scan(&state); err != nil || state != "pending" {
		t.Fatal(state, err)
	}
	request.Context = json.RawMessage(`{}`)
	wake, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var input wakeInput
	if err = json.Unmarshal(wake.Snapshot.Input, &input); err != nil {
		t.Fatal(err)
	}
	var combined struct {
		State SessionContext `json:"state"`
	}
	if err = json.Unmarshal(input.Context, &combined); err != nil || len(input.Messages) != 1 || len(combined.State.Waves) != 1 || combined.State.Waves[0].Target != w.Target() {
		t.Fatal(input, err)
	}
	replay, err := k.ReserveWake(ctx, request)
	if err != nil || !reflect.DeepEqual(wake, replay) {
		t.Fatal(replay, err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	run := wakeStartingRun(k, b, wake)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	readSavedInput(t, k, b.ID, combined.State.Waves[0].Manifest)
}

func TestDependencyInputKeepsResultRemainderAndEvidence(t *testing.T) {
	ctx := context.Background()
	k, b, run, result := recordedTaskResultFixture(t, func(r *contract.TaskResult) { r.Remaining = []string{"verify independent interpretation"} })
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := k.Tasks(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var child domain.Task
	for _, task := range tasks {
		if task.ID != b.TaskID {
			child = task
		}
	}
	snapshot, err := k.BuildSessionInput(ctx, child.SessionID, []contract.Target{child.Target()})
	if err != nil {
		t.Fatal(err)
	}
	reserveInputRun(t, k, child.SessionID, snapshot)
	m := readSavedInput(t, k, child.SessionID, inputContext(t, snapshot).Waves[0].Manifest)
	saved, ok := m.Results[receipt.ResultID]
	if !ok || len(m.Tasks) != 2 || !reflect.DeepEqual(saved.Remaining, result.Remaining) || len(m.Artifacts) != 1 || m.Artifacts[0].ID != result.Artifacts[0] || m.Artifacts[0].Digest == "" {
		t.Fatal("dependency evidence or remaining work lost", m)
	}
}

func TestSessionInputUsesAppliedDirectionAndCurrentPlanOnly(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := mailboxFixture(t)
	d := mustDirection(t, k, w)
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	var queued savedInputManifest
	if err = k.Record(ctx, "session_input_manifest", inputContext(t, snapshot).Waves[0].Manifest.ID, &queued); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(queued.Data), d.Request.Instruction) {
		t.Fatal("queued steering instruction became input")
	}
	if _, err = k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		phase := &plan.Phases[i]
		phase.ID = "new-" + phase.ID
		for j := range phase.Tasks {
			task := &phase.Tasks[j]
			task.ID = "new-" + task.ID
			for d := range task.DependsOn {
				task.DependsOn[d].TaskID = "new-" + task.DependsOn[d].TaskID
			}
		}
		for j := range phase.Completion.RequiredTaskIDs {
			phase.Completion.RequiredTaskIDs[j] = "new-" + phase.Completion.RequiredTaskIDs[j]
		}
	}
	if _, err = k.AdoptPlan(ctx, w.Target(), "new", plan); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	reserveInputRun(t, k, "main", snapshot)
	m := readSavedInput(t, k, "main", inputContext(t, snapshot).Waves[0].Manifest)
	if m.Direction == nil || m.Direction.Request.Instruction != d.Request.Instruction || m.Plan == nil || m.Plan.Target != w.Target() || len(m.Tasks) != 2 {
		t.Fatal("applied direction/current plan lost", m)
	}
	for _, task := range m.Tasks {
		for _, old := range tasks {
			if task.ID == old.ID {
				t.Fatal("retired Task leaked into current input")
			}
		}
	}
}

func TestInputGrantIsExactSnapshotAndMultiWaveBoundariesStaySorted(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	first, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	request.SourceID = "second"
	second, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	targets := []contract.Target{second.Target(), first.Target()}
	old, err := k.BuildSessionInput(ctx, "main", targets)
	if err != nil {
		t.Fatal(err)
	}
	oldRef := inputContext(t, old).Waves[0].Manifest
	w, err := k.Wave(ctx, oldRef.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	mustImport(t, k, ArtifactImport{SessionID: "main", Target: w.Target(), Key: "later-input", Kind: "note", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "latest"}}, "new evidence")
	current, err := k.BuildSessionInput(ctx, "main", targets)
	if err != nil {
		t.Fatal(err)
	}
	state := inputContext(t, current)
	if len(state.Waves) != 2 || state.Waves[0].Target.WaveID >= state.Waves[1].Target.WaveID || state.Waves[0].Manifest.ID == oldRef.ID {
		t.Fatal("input scopes not stable or new facts omitted", state)
	}
	reserveInputRun(t, k, "main", current)
	if _, err = k.ReadContext(ctx, "main", contract.ContextReadRequest{SchemaVersion: 1, Target: oldRef.Target, Section: "manifest", ReferenceID: oldRef.ID, Limit: 2048}); err == nil {
		t.Fatal("old snapshot reference was treated as a current grant")
	}
	for _, wave := range state.Waves {
		readSavedInput(t, k, "main", wave.Manifest)
	}
}

func storedManifest(t *testing.T, k *KernelStore, snapshot contract.InputSnapshot) sessionManifest {
	t.Helper()
	ref := inputContext(t, snapshot).Waves[0].Manifest
	var saved savedInputManifest
	if err := k.Record(context.Background(), "session_input_manifest", ref.ID, &saved); err != nil {
		t.Fatal(err)
	}
	var m sessionManifest
	if err := json.Unmarshal(saved.Data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}
func TestInputPreservesAppliedNonterminalTaskResults(t *testing.T) {
	for _, outcome := range []string{"waiting", "blocked"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			k, b, run, result := recordedTaskResultFixture(t, func(r *contract.TaskResult) {
				r.Outcome = outcome
				r.Remaining = []string{"retain this unfinished work"}
				r.Blockers = []string{"await source clarification"}
				if outcome == "waiting" {
					session := strings.Replace(r.TaskID, "task-", "session-", 1)
					r.SubscriptionIDs = []string{mailboxSubscriptionID(session, r.Target, r.TaskID)}
				}
			})
			if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			receipt, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
			if err != nil || receipt.ResultID == "" || string(receipt.State) != outcome {
				t.Fatal(receipt, err)
			}
			task, err := k.Task(ctx, b.TaskID)
			if err != nil || task.ResultID != "" {
				t.Fatal(task, err)
			}
			for _, session := range []string{b.ID, b.ParentSessionID} {
				snapshot, err := k.BuildSessionInput(ctx, session, []contract.Target{b.Target})
				if err != nil {
					t.Fatal(err)
				}
				m := storedManifest(t, k, snapshot)
				got, ok := m.Results[receipt.ResultID]
				if !ok || !reflect.DeepEqual(got.Remaining, result.Remaining) || !reflect.DeepEqual(got.Blockers, result.Blockers) || !reflect.DeepEqual(got.CriterionResults, result.CriterionResults) || len(m.TaskApplications) != 1 || m.TaskApplications[0].RunID != run.ID {
					t.Fatal("nonterminal result dropped", m)
				}
			}
		})
	}
}
func TestInputPreservesOnlyAppliedMainDecisionsForThisScope(t *testing.T) {
	ctx := context.Background()
	k, b, waves, run, _ := mainResultFixture(t)
	result := mainResult(t, b, waves, "report")
	for i := range result.Decisions {
		result.Decisions[i].Remaining = []string{"remaining-" + waves[i].ID}
		result.Decisions[i].Blockers = []string{"blocker-" + waves[i].ID}
	}
	saveMainCompletion(t, k, b, run, result, true)
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	for i, w := range waves {
		snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
		if err != nil {
			t.Fatal(err)
		}
		m := storedManifest(t, k, snapshot)
		if len(m.MainDecisions) != 1 || m.MainDecisions[0].RunID != run.ID || m.MainDecisions[0].Decision.Target != w.Target() || !reflect.DeepEqual(m.MainDecisions[0].Decision.Remaining, result.Decisions[i].Remaining) || !reflect.DeepEqual(m.MainDecisions[0].Decision.Blockers, result.Decisions[i].Blockers) {
			t.Fatal("main remaining work lost or crossed Wave boundary", m.MainDecisions)
		}
	}
	d := mustDirection(t, k, waves[0])
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, waves[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{current.Target()})
	if err != nil {
		t.Fatal(err)
	}
	m := storedManifest(t, k, snapshot)
	if len(m.MainDecisions) != 0 {
		t.Fatal("old-scope decisions adopted as current input", m.MainDecisions)
	}
}
func TestDirectionInputOmitsObsoletePlanBeforeReplanning(t *testing.T) {
	ctx := context.Background()
	k, w, _ := mailboxFixture(t)
	d := mustDirection(t, k, w)
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	m := storedManifest(t, k, snapshot)
	if m.Plan != nil || len(m.Tasks) != 0 || m.Direction == nil || m.Direction.Request.Instruction != d.Request.Instruction {
		t.Fatal("superseded plan leaked into new direction input", m)
	}
}

func TestTemplateFixInputRetainsItsFindingAssignment(t *testing.T) {
	ctx := context.Background()
	first := kernelTemplate()
	second := kernelTemplate()
	second.ID = "test-owner"
	second.Classifications = []string{"test_defect"}
	first.FixInstruction = "fix assigned findings"
	second.FixInstruction = first.FixInstruction
	k, w, source, _ := templateFixture(t, false, []contract.FixReviewTemplate{first, second}, []string{"artifact_defect", "test_defect"})
	expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: source.ID})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]string{}
	for _, id := range expansion.Admission.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Spec.Kind != "fix" {
			continue
		}
		snapshot, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{task.Target()})
		if err != nil {
			t.Fatal(err)
		}
		m := storedManifest(t, k, snapshot)
		var assignment struct {
			TemplateID    string   `json:"template_id"`
			FindingIDs    []string `json:"finding_ids"`
			SourceResults []string `json:"source_result_ids"`
			InputDigest   string   `json:"input_digest"`
			ExpansionKey  string   `json:"expansion_key"`
		}
		if err = json.Unmarshal(m.TemplateContexts[task.ID], &assignment); err != nil {
			t.Fatal(err)
		}
		if assignment.InputDigest != expansion.InputDigest || assignment.ExpansionKey != expansion.Key || len(assignment.SourceResults) == 0 {
			t.Fatal("unbound template assignment", assignment)
		}
		for _, resultID := range assignment.SourceResults {
			if _, ok := m.Results[resultID]; !ok {
				t.Fatal("assignment source findings absent", resultID)
			}
		}
		found[assignment.TemplateID] = assignment.FindingIDs
	}
	if !reflect.DeepEqual(found[first.ID], []string{"finding-0"}) || !reflect.DeepEqual(found[second.ID], []string{"finding-1"}) {
		t.Fatal("template responsibilities lost", found)
	}
}
