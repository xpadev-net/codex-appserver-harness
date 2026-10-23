package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

func productSourceFixture(t *testing.T) (*KernelStore, domain.Wave, ProductSourceFacts) {
	t.Helper()
	k, request := waveStoreFixture(t)
	request.Source, request.SourceID = "github", "r17-i42"
	request.Policy.Stage, request.Policy.Product = "P", true
	w, err := k.CreateProductWave(context.Background(), ProductWaveRequest{Wave: request, SourcePolicyDigest: strings.Repeat("e", 64), WorkItems: []ProductWorkItemDefinition{{ID: request.SourceID, Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	return k, w, ProductSourceFacts{RepositoryID: 17, IssueNumber: 42, PolicyDigest: strings.Repeat("e", 64), Available: true, IssueState: "open", Ready: true, RequestDigest: ProductSourceRequestDigest(w.OriginalRequest), UpdatedAt: k.clock()}
}

func TestProductSourceObservationCASHistoryReopenAndManualPauseRemainIndependent(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	first, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil || first.Changed || first.Hold != nil || first.Snapshot.Value.Status != "ready" || first.Snapshot.Version != 1 {
		t.Fatal(first, err)
	}
	facts.Hold = true
	if _, err := k.ObserveProductSource(ctx, w.Target(), "stale", facts); err == nil {
		t.Fatal("stale source read replaced newer observation")
	}
	held, err := k.ObserveProductSource(ctx, w.Target(), first.Snapshot.Digest, facts)
	if err != nil || !held.Changed || held.Hold == nil || held.Hold.Reason != "hold" || held.Snapshot.Version != 2 {
		t.Fatal(held, err)
	}
	mustControl(t, k, operatorRequest(w, "manual-pause-source", "pause", ""))
	mustControl(t, k, operatorRequest(w, "manual-resume-source", "resume", ""))
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.Paused || !current.DispatchPaused() || current.SourceHold == nil {
		t.Fatal("manual resume removed source prohibition", current, err)
	}
	mustControl(t, k, operatorRequest(w, "manual-pause-again", "pause", ""))
	facts.Hold = false
	ready, err := k.ObserveProductSource(ctx, w.Target(), held.Snapshot.Digest, facts)
	if err != nil || !ready.Changed || ready.Hold != nil || ready.Snapshot.Version != 3 {
		t.Fatal(ready, err)
	}
	current, err = k.Wave(ctx, w.ID)
	if err != nil || !current.Paused || !current.DispatchPaused() || current.SourceHold != nil {
		t.Fatal("source ready resumed operator pause", current, err)
	}
	var histories int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_source_observation_history'").Scan(&histories); err != nil || histories != 3 {
		t.Fatal(histories, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil || replay.Changed || !reflect.DeepEqual(ready.Snapshot, replay.Snapshot) {
		t.Fatal("exact retry rewrote source history", replay, err)
	}
}

func TestProductSourceObservationReasonsAndCurrentScopeCannotBeReinterpreted(t *testing.T) {
	for _, mode := range []string{"unavailable", "closed", "hold", "not_ready", "request_changed", "scope_unconfirmed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, facts := productSourceFixture(t)
			switch mode {
			case "unavailable":
				facts = ProductSourceFacts{RepositoryID: 17, IssueNumber: 42, PolicyDigest: facts.PolicyDigest, Failure: "read_failed"}
			case "closed":
				facts.IssueState = "closed"
			case "hold":
				facts.Hold = true
			case "not_ready":
				facts.Ready = false
			case "request_changed":
				facts.RequestDigest = ProductSourceRequestDigest("new request cannot become acceptance")
			case "scope_unconfirmed":
				direction, err := k.RequestWaveDirection(ctx, directionRequest(w))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := k.ObserveProductSource(ctx, w.Target(), "", facts); err == nil {
					t.Fatal("old scope observation cleared current prohibition")
				}
				w, _ = k.Wave(ctx, w.ID)
			}
			observed, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
			if err != nil || observed.Hold == nil || observed.Hold.Reason != mode || observed.Snapshot.Value.Status != mode {
				t.Fatal(observed, err)
			}
			current, err := k.Wave(ctx, w.ID)
			if err != nil || current.Target() != w.Target() || current.OriginalRequest != w.OriginalRequest || !current.DispatchPaused() {
				t.Fatal("source observation changed request/epoch", current, err)
			}
		})
	}
}

func TestProductSourceObservationFailureRollsBackWaveHistoryAuthorityAndWatch(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_source_history BEFORE INSERT ON records WHEN NEW.kind='product_source_observation_history' BEGIN SELECT RAISE(FAIL,'source history rollback'); END"); err != nil {
		t.Fatal(err)
	}
	facts.Hold = true
	if _, err := k.ObserveProductSource(ctx, w.Target(), "", facts); err == nil || !strings.Contains(err.Error(), "source history rollback") {
		t.Fatal("rollback injection was not reached", err)
	}
	w, err := k.Wave(ctx, w.ID)
	if err != nil || w.SourceHold != nil {
		t.Fatal("source history failure leaked prohibition", w, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_source_observation','product_source_observation_history')").Scan(&count); err != nil || count != 0 {
		t.Fatal("partial source observation committed", count, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='product_source_hold_changed'").Scan(&count); err != nil || count != 0 {
		t.Fatal("partial source watch event committed", count, err)
	}
}

func TestProductSourceObservationStopsWarmGatewayAndPreparedPRWithoutNotificationTurn(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	p, err := k.SetProductTarget(ctx, "", ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "master", WorkItem: domain.WorkItem{ID: w.SourceID, RepositoryID: 17, ScopeRevision: 1, PolicyRevision: 1, Version: 1, Phase: domain.Reviewing, Candidate: "candidate", Branch: "harness/source"}, Candidate: runner.Candidate{ID: "candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: 1, PolicyRevision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	b, run := reserveInputRun(t, k, "main", snapshot)
	ref := inputContext(t, snapshot).Waves[0].Manifest
	read := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "manifest", ReferenceID: ref.ID, Limit: 2048}
	if _, err := k.ReadContext(ctx, b.ID, read); err != nil {
		t.Fatal(err)
	}
	if _, ok := k.inputCache.get(ref); !ok {
		t.Fatal("warm manifest regression not exercised")
	}
	request := contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: w.SourceID, CandidateID: p.Value.Candidate.ID, SHA: p.Value.Candidate.SHA}}
	gateway := contract.KernelGateway{Backend: k}
	accepted, err := gateway.Handle(ctx, kernelCall(t, b, run, "pr_publish", request))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.PreparePRDispatch(ctx, accepted.OperationID, "source-writer"); err != nil {
		t.Fatal(err)
	}
	var notifications int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	facts.Hold = true
	update, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil || !update.Changed {
		t.Fatal(update, err)
	}
	heldRead := kernelCall(t, b, run, "context_read", read)
	heldRead.CallID = "source-held-read"
	if _, err := gateway.Handle(ctx, heldRead); err == nil {
		t.Fatal("warm saved input bypassed source hold")
	}
	if _, err := k.MarkPRMayHaveSent(ctx, accepted.OperationID, "source-writer"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("prepared old write bypassed source hold")
	}
	var after, runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&after); err != nil || after != notifications {
		t.Fatal("source observation requested an unconditional model turn", after, notifications, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 1 {
		t.Fatal("source observation created a run", runs, err)
	}
	page, err := k.StateEvents(ctx, 0, 32)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range page.Events {
		found = found || event.Kind == "product_source_hold_changed"
	}
	if !found {
		t.Fatal("source prohibition missing from watch")
	}
}

func TestProductSourceObservationCorruptionCannotClearHoldOrLoseEvidence(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	ready, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE records SET payload='corrupt source head' WHERE kind='product_source_observation' AND id=?", w.ID); err != nil {
		t.Fatal(err)
	}
	blocked, err := k.ObserveProductSource(ctx, w.Target(), ready.Snapshot.Digest, facts)
	if !errors.Is(err, ErrTaskEvidence) || !blocked.Changed || blocked.Hold == nil || blocked.Hold.Reason != "state_invalid" {
		t.Fatal(blocked, err)
	}
	var payload string
	if err := k.DB.QueryRow("SELECT payload FROM records WHERE kind='product_source_observation' AND id=?", w.ID).Scan(&payload); err != nil || payload != "corrupt source head" {
		t.Fatal("corrupt source evidence overwritten", payload, err)
	}
	again, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if !errors.Is(err, ErrTaskEvidence) || again.Changed {
		t.Fatal("corrupt retry rewrote prohibition", again, err)
	}
}

func TestProductSourceObservationCancelKeepsTerminalParentObservationDeliverable(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	facts.Hold = true
	held, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil {
		t.Fatal(err)
	}
	mustControl(t, k, operatorRequest(w, "cancel-held-source", "cancel", ""))
	canceled, err := k.Wave(ctx, w.ID)
	if err != nil || canceled.State != domain.WaveCanceled || canceled.SourceHold != nil || canceled.DispatchPaused() {
		t.Fatal("source hold suppressed terminal parent delivery", canceled, err)
	}
	facts.UpdatedAt = facts.UpdatedAt.Add(time.Second)
	after, err := k.ObserveProductSource(ctx, w.Target(), held.Snapshot.Digest, facts)
	if err != nil || after.Hold != nil || after.Changed {
		t.Fatal("observation re-held terminal Wave", after, err)
	}
}

func TestProductSourceObservationRejectsForeignKernelAndIncompleteFacts(t *testing.T) {
	for _, mode := range []string{"repository", "issue", "policy", "timestamp", "state", "unavailable", "kernel"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, facts := productSourceFixture(t)
			switch mode {
			case "repository":
				facts.RepositoryID++
			case "issue":
				facts.IssueNumber++
			case "policy":
				facts.PolicyDigest = "unbounded-assertion"
			case "timestamp":
				facts.UpdatedAt = time.Time{}
			case "state":
				facts.IssueState = "APPROVED"
			case "unavailable":
				facts.Available = false // Stale success fields cannot be reused.
			case "kernel":
				policy := w.Policy
				policy.Stage, policy.Product = "K", false
				var err error
				w, err = k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "r17-i42", OriginalRequest: w.OriginalRequest, Criteria: w.Criteria, Policy: policy})
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ObserveProductSource(ctx, w.Target(), "", facts); err == nil {
				t.Fatal("foreign or incomplete source facts accepted")
			}
			current, err := k.Wave(ctx, w.ID)
			if err != nil || current.SourceHold != nil {
				t.Fatal("rejected facts changed prohibition", current, err)
			}
		})
	}
}

func TestProductSourceObservationDoesNotLoseConcurrentPlanRevision(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	first, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "source-planning", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	next := storagePlanFixture()
	next.Phases[0].ID = "research-2"
	next.Phases[0].Tasks[0].ID = "work-2"
	next.Phases[0].Completion.RequiredTaskIDs = []string{"work-2"}
	next.Phases[1].ID = "evaluation-2"
	next.Phases[1].Tasks[0].ID = "review-2"
	next.Phases[1].Tasks[0].DependsOn[0].TaskID = "work-2"
	next.Phases[1].Completion.RequiredTaskIDs = []string{"review-2"}
	if _, err := k.AdoptPlan(ctx, w.Target(), "source-replanning", next); err != nil {
		t.Fatal(err)
	}
	planned, err := k.Wave(ctx, w.ID)
	if err != nil || planned.PlanRevision == w.PlanRevision {
		t.Fatal(planned, err)
	}
	facts.Hold = true
	if _, err := k.ObserveProductSource(ctx, w.Target(), first.Snapshot.Digest, facts); err != nil {
		t.Fatal("same-source observation depended on plan revision", err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.Target() != planned.Target() || current.SourceHold == nil {
		t.Fatal("source observation rewrote concurrent plan", current, err)
	}
	ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: current.Target()})
	if err != nil || len(ready.Tasks) != 0 {
		t.Fatal("source hold dispatched a generic Task", ready, err)
	}
}

func TestProductSourceObservationWatchFailureRollsBackControlProjection(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_source_control BEFORE INSERT ON kernel_events WHEN NEW.kind='product_source_hold_changed' BEGIN SELECT RAISE(FAIL,'source watch rollback'); END"); err != nil {
		t.Fatal(err)
	}
	facts.Hold = true
	if _, err := k.ObserveProductSource(ctx, w.Target(), "", facts); err == nil || !strings.Contains(err.Error(), "source watch rollback") {
		t.Fatal("watch failure not reached", err)
	}
	w, err := k.Wave(ctx, w.ID)
	if err != nil || w.SourceHold != nil {
		t.Fatal(w, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	scope, err := loadWaveToolScope(ctx, tx, w.ID)
	if err != nil || scope.SourceHeld || scope.DispatchPaused() {
		t.Fatal("watch failure leaked tool prohibition", scope, err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind IN ('product_source_observation','product_source_observation_history')").Scan(&count); err != nil || count != 0 {
		t.Fatal("watch failure leaked source history", count, err)
	}
}

func TestProductSourceObservationScopeRefreshPreservesProhibition(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	registered, err := k.ReadProductWorkItemRegistration(ctx, w.SourceID)
	if err != nil {
		t.Fatal(err)
	}
	direction, err := k.RequestWaveDirection(ctx, directionRequest(w))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	held, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil || held.Hold == nil || held.Hold.Reason != "scope_unconfirmed" {
		t.Fatal(held, err)
	}
	definition := registered.Value.Definition
	newDefinition := definition
	newDefinition.ID = "additional-work"
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", newDefinition); err == nil {
		t.Fatal("new ownership admitted while source held")
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "stale", definition); err == nil {
		t.Fatal("held refresh skipped CAS")
	}
	mustControl(t, k, operatorRequest(w, "pause-refresh", "pause", ""))
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), registered.Digest, definition); err == nil {
		t.Fatal("source refresh bypassed manual pause")
	}
	mustControl(t, k, operatorRequest(w, "resume-refresh", "resume", ""))
	updated, err := k.RegisterProductWorkItem(ctx, w.Target(), registered.Digest, definition)
	if err != nil || updated.Value.ScopeRevision != w.ScopeRevision {
		t.Fatal(updated, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.SourceHold == nil || !current.DispatchPaused() {
		t.Fatal("metadata refresh removed source hold", current, err)
	}
	ready, err := k.ObserveProductSource(ctx, w.Target(), held.Snapshot.Digest, facts)
	if err != nil || ready.Hold != nil || !ready.Changed {
		t.Fatal("refreshed registration could not recover source", ready, err)
	}
}

func TestProductSourceObservationPolicyCannotChangeWithinEpoch(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	// The initial admission pin protects even the very first observation.
	changed := facts
	changed.PolicyDigest = strings.Repeat("f", 64)
	first, err := k.ObserveProductSource(ctx, w.Target(), "", changed)
	if err != nil || first.Hold == nil || first.Hold.Reason != "policy_changed" {
		t.Fatal(first, err)
	}
	restored, err := k.ObserveProductSource(ctx, w.Target(), first.Snapshot.Digest, facts)
	if err != nil || restored.Hold != nil {
		t.Fatal(restored, err)
	}
	facts.Hold = true
	held, err := k.ObserveProductSource(ctx, w.Target(), restored.Snapshot.Digest, facts)
	if err != nil || held.Hold == nil {
		t.Fatal(held, err)
	}
	changedRead, err := k.ObserveProductSource(ctx, w.Target(), held.Snapshot.Digest, changed)
	if err != nil || changedRead.Hold == nil || changedRead.Hold.Reason != "policy_changed" {
		t.Fatal(changedRead, err)
	}
	repeated, err := k.ObserveProductSource(ctx, w.Target(), changedRead.Snapshot.Digest, changed)
	if err != nil || repeated.Changed || repeated.Hold == nil {
		t.Fatal("changed observation became authoritative pin", repeated, err)
	}
	direction, err := k.RequestWaveDirection(ctx, directionRequest(w))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterScope, err := k.ObserveProductSource(ctx, w.Target(), repeated.Snapshot.Digest, changed)
	if err != nil || afterScope.Hold == nil || afterScope.Hold.Reason != "policy_changed" {
		t.Fatal("scope change adopted new source policy", afterScope, err)
	}
	// A trusted controller's explicit policy revision permits a new policy pin.
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.Policy.Revision++
	if err := saveWave(ctx, tx, w); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	registration, err := k.ReadProductWorkItemRegistration(ctx, w.SourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), registration.Digest, registration.Value.Definition); err != nil {
		t.Fatal(err)
	}
	afterPolicy, err := k.ObserveProductSource(ctx, w.Target(), afterScope.Snapshot.Digest, changed)
	if err != nil || afterPolicy.Hold != nil || afterPolicy.Snapshot.Value.PolicyRevision != w.Policy.Revision {
		t.Fatal("explicit policy revision did not permit new policy", afterPolicy, err)
	}
}

func TestProductSourceObservationPolicyPinLossOrCorruptionPreservesEvidence(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, facts := productSourceFixture(t)
			id := productSourcePolicyID(w)
			if mode == "missing" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_source_policy' AND id=?", id); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := k.DB.Exec("UPDATE records SET payload='broken pin' WHERE kind='product_source_policy' AND id=?", id); err != nil {
					t.Fatal(err)
				}
			}
			observed, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
			if !errors.Is(err, ErrTaskEvidence) || observed.Hold == nil || observed.Hold.Reason != "state_invalid" {
				t.Fatal(observed, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_source_observation'").Scan(&count); err != nil || count != 0 {
				t.Fatal("invalid pin admitted observation", count, err)
			}
			var payload string
			err = k.DB.QueryRow("SELECT payload FROM records WHERE kind='product_source_policy' AND id=?", id).Scan(&payload)
			if mode == "missing" && !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("missing pin rebuilt", payload, err)
			}
			if mode == "corrupt" && (err != nil || payload != "broken pin") {
				t.Fatal("corrupt pin overwritten", payload, err)
			}
		})
	}
}

func TestProductSourceObservationFirstPinRollsBackWithObservation(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Source, request.SourceID = "github", "r17-i42"
	request.Policy.Stage, request.Policy.Product = "P", true
	w, err := k.CreateProductWave(ctx, ProductWaveRequest{Wave: request, WorkItems: []ProductWorkItemDefinition{{ID: request.SourceID, Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	facts := ProductSourceFacts{RepositoryID: 17, IssueNumber: 42, PolicyDigest: strings.Repeat("e", 64), Available: true, IssueState: "open", Ready: true, RequestDigest: ProductSourceRequestDigest(w.OriginalRequest), UpdatedAt: k.clock()}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_source_pin_observation BEFORE INSERT ON records WHEN NEW.kind='product_source_observation' BEGIN SELECT RAISE(FAIL,'rollback pin'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSource(ctx, w.Target(), "", facts); err == nil {
		t.Fatal("failed observation accepted")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_source_policy','product_source_policy_history')").Scan(&count); err != nil || count != 0 {
		t.Fatal("pin survived failed observation", count, err)
	}
}

func TestProductSourceObservationCannotCompleteThroughGenericAcceptance(t *testing.T) {
	ctx := context.Background()
	k, w, b, run, _ := waveCompletionFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.Policy.Stage, w.Policy.Product = "P", true
	w.Source, w.SourceID = "github", "r17-i42"
	w.SourceHold = &domain.WaveSourceHold{Reason: "hold", Digest: strings.Repeat("e", 64), ObservedAt: k.clock()}
	if err := saveWave(ctx, tx, w); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("P Wave completed through K artifact acceptance")
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.State == domain.WaveCompleted || current.SourceHold == nil {
		t.Fatal("rejected acceptance changed product hold", current, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='wave_completion'").Scan(&count); err != nil || count != 0 {
		t.Fatal("product completion proof fabricated", count, err)
	}
}

func TestProductSourceObservationMissingPolicyCannotBeRepinnedFromCurrentObservation(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	first, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_source_policy','product_source_policy_history','product_source_observation_history')"); err != nil {
		t.Fatal(err)
	}
	facts.PolicyDigest = strings.Repeat("f", 64)
	observed, err := k.ObserveProductSource(ctx, w.Target(), first.Snapshot.Digest, facts)
	if !errors.Is(err, ErrTaskEvidence) || observed.Hold == nil || observed.Hold.Reason != "state_invalid" {
		t.Fatal(observed, err)
	}
	current, err := k.ReadProductSourceObservation(ctx, w.ID)
	if err != nil || current.Digest != first.Snapshot.Digest {
		t.Fatal("current evidence replaced after pin loss", current, err)
	}
}

func TestProductSourceObservationTerminalFactsAreRetainedAndSameFactsReholdActiveWave(t *testing.T) {
	for _, terminal := range []domain.WaveState{domain.WaveCompleted, domain.WaveCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			ctx := context.Background()
			k, w, facts := productSourceFixture(t)
			transition := func(state domain.WaveState) {
				t.Helper()
				// P completion/reactivation is not exposed yet. Model only the trusted
				// state transition to exercise the observer's independent retry guard.
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				w.State = state
				if err := saveWave(ctx, tx, w); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			transition(terminal)
			facts.IssueState = "closed"
			terminalRead, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
			if err != nil || terminalRead.Hold != nil || terminalRead.Snapshot.Value.Status != "closed" {
				t.Fatal(terminalRead, err)
			}
			repeated, err := k.ObserveProductSource(ctx, w.Target(), terminalRead.Snapshot.Digest, facts)
			if err != nil || repeated.Changed || repeated.Hold != nil {
				t.Fatal(repeated, err)
			}
			transition(domain.WaveRunning)
			activeRead, err := k.ObserveProductSource(ctx, w.Target(), terminalRead.Snapshot.Digest, facts)
			if err != nil || !activeRead.Changed || activeRead.Hold == nil || activeRead.Hold.Reason != "closed" {
				t.Fatal("same facts lost source prohibition after reactivation", activeRead, err)
			}
			if !reflect.DeepEqual(activeRead.Snapshot, terminalRead.Snapshot) {
				t.Fatal("control refresh rewrote source evidence", activeRead.Snapshot, terminalRead.Snapshot)
			}
			current, err := k.Wave(ctx, w.ID)
			if err != nil || !current.DispatchPaused() || current.State != domain.WaveRunning {
				t.Fatal(current, err)
			}
		})
	}
}
