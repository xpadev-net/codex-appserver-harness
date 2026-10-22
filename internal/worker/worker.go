// Package worker manages one bounded Codex turn and its durable resumption metadata.
package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"slices"
	"time"
)

type RPC interface {
	Call(context.Context, string, any, any) error
	Next(context.Context) (codex.Message, error)
}
type Journal interface {
	Put(context.Context, string, string, string, any) error
	Record(context.Context, string, string, any) error
}

// A concurrently handled early tool may establish the reserved run's turn
// before turn/start returns. Preserve that proof without converting an unknown
// or blocked transport outcome back into a running outcome.
func (w Worker) preserveObservedTurn(ctx context.Context, run *Run) error {
	if run.TurnID != "" {
		return nil
	}
	var observed Run
	if err := w.Journal.Record(ctx, "agent_run", run.ID, &observed); err != nil {
		return err
	}
	if observed.ID != run.ID || observed.ThreadID != run.ThreadID || !observed.Started.Equal(run.Started) || observed.BaselineKnown != run.BaselineKnown || !slices.Equal(observed.BaselineTurnIDs, run.BaselineTurnIDs) {
		return errors.New("observed run start identity changed")
	}
	if observed.TurnID != "" {
		if slices.Contains(run.BaselineTurnIDs, observed.TurnID) {
			return errors.New("observed turn belongs to the prior baseline")
		}
		run.TurnID = observed.TurnID
	}
	return nil
}

type Session struct {
	BaselineTurnIDs []string `json:"baseline_turn_ids"`
	BaselineKnown   bool     `json:"baseline_known"`
	WorkItemID      string   `json:"work_item_id"`
	Role            string   `json:"role"`
	ThreadID        string   `json:"thread_id"`
	CWD             string   `json:"cwd"`
	Model           string   `json:"model"`
}
type Run = contract.SessionRun

type Thread struct {
	ID    string `json:"id"`
	CWD   string `json:"cwd"`
	Turns []Turn `json:"turns"`
}
type Turn struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	ItemsView string `json:"itemsView"`
	Items     []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"items"`
}
type Worker struct {
	RPC     RPC
	Journal Journal
}

func (w Worker) Prepare(ctx context.Context, s Session) (Session, error) {
	if s.WorkItemID == "" || s.CWD == "" || s.Model == "" || (s.Role != "implementation" && s.Role != "verification") {
		return s, errors.New("invalid session")
	}
	method := "thread/start"
	params := map[string]any{"cwd": s.CWD, "model": s.Model, "approvalPolicy": "never", "sandbox": "workspace-write"}
	if s.ThreadID != "" {
		method = "thread/resume"
		params["threadId"] = s.ThreadID
	}
	var reply struct {
		Thread Thread `json:"thread"`
	}
	if err := w.RPC.Call(ctx, method, params, &reply); err != nil {
		return s, err
	}
	if reply.Thread.ID == "" || reply.Thread.CWD != s.CWD {
		return s, errors.New("thread identity/workspace mismatch")
	}
	if s.ThreadID != "" && s.ThreadID != reply.Thread.ID {
		return s, errors.New("resume returned different thread")
	}
	s.ThreadID = reply.Thread.ID
	s.BaselineTurnIDs = []string{}
	s.BaselineKnown = method == "thread/start" || reply.Thread.Turns != nil
	if !s.BaselineKnown {
		var history struct {
			Thread Thread `json:"thread"`
		}
		if err := w.RPC.Call(ctx, "thread/read", map[string]any{"threadId": s.ThreadID, "includeTurns": true}, &history); err != nil {
			return s, err
		}
		if history.Thread.ID != s.ThreadID || history.Thread.CWD != s.CWD {
			return s, errors.New("resume history identity mismatch")
		}
		reply.Thread = history.Thread
		s.BaselineKnown = true
	}
	for _, turn := range reply.Thread.Turns {

		s.BaselineTurnIDs = append(s.BaselineTurnIDs, turn.ID)
	}
	return s, w.Journal.Put(ctx, "role_thread", s.WorkItemID+":"+s.Role, s.WorkItemID, s)
}

// Execute is invoked only after controller leases and isolated launch policy checks.
// Persisting intent before turn/start makes a lost response an explicit unknown state.
func (w Worker) Execute(ctx context.Context, s Session, runID, reason, prompt string, schema any) (Run, error) {
	r := Run{BaselineTurnIDs: append([]string{}, s.BaselineTurnIDs...), BaselineKnown: s.BaselineKnown, ID: runID, ThreadID: s.ThreadID, State: "starting", Reason: reason, Started: time.Now().UTC()}
	if runID == "" || s.ThreadID == "" || reason == "" {
		return r, errors.New("invalid run")
	}
	var existing Run
	if err := w.Journal.Record(ctx, "agent_run", runID, &existing); err == nil {
		return existing, errors.New("run already exists; reconcile before starting")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return r, err
	}
	if err := w.Journal.Put(ctx, "agent_run", runID, s.WorkItemID, r); err != nil {
		return r, err
	}
	var reply struct {
		Turn Turn `json:"turn"`
	}
	err := w.RPC.Call(ctx, "turn/start", map[string]any{"threadId": s.ThreadID, "input": []any{map[string]string{"type": "text", "text": prompt}}, "outputSchema": schema}, &reply)
	if err != nil {
		r.State = "unknown"
		var requestError *codex.RequestError
		if errors.As(err, &requestError) {
			r.State = "blocked"
			r.Request = &requestError.Request
			var ids struct {
				ThreadID string `json:"threadId"`
				TurnID   string `json:"turnId"`
			}
			if json.Unmarshal(requestError.Request.Params, &ids) == nil && ids.ThreadID == s.ThreadID {
				r.TurnID = ids.TurnID
			}
		}
		if mergeErr := w.preserveObservedTurn(context.WithoutCancel(ctx), &r); mergeErr != nil {
			return r, mergeErr
		}
		if saveErr := w.Journal.Put(context.WithoutCancel(ctx), "agent_run", runID, s.WorkItemID, r); saveErr != nil {
			return r, saveErr
		}
		return r, err
	}
	r.TurnID = reply.Turn.ID
	r.State = "running"
	if r.TurnID == "" {
		r.State = "unknown"
		if mergeErr := w.preserveObservedTurn(context.WithoutCancel(ctx), &r); mergeErr != nil {
			return r, mergeErr
		}
		if saveErr := w.Journal.Put(context.WithoutCancel(ctx), "agent_run", runID, s.WorkItemID, r); saveErr != nil {
			return r, saveErr
		}
		return r, errors.New("missing turn id")
	}
	if err = w.Journal.Put(context.WithoutCancel(ctx), "agent_run", runID, s.WorkItemID, r); err != nil {
		return r, err
	}
	for {
		m, err := w.RPC.Next(ctx)
		if err != nil {
			r.State = "unknown"
			_ = w.Journal.Put(context.WithoutCancel(ctx), "agent_run", runID, s.WorkItemID, r)
			return r, err
		}
		if len(m.ID) > 0 && m.Method != "" {
			r.State = "blocked"
			r.Request = &m
			err = w.Journal.Put(context.WithoutCancel(ctx), "agent_run", runID, s.WorkItemID, r)
			if err != nil {
				return r, err
			}
			return r, fmt.Errorf("operator request: %s", m.Method)
		}
		if m.Method != "item/completed" && m.Method != "thread/tokenUsage/updated" && m.Method != "turn/completed" {
			continue
		}
		var p struct {
			ThreadID string `json:"threadId"`
			Turn     Turn   `json:"turn"`
			TurnID   string `json:"turnId"`
			Item     struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"item"`
			TokenUsage json.RawMessage `json:"tokenUsage"`
		}
		if err = json.Unmarshal(m.Params, &p); err != nil {
			return r, err
		}
		if p.ThreadID != s.ThreadID {
			continue
		}
		switch m.Method {
		case "item/completed":
			if p.TurnID == r.TurnID && p.Item.Type == "agentMessage" {
				r.Final = p.Item.Text
			}
		case "thread/tokenUsage/updated":
			if p.TurnID == r.TurnID {
				r.Usage = p.TokenUsage
			}
		case "turn/completed":
			if p.Turn.ID != r.TurnID {
				continue
			}
			r.State = p.Turn.Status
			if p.Turn.ItemsView == "" || p.Turn.ItemsView == "full" {
				for _, item := range p.Turn.Items {
					if item.Type == "agentMessage" {
						r.Final = item.Text
					}
				}
			}
			if err = w.Journal.Put(context.WithoutCancel(ctx), "agent_run", runID, s.WorkItemID, r); err != nil {
				return r, err
			}
			if r.State != "completed" {
				return r, fmt.Errorf("turn ended: %s", r.State)
			}
			return r, nil
		}
	}
}

// Reconcile reads history without starting another turn. An unknown start remains held
// unless the caller can uniquely associate its turn; no request-ID deduplication is assumed.
func (w Worker) Reconcile(ctx context.Context, s Session, r Run) (Run, error) {
	var reply struct {
		Thread Thread `json:"thread"`
	}
	if err := w.RPC.Call(ctx, "thread/read", map[string]any{"threadId": s.ThreadID, "includeTurns": true}, &reply); err != nil {
		return r, err
	}
	if reply.Thread.ID != s.ThreadID || reply.Thread.CWD != s.CWD {
		return r, errors.New("thread mismatch")
	}
	if r.TurnID == "" && r.BaselineKnown {
		old := map[string]bool{}
		for _, id := range r.BaselineTurnIDs {
			old[id] = true
		}
		found := []string{}
		for _, t := range reply.Thread.Turns {
			if !old[t.ID] {
				found = append(found, t.ID)
			}
		}
		if len(found) == 1 {
			r.TurnID = found[0]
		}
	}
	if r.TurnID == "" {
		return r, errors.New("unknown turn start requires explicit association")
	}
	for _, t := range reply.Thread.Turns {
		if t.ID == r.TurnID {
			if t.ItemsView != "" && t.ItemsView != "full" {
				return r, errors.New("turn history is not fully loaded")
			}
			r.Final = ""
			r.State = t.Status
			for _, i := range t.Items {
				if i.Type == "agentMessage" {
					r.Final = i.Text
				}
			}
			if err := w.Journal.Put(ctx, "agent_run", r.ID, s.WorkItemID, r); err != nil {
				return r, err
			}
			if r.State != "completed" {
				return r, fmt.Errorf("recovered turn is not completed: %s", r.State)
			}
			return r, nil
		}
	}
	return r, errors.New("saved turn absent; do not resend")
}
