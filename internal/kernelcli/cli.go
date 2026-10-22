package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type DirectRequest struct {
	SchemaVersion int                    `json:"schema_version"`
	RequestID     string                 `json:"request_id"`
	Request       string                 `json:"request"`
	Criteria      []domain.WaveCriterion `json:"criteria"`
}

// Run implements only local kernel commands; read commands have no runtime or
// model factory. Mutations commit their facts before sending a best-effort hint.
func Run(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("wave commands: init, create, list, inspect, pause, resume, cancel, steer, interrupt, watch")
	}
	command := args[0]
	switch command {
	case "init", "create", "list", "inspect", "pause", "resume", "cancel", "steer", "interrupt", "watch":
	default:
		return errors.New("unknown wave command")
	}
	fs := flag.NewFlagSet("wave "+command, flag.ContinueOnError)
	fs.SetOutput(errOut)
	root := fs.String("root", "", "absolute private kernel root")
	session := fs.String("session", "main", "logical main session ID (init)")
	model := fs.String("model", "", "configured model (init)")
	file := fs.String("file", "", "direct request or versioned operator JSON")
	id := fs.String("id", "", "Wave ID (inspect)")
	after := fs.String("after", "", "last Wave/Task ID from a previous page")
	afterPhase := fs.String("after-phase", "", "last Phase ID from a previous inspect page")
	defaultLimit := 64
	if command == "watch" {
		defaultLimit = contract.MaxStatePage
	}
	limit := fs.Int("limit", defaultLimit, "page size, 1..128 (watch: 1..32)")
	cursor := fs.Int64("cursor", 0, "last successfully displayed state cursor (watch)")
	once := fs.Bool("once", false, "read one stored page without a running daemon (watch)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *root == "" || *limit < 1 || *limit > 128 {
		return errors.New("--root and valid page bounds required; positional arguments are not accepted")
	}
	allowed := map[string]bool{"root": true}
	switch command {
	case "init":
		allowed["session"], allowed["model"] = true, true
	case "create", "pause", "resume", "cancel", "steer", "interrupt":
		allowed["file"] = true
	case "list":
		allowed["after"], allowed["limit"] = true, true
	case "inspect":
		allowed["id"], allowed["after"], allowed["after-phase"], allowed["limit"] = true, true, true, true
	case "watch":
		allowed["cursor"], allowed["once"], allowed["limit"] = true, true, true
	}
	var flagErr error
	fs.Visit(func(f *flag.Flag) {
		if !allowed[f.Name] {
			flagErr = errors.New("flag --" + f.Name + " does not apply to wave " + command)
		}
	})
	if flagErr != nil {
		return flagErr
	}
	encode := func(value any) error { return json.NewEncoder(out).Encode(value) }
	if command == "init" {
		c, err := initialize(ctx, *root, *session, *model)
		if err != nil {
			return err
		}
		return encode(c)
	}
	k, c, err := openKernel(ctx, *root)
	if err != nil {
		return err
	}
	defer k.Close()
	switch command {
	case "watch":
		return watchEvents(ctx, k, *root, *cursor, *limit, *once, out)
	case "pause", "resume", "cancel", "steer", "interrupt":
		result, err := applyControlFile(ctx, k, command, *file)
		if err != nil {
			return err
		}
		_ = ipc.Notify(ctx, filepath.Join(*root, ipc.SocketName), "")
		return encode(result)
	case "create":
		if *file == "" {
			return errors.New("--file required")
		}
		raw, err := readFile(*file, 12<<20, false)
		if err != nil {
			return err
		}
		var r DirectRequest
		if err = contract.DecodeStrictBounded(raw, &r, 12<<20); err != nil {
			return err
		}
		if r.SchemaVersion != 1 || r.RequestID == "" || len(r.RequestID) > contract.MaxInline {
			return errors.New("versioned stable request_id required")
		}
		request := storage.WaveRequest{OrchestratorSessionID: c.SessionID, Source: "direct", SourceID: r.RequestID, OriginalRequest: r.Request, Criteria: r.Criteria, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: c.Limits}}
		var w domain.Wave
		err = retryMutation(ctx, func(attempt context.Context) error {
			var createErr error
			w, createErr = k.CreateWave(attempt, request)
			return createErr
		})
		if err != nil {
			return err
		}
		_ = ipc.Notify(ctx, filepath.Join(*root, ipc.SocketName), "")
		return encode(w)
	case "list":
		page, err := k.WavePage(ctx, *after, *limit)
		if err != nil {
			return err
		}
		return encode(page)
	case "inspect":
		if *id == "" {
			return errors.New("--id required")
		}
		view, err := k.InspectWave(ctx, *id, *after, *afterPhase, *limit)
		if err != nil {
			return err
		}
		return encode(view)
	}
	return errors.New("unknown wave command")
}
