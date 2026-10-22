// Package ipc provides private, read-only state streams and advisory wake hints.
// It cannot accept a prompt, scope mutation, operation result, or model command.
package ipc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

const Version = 1
const MaxFrame = 4 << 20
const SocketName = "controller.sock"

var jobID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type Request struct {
	Version int    `json:"version"`
	Method  string `json:"method"`
	JobID   string `json:"job_id,omitempty"`
	After   int64  `json:"after,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}
type Response struct {
	Version  int                      `json:"version"`
	Accepted bool                     `json:"accepted,omitempty"`
	Page     *contract.StateEventPage `json:"page,omitempty"`
	Error    string                   `json:"error,omitempty"`
}
type Reader interface {
	StateEvents(context.Context, int64, int) (contract.StateEventPage, error)
}

func (r Request) validate() error {
	if r.Version != Version {
		return errors.New("unsupported IPC version")
	}
	switch r.Method {
	case "wake":
		if r.JobID != "" || r.After != 0 || r.Limit != 0 {
			return errors.New("wake has no payload")
		}
	case "runner":
		if !jobID.MatchString(r.JobID) || r.After != 0 || r.Limit != 0 {
			return errors.New("runner hint needs only job ID")
		}
	case "events":
		if r.JobID != "" || r.After < 0 || r.Limit < 1 || r.Limit > contract.MaxStatePage {
			return errors.New("invalid state stream cursor/limit")
		}
	default:
		return errors.New("unsupported IPC method")
	}
	return nil
}
func decode(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("one IPC envelope required")
	}
	return nil
}
func encode(value any) ([]byte, error) {
	var buffer bytes.Buffer
	e := json.NewEncoder(&buffer)
	e.SetEscapeHTML(false)
	if err := e.Encode(value); err != nil {
		return nil, err
	}
	if buffer.Len() > MaxFrame {
		return nil, errors.New("IPC frame exceeds bound")
	}
	return buffer.Bytes(), nil
}
