package ipc

import (
	"bufio"
	"context"
	"errors"
	"net"
	"path/filepath"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func connect(ctx context.Context, path string) (*net.UnixConn, error) {
	if err := privateRoot(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("absolute IPC path required")
	}
	if _, err := ownedSocket(path); err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return nil, errors.New("Unix IPC required")
	}
	if err = sameUser(unix); err != nil {
		unix.Close()
		return nil, err
	}
	return unix, nil
}
func sendRequest(conn *net.UnixConn, request Request) error {
	if err := request.validate(); err != nil {
		return err
	}
	data, err := encode(request)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

// Notify is best effort. A returned error must never roll back the durable
// record/job result already committed before this call. job is empty for a
// generic wake; a runner sends only its job ID, never a claimed result.
func Notify(ctx context.Context, path, job string) error {
	bounded, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	request := Request{Version: Version, Method: "wake"}
	if job != "" {
		request.Method = "runner"
		request.JobID = job
	}
	if err := request.validate(); err != nil {
		return err
	}
	conn, err := connect(bounded, path)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := bounded.Deadline()
	_ = conn.SetDeadline(deadline)
	if err = sendRequest(conn, request); err != nil {
		return err
	}
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 512), 4096)
	if !scan.Scan() {
		if err = scan.Err(); err != nil {
			return err
		}
		return errors.New("no IPC acknowledgement")
	}
	var response Response
	if err = decode(scan.Bytes(), &response); err != nil {
		return err
	}
	if response.Version != Version || !response.Accepted || response.Page != nil || response.Error != "" {
		return errors.New("IPC hint rejected")
	}
	return nil
}

// Stream starts at a caller-owned durable cursor. On disconnect the caller can
// reconnect with the last page accepted by consume; no model wake is issued.
func Stream(ctx context.Context, path string, after int64, limit int, consume func(contract.StateEventPage) error) error {
	request := Request{Version: Version, Method: "events", After: after, Limit: limit}
	if err := request.validate(); err != nil {
		return err
	}
	if consume == nil {
		return errors.New("state consumer required")
	}
	conn, err := connect(ctx, path)
	if err != nil {
		return err
	}
	defer conn.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	if err = conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	if err = sendRequest(conn, request); err != nil {
		return err
	}
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), MaxFrame)
	for scan.Scan() {
		var response Response
		if err = decode(scan.Bytes(), &response); err != nil {
			return err
		}
		if response.Version != Version || response.Accepted || response.Error != "" || response.Page == nil {
			return errors.New("invalid state stream response")
		}
		if err = response.Page.Validate(after, limit); err != nil {
			return err
		}
		if err = consume(*response.Page); err != nil {
			return err
		}
		after = response.Page.Cursor
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err = scan.Err(); err != nil {
		return err
	}
	return errors.New("state stream disconnected")
}
