// Package codex implements the pinned app-server newline-delimited RPC transport.
package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
)

type Message struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

// Client is sequential: a single owner must consume calls and notifications.
type Client struct {
	cmd      *exec.Cmd
	in       io.WriteCloser
	messages chan Message
	terminal error
	pending  []Message
	seq      int
}

func Start(ctx context.Context, binary string, args ...string) (*Client, error) {
	cmd := exec.CommandContext(ctx, binary, append([]string{"app-server", "--listen", "stdio://"}, args...)...)
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	// Inherited credentials are consumed by Codex; stderr is intentionally not exposed.
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	c := &Client{cmd: cmd, in: in, messages: make(chan Message, 64)}
	go func() {
		defer close(c.messages)
		scanner := bufio.NewScanner(out)
		scanner.Buffer(make([]byte, 65536), 16<<20)
		for scanner.Scan() {
			var m Message
			if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
				c.terminal = err
				return
			}
			select {
			case c.messages <- m:
			case <-ctx.Done():
				c.terminal = ctx.Err()
				return
			}
		}
		err := scanner.Err()
		if err == nil {
			err = io.EOF
		}
		c.terminal = err
	}()
	return c, nil
}
func (c *Client) Close() error     { _ = c.in.Close(); _ = c.cmd.Process.Kill(); return c.cmd.Wait() }
func (c *Client) Send(v any) error { return json.NewEncoder(c.in).Encode(v) }
func (c *Client) Next(ctx context.Context) (Message, error) {
	if len(c.pending) > 0 {
		m := c.pending[0]
		c.pending = c.pending[1:]
		return m, nil
	}
	return c.read(ctx)
}
func (c *Client) read(ctx context.Context) (Message, error) {
	select {
	case m, ok := <-c.messages:
		if !ok {
			return Message{}, c.terminal
		}
		return m, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}
func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	c.seq++
	id := c.seq
	if err := c.Send(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	for {
		m, err := c.read(ctx)
		if err != nil {
			return err
		}
		if m.Method != "" {
			c.pending = append(c.pending, m)
			if len(m.ID) > 0 {
				return errors.New("app-server requires operator input: " + m.Method)
			}
			continue
		}
		if string(m.ID) != fmt.Sprint(id) {
			return errors.New("unexpected RPC response id")
		}
		if len(m.Error) > 0 {
			return fmt.Errorf("app-server RPC %s failed", method)
		}
		if result == nil {
			return nil
		}
		return json.Unmarshal(m.Result, result)
	}
}
func (c *Client) Initialize(ctx context.Context) error {
	if err := c.Call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "issue_pr_harness", "version": "0.1.0"}}, nil); err != nil {
		return err
	}
	return c.Send(map[string]any{"method": "initialized"})
}
