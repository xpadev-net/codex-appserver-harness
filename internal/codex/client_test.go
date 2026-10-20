package codex

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestTerminalReadRepeatable(t *testing.T) {
	c := &Client{messages: make(chan Message), terminal: io.EOF}
	close(c.messages)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range 3 {
		if _, err := c.Next(ctx); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}
	}
}

func TestCallPreservesNotifications(t *testing.T) {
	c := &Client{in: nopWriteCloser{io.Discard}, messages: make(chan Message, 2)}
	c.messages <- Message{Method: "turn/completed"}
	c.messages <- Message{ID: []byte("1"), Result: []byte(`{}`)}
	if err := c.Call(context.Background(), "turn/start", nil, nil); err != nil {
		t.Fatal(err)
	}
	m, err := c.Next(context.Background())
	if err != nil || m.Method != "turn/completed" {
		t.Fatalf("lost notification: %v %v", m, err)
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
