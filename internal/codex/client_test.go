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
