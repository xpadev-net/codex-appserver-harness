package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"io"
	"os"
	"sync"
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
func TestSendSerializesConcurrentWriters(t *testing.T) {
	var buffer bytes.Buffer
	c := &Client{in: nopWriteCloser{&buffer}}
	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Send(map[string]int{"id": i}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	decoder := json.NewDecoder(&buffer)
	for range 30 {
		var message map[string]int
		if err := decoder.Decode(&message); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCloseIsConcurrentRepeatableAndConfirmsChildGroupStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := StartCommand(ctx, "sh", []string{"-c", `sleep 30 & printf '%s\n' '{"method":"ready","params":{}}'; wait`}, os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if !c.ProcessIdentity().Valid() {
		t.Fatal(c.ProcessIdentity())
	}
	if c.GroupStopped() {
		t.Fatal("live transport already quiesced")
	}
	if _, err := c.Next(ctx); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 8)
	for range 8 {
		go func() { results <- c.Close() }()
	}
	first := <-results
	for range 7 {
		if err := <-results; err != first {
			t.Fatal("Close returned a new outcome", first, err)
		}
	}
	if !c.GroupStopped() {
		t.Fatal("group stop not confirmed", first)
	}
	if alive, err := process.GroupAlive(c.ProcessIdentity()); err != nil || alive {
		t.Fatal("live child remained", alive, err)
	}
	if err := c.Close(); err != first {
		t.Fatal("repeat Close changed result", first, err)
	}
}
func TestCloseConfirmsAfterCancellationButNeverWithMissingIdentity(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "unidentified"}[missing], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c, err := StartCommand(ctx, "sleep", []string{"30"}, os.Environ())
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			if missing {
				c.identity.StartTicks = 0
			}
			cancel()
			err = c.Close()
			if missing {
				if c.GroupStopped() || !errors.Is(err, process.ErrUnconfirmed) {
					t.Fatal("missing identity confirmed stop", err)
				}
			} else if !c.GroupStopped() {
				t.Fatal("cancelled context hid actual stopping proof", err)
			}
		})
	}
}
