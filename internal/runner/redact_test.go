package runner

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestRedactsSplitAndOverlappingSecrets(t *testing.T) {
	var out bytes.Buffer
	r := newRedactedLog(&out, []string{"long-secret-token", "long-secret-token-extended"})
	for _, part := range []string{"before long-sec", "ret-token-extended after long-", "secret-token end"} {
		if _, err := r.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "before [REDACTED] after [REDACTED] end" {
		t.Fatal(got)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}
func TestShortWritesAreErrors(t *testing.T) {
	r := newRedactedLog(shortWriter{}, nil)
	if _, err := r.Write([]byte("output")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("short write lost", err)
	}
	bounded := &boundedLog{writer: shortWriter{}, limit: 100}
	if _, err := bounded.Write([]byte("output")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("bounded short write lost", err)
	}
}
