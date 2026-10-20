package runner

import (
	"bytes"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
)

// redactedLog retains enough raw tail to redact credentials split across writes.
// Secrets are read only by the trusted runner process, never passed to the job.
type redactedLog struct {
	mu      sync.Mutex
	writer  io.Writer
	secrets [][]byte
	pending []byte
	retain  int
}

func newRedactedLog(writer io.Writer, secrets []string) *redactedLog {
	r := &redactedLog{writer: writer}
	seen := map[string]bool{}
	for _, secret := range secrets {
		if len(secret) == 0 || seen[secret] {
			continue
		}
		seen[secret] = true
		r.secrets = append(r.secrets, []byte(secret))
		if len(secret)-1 > r.retain {
			r.retain = len(secret) - 1
		}
	}
	return r
}
func (r *redactedLog) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending = append(r.pending, p...)
	return len(p), r.flush(false)
}
func (r *redactedLog) Flush() error { r.mu.Lock(); defer r.mu.Unlock(); return r.flush(true) }
func (r *redactedLog) flush(final bool) error {
	for {
		limit := len(r.pending) - r.retain
		if final {
			limit = len(r.pending)
		}
		if limit <= 0 {
			return nil
		}
		index := -1
		length := 0
		for _, secret := range r.secrets {
			at := bytes.Index(r.pending, secret)
			if at >= 0 && at < limit && (index < 0 || at < index || (at == index && len(secret) > length)) {
				index = at
				length = len(secret)
			}
		}
		if index >= 0 {
			if err := writeComplete(r.writer, r.pending[:index]); err != nil {
				return err
			}
			if err := writeComplete(r.writer, []byte("[REDACTED]")); err != nil {
				return err
			}
			r.pending = r.pending[index+length:]
			continue
		}
		if err := writeComplete(r.writer, r.pending[:limit]); err != nil {
			return err
		}
		r.pending = r.pending[limit:]
	}
}
func secretValues() []string {
	values := []string{}
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		upper := strings.ToUpper(name)
		if strings.Contains(upper, "TOKEN") || strings.Contains(upper, "SECRET") || strings.Contains(upper, "PASSWORD") || strings.Contains(upper, "API_KEY") || strings.Contains(upper, "AUTHORIZATION") {
			values = append(values, value)
		}
		if strings.HasSuffix(upper, "PROXY") {
			if u, err := url.Parse(value); err == nil && u.User != nil {
				if password, ok := u.User.Password(); ok {
					values = append(values, password, u.User.String())
				}
				values = append(values, value)
			}
		}
	}
	return values
}

func writeComplete(writer io.Writer, p []byte) error {
	n, err := writer.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}
