package runner

import (
	"bytes"
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
