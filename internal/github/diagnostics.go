package github

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"regexp"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

// CheckDiagnostics reads private diagnostics with controller credentials. Worker
// threads receive bounded redacted text and never need GitHub write credentials.
func (c Client) CheckDiagnostics(ctx context.Context, check Check) (string, error) {
	if err := c.valid(); err != nil {
		return "", err
	}
	if check.ID <= 0 {
		return "", fmt.Errorf("invalid check identity")
	}
	var annotations []struct {
		Path       string `json:"path"`
		StartLine  int    `json:"start_line"`
		Title      string `json:"title"`
		Message    string `json:"message"`
		RawDetails string `json:"raw_details"`
	}
	annotationErr := c.Pages(ctx, fmt.Sprintf("check-runs/%d/annotations?per_page=100", check.ID), &annotations)
	tail := &diagnosticTail{limit: 65536}
	output := runner.NewRedactedWriter(tail)
	for _, a := range annotations {
		fmt.Fprintf(output, "%s:%d %s\n%s\n%s\n", a.Path, a.StartLine, a.Title, a.Message, a.RawDetails)
	}
	logErr := error(nil)
	u, err := url.Parse(check.DetailsURL)
	if err == nil && u.Scheme == "https" && u.Host == "github.com" && u.User == nil {
		pattern := regexp.MustCompile("^/" + regexp.QuoteMeta(c.Repository) + `/actions/runs/[0-9]+/job/([0-9]+)/?$`)
		if match := pattern.FindStringSubmatch(u.Path); match != nil {
			cmd := exec.CommandContext(ctx, "gh", "api", "repos/"+c.Repository+"/actions/jobs/"+match[1]+"/logs")
			logTail := &diagnosticTail{limit: 65536}
			logOutput := runner.NewRedactedWriter(logTail)
			cmd.Stdout = logOutput
			logErr = cmd.Run()
			flushErr := logOutput.Flush()
			if logErr == nil {
				logErr = flushErr
			}
			if logErr == nil {
				_, logErr = output.Write(logTail.data)
			}
		}
	}
	if err := output.Flush(); err != nil {
		return "", err
	}
	text := strings.TrimSpace(string(tail.data))
	if text != "" {
		return text, nil
	}
	if annotationErr != nil || logErr != nil {
		return "", fmt.Errorf("authenticated CI diagnostics unavailable")
	}
	return "", nil
}

type diagnosticTail struct {
	data  []byte
	limit int
}

func (b *diagnosticTail) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return n, nil
	}
	if excess := len(b.data) + len(p) - b.limit; excess > 0 {
		b.data = b.data[excess:]
	}
	b.data = append(b.data, p...)
	return n, nil
}
