package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"sync"
)

const MaxResourceReadBytes = 4 << 20

type resourceBudgetKey struct{}
type resourceReadBudget struct {
	mu        sync.Mutex
	remaining int64
	parent    *resourceReadBudget
}

// WithResourceReadBudget shares wire/output accounting across this scan, even
// when JSON decoding, entity validation, or a per-response limit fails. Nested
// limits also charge their parent; they cannot reset the enclosing scan budget.
func WithResourceReadBudget(ctx context.Context, limit int64) context.Context {
	parent, _ := ctx.Value(resourceBudgetKey{}).(*resourceReadBudget)
	return context.WithValue(ctx, resourceBudgetKey{}, &resourceReadBudget{remaining: limit, parent: parent})
}

// ResourceReadBudgetAvailable lets a bounded scanner stop before classifying
// unattempted resources as failures. A context without a budget is unrestricted.
func ResourceReadBudgetAvailable(ctx context.Context) bool {
	budget, _ := ctx.Value(resourceBudgetKey{}).(*resourceReadBudget)
	return budget == nil || budget.available()
}
func (b *resourceReadBudget) available() bool {
	b.mu.Lock()
	available := b.remaining > 0
	b.mu.Unlock()
	return available && (b.parent == nil || b.parent.available())
}
func (b *resourceReadBudget) charge(n int) error {
	b.mu.Lock()
	b.remaining -= int64(n)
	exhausted := b.remaining < 0
	b.mu.Unlock()
	// Charge every ancestor even when this response already exceeded its local
	// limit: those bytes were consumed from the transport all the same.
	if b.parent != nil {
		if err := b.parent.charge(n); err != nil {
			return err
		}
	}
	if exhausted {
		return errors.New("shared resource read budget exhausted")
	}
	return nil
}

type boundedResourceOutput struct {
	buffer bytes.Buffer
	budget *resourceReadBudget
}

func (b *boundedResourceOutput) Write(p []byte) (int, error) {
	if b.budget != nil {
		if err := b.budget.charge(len(p)); err != nil {
			return 0, err
		}
	}
	if len(p) > MaxResourceReadBytes-b.buffer.Len() {
		return 0, errors.New("resource response exceeds read budget")
	}
	return b.buffer.Write(p)
}

// ResourceGET and ResourcePages expose only reads and bound aggregate output.
// A failure is not an empty successful collection; callers must retain unknown.
func (c Client) resourceRead(ctx context.Context, endpoint string, paginated bool) ([]byte, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	if strings.ContainsAny(endpoint, "\x00\r\n") || strings.HasPrefix(endpoint, "/") {
		return nil, errors.New("invalid resource endpoint")
	}
	path := "repos/" + c.Repository
	if endpoint != "" {
		path += "/" + endpoint
	}
	args := []string{"api", "--method", "GET"}
	if paginated {
		args = append(args, "--paginate", "--slurp")
	}
	args = append(args, path)
	return c.resourceReadCommand(ctx, args, nil)
}

func (c Client) resourceReadCommand(ctx context.Context, args []string, input []byte) ([]byte, error) {
	if err := c.valid(); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "gh", args...)
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	budget, _ := ctx.Value(resourceBudgetKey{}).(*resourceReadBudget)
	if budget != nil && !budget.available() {
		return nil, errors.New("shared resource read budget exhausted")
	}
	output := boundedResourceOutput{budget: budget}
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return output.buffer.Bytes(), nil
}
func (c Client) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	raw, err := c.resourceRead(ctx, path, false)
	if err == nil && !json.Valid(raw) {
		err = errors.New("invalid resource JSON")
	}
	return json.RawMessage(raw), err
}
func (c Client) ResourcePages(ctx context.Context, path, field string) ([]json.RawMessage, error) {
	raw, err := c.resourceRead(ctx, path, true)
	if err != nil {
		return nil, err
	}
	var pages []json.RawMessage
	if err = json.Unmarshal(raw, &pages); err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, errors.New("resource response lacks a page")
	}
	items := []json.RawMessage{}
	for _, page := range pages {
		if field != "" {
			var object map[string]json.RawMessage
			if err = json.Unmarshal(page, &object); err != nil {
				return nil, err
			}
			page = object[field]
		}
		if len(page) == 0 || page[0] != '[' {
			return nil, errors.New("resource page lacks complete collection")
		}
		var part []json.RawMessage
		if err = json.Unmarshal(page, &part); err != nil {
			return nil, err
		}
		items = append(items, part...)
		if len(items) > 2048 {
			return nil, errors.New("resource collection exceeds entity budget")
		}
	}
	return items, nil
}
