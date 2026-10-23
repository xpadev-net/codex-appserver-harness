package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
)

const MaxResourceReadBytes = 4 << 20

type boundedResourceOutput struct{ buffer bytes.Buffer }

func (b *boundedResourceOutput) Write(p []byte) (int, error) {
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
	cmd := exec.CommandContext(ctx, "gh", args...)
	var output boundedResourceOutput
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
