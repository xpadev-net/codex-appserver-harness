package github

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func issueMetadataResponse() map[string]any {
	return map[string]any{"data": map[string]any{"repository": map[string]any{"databaseId": 17, "nameWithOwner": "owner/repo", "issues": map[string]any{
		"nodes":    []any{map[string]any{"number": 42, "state": "OPEN", "labels": map[string]any{"nodes": []any{map[string]any{"name": "ready"}}, "pageInfo": map[string]any{"hasNextPage": false}}}},
		"pageInfo": map[string]any{"hasNextPage": true, "endCursor": "next-cursor"},
	}}}}
}

func issueMetadataTransport(t *testing.T) (Client, string, string, string) {
	t.Helper()
	dir := t.TempDir()
	payload, request, args := filepath.Join(dir, "payload"), filepath.Join(dir, "request"), filepath.Join(dir, "args")
	script := `#!/bin/sh
printf '%s\n' "$*" > "$ISSUE_METADATA_TEST_ARGS"
/bin/cat > "$ISSUE_METADATA_TEST_REQUEST"
/bin/cat "$ISSUE_METADATA_TEST_PAYLOAD"
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("ISSUE_METADATA_TEST_ARGS", args)
	t.Setenv("ISSUE_METADATA_TEST_REQUEST", request)
	t.Setenv("ISSUE_METADATA_TEST_PAYLOAD", payload)
	return Client{Repository: "owner/repo", RepositoryID: 17}, payload, request, args
}

func TestIssueMetadataUsesFixedReadQueryAndBoundedCursorVariables(t *testing.T) {
	c, payload, request, args := issueMetadataTransport(t)
	raw, err := json.Marshal(issueMetadataResponse())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(payload, raw, 0600); err != nil {
		t.Fatal(err)
	}
	page, err := c.IssuePoolMetadata(context.Background(), []string{"ready", "P0"}, "previous-cursor")
	if err != nil || !page.HasNext || page.EndCursor != "next-cursor" || len(page.Nodes) != 1 || page.Nodes[0].Number != 42 || !page.Nodes[0].LabelsComplete || page.Bytes != len(raw) {
		t.Fatal(page, err)
	}
	encoded, err := os.ReadFile(request)
	if err != nil {
		t.Fatal(err)
	}
	var sent struct {
		Query     string `json:"query"`
		Variables struct {
			Owner  string   `json:"owner"`
			Name   string   `json:"name"`
			Labels []string `json:"labels"`
			After  string   `json:"after"`
		} `json:"variables"`
	}
	if err := json.Unmarshal(encoded, &sent); err != nil || sent.Query != issuePoolQuery || strings.Contains(sent.Query, "mutation") || strings.Contains(sent.Query, "body") || sent.Variables.Owner != "owner" || sent.Variables.Name != "repo" || sent.Variables.After != "previous-cursor" || len(sent.Variables.Labels) != 2 {
		t.Fatal("metadata query was not fixed and body-free", sent, err)
	}
	argv, err := os.ReadFile(args)
	if err != nil || string(argv) != "api --method POST graphql --input -\n" {
		t.Fatal(string(argv), err)
	}
	if _, err := c.IssuePoolMetadata(context.Background(), []string{"ready"}, "invalid\x00cursor"); err == nil {
		t.Fatal("unbounded cursor accepted")
	}
}

func TestIssueMetadataRejectsIncompleteForeignAndNonAdvancingPages(t *testing.T) {
	c, payload, _, _ := issueMetadataTransport(t)
	for _, mode := range []string{"graphql_error", "missing_data", "foreign", "missing_nodes", "null_nodes", "missing_boundary", "missing_cursor", "repeat_cursor", "empty_next", "node_identity", "missing_labels_boundary", "too_many", "partial_labels", "empty_complete"} {
		t.Run(mode, func(t *testing.T) {
			response := issueMetadataResponse()
			repo := response["data"].(map[string]any)["repository"].(map[string]any)
			issues := repo["issues"].(map[string]any)
			info := issues["pageInfo"].(map[string]any)
			node := issues["nodes"].([]any)[0].(map[string]any)
			switch mode {
			case "graphql_error":
				response["errors"] = []any{map[string]any{"message": "unavailable"}}
			case "missing_data":
				delete(response, "data")
			case "foreign":
				repo["databaseId"] = 18
			case "missing_nodes":
				delete(issues, "nodes")
			case "null_nodes":
				issues["nodes"] = nil
			case "missing_boundary":
				delete(info, "hasNextPage")
			case "missing_cursor":
				info["endCursor"] = nil
			case "repeat_cursor":
				info["endCursor"] = "previous-cursor"
			case "empty_next":
				issues["nodes"] = []any{}
			case "node_identity":
				node["number"] = 0
			case "missing_labels_boundary":
				delete(node["labels"].(map[string]any), "pageInfo")
			case "too_many":
				issues["nodes"] = make([]any, IssuePoolPageSize+1)
			case "partial_labels":
				node["labels"].(map[string]any)["pageInfo"].(map[string]any)["hasNextPage"] = true
			case "empty_complete":
				issues["nodes"] = []any{}
				info["hasNextPage"], info["endCursor"] = false, nil
			}
			raw, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(payload, raw, 0600); err != nil {
				t.Fatal(err)
			}
			page, err := c.IssuePoolMetadata(context.Background(), []string{"ready"}, "previous-cursor")
			valid := mode == "partial_labels" || mode == "empty_complete"
			if (err == nil) != valid || (mode == "partial_labels" && page.Nodes[0].LabelsComplete) {
				t.Fatal(mode, page, err)
			}
		})
	}
}
