package github

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const IssuePoolPageSize = 32

// This constant query cannot perform GraphQL mutations. Issue bodies are fetched
// separately through the existing bounded GET source checks after selection.
const issuePoolQuery = `query HarnessIssuePool($owner:String!,$name:String!,$labels:[String!],$after:String) {
 repository(owner:$owner,name:$name) {
  databaseId nameWithOwner
  issues(first:32,after:$after,states:[OPEN],labels:$labels,orderBy:{field:CREATED_AT,direction:ASC}) {
   nodes { number state labels(first:100) { nodes { name } pageInfo { hasNextPage } } }
   pageInfo { hasNextPage endCursor }
  }
 }
}`

type IssuePoolNode struct {
	Number         int      `json:"number"`
	State          string   `json:"state"`
	Labels         []string `json:"labels"`
	LabelsComplete bool     `json:"labels_complete"`
}
type IssuePoolPage struct {
	RepositoryID int64           `json:"repository_id"`
	Repository   string          `json:"repository"`
	Nodes        []IssuePoolNode `json:"nodes"`
	HasNext      bool            `json:"has_next"`
	EndCursor    string          `json:"end_cursor"`
	Bytes        int             `json:"-"`
}

func (p IssuePoolPage) Validate(repositoryID int64, repository, after string) error {
	if p.RepositoryID != repositoryID || !strings.EqualFold(p.Repository, repository) || p.Nodes == nil || len(p.Nodes) > IssuePoolPageSize || p.Bytes < 0 || p.Bytes > MaxResourceReadBytes || !validPoolCursor(p.EndCursor) || (p.HasNext && (p.EndCursor == "" || p.EndCursor == after || len(p.Nodes) == 0)) {
		return errors.New("invalid or foreign Issue metadata page")
	}
	seen := map[int]bool{}
	for _, item := range p.Nodes {
		if item.Number < 1 || seen[item.Number] || (item.State != "OPEN" && item.State != "CLOSED") || len(item.Labels) > 100 {
			return errors.New("invalid Issue metadata node")
		}
		seen[item.Number] = true
		labels := map[string]bool{}
		for _, name := range item.Labels {
			if name == "" || len(name) > 4096 || !utf8.ValidString(name) || strings.ContainsAny(name, "\x00\r\n") || labels[name] {
				return errors.New("invalid Issue metadata labels")
			}
			labels[name] = true
		}
	}
	return nil
}

func validPoolCursor(cursor string) bool {
	return len(cursor) <= 4096 && utf8.ValidString(cursor) && !strings.ContainsAny(cursor, "\x00\r\n")
}

// IssuePoolMetadata exposes only this fixed read query, never arbitrary GraphQL.
// POST is the GraphQL transport; it grants no GitHub mutation capability.
func (c Client) IssuePoolMetadata(ctx context.Context, labels []string, after string) (IssuePoolPage, error) {
	var page IssuePoolPage
	if err := c.valid(); err != nil {
		return page, err
	}
	if len(labels) < 1 || len(labels) > 17 || !validPoolCursor(after) {
		return page, errors.New("bounded Issue labels and cursor required")
	}
	for _, label := range labels {
		if strings.TrimSpace(label) == "" || len(label) > 256 || !utf8.ValidString(label) || strings.ContainsAny(label, "\x00\r\n") {
			return page, errors.New("bounded Issue labels required")
		}
	}
	parts := strings.Split(c.Repository, "/")
	var cursor *string
	if after != "" {
		cursor = &after
	}
	input, err := json.Marshal(map[string]any{"query": issuePoolQuery, "variables": map[string]any{"owner": parts[0], "name": parts[1], "labels": labels, "after": cursor}})
	if err != nil {
		return page, err
	}
	raw, err := c.resourceReadCommand(ctx, []string{"api", "--method", "POST", "graphql", "--input", "-"}, input)
	if err != nil {
		return page, err
	}
	var response struct {
		Errors []json.RawMessage `json:"errors"`
		Data   *struct {
			Repository *struct {
				ID     int64  `json:"databaseId"`
				Name   string `json:"nameWithOwner"`
				Issues *struct {
					Nodes *[]struct {
						Number int    `json:"number"`
						State  string `json:"state"`
						Labels *struct {
							Nodes *[]struct {
								Name string `json:"name"`
							} `json:"nodes"`
							PageInfo *struct {
								HasNext *bool `json:"hasNextPage"`
							} `json:"pageInfo"`
						} `json:"labels"`
					} `json:"nodes"`
					PageInfo *struct {
						HasNext *bool   `json:"hasNextPage"`
						End     *string `json:"endCursor"`
					} `json:"pageInfo"`
				} `json:"issues"`
			} `json:"repository"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return page, err
	}
	if len(response.Errors) != 0 || response.Data == nil || response.Data.Repository == nil {
		return page, errors.New("Issue metadata query unavailable")
	}
	repo := response.Data.Repository
	if repo.ID != c.RepositoryID || !strings.EqualFold(repo.Name, c.Repository) || repo.Issues == nil || repo.Issues.Nodes == nil || repo.Issues.PageInfo == nil || repo.Issues.PageInfo.HasNext == nil {
		return page, errors.New("incomplete or foreign Issue metadata page")
	}
	page.RepositoryID, page.Repository, page.Bytes = repo.ID, repo.Name, len(raw)
	page.HasNext = *repo.Issues.PageInfo.HasNext
	if repo.Issues.PageInfo.End != nil {
		page.EndCursor = *repo.Issues.PageInfo.End
	}
	if !validPoolCursor(page.EndCursor) || (page.HasNext && (page.EndCursor == "" || page.EndCursor == after)) || len(*repo.Issues.Nodes) > IssuePoolPageSize {
		return page, errors.New("invalid Issue pagination boundary")
	}
	page.Nodes = []IssuePoolNode{}
	seen := map[int]bool{}
	for _, node := range *repo.Issues.Nodes {
		if node.Number < 1 || seen[node.Number] || (node.State != "OPEN" && node.State != "CLOSED") || node.Labels == nil || node.Labels.Nodes == nil || node.Labels.PageInfo == nil || node.Labels.PageInfo.HasNext == nil || len(*node.Labels.Nodes) > 100 {
			return page, errors.New("invalid Issue metadata node")
		}
		seen[node.Number] = true
		item := IssuePoolNode{Number: node.Number, State: node.State, LabelsComplete: !*node.Labels.PageInfo.HasNext, Labels: []string{}}
		labels := map[string]bool{}
		for _, label := range *node.Labels.Nodes {
			if label.Name == "" || len(label.Name) > 4096 || !utf8.ValidString(label.Name) || labels[label.Name] {
				return page, errors.New("invalid Issue metadata labels")
			}
			labels[label.Name] = true
			item.Labels = append(item.Labels, label.Name)
		}
		page.Nodes = append(page.Nodes, item)
	}
	return page, page.Validate(c.RepositoryID, c.Repository, after)
}
