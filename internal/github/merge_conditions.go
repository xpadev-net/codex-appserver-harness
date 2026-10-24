package github

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
)

// ResourceConditions uses the same bounded/cancellable output accounting as
// other resource reads. CLEAN is useful only under a verified rules contract;
// it never supplies that capability itself.
func (c Client) ResourceConditions(ctx context.Context, number int) (string, error) {
	if number < 1 {
		return "", errors.New("positive PR number required")
	}
	raw, err := c.resourceReadCommand(ctx, []string{"pr", "view", strconv.Itoa(number), "--repo", c.Repository, "--json", "mergeStateStatus"}, nil)
	if err != nil {
		return "", err
	}
	var result struct {
		State string `json:"mergeStateStatus"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", err
	}
	return result.State, nil
}
