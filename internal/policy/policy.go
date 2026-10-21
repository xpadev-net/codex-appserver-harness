// Package policy holds trusted configuration snapshots outside candidate worktrees.
package policy

import (
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

// GoTestSuccessPattern requires an executed passing test, not a package summary.
const GoTestSuccessPattern = `(?m)^--- PASS: [^\r\n]+`

type Profile struct {
	EntryPointSHA256 string   `json:"entrypoint_sha256,omitempty"`
	Revision         int      `json:"revision"`
	Argv             []string `json:"argv"`
	TimeoutSeconds   int      `json:"timeout_seconds"`
	SuccessPattern   string   `json:"success_pattern"`
}
type Verification struct {
	Revision         int                `json:"revision"`
	RequiredProfiles []string           `json:"required_profiles"`
	Profiles         map[string]Profile `json:"profiles"`
}

func (v Verification) Validate() error {
	if v.Revision < 1 || len(v.RequiredProfiles) == 0 {
		return errors.New("verification policy requires revision and profiles")
	}
	for id, p := range v.Profiles {
		if p.EntryPointSHA256 != "" && !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(p.EntryPointSHA256) {
			return errors.New("invalid trusted profile entrypoint hash")
		}
		if len(p.Argv) > 0 && strings.Contains(p.Argv[0], "/") && !filepath.IsAbs(p.Argv[0]) && p.EntryPointSHA256 == "" {
			return errors.New("relative profile entrypoint requires trusted SHA256")
		}
		if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(id) || p.Revision < 1 || len(p.Argv) == 0 || p.Argv[0] == "" || p.TimeoutSeconds < 1 {
			return errors.New("invalid profile")
		}
		if err := p.ValidateEvidence(); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, id := range v.RequiredProfiles {
		if _, ok := v.Profiles[id]; !ok || seen[id] {
			return errors.New("invalid required profile")
		}
		seen[id] = true
	}
	return nil
}
func (v Verification) ExecutionSet(requested []string) ([]string, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	out := append([]string{}, v.RequiredProfiles...)
	seen := map[string]bool{}
	for _, id := range out {
		seen[id] = true
	}
	for _, id := range requested {
		if _, ok := v.Profiles[id]; !ok {
			return nil, errors.New("unregistered profile")
		}
		if !seen[id] {
			out = append(out, id)
			seen[id] = true
		}
	}
	return out, nil
}

// ValidateEvidence rejects profiles that can pass without execution evidence.
func (p Profile) ValidateEvidence() error {
	if strings.TrimSpace(p.SuccessPattern) == "" {
		return errors.New("profile requires a success evidence pattern")
	}
	pattern, err := regexp.Compile(p.SuccessPattern)
	if err != nil {
		return err
	}
	if pattern.MatchString("") {
		return errors.New("success evidence pattern must not match empty output")
	}
	return nil
}
