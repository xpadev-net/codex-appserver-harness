package verification

import (
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"strings"
)

// ParseScope reads the existing acceptance fence without deriving host policy.
func ParseScope(body string) (Scope, error) {
	var scope Scope
	const fence = "```harness-scope\n"
	start := strings.Index(body, fence)
	if start < 0 {
		return scope, errors.New("ready Issue requires a fenced harness-scope JSON acceptance contract")
	}
	rest := body[start+len(fence):]
	end := strings.Index(rest, "```")
	if end < 0 {
		return scope, errors.New("unterminated harness-scope")
	}
	if err := contract.DecodeStrict([]byte(rest[:end]), &scope); err != nil {
		return scope, err
	}
	scope.OriginalRequest = body
	return scope, scope.Validate()
}
