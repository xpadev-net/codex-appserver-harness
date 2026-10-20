package policy

import "testing"

func TestCannotShrinkProfiles(t *testing.T) {
	v := Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]Profile{"unit": {Revision: 1, Argv: []string{"true"}, TimeoutSeconds: 1}}}
	set, err := v.ExecutionSet(nil)
	if err != nil || len(set) != 1 || set[0] != "unit" {
		t.Fatal(set, err)
	}
	if _, err = v.ExecutionSet([]string{"injected"}); err == nil {
		t.Fatal("unregistered profile accepted")
	}
}
