package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
)

func verificationPolicyFixture(waveID, repository string, revision int) ProductVerificationPolicy {
	return ProductVerificationPolicy{WaveID: waveID, PolicyRevision: revision, RepositoryID: 17, Repository: repository, VerifierPaths: []string{"tests/", "fixtures/"}, Verification: policy.Verification{Revision: revision, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"trusted-runner", "private-profile-argument"}, TimeoutSeconds: 30, SuccessPattern: "PASS"}}}}
}

func TestProductVerificationPolicyPinsRevisionAndDoesNotPublishPrivateArgv(t *testing.T) {
	ctx := context.Background()
	k, w, task, source := productSubjectFixture(t, "verification")
	seal, err := k.ProductSeal(ctx, source.Value.TaskID, source.Value.RunID)
	if err != nil {
		t.Fatal(err)
	}
	value := verificationPolicyFixture(w.ID, seal.Value.Workspace.Value.Request.Repository, w.Policy.Revision)
	saved, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), value)
	if err != nil {
		t.Fatal(err)
	}
	if value.VerifierPaths[0] != "tests/" {
		t.Fatal("policy normalization mutated caller")
	}
	value.Verification.Profiles["extra"] = policy.Profile{Revision: 1, Argv: []string{"extra"}, TimeoutSeconds: 10, SuccessPattern: "PASS"}
	if len(saved.Value.Verification.Profiles) != 1 {
		t.Fatal("snapshot aliases caller map")
	}
	if _, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), value); !errors.Is(err, ErrProductVerificationPolicyConflict) {
		t.Fatal("policy silently expanded", err)
	}
	again, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), saved.Value)
	if err != nil || !reflect.DeepEqual(again, saved) {
		t.Fatal(again, err)
	}
	manifest := registrationManifest(t, k, task.SessionID, w.Target())
	if manifest.ProductVerificationPolicy == nil || manifest.ProductVerificationPolicy.Digest != saved.Digest || !reflect.DeepEqual(manifest.ProductVerificationPolicy.RequiredProfiles, []string{"unit"}) {
		t.Fatal(manifest.ProductVerificationPolicy)
	}
	raw, err := json.Marshal(manifest)
	if err != nil || strings.Contains(string(raw), "private-profile-argument") || strings.Contains(string(raw), "trusted-runner") {
		t.Fatal("trusted argv leaked to model", err)
	}
	parent := registrationManifest(t, k, w.OrchestratorSessionID, w.Target())
	if parent.ProductVerificationPolicy != nil {
		t.Fatal("parent received Task execution settings")
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	read, err := k.ReadProductVerificationPolicy(ctx, w.Target(), 17)
	if err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal(read, err)
	}
}

func TestProductVerificationPolicyRejectsBadPathsRevisionAndRepository(t *testing.T) {
	for _, mode := range []string{"parent", "parent_exact", "parent_prefix", "root", "glob", "git", "duplicate", "revision", "repository", "profile", "empty_evidence"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, _, request := productWorkspaceFixture(t)
			value := verificationPolicyFixture(w.ID, request.Repository, w.Policy.Revision)
			switch mode {
			case "parent":
				value.VerifierPaths = []string{"../tests/"}
			case "parent_exact":
				value.VerifierPaths = []string{".."}
			case "parent_prefix":
				value.VerifierPaths = []string{"../"}
			case "root":
				value.VerifierPaths = []string{"./"}
			case "glob":
				value.VerifierPaths = []string{"**/*_test.go"}
			case "git":
				value.VerifierPaths = []string{"tests/.git/"}
			case "duplicate":
				value.VerifierPaths = []string{"tests/", "tests/"}
			case "revision":
				value.Verification.Revision++
			case "repository":
				value.RepositoryID++
			case "profile":
				value.Verification.RequiredProfiles = []string{"missing"}
			case "empty_evidence":
				p := value.Verification.Profiles["unit"]
				p.SuccessPattern = ""
				value.Verification.Profiles["unit"] = p
			}
			if _, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), value); err == nil {
				t.Fatal("bad host policy pinned", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_verification_policy%'").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}

func TestProductVerificationPolicyLossCannotRepinAndHistoryFailureRollsBack(t *testing.T) {
	for _, mode := range []string{"head", "history", "corrupt", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, _, request := productWorkspaceFixture(t)
			value := verificationPolicyFixture(w.ID, request.Repository, w.Policy.Revision)
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_verification_policy BEFORE INSERT ON records WHEN NEW.kind='product_verification_policy_history' BEGIN SELECT RAISE(FAIL,'verification policy rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), value); err == nil || !strings.Contains(err.Error(), "verification policy rollback") {
					t.Fatal(err)
				}
				var count int
				if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_verification_policy%'").Scan(&count); err != nil || count != 0 {
					t.Fatal("partial policy pin", count, err)
				}
				return
			}
			if _, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), value); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mode {
			case "head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_verification_policy'")
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_verification_policy_history'")
			case "corrupt":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_verification_policy'")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), value); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost policy was repinned", err)
			}
		})
	}
}
