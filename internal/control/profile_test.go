package control

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestSessionProfilesSeparateThreadStoresAndRestoreApprovedConfiguration(t *testing.T) {
	template, homeA, homeB := t.TempDir(), t.TempDir(), t.TempDir()
	write := func(path, body string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(template, "auth.json"), `{"OPENAI_API_KEY":"fixture-source-key"}`, 0600)
	write(filepath.Join(template, "config.toml"), `model_reasoning_effort = "high"`, 0600)
	if err := os.MkdirAll(filepath.Join(template, "skills", "approved"), 0700); err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(template, "skills", "approved", "run.sh"), "#!/bin/sh\nexit 0\n", 0700)
	a, err := sessionProfile(template, homeA)
	if err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(a, "auth.json"), `{"OPENAI_API_KEY":"unapproved-session-key"}`, 0600)
	write(filepath.Join(a, "config.toml"), "worker-mutated", 0600)
	write(filepath.Join(a, "thread-history"), "private session A history", 0600)
	b, err := sessionProfile(template, homeB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(b, "thread-history")); !os.IsNotExist(err) {
		t.Fatal("session A history reached session B", err)
	}
	if _, err = sessionProfile(template, homeA); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct{ path, want string }{
		{filepath.Join(template, "auth.json"), `{"OPENAI_API_KEY":"fixture-source-key"}`},
		{filepath.Join(a, "auth.json"), `{"OPENAI_API_KEY":"fixture-source-key"}`},
		{filepath.Join(b, "auth.json"), `{"OPENAI_API_KEY":"fixture-source-key"}`},
		{filepath.Join(a, "config.toml"), `model_reasoning_effort = "high"`},
		{filepath.Join(b, "config.toml"), `model_reasoning_effort = "high"`},
		{filepath.Join(a, "thread-history"), "private session A history"},
	} {
		data, err := os.ReadFile(check.path)
		if err != nil || string(data) != check.want {
			t.Fatal("profile authority/history changed", check.path, err)
		}
	}
	info, err := os.Stat(filepath.Join(a, "skills", "approved", "run.sh"))
	if err != nil || info.Mode().Perm()&0100 == 0 {
		t.Fatal("approved executable skill lost its mode", info, err)
	}
	if err = os.Remove(filepath.Join(template, "config.toml")); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(filepath.Join(template, "skills")); err != nil {
		t.Fatal(err)
	}
	if _, err = sessionProfile(template, homeA); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"config.toml", "skills"} {
		if _, err = os.Lstat(filepath.Join(a, name)); !os.IsNotExist(err) {
			t.Fatal("removed approval persisted in session profile", name, err)
		}
	}
}

func TestSessionProfileRejectsRedirectsSpecialFilesAndOversizeInput(t *testing.T) {
	for _, kind := range []string{"profile-redirect", "source-redirect", "auth-oversize", "stored-auth-oversize", "stored-auth-fifo", "stored-auth-redirect", "asset-redirect", "asset-fifo", "asset-oversize"} {
		t.Run(kind, func(t *testing.T) {
			template, home := t.TempDir(), t.TempDir()
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte("unchanged"), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "profile-redirect":
				err = os.Symlink(filepath.Dir(outside), filepath.Join(home, "codex"))
			case "source-redirect":
				err = os.Symlink(outside, filepath.Join(template, "config.toml"))
			case "auth-oversize":
				err = os.WriteFile(filepath.Join(template, "auth.json"), make([]byte, (1<<20)+1), 0600)
			case "stored-auth-oversize", "stored-auth-fifo", "stored-auth-redirect":
				profile := filepath.Join(home, "codex")
				if err = os.Mkdir(profile, 0700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(profile, "auth.json")
				switch kind {
				case "stored-auth-oversize":
					err = os.WriteFile(path, make([]byte, (1<<20)+1), 0600)
				case "stored-auth-fifo":
					err = syscall.Mkfifo(path, 0600)
				case "stored-auth-redirect":
					err = os.Symlink(outside, path)
				}
			default:
				if err = os.Mkdir(filepath.Join(template, "skills"), 0700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(template, "skills", "asset")
				switch kind {
				case "asset-redirect":
					err = os.Symlink(outside, path)
				case "asset-fifo":
					err = syscall.Mkfifo(path, 0600)
				case "asset-oversize":
					var file *os.File
					file, err = os.Create(path)
					if err == nil {
						err = file.Truncate((16 << 20) + 1)
						file.Close()
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = sessionProfile(template, home); err == nil {
				t.Fatal("unsafe profile was accepted")
			}
			data, err := os.ReadFile(outside)
			if err != nil || string(data) != "unchanged" {
				t.Fatal("profile preparation modified another path", err)
			}
		})
	}
}

func TestSessionProfileRejectsClonedOAuthRefreshOwnership(t *testing.T) {
	for _, raw := range []string{`{"auth_mode":"chatgpt","tokens":{"refresh_token":"fixture-refresh"}}`, `{"tokens":{"refresh_token":"fixture-refresh"}}`} {
		template, home := t.TempDir(), t.TempDir()
		if err := os.WriteFile(filepath.Join(template, "auth.json"), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := sessionProfile(template, home); err == nil {
			t.Fatal("OAuth credentials copied into an independent profile")
		}
		if _, err := os.Stat(filepath.Join(home, "codex", "auth.json")); !os.IsNotExist(err) {
			t.Fatal("rejected OAuth auth file was published", err)
		}
	}
}
