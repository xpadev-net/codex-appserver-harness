package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

const ConfigVersion = 2
const configName = "kernel.json"
const databaseName = "state.db"

// K configuration intentionally needs no GitHub repository or write identity.
type Config struct {
	SchemaVersion int                 `json:"schema_version"`
	SessionID     string              `json:"orchestrator_session_id"`
	Model         string              `json:"model"`
	Limits        domain.KernelLimits `json:"limits"`
}

var sessionIDPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)

func (c Config) validate() error {
	if c.SchemaVersion != ConfigVersion || !sessionIDPattern.MatchString(c.SessionID) || strings.TrimSpace(c.Model) == "" || !utf8.ValidString(c.Model) || len(c.Model) > contract.MaxInline {
		return errors.New("version 2 kernel configuration, bounded session ID and model required")
	}
	return c.Limits.Validate()
}
func canonicalRoot(root string) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return errors.New("canonical absolute --root required")
	}
	actual, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if actual != root {
		return errors.New("kernel root must not traverse symlinks")
	}
	return nil
}
func readFile(path string, limit int64, private bool) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("bounded regular input file required")
	}
	if private {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("private same-user configuration required")
		}
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, errors.New("input file exceeds limit")
	}
	return raw, nil
}
func loadConfig(root string) (Config, error) {
	var c Config
	if err := canonicalRoot(root); err != nil {
		return c, err
	}
	raw, err := readFile(filepath.Join(root, configName), 64<<10, true)
	if err != nil {
		return c, err
	}
	if err = contract.DecodeStrict(raw, &c); err != nil {
		return c, err
	}
	return c, c.validate()
}
func openKernel(ctx context.Context, root string) (*storage.KernelStore, Config, error) {
	c, err := loadConfig(root)
	if err != nil {
		return nil, c, err
	}
	path := filepath.Join(root, databaseName)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, c, err
	}
	if !info.Mode().IsRegular() {
		return nil, c, errors.New("existing kernel database required")
	}
	k, err := storage.OpenKernel(path)
	if err != nil {
		return nil, c, err
	}
	binding, err := k.SessionBinding(ctx, c.SessionID)
	if err == nil && (binding.Kind != "orchestrator" || binding.Model != c.Model || binding.CWD != filepath.Join(root, "workspaces", c.SessionID) || binding.Sandbox != "read-only") {
		err = errors.New("kernel configuration differs from saved main identity")
	}
	if err != nil {
		k.Close()
		return nil, c, err
	}
	k.ControllerArtifactBytes = c.Limits.ControllerArtifactBytes
	return k, c, nil
}
func writeConfigExclusive(root string, c Config) (bool, error) {
	f, err := os.CreateTemp(root, ".kernel-config-*")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		f.Close()
		return false, err
	}
	if _, err = f.Write(append(raw, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	if err = os.Link(f.Name(), filepath.Join(root, configName)); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, err := loadConfig(root)
			if err != nil {
				return false, err
			}
			if !reflect.DeepEqual(existing, c) {
				return false, errors.New("kernel init configuration conflict")
			}
			return false, nil
		}
		return false, err
	}
	if err = os.Remove(f.Name()); err != nil {
		return false, err
	}
	directory, err := os.Open(root)
	if err != nil {
		return false, err
	}
	err = directory.Sync()
	closeErr = directory.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
func initialize(ctx context.Context, root, session, model string) (Config, error) {
	c := Config{SchemaVersion: ConfigVersion, SessionID: session, Model: model, Limits: domain.DefaultKernelLimits()}
	if err := c.validate(); err != nil {
		return c, err
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return c, errors.New("canonical absolute --root required")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return c, err
	}
	if err := canonicalRoot(root); err != nil {
		return c, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return c, err
	}
	if info.Mode().Perm()&0077 != 0 {
		return c, errors.New("private kernel root required")
	}
	if _, err := os.Lstat(filepath.Join(root, configName)); errors.Is(err, os.ErrNotExist) {
		if _, dbErr := os.Lstat(filepath.Join(root, databaseName)); dbErr == nil {
			return c, errors.New("existing database without kernel configuration requires explicit recovery/migration")
		} else if !errors.Is(dbErr, os.ErrNotExist) {
			return c, dbErr
		}
	} else if err != nil {
		return c, err
	}
	created, err := writeConfigExclusive(root, c)
	if err != nil {
		return c, err
	}
	k, err := storage.OpenKernel(filepath.Join(root, databaseName))
	if err != nil {
		if created {
			_ = os.Remove(filepath.Join(root, configName))
		}
		return c, err
	}
	defer k.Close()
	workspace := filepath.Join(root, "workspaces", session)
	if err = os.MkdirAll(workspace, 0700); err != nil {
		return c, err
	}
	if err = canonicalRoot(workspace); err != nil {
		return c, err
	}
	if _, err = k.CreateOrchestrator(ctx, session, model, workspace, "read-only"); err != nil {
		return c, err
	}
	return c, nil
}
