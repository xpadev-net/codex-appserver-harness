package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"golang.org/x/sys/unix"
)

// The legacy projection must still match before its policy identity is used.
// A policy edit requires removing this origin and explicitly adopting the edit.
type ConfigUpgradeOrigin struct {
	SourceSHA256      string `json:"source_sha256"`
	PolicyDigest      string `json:"policy_digest"`
	ImplicitExecution bool   `json:"implicit_execution"`
}

type ConfigUpgradeReceipt struct {
	Destination       string              `json:"destination"`
	DestinationSHA256 string              `json:"destination_sha256"`
	Origin            ConfigUpgradeOrigin `json:"origin"`
}

func configDigest(c Config) string {
	raw, _ := json.Marshal(c)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func (c Config) legacyPolicyProjection() Config {
	if c.UpgradeOrigin.ImplicitExecution {
		c.Execution = nil
	}
	c.Version = 1
	c.UpgradeOrigin = nil
	return c
}

func (c Config) policySnapshotConfig() (Config, error) {
	if err := c.validateUpgradeOrigin(); err != nil {
		return Config{}, err
	}
	if c.UpgradeOrigin != nil {
		c = c.legacyPolicyProjection()
	}
	return c, nil
}

func (c Config) validateUpgradeOrigin() error {
	origin := c.UpgradeOrigin
	if origin == nil {
		return nil
	}
	sha := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if c.Version != 2 || !sha.MatchString(origin.SourceSHA256) || !sha.MatchString(origin.PolicyDigest) || c.Execution == nil {
		return errors.New("valid v2 config upgrade origin required")
	}
	if origin.ImplicitExecution {
		if *c.Execution != domain.LegacyExecutionLimits() {
			return errors.New("implicit legacy execution must remain serial; explicitly adopt policy changes")
		}
	}
	if configDigest(c.legacyPolicyProjection()) != origin.PolicyDigest {
		return errors.New("upgraded config differs from its legacy policy; explicitly adopt policy changes")
	}
	return nil
}

func trustedConfigPolicyDigest(c Config) string {
	if c.UpgradeOrigin != nil && c.validateUpgradeOrigin() == nil {
		return c.UpgradeOrigin.PolicyDigest
	}
	return configDigest(c)
}

func readUpgradeFile(path string, private bool) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || info.Mode().Perm()&0022 != 0 || private && info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return nil, errors.New("bounded same-user trusted regular config required")
	}
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err == nil && len(raw) > 1<<20 {
		err = errors.New("config exceeds size limit")
	}
	return raw, err
}

func syncConfigDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// UpgradeTrustedConfig publishes a separate v2 file. It neither opens a DB nor
// grants migration/runtime admission, and never overwrites its input or output.
func UpgradeTrustedConfig(ctx context.Context, source, destination string) (ConfigUpgradeReceipt, error) {
	return upgradeTrustedConfig(ctx, source, destination, nil)
}

func upgradeTrustedConfig(ctx context.Context, source, destination string, checkpoint func(string) error) (ConfigUpgradeReceipt, error) {
	var receipt ConfigUpgradeReceipt
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	for _, path := range []string{source, destination} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || canonical(path) != path {
			return receipt, errors.New("canonical absolute config paths required")
		}
	}
	if source == destination {
		return receipt, errors.New("separate config upgrade destination required")
	}
	raw, err := readUpgradeFile(source, false)
	if err != nil {
		return receipt, err
	}
	c, err := loadConfigData(source, raw)
	if err != nil {
		return receipt, err
	}
	if c.Version != 1 || c.UpgradeOrigin != nil {
		return receipt, errors.New("upgrade source must be trusted config version 1")
	}
	origin := ConfigUpgradeOrigin{SourceSHA256: fmt.Sprintf("%x", sha256.Sum256(raw)), PolicyDigest: configDigest(c), ImplicitExecution: c.Execution == nil}
	execution := c.ExecutionLimits()
	c.Version, c.Execution, c.UpgradeOrigin = 2, &execution, &origin
	converted, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return receipt, err
	}
	converted = append(converted, '\n')
	if _, err := loadConfigData(destination, converted); err != nil {
		return receipt, err
	}
	parent := filepath.Dir(destination)
	info, err := os.Stat(parent)
	if err != nil {
		return receipt, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		return receipt, errors.New("same-user trusted destination directory required")
	}
	receipt = ConfigUpgradeReceipt{Destination: destination, DestinationSHA256: fmt.Sprintf("%x", sha256.Sum256(converted)), Origin: origin}
	if existing, err := readUpgradeFile(destination, true); err == nil {
		if !bytes.Equal(existing, converted) {
			return ConfigUpgradeReceipt{}, errors.New("config upgrade destination conflicts")
		}
		return receipt, syncConfigDirectory(parent)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ConfigUpgradeReceipt{}, err
	}
	f, err := os.CreateTemp(parent, ".config-upgrade-*")
	if err != nil {
		return ConfigUpgradeReceipt{}, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(converted); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return ConfigUpgradeReceipt{}, err
	}
	if checkpoint != nil {
		if err := checkpoint("before_publish"); err != nil {
			return ConfigUpgradeReceipt{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return ConfigUpgradeReceipt{}, err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, f.Name(), unix.AT_FDCWD, destination, unix.RENAME_NOREPLACE); err != nil {
		return ConfigUpgradeReceipt{}, err
	}
	if checkpoint != nil {
		if err := checkpoint("after_publish"); err != nil {
			return receipt, err
		}
	}
	return receipt, syncConfigDirectory(parent)
}
