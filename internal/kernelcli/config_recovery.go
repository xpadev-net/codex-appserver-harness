package kernelcli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Called only while holding the exclusive initialization lock. Earlier
// publication used a temporary hard link; recover exactly those private links
// without relaxing the ordinary single-link configuration requirement.
func recoverConfigPublication(root string) error {
	f, err := os.OpenFile(filepath.Join(root, configName), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
		return errors.New("private regular configuration required for publication recovery")
	}
	if stat.Nlink == 1 {
		return nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	links := []string{}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".kernel-config-") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		candidate, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if candidate.Mode().IsRegular() && os.SameFile(info, candidate) {
			links = append(links, path)
		}
	}
	if uint64(len(links))+1 != uint64(stat.Nlink) {
		return errors.New("unrecognized configuration hard links require explicit recovery")
	}
	for _, path := range links {
		current, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !current.Mode().IsRegular() || !os.SameFile(info, current) {
			return errors.New("configuration publication link changed during recovery")
		}
		if err = os.Remove(path); err != nil {
			return err
		}
	}
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
