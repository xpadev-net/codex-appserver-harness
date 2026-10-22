package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// A shared authentication/configuration template is never mounted in a worker.
// Each persistent session owns its native thread store. Static authentication
// and approved configuration/assets are restored on each launch. OAuth refresh
// tokens must be owned by a central provider, never cloned across profiles.
func sessionProfile(template, home string) (string, error) {
	return sessionProfileWithAuth(template, home, false)
}
func sessionProfileWithAuth(template, home string, externalAuthentication bool) (string, error) {
	profile := filepath.Join(home, "codex")
	if err := os.MkdirAll(profile, 0700); err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(profile)
	if err != nil || canonical != profile {
		return "", errors.New("session profile must not redirect")
	}
	source, err := os.OpenRoot(template)
	if err != nil {
		return "", err
	}
	defer source.Close()
	destination, err := os.OpenRoot(profile)
	if err != nil {
		return "", err
	}
	defer destination.Close()
	if info, err := destination.Lstat("auth.json"); err == nil {
		if !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return "", errors.New("session authentication must be regular and bounded")
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	for _, name := range []string{"auth.json", "config.toml", "AGENTS.md", "AGENTS.override.md"} {
		if name == "auth.json" && externalAuthentication {
			if err = destination.RemoveAll(name); err != nil {
				return "", err
			}
			continue
		}
		info, err := source.Lstat(name)
		if os.IsNotExist(err) {
			if err = destination.RemoveAll(name); err != nil {
				return "", err
			}
			continue
		}
		if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return "", errors.New("worker profile template file must be regular and bounded")
		}
		input, err := source.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return "", err
		}
		current, err := input.Stat()
		if err != nil || !current.Mode().IsRegular() || current.Size() > 1<<20 {
			input.Close()
			return "", errors.New("worker profile file changed during copy")
		}
		temp, err := os.CreateTemp(profile, ".profile-copy-")
		if err != nil {
			input.Close()
			return "", err
		}
		path := temp.Name()
		var auth bytes.Buffer
		reader := io.Reader(input)
		if name == "auth.json" {
			reader = io.TeeReader(input, &auth)
		}
		n, copyErr := io.Copy(temp, io.LimitReader(reader, (1<<20)+1))
		if copyErr == nil && name == "auth.json" {
			copyErr = validateStaticAuthentication(auth.Bytes())
		}
		inputErr := input.Close()
		if copyErr == nil && n > 1<<20 {
			copyErr = errors.New("worker profile file grew beyond its bound")
		}
		syncErr := temp.Sync()
		closeErr := temp.Close()
		if err = errors.Join(copyErr, inputErr, syncErr, closeErr); err != nil {
			os.Remove(path)
			return "", err
		}
		if err = destination.Rename(filepath.Base(path), name); err != nil {
			os.Remove(path)
			return "", err
		}
	}
	for _, name := range []string{"skills", "rules"} {
		if err = destination.RemoveAll(name); err != nil {
			return "", err
		}
		info, err := source.Lstat(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("approved worker assets must be directories")
		}
		if err = copyProfileAssets(filepath.Join(template, name), destination, name); err != nil {
			return "", errors.New("approved worker assets cannot be copied")
		}
	}
	return profile, nil
}

func copyProfileAssets(sourcePath string, destination *os.Root, name string) error {
	source, err := os.OpenRoot(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	remaining := int64(16 << 20)
	entries := 0
	return fs.WalkDir(source.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		if entries > 2048 || strings.Count(path, "/") > 32 {
			return errors.New("worker assets exceed entry/depth bounds")
		}
		target := filepath.Join(name, path)
		if entry.IsDir() {
			return destination.MkdirAll(target, 0700)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("worker assets must not contain symlinks")
		}
		input, err := source.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		info, err := input.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > remaining {
			input.Close()
			return errors.New("worker assets exceed type/size bounds")
		}
		output, err := destination.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600|(info.Mode().Perm()&0111))
		if err != nil {
			input.Close()
			return err
		}
		n, copyErr := io.Copy(output, io.LimitReader(input, remaining+1))
		closeErr := errors.Join(input.Close(), output.Close())
		if n > remaining {
			return errors.New("worker asset grew beyond its bound")
		}
		remaining -= n
		return errors.Join(copyErr, closeErr)
	})
}

// Static API keys have no rotating refresh ownership. ChatGPT OAuth must use a
// centralized token provider (a separate runtime slice); rejecting it here keeps
// new or existing session profiles from becoming independent refresh owners.
func validateStaticAuthentication(raw []byte) error {
	var auth struct {
		Mode *string `json:"auth_mode"`
		Key  string  `json:"OPENAI_API_KEY"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&auth) != nil || auth.Key == "" || auth.Mode != nil && *auth.Mode != "apikey" {
		return errors.New("profile authentication requires a static API key; OAuth requires a central provider")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("invalid static authentication")
	}
	return nil
}
