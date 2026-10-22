package ipc

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

func privateRoot(root string) error {
	if !filepath.IsAbs(root) {
		return errors.New("IPC root must be absolute")
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return errors.New("IPC root must be canonical without symlinks")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("IPC root must be private and owned by controller")
	}
	return nil
}
func ownedSocket(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("private controller socket required")
	}
	return info, nil
}
func sameUser(conn *net.UnixConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var peerErr error
	if err = raw.Control(func(fd uintptr) {
		credentials, e := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if e != nil {
			peerErr = e
			return
		}
		if credentials.Uid != uint32(os.Geteuid()) {
			peerErr = errors.New("foreign IPC peer")
		}
	}); err != nil {
		return err
	}
	return peerErr
}
