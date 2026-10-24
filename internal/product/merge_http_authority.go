package product

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

const mergeHTTPAuthorityName = "merge-authorize.sock"
const mergeHTTPAuthorityLockName = "merge-authorize.lock"

func MergeHTTPAuthorityPath(root string) string { return filepath.Join(root, mergeHTTPAuthorityName) }

// MergeHTTPProtectedPaths must be hidden from isolated model/runner processes.
func MergeHTTPProtectedPaths(root string) []string {
	return []string{filepath.Join(root, "merge-http"), MergeHTTPAuthorityPath(root), filepath.Join(root, mergeHTTPAuthorityLockName)}
}

type mergeHTTPAuthorityRequest struct {
	MergeID    string `json:"merge_id"`
	SpecDigest string `json:"spec_digest"`
}
type mergeHTTPAuthorityReply struct {
	Observation *storage.ProductMergeGateObservation `json:"observation,omitempty"`
	Error       string                               `json:"error,omitempty"`
}

type MergeHTTPAuthority struct {
	running   atomic.Bool
	store     *storage.KernelStore
	check     func(context.Context, storage.ProductMerge) (storage.ProductMergeGateObservation, error)
	listener  *net.UnixListener
	directory *os.File
	lock      *os.File
	ctx       context.Context
	cancel    context.CancelFunc
	socket    os.FileInfo
	once      sync.Once
	mu        sync.Mutex
	closed    bool
	conns     map[*net.UnixConn]bool
	workers   sync.WaitGroup
}

func mergeHTTPPeer(conn *net.UnixConn) (*syscall.Ucred, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var cred *syscall.Ucred
	var peerErr error
	if err := raw.Control(func(fd uintptr) {
		cred, peerErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return nil, err
	}
	if peerErr != nil {
		return nil, peerErr
	}
	if cred.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("foreign merge HTTP peer")
	}
	return cred, nil
}
func mergeHTTPAddress(root string) (*os.File, string, error) {
	if err := mergeHTTPDirectory(root); err != nil {
		return nil, "", err
	}
	dir, err := os.Open(root)
	if err != nil {
		return nil, "", err
	}
	return dir, filepath.Join("/proc/self/fd", strconv.FormatUint(uint64(dir.Fd()), 10), mergeHTTPAuthorityName), nil
}

// ListenMergeHTTPAuthority binds a separate private host-only protocol. It does
// not extend the advisory/read-only IPC or accept model commands/results. Check
// must freshly run the complete canonical source/quality/Cloud/CI/rules gate.
func ListenMergeHTTPAuthority(k *storage.KernelStore, check func(context.Context, storage.ProductMerge) (storage.ProductMergeGateObservation, error)) (*MergeHTTPAuthority, error) {
	if k == nil || check == nil {
		return nil, errors.New("explicit merge HTTP authority store and fresh gate required")
	}
	dir, address, err := mergeHTTPAddress(k.Root)
	if err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(k.Root, mergeHTTPAuthorityLockName), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		dir.Close()
		return nil, err
	}
	info, err := lock.Stat()
	stat, ok := infoSysStat(info)
	if err != nil || !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		lock.Close()
		dir.Close()
		return nil, errors.New("private merge authority lock required")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		dir.Close()
		return nil, errors.New("another merge authority owns this root")
	}
	success := false
	defer func() {
		if !success {
			lock.Close()
			dir.Close()
		}
	}()
	path := MergeHTTPAuthorityPath(k.Root)
	if old, e := os.Lstat(path); e == nil {
		st, ok := old.Sys().(*syscall.Stat_t)
		if !ok || old.Mode()&os.ModeSocket == 0 || old.Mode().Perm() != 0600 || st.Uid != uint32(os.Geteuid()) {
			return nil, errors.New("foreign merge authority path")
		}
		probe, e := net.DialTimeout("unix", address, 100*time.Millisecond)
		if e == nil {
			probe.Close()
			return nil, errors.New("merge authority still active")
		}
		if !errors.Is(e, syscall.ECONNREFUSED) {
			return nil, errors.New("merge authority stopping unconfirmed")
		}
		current, e := os.Lstat(path)
		if e != nil || !os.SameFile(old, current) {
			return nil, errors.New("merge authority socket changed")
		}
		if e := os.Remove(path); e != nil {
			return nil, e
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}

	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: address, Net: "unix"})
	if err != nil {
		dir.Close()
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		dir.Close()
		return nil, err
	}
	info, err = os.Lstat(path)
	if err != nil {
		listener.Close()
		dir.Close()
		return nil, err
	}
	internal, cancel := context.WithCancel(context.Background())
	success = true
	return &MergeHTTPAuthority{store: k, check: check, listener: listener, directory: dir, lock: lock, ctx: internal, cancel: cancel, socket: info, conns: map[*net.UnixConn]bool{}}, nil
}
func (s *MergeHTTPAuthority) Close() {
	s.once.Do(func() {
		s.mu.Lock()
		s.cancel()
		s.closed = true
		_ = s.listener.Close()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.workers.Wait()
		path := MergeHTTPAuthorityPath(s.store.Root)
		if info, err := os.Lstat(path); err == nil && os.SameFile(info, s.socket) {
			_ = os.Remove(path)
		}
		_ = s.directory.Close()
		_ = s.lock.Close()
	})
}
func (s *MergeHTTPAuthority) Serve(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return errors.New("merge authority already serving")
	}
	stop := context.AfterFunc(ctx, s.Close)
	defer stop()
	defer s.Close()
	defer s.workers.Wait()
	slots := make(chan struct{}, 8)
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		s.mu.Lock()
		closed := s.closed
		if !closed {
			s.workers.Add(1)
			s.conns[conn] = true
		}
		s.mu.Unlock()
		if closed {
			conn.Close()
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
			s.workers.Done()
			conn.Close()
			continue
		}
		go func() {
			defer s.workers.Done()
			defer func() { <-slots; s.mu.Lock(); delete(s.conns, conn); s.mu.Unlock(); conn.Close() }()
			work, cancel := context.WithTimeout(s.ctx, 30*time.Second)
			defer cancel()
			_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
			reply := mergeHTTPAuthorityReply{Error: "merge authorization unavailable"}
			peer, err := mergeHTTPPeer(conn)
			var request mergeHTTPAuthorityRequest
			decoder := json.NewDecoder(io.LimitReader(conn, 8192))
			decoder.DisallowUnknownFields()
			if err == nil {
				err = decoder.Decode(&request)
			}
			if err == nil && !mergeHTTPFixedDigest(request.SpecDigest) {
				err = storage.ErrProductMergeHeld
			}
			var job storage.ProductMerge
			if err == nil {
				job, err = s.store.ReadProductMerge(work, request.MergeID)
			}
			if err == nil && (job.State != "unknown" || job.HTTP == nil || job.Stop != nil || job.HTTPAbort != nil || job.HTTPWrite != nil || job.HTTP.SpecDigest != request.SpecDigest || int(peer.Pid) != job.HTTP.Process.PID || process.Identify(int(peer.Pid)) != job.HTTP.Process) {
				err = storage.ErrProductMergeHeld
			}
			if err == nil {
				var observation storage.ProductMergeGateObservation
				observation, err = s.check(work, job)
				if err == nil {
					reply = mergeHTTPAuthorityReply{Observation: &observation}
				}
			}
			raw, e := json.Marshal(reply)
			if e == nil && len(raw) <= mergeHTTPBytes {
				raw = append(raw, '\n')
				_, _ = conn.Write(raw)
			}
		}()
	}
}
func requestMergeHTTPAuthority(ctx context.Context, k *storage.KernelStore, job storage.ProductMerge, specDigest string) (storage.ProductMergeGateObservation, error) {
	var empty storage.ProductMergeGateObservation
	dir, address, err := mergeHTTPAddress(k.Root)
	if err != nil {
		return empty, err
	}
	defer dir.Close()
	info, err := os.Lstat(MergeHTTPAuthorityPath(k.Root))
	if err != nil {
		return empty, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 {
		return empty, errors.New("private merge HTTP authority socket required")
	}
	rawConn, err := (&net.Dialer{}).DialContext(ctx, "unix", address)
	if err != nil {
		return empty, err
	}
	conn, ok := rawConn.(*net.UnixConn)
	if !ok {
		rawConn.Close()
		return empty, errors.New("Unix merge authority required")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := mergeHTTPPeer(conn); err != nil {
		return empty, err
	}
	if err := json.NewEncoder(conn).Encode(mergeHTTPAuthorityRequest{MergeID: job.ID, SpecDigest: specDigest}); err != nil {
		return empty, err
	}
	var reply mergeHTTPAuthorityReply
	decoder := json.NewDecoder(io.LimitReader(conn, mergeHTTPBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return empty, err
	}
	if reply.Error != "" || reply.Observation == nil {
		return empty, storage.ErrProductMergeHeld
	}
	return *reply.Observation, nil
}

func infoSysStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	return st, ok
}
