package ipc

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type Server struct {
	// Set before Serve; hints never carry authority, they only invoke Wake.
	Reader    Reader
	Wake      func()
	PollEvery time.Duration
	path      string
	socket    os.FileInfo
	listener  *net.UnixListener
	lock      *os.File
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
	running   atomic.Bool
	mu        sync.Mutex
	closed    bool
	conns     map[*net.UnixConn]bool
	workers   sync.WaitGroup
	slots     chan struct{}
}

// Listen holds an IPC lifetime lock. The daemon separately owns its controller
// lock before dispatching work. Only a proven stale, owned socket is removed.
func Listen(root string, reader Reader, wake func()) (*Server, error) {
	if err := privateRoot(root); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(root, "ipc.lock"), syscall.O_NOFOLLOW|syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "ipc.lock")
	success := false
	defer func() {
		if !success {
			lock.Close()
		}
	}()
	info, err := lock.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("private IPC lock required")
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.New("another daemon owns IPC root")
	}
	path := filepath.Join(root, SocketName)
	if old, e := os.Lstat(path); e == nil {
		if _, e = ownedSocket(path); e != nil {
			return nil, e
		}
		conn, e := net.DialTimeout("unix", path, 100*time.Millisecond)
		if e == nil {
			conn.Close()
			return nil, errors.New("controller socket already active")
		}
		if !errors.Is(e, syscall.ECONNREFUSED) {
			return nil, errors.New("socket stopping is uncertain")
		}
		current, e := os.Lstat(path)
		if e != nil || !os.SameFile(old, current) {
			return nil, errors.New("stale socket identity changed")
		}
		if e = os.Remove(path); e != nil {
			return nil, e
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	if err = os.Chmod(path, 0600); err != nil {
		listener.Close()
		os.Remove(path)
		return nil, err
	}
	info, err = ownedSocket(path)
	if err != nil {
		listener.Close()
		os.Remove(path)
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	success = true
	return &Server{Reader: reader, Wake: wake, PollEvery: time.Second, path: path, socket: info, listener: listener, lock: lock, ctx: ctx, cancel: cancel, conns: map[*net.UnixConn]bool{}, slots: make(chan struct{}, 32)}, nil
}
func (s *Server) Close() error {
	var result error
	s.once.Do(func() {
		s.cancel()
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		result = s.listener.Close()
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.workers.Wait()
		if current, err := os.Lstat(s.path); err == nil && os.SameFile(current, s.socket) {
			result = errors.Join(result, os.Remove(s.path))
		}
		result = errors.Join(result, s.lock.Close())
	})
	return result
}
func (s *Server) Serve(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return errors.New("IPC server already serving")
	}
	if s.PollEvery <= 0 {
		return errors.New("positive IPC poll interval required")
	}
	defer s.Close()
	watcherDone := make(chan struct{})
	defer close(watcherDone)
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-watcherDone:
		}
	}()
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if s.ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err = sameUser(conn); err != nil {
			conn.Close()
			continue
		}
		select {
		case s.slots <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			<-s.slots
			conn.Close()
			continue
		}
		s.conns[conn] = true
		s.workers.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.workers.Done()
			defer func() { conn.Close(); s.mu.Lock(); delete(s.conns, conn); s.mu.Unlock(); <-s.slots }()
			s.handle(conn)
		}()
	}
}
func (s *Server) send(conn *net.UnixConn, value Response) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	if err = conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}
func (s *Server) handle(conn *net.UnixConn) {
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 4096)
	if !scanner.Scan() {
		return
	}
	var request Request
	if err := contract.DecodeStrict(scanner.Bytes(), &request); err != nil {
		_ = s.send(conn, Response{Version: Version, Error: "invalid IPC request"})
		return
	}
	if err := request.validate(); err != nil {
		_ = s.send(conn, Response{Version: Version, Error: err.Error()})
		return
	}
	if request.Method != "events" {
		if s.Wake != nil {
			s.Wake()
		}
		_ = s.send(conn, Response{Version: Version, Accepted: true})
		return
	}
	if s.Reader == nil {
		_ = s.send(conn, Response{Version: Version, Error: "state stream unavailable"})
		return
	}
	for {
		query, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		page, err := s.Reader.StateEvents(query, request.After, request.Limit)
		cancel()
		if err != nil {
			_ = s.send(conn, Response{Version: Version, Error: "state cursor read failed"})
			return
		}
		if err = page.Validate(request.After, request.Limit); err != nil {
			_ = s.send(conn, Response{Version: Version, Error: "invalid state page"})
			return
		}
		if err = s.send(conn, Response{Version: Version, Page: &page}); err != nil {
			return
		}
		request.After = page.Cursor
		if page.More {
			continue
		}
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(s.PollEvery):
		}
	}
}
