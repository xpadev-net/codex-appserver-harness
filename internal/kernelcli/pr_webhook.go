package kernelcli

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Host configuration chooses a single repository and a loopback HTTP ingress.
// A reverse proxy can expose /github; the application still verifies HMAC.
type PRWebhookConfig struct {
	SchemaVersion int    `json:"schema_version"`
	RepositoryID  int64  `json:"repository_id"`
	Listen        string `json:"listen"`
	SecretFile    string `json:"secret_file"`
}

func loadPRWebhook(path string) (daemonWorker, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := readFile(path, 4096, true)
	if err != nil {
		return nil, err
	}
	var c PRWebhookConfig
	if err = contract.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	addr, err := netip.ParseAddrPort(c.Listen)
	if err != nil || c.SchemaVersion != 1 || c.RepositoryID <= 0 || !addr.Addr().IsLoopback() || addr.Port() == 0 {
		return nil, errors.New("PR webhook schema 1, repository ID and numeric loopback host:port required")
	}
	secret, err := readFile(c.SecretFile, 4096, true)
	if err != nil {
		return nil, err
	}
	if len(secret) == 0 {
		return nil, errors.New("nonempty private webhook secret required")
	}
	return func(ctx context.Context, k *storage.KernelStore, wake func(), report func(error)) error {
		listener, err := net.Listen("tcp", c.Listen)
		if err != nil {
			return err
		}
		return servePRWebhook(ctx, listener, storage.PRInbox{Store: k, RepositoryID: c.RepositoryID}, secret, wake)
	}, nil
}

type prWakingInbox struct {
	github.Inbox
	wake func()
}

func (in prWakingInbox) Receive(ctx context.Context, id, event string, payload []byte) error {
	if err := in.Inbox.Receive(ctx, id, event, payload); err != nil {
		return err
	}
	in.wake()
	return nil
}

// Join handlers as well as Serve before releasing the controller's store.
// The accepting gate prevents Add racing with Wait after forced HTTP shutdown.
func servePRWebhook(ctx context.Context, listener net.Listener, inbox github.Inbox, secret []byte, wake func()) error {
	requests, cancel := context.WithCancel(ctx)
	defer cancel()
	defer listener.Close()
	var mu sync.Mutex
	var active sync.WaitGroup
	accepting := true
	slots := make(chan struct{}, 32)
	verified := github.Webhook(secret, prWakingInbox{Inbox: inbox, wake: wake})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if !accepting {
			mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		select {
		case slots <- struct{}{}:
		default:
			mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		active.Add(1)
		mu.Unlock()
		defer active.Done()
		defer func() { <-slots }()
		if r.URL.Path != "/github" {
			http.NotFound(w, r)
			return
		}
		verified.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: handler, BaseContext: func(net.Listener) context.Context { return requests }, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	var serveErr error
	returned := false
	select {
	case serveErr = <-done:
		returned = true
	case <-ctx.Done():
	}
	mu.Lock()
	accepting = false
	mu.Unlock()
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	shutdownErr := server.Shutdown(shutdown)
	_ = server.Close()
	if !returned {
		serveErr = <-done
	}
	active.Wait()
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr)
}
