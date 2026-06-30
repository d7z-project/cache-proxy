package git

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

type gitConfig struct {
	name             string
	billyFs          billy.Filesystem
	upstream         string
	auth             transport.AuthMethod
	proxyURL         string
	syncInterval     time.Duration
	operationTimeout time.Duration
	forceOverwrite   bool
}

type gitHandler struct {
	name             string
	upstream         string
	auth             transport.AuthMethod
	proxyURL         string
	syncInterval     time.Duration
	operationTimeout time.Duration
	forceOverwrite   bool

	storer *filesystem.Storage
	svr    transport.Transport

	mu    sync.RWMutex
	repo  *git.Repository
	state gitState

	syncerCancel context.CancelFunc
	syncerDone   chan struct{}

	requestMu sync.RWMutex
	stats     *gitStats
}

func newGitHandler(cfg gitConfig) *gitHandler {
	st := filesystem.NewStorage(cfg.billyFs, cache.NewObjectLRUDefault())
	h := &gitHandler{
		name:             cfg.name,
		storer:           st,
		svr:              server.NewServer(&singleLoader{storer: st}),
		upstream:         cfg.upstream,
		auth:             cfg.auth,
		proxyURL:         cfg.proxyURL,
		syncInterval:     cfg.syncInterval,
		operationTimeout: cfg.operationTimeout,
		forceOverwrite:   cfg.forceOverwrite,
		state:            gitStateCloning,
		stats:            newGitStats(cfg.name),
	}
	return h
}

func (h *gitHandler) Start(ctx context.Context) error {
	h.mu.Lock()
	if h.syncerCancel != nil {
		h.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	h.syncerCancel = cancel
	h.syncerDone = make(chan struct{})
	h.mu.Unlock()

	go func() {
		defer close(h.syncerDone)
		h.cloneAndSync(ctx)
	}()
	return nil
}

func (h *gitHandler) Stop(ctx context.Context) error {
	h.mu.Lock()
	cancel := h.syncerCancel
	done := h.syncerDone
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	doneCh := make(chan struct{})
	go func() {
		h.requestMu.Lock()
		h.requestMu.Unlock()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *gitHandler) Cleanup(_ context.Context, _ config.CleanupConfig) error {
	return nil
}

func (h *gitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.requestMu.RLock()
	defer h.requestMu.RUnlock()

	h.mu.RLock()
	state := h.state
	h.mu.RUnlock()

	if state != gitStateReady {
		h.serveRepositoryState(w, state)
		return
	}

	serveGitHTTP(w, r, h.svr, h.name)
}

func (h *gitHandler) serveRepositoryState(w http.ResponseWriter, state gitState) {
	switch state {
	case gitStateCloning:
		w.Header().Set("Retry-After", "10")
		http.Error(w, "repository is being cloned, retry later", http.StatusServiceUnavailable)
	case gitStateSyncing:
		w.Header().Set("Retry-After", "5")
		http.Error(w, "repository is syncing, retry later", http.StatusServiceUnavailable)
	case gitStateFailed:
		http.Error(w, "clone failed", http.StatusInternalServerError)
	default:
		http.Error(w, "repository not ready", http.StatusServiceUnavailable)
	}
}

func (h *gitHandler) redactedUpstream() string {
	return redactURL(h.upstream)
}

func (h *gitHandler) DashboardStatus() (color, label, extra string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	switch h.state {
	case gitStateCloning:
		return "blue", "cloning...", ""
	case gitStateSyncing:
		return "blue", "syncing...", ""
	case gitStateReady:
		return "green", "ready", ""
	case gitStateFailed:
		return "red", "failed", ""
	default:
		return "gray", "unknown", ""
	}
}
