package git

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing/transport"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

type gitConfig struct {
	name             string
	billyFs          *billyAdapter
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
	billyFs          *billyAdapter
	bootstrapClient  *http.Client

	mu         sync.RWMutex
	current    *gitGeneration
	readers    map[string]int
	state      gitState
	refreshErr string

	syncerCancel context.CancelFunc
	syncerDone   chan struct{}

	requestMu       sync.Mutex
	activeRequests  int
	requestDraining bool
	requestsIdle    chan struct{}
	stats           *gitStats
}

func newGitHandler(cfg gitConfig) *gitHandler {
	bootstrapTransport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.proxyURL != "" {
		if proxyURL, err := url.Parse(cfg.proxyURL); err == nil {
			bootstrapTransport.Proxy = http.ProxyURL(proxyURL)
		}
	}
	return &gitHandler{
		name:             cfg.name,
		upstream:         cfg.upstream,
		auth:             cfg.auth,
		proxyURL:         cfg.proxyURL,
		syncInterval:     cfg.syncInterval,
		operationTimeout: cfg.operationTimeout,
		forceOverwrite:   cfg.forceOverwrite,
		billyFs:          cfg.billyFs,
		bootstrapClient:  &http.Client{Transport: bootstrapTransport, Timeout: cfg.operationTimeout},
		state:            gitStateCloning,
		readers:          map[string]int{},
		stats:            newGitStats(cfg.name),
	}
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
	if err := h.restoreCurrent(); err != nil {
		cancel()
		h.mu.Lock()
		h.syncerCancel = nil
		close(h.syncerDone)
		h.syncerDone = nil
		h.mu.Unlock()
		return err
	}

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

	return h.drainRequests(ctx)
}

func (h *gitHandler) Cleanup(ctx context.Context, _ config.CleanupConfig) error {
	entries, err := h.billyFs.ReadDir("generations")
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		entries = nil
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ModTime().Equal(entries[j].ModTime()) {
			return entries[i].Name() > entries[j].Name()
		}
		return entries[i].ModTime().After(entries[j].ModTime())
	})
	h.mu.RLock()
	retained := make(map[string]struct{}, len(h.readers)+2)
	if h.current != nil {
		retained[h.current.name] = struct{}{}
	}
	for generation, readers := range h.readers {
		if readers > 0 {
			retained[generation] = struct{}{}
		}
	}
	h.mu.RUnlock()
	keptPrevious := false
	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !entry.IsDir() {
			continue
		}
		if _, keep := retained[entry.Name()]; keep {
			continue
		}
		if !keptPrevious {
			keptPrevious = true
			continue
		}
		if err := removeGitTree(h.billyFs, "generations/"+entry.Name()); err != nil {
			return err
		}
	}
	for _, legacyPath := range []string{
		"HEAD", "branches", "config", "description", "hooks", "index", "info", "logs",
		"modules", "objects", "packed-refs", "refs", "shallow",
	} {
		if err := removeGitTree(h.billyFs, legacyPath); err != nil {
			return err
		}
	}
	return nil
}

func (h *gitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.requestMu.Lock()
	if h.requestDraining {
		h.requestMu.Unlock()
		w.Header().Set("Retry-After", "1")
		http.Error(w, "repository handler is stopping", http.StatusServiceUnavailable)
		return
	}
	if h.activeRequests == 0 {
		h.requestsIdle = make(chan struct{})
	}
	h.activeRequests++
	h.requestMu.Unlock()
	defer func() {
		h.requestMu.Lock()
		h.activeRequests--
		if h.activeRequests == 0 {
			close(h.requestsIdle)
		}
		h.requestMu.Unlock()
	}()

	generation, release := h.acquireCurrent()
	if generation == nil {
		h.proxyBootstrap(w, r)
		return
	}
	defer release()

	serveGitHTTP(w, r, generation.server, h.name)
}

func (h *gitHandler) acquireCurrent() (*gitGeneration, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.current == nil {
		return nil, func() {}
	}
	generation := h.current
	h.readers[generation.name]++
	return generation, func() {
		h.mu.Lock()
		h.readers[generation.name]--
		if h.readers[generation.name] == 0 {
			delete(h.readers, generation.name)
		}
		h.mu.Unlock()
	}
}

func (h *gitHandler) drainRequests(ctx context.Context) error {
	h.requestMu.Lock()
	h.requestDraining = true
	if h.activeRequests == 0 {
		h.requestDraining = false
		h.requestMu.Unlock()
		return nil
	}
	done := h.requestsIdle
	h.requestMu.Unlock()

	defer func() {
		h.requestMu.Lock()
		h.requestDraining = false
		h.requestMu.Unlock()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *gitHandler) redactedUpstream() string {
	return redactURL(h.upstream)
}

func (h *gitHandler) DashboardStatus() (color, label, extra string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.current != nil {
		if h.state == gitStateSyncing {
			return "green", "ready", "syncing"
		}
		return "green", "ready", ""
	}
	switch h.state {
	case gitStateCloning:
		return "blue", "cloning...", ""
	case gitStateSyncing:
		return "blue", "syncing...", ""
	case gitStateReady:
		return "green", "ready", ""
	case gitStateFailed:
		return "red", "failed", h.refreshErr
	default:
		return "gray", "unknown", ""
	}
}
