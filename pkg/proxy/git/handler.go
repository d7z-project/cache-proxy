package git

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/spf13/afero"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type gitConfig struct {
	name             string
	billyFs          *billyAdapter
	upstream         string
	auth             transport.AuthMethod
	proxyURL         string
	operationTimeout time.Duration
	upstreamGate     *httpcache.UpstreamGate
}

type gitHandler struct {
	name             string
	upstream         string
	auth             transport.AuthMethod
	proxyURL         string
	operationTimeout time.Duration
	billyFs          *billyAdapter
	bootstrapClient  *http.Client
	upstreamGate     *httpcache.UpstreamGate

	repositoryMu sync.RWMutex
	repository   *gitlib.Repository
	server       transport.Transport
	syncing      bool
	lastError    string

	requestMu sync.Mutex
	closing   bool
	requests  sync.WaitGroup
}

func newGitHandler(cfg gitConfig) *gitHandler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.proxyURL != "" {
		if proxyURL, err := url.Parse(cfg.proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}
	return &gitHandler{
		name: cfg.name, upstream: cfg.upstream, auth: cfg.auth, proxyURL: cfg.proxyURL,
		operationTimeout: cfg.operationTimeout, billyFs: cfg.billyFs,
		bootstrapClient: &http.Client{Transport: transport, Timeout: cfg.operationTimeout}, upstreamGate: cfg.upstreamGate,
	}
}

func (h *gitHandler) Start(context.Context) error {
	storage := filesystem.NewStorage(h.billyFs, cache.NewObjectLRUDefault())
	repository, err := gitlib.Open(storage, nil)
	if err != nil {
		return nil
	}
	if _, err := repository.Head(); err != nil {
		return nil
	}
	h.repositoryMu.Lock()
	h.repository = repository
	h.server = server.NewServer(&singleLoader{storer: storage})
	h.repositoryMu.Unlock()
	return nil
}

func (h *gitHandler) Stop(ctx context.Context) error {
	h.requestMu.Lock()
	h.closing = true
	h.requestMu.Unlock()
	return utils.WaitGroupContext(ctx, &h.requests)
}

func (h *gitHandler) Sync(ctx context.Context) error {
	releaseAdmission, err := h.upstreamGate.Acquire(ctx, h.upstream, httpcache.AdmissionRefresh)
	if err != nil {
		return err
	}
	defer releaseAdmission()

	h.repositoryMu.Lock()
	defer h.repositoryMu.Unlock()
	h.syncing = true
	defer func() { h.syncing = false }()

	operationCtx := ctx
	cancel := func() {}
	if h.operationTimeout > 0 {
		operationCtx, cancel = context.WithTimeout(ctx, h.operationTimeout)
	}
	defer cancel()

	if h.repository == nil {
		if err := h.clearRepository(); err != nil {
			return err
		}
		storage := filesystem.NewStorage(h.billyFs, cache.NewObjectLRUDefault())
		options := &gitlib.CloneOptions{URL: h.upstream, Auth: h.auth, Tags: gitlib.AllTags}
		if h.proxyURL != "" {
			options.ProxyOptions = proxyOptions(h.proxyURL)
		}
		repository, err := gitlib.CloneContext(operationCtx, storage, nil, options)
		if err != nil {
			h.lastError = err.Error()
			return err
		}
		if _, err := repository.Head(); err != nil {
			h.lastError = err.Error()
			return fmt.Errorf("validate git mirror: %w", err)
		}
		h.repository = repository
		h.server = server.NewServer(&singleLoader{storer: storage})
		h.lastError = ""
		slog.Info("git mirror cloned", "instance", h.name, "upstream", h.redactedUpstream())
		return nil
	}

	options := &gitlib.FetchOptions{
		Auth: h.auth, Force: true, Prune: true,
		RefSpecs: []gitconfig.RefSpec{"+refs/*:refs/*"},
	}
	if h.proxyURL != "" {
		options.ProxyOptions = proxyOptions(h.proxyURL)
	}
	err = h.repository.FetchContext(operationCtx, options)
	if errors.Is(err, gitlib.NoErrAlreadyUpToDate) {
		err = nil
	}
	if err != nil {
		h.lastError = err.Error()
		return err
	}
	h.lastError = ""
	return nil
}

func (h *gitHandler) clearRepository() error {
	entries, err := afero.ReadDir(h.billyFs.fs, ".")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("read git mirror directory: %w", err)
	}
	for _, entry := range entries {
		if err := (afero.Afero{Fs: h.billyFs.fs}).RemoveAll(entry.Name()); err != nil {
			return fmt.Errorf("clear git mirror %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func (h *gitHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.requestMu.Lock()
	if h.closing {
		h.requestMu.Unlock()
		w.Header().Set("Retry-After", "1")
		http.Error(w, "repository handler is stopping", http.StatusServiceUnavailable)
		return
	}
	h.requests.Add(1)
	h.requestMu.Unlock()
	defer h.requests.Done()

	if !h.repositoryMu.TryRLock() {
		h.proxyBootstrap(w, req)
		return
	}
	if h.server == nil {
		h.repositoryMu.RUnlock()
		h.proxyBootstrap(w, req)
		return
	}
	serveGitHTTP(w, req, h.server, h.name)
	h.repositoryMu.RUnlock()
}

func (h *gitHandler) DashboardStatus() (color, label, extra string) {
	h.repositoryMu.RLock()
	defer h.repositoryMu.RUnlock()
	if h.repository != nil {
		if h.syncing {
			return "green", "ready", "syncing"
		}
		return "green", "ready", h.lastError
	}
	if h.syncing {
		return "blue", "cloning...", ""
	}
	if h.lastError != "" {
		return "red", "failed", h.lastError
	}
	return "gray", "starting", ""
}

func (h *gitHandler) redactedUpstream() string { return redactURL(h.upstream) }

func (h *gitHandler) writeAdmissionError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	var limited *httpcache.UpstreamRateLimitError
	if errors.As(err, &limited) {
		status = http.StatusTooManyRequests
	}
	if seconds, ok := httpcache.AdmissionRetryAfterSeconds(err); ok {
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
	http.Error(w, http.StatusText(status), status)
}

func proxyOptions(rawURL string) transport.ProxyOptions {
	return transport.ProxyOptions{URL: rawURL}
}
