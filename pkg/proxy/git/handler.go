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

	proxytransport "gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type gitConfig struct {
	name             string
	billyFs          *billyAdapter
	upstream         string
	auth             transport.AuthMethod
	proxyURL         string
	operationTimeout time.Duration
	upstreamGate     *proxytransport.UpstreamGate
}

type gitHandler struct {
	name             string
	upstream         string
	auth             transport.AuthMethod
	proxyURL         string
	operationTimeout time.Duration
	billyFs          *billyAdapter
	bootstrapClient  *http.Client
	upstreamGate     *proxytransport.UpstreamGate

	repositoryMu sync.RWMutex
	repository   *gitlib.Repository
	server       transport.Transport
	syncing      bool
	lastError    string
	lifecycle    *storeio.Lifecycle
}

func newGitHandler(cfg gitConfig) *gitHandler {
	if cfg.operationTimeout <= 0 {
		cfg.operationTimeout = defaultOperationTimeout
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = 1 << 20
	if cfg.proxyURL != "" {
		if proxyURL, err := url.Parse(cfg.proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}
	bootstrapClient := &http.Client{
		Transport: transport, Timeout: cfg.operationTimeout,
		CheckRedirect: proxytransport.CheckReadOnlyRedirect,
	}
	proxytransport.ConfigureAdmission(bootstrapClient, cfg.upstreamGate)
	return &gitHandler{
		name: cfg.name, upstream: cfg.upstream, auth: cfg.auth, proxyURL: cfg.proxyURL,
		operationTimeout: cfg.operationTimeout, billyFs: cfg.billyFs,
		bootstrapClient: bootstrapClient, upstreamGate: cfg.upstreamGate, lifecycle: storeio.NewLifecycle(),
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
	h.bootstrapClient.CloseIdleConnections()
	return h.lifecycle.Close(ctx)
}

func (h *gitHandler) Sync(ctx context.Context) error {
	if h.operationTimeout <= 0 {
		return h.syncRepository(ctx)
	}
	operationCtx, cancel := context.WithTimeout(ctx, h.operationTimeout)
	defer cancel()
	return h.syncRepository(operationCtx)
}

func (h *gitHandler) syncRepository(ctx context.Context) error {
	h.repositoryMu.Lock()
	defer h.repositoryMu.Unlock()
	h.syncing = true
	defer func() { h.syncing = false }()

	releaseAdmission, err := h.upstreamGate.Acquire(ctx, h.upstream, proxytransport.AdmissionRefresh)
	if err != nil {
		return err
	}
	defer releaseAdmission()

	if h.repository == nil {
		if err := h.clearRepository(); err != nil {
			return err
		}
		storage := filesystem.NewStorage(h.billyFs, cache.NewObjectLRUDefault())
		options := &gitlib.CloneOptions{URL: h.upstream, Auth: h.auth, Tags: gitlib.AllTags}
		if h.proxyURL != "" {
			options.ProxyOptions = proxyOptions(h.proxyURL)
		}
		repository, err := gitlib.CloneContext(ctx, storage, nil, options)
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
	err = h.repository.FetchContext(ctx, options)
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
	cleaned, err := storeio.CleanURLPath(req.URL)
	if err != nil {
		http.Error(w, "invalid Git path", http.StatusBadRequest)
		return
	}
	if !isGitReadRequest(req) {
		if req.Method == http.MethodGet && req.URL.Path == "/info/refs" && req.URL.Query().Get("service") != "" {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		if !proxyruntime.RequireReadMethod(w, req.Method) {
			return
		}
	}
	_, done, err := h.lifecycle.Begin()
	if err != nil {
		w.Header().Set("Retry-After", "1")
		proxytransport.WriteError(w, http.StatusServiceUnavailable)
		return
	}
	defer done()
	if !isGitReadRequest(req) || shouldForwardGitRead(req) {
		h.forwardUpstream(w, req, cleaned)
		return
	}

	if !h.repositoryMu.TryRLock() {
		h.forwardUpstream(w, req, cleaned)
		return
	}
	if h.server == nil {
		h.repositoryMu.RUnlock()
		h.forwardUpstream(w, req, cleaned)
		return
	}
	serveGitHTTP(w, req, h.server, h.name)
	h.repositoryMu.RUnlock()
}

func (h *gitHandler) DashboardStatus() (color, label, extra string) {
	if !h.repositoryMu.TryRLock() {
		return "blue", "syncing", ""
	}
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
	var limited *proxyruntime.UpstreamRateLimitError
	if errors.As(err, &limited) {
		status = http.StatusTooManyRequests
	}
	if seconds, ok := proxyruntime.AdmissionRetryAfterSeconds(err); ok {
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
	proxytransport.WriteError(w, status)
}

func proxyOptions(rawURL string) transport.ProxyOptions {
	return transport.ProxyOptions{URL: rawURL}
}
