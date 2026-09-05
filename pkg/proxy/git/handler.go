package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	billyutil "github.com/go-git/go-billy/v5/util"
	gitlib "github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage/filesystem"

	proxytransport "gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const maxUploadPackRequestBytes = 16 << 20

var errMirrorBusy = errors.New("git mirror is busy")

type gitConfig struct {
	name             string
	repositoryFS     billy.Filesystem
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
	repositoryFS     billy.Filesystem
	bootstrapClient  *http.Client
	upstreamGate     *proxytransport.UpstreamGate

	repositoryMu sync.RWMutex
	repository   *gitlib.Repository
	server       transport.Transport
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
		operationTimeout: cfg.operationTimeout, repositoryFS: cfg.repositoryFS,
		bootstrapClient: bootstrapClient, upstreamGate: cfg.upstreamGate, lifecycle: storeio.NewLifecycle(),
	}
}

func (h *gitHandler) Start(context.Context) error {
	storage := filesystem.NewStorage(h.repositoryFS, cache.NewObjectLRUDefault())
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if !h.repositoryMu.TryLock() {
		return errMirrorBusy
	}
	defer h.repositoryMu.Unlock()

	releaseAdmission, err := h.upstreamGate.Acquire(ctx, h.upstream, proxytransport.AdmissionRefresh)
	if err != nil {
		return err
	}
	defer releaseAdmission()

	if h.repository == nil {
		if err := h.clearRepository(); err != nil {
			return err
		}
		storage := filesystem.NewStorage(h.repositoryFS, cache.NewObjectLRUDefault())
		options := &gitlib.CloneOptions{URL: h.upstream, Auth: h.auth, Tags: gitlib.AllTags}
		if h.proxyURL != "" {
			options.ProxyOptions = transport.ProxyOptions{URL: h.proxyURL}
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
		options.ProxyOptions = transport.ProxyOptions{URL: h.proxyURL}
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
	entries, err := h.repositoryFS.ReadDir(".")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("read git mirror directory: %w", err)
	}
	for _, entry := range entries {
		if err := billyutil.RemoveAll(h.repositoryFS, entry.Name()); err != nil {
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
	if req.Method == http.MethodPost {
		if req.ContentLength > maxUploadPackRequestBytes {
			http.Error(w, "git request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(time.Now().Add(h.operationTimeout))
		defer func() { _ = controller.SetReadDeadline(time.Time{}) }()
		defer req.Body.Close()
		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxUploadPackRequestBytes))
		if err != nil {
			status := http.StatusBadRequest
			var oversized *http.MaxBytesError
			if errors.As(err, &oversized) {
				status = http.StatusRequestEntityTooLarge
			} else if os.IsTimeout(err) {
				status = http.StatusRequestTimeout
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
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
	defer h.repositoryMu.RUnlock()
	serveGitHTTP(w, req, h.server, h.name)
}

func (h *gitHandler) DashboardStatus() (color, label, extra string) {
	if !h.repositoryMu.TryRLock() {
		return "blue", "syncing", ""
	}
	defer h.repositoryMu.RUnlock()
	if h.repository != nil {
		return "green", "ready", h.lastError
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
