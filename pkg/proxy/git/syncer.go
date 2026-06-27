package git

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

type gitState int

const (
	gitStateCloning gitState = iota
	gitStateReady
	gitStateFailed
)

func (h *gitHandler) cloneAndSync(ctx context.Context) {
	err := h.doClone(ctx)
	if err != nil {
		slog.Error("git clone failed", "instance", h.name, "upstream", h.redactedUpstream(), "err", err)
		h.mu.Lock()
		h.state = gitStateFailed
		h.mu.Unlock()
		h.stats.cloneFailed.Inc()
		return
	}
	slog.Info("git clone succeeded", "instance", h.name, "upstream", h.redactedUpstream())
	h.mu.Lock()
	h.state = gitStateReady
	h.mu.Unlock()
	h.stats.cloneSuccess.Inc()
	h.stats.lastSync.SetToCurrentTime()

	if h.syncInterval <= 0 {
		return
	}
	ticker := time.NewTicker(h.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.doSync(ctx)
		}
	}
}

func (h *gitHandler) doClone(ctx context.Context) error {
	opts := &git.CloneOptions{
		URL:          h.upstream,
		Auth:         h.auth,
		Depth:        0,
		SingleBranch: false,
		Tags:         git.AllTags,
	}
	if h.proxyURL != "" {
		opts.ProxyOptions = proxyOptions(h.proxyURL)
	}
	slog.Debug("starting git clone", "instance", h.name, "url", h.redactedUpstream())
	repo, err := git.CloneContext(ctx, h.storer, nil, opts)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.repo = repo
	h.mu.Unlock()
	return nil
}

func (h *gitHandler) doSync(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.repo == nil || h.state != gitStateReady {
		return
	}
	opts := &git.FetchOptions{
		Auth:  h.auth,
		Force: h.forceOverwrite,
		Prune: true,
		RefSpecs: []config.RefSpec{
			"+refs/*:refs/*",
		},
	}
	if h.proxyURL != "" {
		opts.ProxyOptions = proxyOptions(h.proxyURL)
	}
	slog.Debug("syncing git mirror", "instance", h.name)
	err := h.repo.FetchContext(ctx, opts)
	if err != nil {
		if !errors.Is(err, git.NoErrAlreadyUpToDate) {
			slog.Warn("git sync failed", "instance", h.name, "err", err)
			h.stats.syncFailed.Inc()
		}
		return
	}
	h.stats.syncSuccess.Inc()
	h.stats.lastSync.SetToCurrentTime()
	slog.Debug("git sync succeeded", "instance", h.name)
}

func proxyOptions(raw string) transport.ProxyOptions {
	u, err := url.Parse(raw)
	if err != nil {
		return transport.ProxyOptions{URL: raw}
	}
	opts := transport.ProxyOptions{URL: u.String()}
	if u.User != nil {
		opts.Username = u.User.Username()
		opts.Password, _ = u.User.Password()
	}
	return opts
}
