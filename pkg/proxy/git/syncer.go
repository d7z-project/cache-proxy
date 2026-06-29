package git

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

type gitState int

const (
	gitStateCloning gitState = iota
	gitStateSyncing
	gitStateReady
	gitStateFailed
)

func (h *gitHandler) cloneAndSync(ctx context.Context) {
	backoff := 10 * time.Second
	for {
		err := h.doClone(ctx)
		if err == nil {
			break
		}
		h.stats.cloneFailed.Inc()
		if isPermanentCloneError(err) {
			slog.Error("git clone permanently failed", "instance", h.name, "upstream", h.redactedUpstream(), "err", err)
			h.mu.Lock()
			h.state = gitStateFailed
			h.mu.Unlock()
			return
		}
		slog.Warn("git clone failed, retrying", "instance", h.name, "upstream", h.redactedUpstream(), "backoff", backoff, "err", err)
		select {
		case <-ctx.Done():
			h.mu.Lock()
			h.state = gitStateFailed
			h.mu.Unlock()
			return
		case <-time.After(backoff):
		}
		if backoff < 10*time.Minute {
			backoff *= 2
		}
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
	if err == nil {
		h.mu.Lock()
		h.repo = repo
		h.mu.Unlock()
		return nil
	}
	if errors.Is(err, git.ErrRepositoryAlreadyExists) || errors.Is(err, git.ErrRemoteExists) {
		repo, err2 := git.Open(h.storer, nil)
		if err2 != nil {
			return fmt.Errorf("open partial clone: %w", err2)
		}
		fetchOpts := &git.FetchOptions{
			Auth:  h.auth,
			Force: h.forceOverwrite,
			Prune: true,
			RefSpecs: []config.RefSpec{
				"+refs/*:refs/*",
			},
		}
		if h.proxyURL != "" {
			fetchOpts.ProxyOptions = proxyOptions(h.proxyURL)
		}
		slog.Debug("resuming partial git clone", "instance", h.name, "url", h.redactedUpstream())
		if err2 := repo.FetchContext(ctx, fetchOpts); err2 != nil && !errors.Is(err2, git.NoErrAlreadyUpToDate) {
			return fmt.Errorf("resume fetch: %w", err2)
		}
		h.mu.Lock()
		h.repo = repo
		h.mu.Unlock()
		return nil
	}
	return err
}

func (h *gitHandler) doSync(ctx context.Context) {
	h.mu.Lock()
	if h.repo == nil || h.state != gitStateReady {
		h.mu.Unlock()
		return
	}
	h.state = gitStateSyncing
	h.mu.Unlock()

	h.requestMu.Lock()
	h.requestMu.Unlock()

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

	h.mu.Lock()
	h.state = gitStateReady
	h.mu.Unlock()

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

func isPermanentCloneError(err error) bool {
	if errors.Is(err, transport.ErrRepositoryNotFound) ||
		errors.Is(err, transport.ErrAuthenticationRequired) ||
		errors.Is(err, transport.ErrAuthorizationFailed) ||
		errors.Is(err, transport.ErrEmptyRemoteRepository) {
		return true
	}
	var perm *plumbing.PermanentError
	return errors.As(err, &perm)
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
