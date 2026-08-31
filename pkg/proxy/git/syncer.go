package git

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5"
	billyutil "github.com/go-git/go-billy/v5/util"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

const gitCurrentSchemaVersion = 1

type gitState int

const (
	gitStateCloning gitState = iota
	gitStateSyncing
	gitStateReady
	gitStateFailed
)

type gitGeneration struct {
	name   string
	repo   *git.Repository
	server transport.Transport
}

type gitCurrentReference struct {
	Version    int    `yaml:"version"`
	Generation string `yaml:"generation"`
}

func (s gitState) String() string {
	switch s {
	case gitStateCloning:
		return "cloning"
	case gitStateSyncing:
		return "syncing"
	case gitStateReady:
		return "ready"
	case gitStateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func (h *gitHandler) cloneAndSync(ctx context.Context) {
	backoff := 10 * time.Second
	for {
		err := h.refreshRepository(ctx)
		if err == nil {
			break
		}
		h.stats.cloneFailed.Inc()
		h.mu.Lock()
		h.refreshErr = err.Error()
		hasCurrent := h.current != nil
		if hasCurrent {
			h.state = gitStateReady
		} else if isPermanentCloneError(err) {
			h.state = gitStateFailed
		}
		h.mu.Unlock()
		if hasCurrent {
			slog.Warn("git initial refresh failed; serving current generation", "instance", h.name, "upstream", h.redactedUpstream(), "err", err)
			break
		}
		if isPermanentCloneError(err) {
			slog.Error("git clone permanently failed", "instance", h.name, "upstream", h.redactedUpstream(), "err", err)
			return
		}
		slog.Warn("git clone failed, retrying", "instance", h.name, "upstream", h.redactedUpstream(), "backoff", backoff, "err", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			h.mu.Lock()
			if h.current == nil {
				h.state = gitStateFailed
				h.refreshErr = ctx.Err().Error()
			}
			h.mu.Unlock()
			return
		case <-timer.C:
		}
		if backoff < 10*time.Minute {
			backoff *= 2
		}
	}

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
			h.syncRepository(ctx)
		}
	}
}

func (h *gitHandler) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if h.operationTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, h.operationTimeout)
}

func (h *gitHandler) syncRepository(ctx context.Context) {
	if err := h.refreshRepository(ctx); err != nil {
		h.stats.syncFailed.Inc()
		h.mu.Lock()
		h.refreshErr = err.Error()
		if h.current != nil {
			h.state = gitStateReady
		}
		h.mu.Unlock()
		slog.Warn("git sync failed; serving current generation", "instance", h.name, "err", err)
		return
	}
	h.stats.syncSuccess.Inc()
	h.stats.lastSync.SetToCurrentTime()
}

func (h *gitHandler) refreshRepository(ctx context.Context) error {
	h.mu.Lock()
	if h.current == nil {
		h.state = gitStateCloning
	} else {
		h.state = gitStateSyncing
	}
	h.refreshErr = ""
	current := h.current
	h.mu.Unlock()

	generationName := strconv.FormatInt(time.Now().UnixNano(), 36)
	generationPath := path.Join("generations", generationName)
	if err := h.billyFs.MkdirAll(generationPath, 0o755); err != nil {
		return fmt.Errorf("create git generation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = removeGitTree(h.billyFs, generationPath)
		}
	}()

	if current != nil {
		if err := copyGitTree(h.billyFs, path.Join("generations", current.name), generationPath); err != nil {
			return fmt.Errorf("copy current git generation: %w", err)
		}
	}
	generationFS := newBillyAdapter(afero.NewBasePathFs(h.billyFs.fs, generationPath), "")
	storage := filesystem.NewStorage(generationFS, cache.NewObjectLRUDefault())
	operationCtx, cancel := h.operationContext(ctx)
	defer cancel()

	var repo *git.Repository
	var err error
	if current == nil {
		opts := &git.CloneOptions{
			URL: h.upstream, Auth: h.auth, Depth: 0, SingleBranch: false, Tags: git.AllTags,
		}
		if h.proxyURL != "" {
			opts.ProxyOptions = proxyOptions(h.proxyURL)
		}
		repo, err = git.CloneContext(operationCtx, storage, nil, opts)
	} else {
		repo, err = git.Open(storage, nil)
		if err == nil {
			fetchOpts := &git.FetchOptions{
				Auth: h.auth, Force: h.forceOverwrite, Prune: true,
				RefSpecs: []config.RefSpec{"+refs/*:refs/*"},
			}
			if h.proxyURL != "" {
				fetchOpts.ProxyOptions = proxyOptions(h.proxyURL)
			}
			fetchErr := repo.FetchContext(operationCtx, fetchOpts)
			if fetchErr != nil && !errors.Is(fetchErr, git.NoErrAlreadyUpToDate) {
				err = fetchErr
			}
		}
	}
	if err != nil {
		return err
	}
	if _, err := repo.Head(); err != nil {
		return fmt.Errorf("validate git generation HEAD: %w", err)
	}

	generation := &gitGeneration{
		name: generationName,
		repo: repo,
		server: server.NewServer(&singleLoader{
			storer: storage,
		}),
	}
	if err := h.publishCurrent(generation); err != nil {
		return err
	}
	committed = true
	slog.Info("git generation published", "instance", h.name, "generation", generationName, "upstream", h.redactedUpstream())
	return nil
}

func (h *gitHandler) publishCurrent(generation *gitGeneration) error {
	data, err := yaml.Marshal(gitCurrentReference{Version: gitCurrentSchemaVersion, Generation: generation.name})
	if err != nil {
		return fmt.Errorf("marshal git current reference: %w", err)
	}
	tempPath := "current.yaml.tmp." + generation.name
	if err := billyutil.WriteFile(h.billyFs, tempPath, data, 0o644); err != nil {
		return fmt.Errorf("write git current reference: %w", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.billyFs.Rename(tempPath, "current.yaml"); err != nil {
		_ = h.billyFs.Remove(tempPath)
		return fmt.Errorf("publish git current reference: %w", err)
	}
	h.current = generation
	h.state = gitStateReady
	h.refreshErr = ""
	return nil
}

func (h *gitHandler) restoreCurrent() error {
	file, err := h.billyFs.Open("current.yaml")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open git current reference: %w", err)
	}
	var reference gitCurrentReference
	decodeErr := yaml.NewDecoder(io.LimitReader(file, 16<<10)).Decode(&reference)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil || reference.Version != gitCurrentSchemaVersion ||
		reference.Generation == "" || path.Base(reference.Generation) != reference.Generation || strings.Contains(reference.Generation, "\\") {
		_ = h.billyFs.Remove("current.yaml")
		return nil
	}
	generationFS := newBillyAdapter(
		afero.NewBasePathFs(h.billyFs.fs, path.Join("generations", reference.Generation)),
		"",
	)
	storage := filesystem.NewStorage(generationFS, cache.NewObjectLRUDefault())
	repo, err := git.Open(storage, nil)
	if err != nil {
		_ = h.billyFs.Remove("current.yaml")
		return nil
	}
	if _, err := repo.Head(); err != nil {
		_ = h.billyFs.Remove("current.yaml")
		return nil
	}
	h.mu.Lock()
	h.current = &gitGeneration{name: reference.Generation, repo: repo, server: server.NewServer(&singleLoader{storer: storage})}
	h.state = gitStateReady
	h.mu.Unlock()
	return nil
}

func copyGitTree(filesystem billy.Filesystem, sourceRoot, destinationRoot string) error {
	return billyutil.Walk(filesystem, sourceRoot, func(current string, info fs.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative := strings.TrimPrefix(strings.TrimPrefix(current, sourceRoot), "/")
		destination := path.Join(destinationRoot, relative)
		if info.IsDir() {
			return filesystem.MkdirAll(destination, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported git generation entry %s", current)
		}
		source, err := filesystem.Open(current)
		if err != nil {
			return err
		}
		if err := filesystem.MkdirAll(path.Dir(destination), 0o755); err != nil {
			_ = source.Close()
			return err
		}
		target, err := filesystem.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			_ = source.Close()
			return err
		}
		_, copyErr := io.Copy(target, source)
		closeErr := errors.Join(source.Close(), target.Close())
		return errors.Join(copyErr, closeErr)
	})
}

func removeGitTree(filesystem billy.Filesystem, treePath string) error {
	if err := billyutil.RemoveAll(filesystem, treePath); err != nil {
		return fmt.Errorf("remove git generation %s: %w", treePath, err)
	}
	return nil
}

func isPermanentCloneError(err error) bool {
	if errors.Is(err, transport.ErrRepositoryNotFound) ||
		errors.Is(err, transport.ErrAuthenticationRequired) ||
		errors.Is(err, transport.ErrAuthorizationFailed) ||
		errors.Is(err, transport.ErrEmptyRemoteRepository) {
		return true
	}
	var permanent *plumbing.PermanentError
	return errors.As(err, &permanent)
}

func proxyOptions(raw string) transport.ProxyOptions {
	u, err := url.Parse(raw)
	if err != nil {
		return transport.ProxyOptions{URL: raw}
	}
	options := transport.ProxyOptions{URL: u.String()}
	if u.User != nil {
		options.Username = u.User.Username()
		options.Password, _ = u.User.Password()
	}
	return options
}
