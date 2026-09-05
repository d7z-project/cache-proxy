package git

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5/plumbing"
	gitserver "github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

type abortResponseWriter struct{ *httptest.ResponseRecorder }

func (abortResponseWriter) WriteHeader(int) { panic(http.ErrAbortHandler) }

func TestGitRequestPanicReleasesRepositoryLock(t *testing.T) {
	storage := memory.NewStorage()
	branch := plumbing.NewBranchReferenceName("main")
	require.NoError(t, storage.SetReference(plumbing.NewHashReference(branch, plumbing.NewHash(strings.Repeat("a", 40)))))
	require.NoError(t, storage.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branch)))
	h := newGitHandler(gitConfig{name: "git", upstream: "https://git.example/repo", repositoryFS: memfs.New()})
	defer func() { require.NoError(t, h.Stop(context.Background())) }()
	h.server = gitserver.NewServer(&singleLoader{storer: storage})
	require.Panics(t, func() {
		h.ServeHTTP(abortResponseWriter{httptest.NewRecorder()}, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil))
	})
	require.True(t, h.repositoryMu.TryLock())
	h.repositoryMu.Unlock()
}

type unlockedRequestBody struct {
	handler *gitHandler
	testing *testing.T
}

func (b unlockedRequestBody) Read([]byte) (int, error) {
	acquired := b.handler.repositoryMu.TryLock()
	if acquired {
		b.handler.repositoryMu.Unlock()
	}
	require.True(b.testing, acquired, "request body read holds the repository lock")
	return 0, io.EOF
}
func (unlockedRequestBody) Close() error { return nil }

func TestGitUploadPackRequestBoundaries(t *testing.T) {
	h := newGitHandler(gitConfig{name: "git", upstream: "https://git.example/repo", repositoryFS: memfs.New()})
	defer func() { require.NoError(t, h.Stop(context.Background())) }()
	h.server = gitserver.NewServer(&singleLoader{storer: memory.NewStorage()})
	request := httptest.NewRequest(http.MethodPost, "/git-upload-pack", nil)
	request.Body = unlockedRequestBody{handler: h, testing: t}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	for _, declared := range []bool{false, true} {
		request = httptest.NewRequest(http.MethodPost, "/git-upload-pack", strings.NewReader(strings.Repeat("x", maxUploadPackRequestBytes+1)))
		if !declared {
			request.ContentLength = -1
		}
		response = httptest.NewRecorder()
		h.ServeHTTP(response, request)
		require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
	}
}

func TestGitUploadPackBodyReadDeadline(t *testing.T) {
	h := newGitHandler(gitConfig{name: "git", upstream: "https://git.example/repo", repositoryFS: memfs.New(), operationTimeout: 20 * time.Millisecond})
	defer func() { require.NoError(t, h.Stop(context.Background())) }()
	server := httptest.NewServer(h)
	defer server.Close()
	conn, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	_, err = io.WriteString(conn, "POST /git-upload-pack HTTP/1.1\r\nHost: git.example\r\nContent-Length: 1\r\n\r\n")
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusRequestTimeout, response.StatusCode)
	require.True(t, h.repositoryMu.TryLock())
	h.repositoryMu.Unlock()
}

func TestBusyGitSyncAllowsOtherScheduledTasks(t *testing.T) {
	s, err := scheduler.NewPersistent(filepath.Join(t.TempDir(), "scheduler.json"))
	require.NoError(t, err)
	plan := proxyruntime.NewPlanContext(t.TempDir(), nil, nil, nil, 1<<20, config.CleanupConfig{}, ":8080", "/metrics", s)
	defer plan.CloseStores()
	instance, err := plan.Instance(config.Instance{Upstream: "https://git.example/repo", Path: "/git"}, config.SelectedMode{Name: "git", Mode: "git", Enabled: true})
	require.NoError(t, err)
	require.NoError(t, Plan(context.Background(), instance))
	result, err := plan.Finalize()
	require.NoError(t, err)
	h := result.Entries[0].Runtime.(*gitHandler)
	defer func() { require.NoError(t, h.Stop(context.Background())) }()
	h.repositoryMu.RLock()
	defer h.repositoryMu.RUnlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, h.Sync(ctx), context.Canceled)
	runs := make(chan scheduler.TaskRun, 2)
	s.SetRunObserver(func(run scheduler.TaskRun) { runs <- run })
	s.Register(scheduler.TaskDef{Key: scheduler.NewTaskKey("other", scheduler.TypeExpireCleanup, ""), Interval: time.Hour, RunImmediately: true, Handler: func(context.Context) (*scheduler.TaskOutcome, error) { return nil, nil }})
	s.Start(context.Background())
	defer func() { require.NoError(t, s.Stop(context.Background())) }()
	for range 2 {
		select {
		case run := <-runs:
			require.Empty(t, run.Err)
			if run.Key.Instance() == "git" {
				require.Equal(t, "deferred", run.Result)
			}
		case <-time.After(time.Second):
			t.Fatal("busy Git sync blocked the scheduler")
		}
	}
}
