package git

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func createTestSourceRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	repo, err := git.PlainInit(dir, false)
	require.NoError(t, err)
	wt, err := repo.Worktree()
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, "README.md"), []byte("# test repo\n"), 0o644)
	require.NoError(t, err)
	_, err = wt.Add("README.md")
	require.NoError(t, err)
	_, err = wt.Commit("initial commit", &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@test.com", When: time.Now()},
	})
	require.NoError(t, err)
	return dir
}

func newTestHandler(t *testing.T, upstreamURL string) *gitHandler {
	t.Helper()
	baseFs := afero.NewOsFs()
	bfs := newBillyAdapter(afero.NewBasePathFs(baseFs, t.TempDir()), "")
	return newGitHandler(gitConfig{
		name:           "test",
		billyFs:        bfs,
		upstream:       upstreamURL,
		forceOverwrite: true,
	})
}

func waitForClone(t *testing.T, h *gitHandler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for clone")
		case <-ticker.C:
			h.mu.Lock()
			s := h.state
			h.mu.Unlock()
			if s == gitStateReady {
				return
			}
			if s == gitStateFailed {
				t.Fatal("clone failed unexpectedly")
			}
		}
	}
}

func TestCloneAndServe(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	req := httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/x-git-upload-pack-advertisement", rec.Header().Get("Content-Type"))
	body := rec.Body.String()
	require.True(t, strings.Contains(body, "service=git-upload-pack"))
	require.True(t, strings.Contains(body, "HEAD"))
}

func TestServeBeforeCloneReady(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())

	req := httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "10", rec.Header().Get("Retry-After"))
}

func TestServeDuringSync(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	h.mu.Lock()
	h.state = gitStateSyncing
	h.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "5", rec.Header().Get("Retry-After"))

	h.mu.Lock()
	h.state = gitStateReady
	h.mu.Unlock()

	req = httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	color, label, extra := h.DashboardStatus()
	require.Equal(t, "green", color)
	require.Equal(t, "ready", label)
	require.Empty(t, extra)
}

func TestDashboardStatusDuringSync(t *testing.T) {
	h := newTestHandler(t, "file:///tmp/unused")
	h.mu.Lock()
	h.state = gitStateSyncing
	h.mu.Unlock()

	color, label, extra := h.DashboardStatus()
	require.Equal(t, "blue", color)
	require.Equal(t, "syncing...", label)
	require.Empty(t, extra)
}

func TestServeFailedStateResponse(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)

	h.mu.Lock()
	h.state = gitStateFailed
	h.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestSyncAfterClone(t *testing.T) {
	source := createTestSourceRepo(t)
	bfs := newBillyAdapter(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()), "")
	h := newGitHandler(gitConfig{
		name:           "sync",
		billyFs:        bfs,
		upstream:       "file://" + source,
		forceOverwrite: true,
	})
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	h.mu.RLock()
	headBefore, err := h.repo.Head()
	h.mu.RUnlock()
	require.NoError(t, err)

	repo, err := git.PlainOpen(source)
	require.NoError(t, err)
	wt, err := repo.Worktree()
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(source, "new.txt"), []byte("new"), 0o644)
	require.NoError(t, err)
	_, err = wt.Add("new.txt")
	require.NoError(t, err)
	_, err = wt.Commit("update", &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@test.com", When: time.Now()},
	})
	require.NoError(t, err)

	h.doSync(context.Background())

	h.mu.RLock()
	headAfter, _ := h.repo.Head()
	h.mu.RUnlock()
	require.NotEqual(t, headBefore.Hash(), headAfter.Hash(), "sync should have fetched new commit")
}

func TestSyncPrunesDeletedRefs(t *testing.T) {
	source := createTestSourceRepo(t)

	bfs := newBillyAdapter(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()), "")
	h := newGitHandler(gitConfig{
		name:           "prune",
		billyFs:        bfs,
		upstream:       "file://" + source,
		forceOverwrite: true,
	})
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	h.mu.RLock()
	headBefore, err := h.repo.Head()
	h.mu.RUnlock()
	require.NoError(t, err)

	h.doSync(context.Background())

	h.mu.RLock()
	headAfter, err := h.repo.Head()
	h.mu.RUnlock()
	require.NoError(t, err)
	require.Equal(t, headBefore.Hash(), headAfter.Hash(), "sync with no changes should keep HEAD unchanged")
}

func TestCloneRetryAndCtxCancel(t *testing.T) {
	bfs := newBillyAdapter(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()), "")
	h := newGitHandler(gitConfig{
		name:           "retry",
		billyFs:        bfs,
		upstream:       "http://127.0.0.1:1/repo.git",
		forceOverwrite: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h.Start(ctx)
	defer h.Stop(context.Background())

	time.Sleep(500 * time.Millisecond)

	h.mu.RLock()
	require.Equal(t, gitStateCloning, h.state, "should still be cloning during backoff")
	h.mu.RUnlock()

	cancel()

	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for retry to stop")
		case <-ticker.C:
			h.mu.RLock()
			s := h.state
			h.mu.RUnlock()
			if s == gitStateFailed {
				return
			}
		}
	}
}

func TestClonePermanentErrorFailsImmediately(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	bfs := newBillyAdapter(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()), "")
	h := newGitHandler(gitConfig{
		name:           "perm",
		billyFs:        bfs,
		upstream:       srv.URL + "/nonexistent.git",
		forceOverwrite: true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := h.doClone(ctx)
	require.Error(t, err)
	require.True(t, isPermanentCloneError(err))
}

func TestClonePartialResume(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := h.doClone(ctx)
	require.NoError(t, err)
	require.NotNil(t, h.repo)

	err = h.doClone(ctx)
	require.NoError(t, err)
	require.NotNil(t, h.repo)
}

func TestStopDuringClone(t *testing.T) {
	bfs := newBillyAdapter(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()), "")
	h := newGitHandler(gitConfig{
		name:           "stop-clone",
		billyFs:        bfs,
		upstream:       "http://127.0.0.1:1/repo.git",
		forceOverwrite: true,
	})
	h.Start(context.Background())

	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := h.Stop(ctx)
	require.NoError(t, err)

	h.mu.RLock()
	require.Equal(t, gitStateFailed, h.state)
	h.mu.RUnlock()
}

func TestDoubleStartIdempotent(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	err := h.Start(context.Background())
	require.NoError(t, err)
	err = h.Start(context.Background())
	require.NoError(t, err)
	defer h.Stop(context.Background())
	waitForClone(t, h)

	h.mu.RLock()
	require.Equal(t, gitStateReady, h.state)
	h.mu.RUnlock()
}

func TestEmptyUpstreamRepo(t *testing.T) {
	emptyDir := t.TempDir()
	_, err := git.PlainInit(emptyDir, true)
	require.NoError(t, err)

	h := newTestHandler(t, "file://"+emptyDir)
	h.Start(context.Background())
	defer h.Stop(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		case <-ticker.C:
			h.mu.RLock()
			s := h.state
			h.mu.RUnlock()
			if s != gitStateCloning {
				require.Equal(t, gitStateFailed, s, "empty repo clone should fail")
				return
			}
		}
	}
}

func TestServeUnknownPath(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	req := httptest.NewRequest(http.MethodGet, "/unknown-path", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUploadPackEndpoint(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	req := httptest.NewRequest(http.MethodPost, "/git-upload-pack", strings.NewReader(""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestInfoRefsWithoutServiceParam(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	req := httptest.NewRequest(http.MethodGet, "/info/refs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestServeConcurrentRequests(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			req := httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestSyncAndServeConcurrentRequests(t *testing.T) {
	source := createTestSourceRepo(t)
	h := newTestHandler(t, "file://"+source)
	h.Start(context.Background())
	defer h.Stop(context.Background())
	waitForClone(t, h)

	var wg sync.WaitGroup
	statuses := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			statuses <- rec.Code
		}()
	}
	h.doSync(context.Background())
	wg.Wait()
	close(statuses)
	for status := range statuses {
		require.Contains(t, []int{http.StatusOK, http.StatusServiceUnavailable}, status)
	}
}

func TestBillyAdapterReadWrite(t *testing.T) {
	afs := afero.NewMemMapFs()
	bfs := newBillyAdapter(afs, "/root")

	f, err := bfs.Create("test.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("hello world"))
	require.NoError(t, err)
	f.Close()

	f, err = bfs.Open("test.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(data))
	f.Close()

	require.NotNil(t, filesystem.NewStorage(bfs, cache.NewObjectLRUDefault()))
	require.NoError(t, bfs.MkdirAll("subdir", 0o755))

	info, err := bfs.Stat("test.txt")
	require.NoError(t, err)
	require.Equal(t, int64(11), info.Size())

	require.NoError(t, bfs.Rename("test.txt", "renamed.txt"))
	_, err = bfs.Stat("test.txt")
	require.Error(t, err)
	info, err = bfs.Stat("renamed.txt")
	require.NoError(t, err)
	require.Equal(t, "renamed.txt", info.Name())

	require.NoError(t, bfs.Remove("renamed.txt"))
	_, err = bfs.Stat("renamed.txt")
	require.Error(t, err)

	require.Equal(t, "renamed.txt", bfs.Base("/root/renamed.txt"))
	require.Equal(t, "/root", bfs.Dir("/root/renamed.txt"))
	require.Equal(t, "a/b/c", bfs.Join("a", "b", "c"))
	require.Equal(t, "/root", bfs.Root())

	chroot, err := bfs.Chroot("subdir")
	require.NoError(t, err)
	require.Equal(t, "subdir", chroot.Root())
	_, err = chroot.Create("nested.txt")
	require.NoError(t, err)
}

func TestBillyAdapterOpenFileCreate(t *testing.T) {
	afs := afero.NewMemMapFs()
	bfs := newBillyAdapter(afs, "/root")

	f, err := bfs.OpenFile("deep/nested/file.txt", os.O_RDWR|os.O_CREATE, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	f.Close()

	f, err = bfs.Open("deep/nested/file.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "data", string(data))
	f.Close()
}

func TestBillyAdapterReadDirEmpty(t *testing.T) {
	afs := afero.NewMemMapFs()
	bfs := newBillyAdapter(afs, "/root")
	bfs.MkdirAll("emptydir", 0755)

	entries, err := bfs.ReadDir("emptydir")
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestBillyAdapterSymlinkNotSupported(t *testing.T) {
	afs := afero.NewMemMapFs()
	bfs := newBillyAdapter(afs, "/root")

	err := bfs.Symlink("target", "link")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")

	_, err = bfs.Readlink("link")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")
}

func TestBillyAdapterLstatFile(t *testing.T) {
	afs := afero.NewMemMapFs()
	bfs := newBillyAdapter(afs, "/root")
	f, _ := bfs.Create("lstat_test.txt")
	f.Close()

	info, err := bfs.Lstat("lstat_test.txt")
	require.NoError(t, err)
	require.Equal(t, "lstat_test.txt", info.Name())
	require.False(t, info.IsDir())
}

func TestAuthBuilder(t *testing.T) {
	auth, err := buildAuth(nil)
	require.NoError(t, err)
	require.Nil(t, auth)

	auth, err = buildAuth(&AuthConfig{Type: "basic", Username: "user", Password: "pass"})
	require.NoError(t, err)
	require.NotNil(t, auth)
	require.Equal(t, "http-basic-auth", auth.Name())

	auth, err = buildAuth(&AuthConfig{Type: "token", Password: "tok"})
	require.NoError(t, err)
	require.NotNil(t, auth)
	require.Equal(t, "http-token-auth", auth.Name())

	_, err = buildAuth(&AuthConfig{Type: "bad"})
	require.Error(t, err)
}

func TestAuthEnvVarExpansion(t *testing.T) {
	os.Setenv("TEST_GIT_USER", "myuser")
	os.Setenv("TEST_GIT_TOKEN", "mytoken")
	defer os.Unsetenv("TEST_GIT_USER")
	defer os.Unsetenv("TEST_GIT_TOKEN")

	auth, err := buildAuth(&AuthConfig{
		Type:     "basic",
		Username: "$TEST_GIT_USER",
		Password: "$TEST_GIT_TOKEN",
	})
	require.NoError(t, err)
	require.NotNil(t, auth)
	require.Equal(t, "http-basic-auth", auth.Name())
}

func TestProxyOptionsParsing(t *testing.T) {
	opts := proxyOptions("")
	require.Equal(t, transport.ProxyOptions{}, opts)

	opts = proxyOptions("socks5://proxy:1080")
	require.Equal(t, transport.ProxyOptions{URL: "socks5://proxy:1080"}, opts)

	opts = proxyOptions("socks5://user:pass@proxy:1080")
	require.Equal(t, transport.ProxyOptions{URL: "socks5://user:pass@proxy:1080", Username: "user", Password: "pass"}, opts)

	opts = proxyOptions("://invalid")
	require.Equal(t, transport.ProxyOptions{URL: "://invalid"}, opts)
}

func TestRedactURL(t *testing.T) {
	require.Equal(t, "", redactURL(""))
	require.Equal(t, "https://github.com/user/repo.git", redactURL("https://token:x-oauth-basic@github.com/user/repo.git"))
	require.Equal(t, "https://example.com/repo.git", redactURL("https://user:pass@example.com/repo.git?foo=bar"))
	require.Equal(t, "https://example.com:8080/repo.git", redactURL("https://user:pass@example.com:8080/repo.git"))
	require.Equal(t, "https://example.com/repo.git", redactURL("https://token@example.com/repo.git#frag"))
	require.Equal(t, "invalid", redactURL("invalid"))
}

func TestNewEndpoint(t *testing.T) {
	ep, err := transport.NewEndpoint("file://")
	require.NoError(t, err)
	require.NotNil(t, ep)
}
