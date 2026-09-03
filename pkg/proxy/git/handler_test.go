package git

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitserver "github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	proxytransport "gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestColdMirrorPassesThroughSmartHTTP(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/repo/info/refs", req.URL.Path)
		_, _ = io.WriteString(w, "advertisement")
	}))
	defer upstream.Close()
	handler := newGitHandler(gitConfig{name: "test", upstream: upstream.URL + "/repo", billyFs: newBillyAdapter(afero.NewMemMapFs(), "")})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "advertisement", recorder.Body.String())
	require.NoError(t, handler.Stop(context.Background()))
}

func TestGitInfoRefsUsesSmartHTTPPreamble(t *testing.T) {
	storage := memory.NewStorage()
	branch := plumbing.NewBranchReferenceName("main")
	hash := plumbing.NewHash(strings.Repeat("a", 40))
	require.NoError(t, storage.SetReference(plumbing.NewHashReference(branch, hash)))
	require.NoError(t, storage.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branch)))

	recorder := httptest.NewRecorder()
	handleInfoRefs(recorder, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil), gitserver.NewServer(&singleLoader{storer: storage}), "test")
	require.Equal(t, http.StatusOK, recorder.Code)
	require.True(t, bytes.HasPrefix(recorder.Body.Bytes(), []byte("001e# service=git-upload-pack\n0000")))
}

func TestGitReadOnlyProtocolSurface(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "/repo/git-upload-pack", request.URL.Path)
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.Equal(t, "fetch", string(body))
		_, _ = io.WriteString(w, "pack")
	}))
	defer upstream.Close()
	handler := newGitHandler(gitConfig{name: "test", upstream: upstream.URL + "/repo", billyFs: newBillyAdapter(afero.NewMemMapFs(), "")})
	t.Cleanup(func() { require.NoError(t, handler.Stop(context.Background())) })

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/git-upload-pack", strings.NewReader("fetch")))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "pack", response.Body.String())

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/git-receive-pack", strings.NewReader("push")))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Equal(t, int32(1), requests.Load())

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-receive-pack", nil))
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Equal(t, int32(1), requests.Load())

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack&service=git-receive-pack", nil))
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Equal(t, int32(1), requests.Load())

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/objects/../config", nil))
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, int32(1), requests.Load())
}

func TestSyncingMirrorPassesThroughWithSharedAdmission(t *testing.T) {
	upstreamRequest := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequest <- struct{}{}
		_, _ = io.WriteString(w, "advertisement")
	}))
	defer upstream.Close()

	gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), upstream.URL, proxytransport.AdmissionForeground)
	require.NoError(t, err)
	handler := newGitHandler(gitConfig{
		name: "test", upstream: upstream.URL + "/repo", billyFs: newBillyAdapter(afero.NewMemMapFs(), ""), upstreamGate: gate,
	})
	handler.repositoryMu.Lock()

	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil))
		close(done)
	}()
	require.Never(t, func() bool { return len(upstreamRequest) != 0 }, 30*time.Millisecond, 2*time.Millisecond)
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		handler.repositoryMu.Unlock()
		<-done
		t.Fatal("request waited for the syncing mirror lock")
	}
	handler.repositoryMu.Unlock()

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "advertisement", recorder.Body.String())
	require.NoError(t, handler.Stop(context.Background()))
}

func TestGitSyncPublishesUpstreamCommit(t *testing.T) {
	upstreamPath := filepath.Join(t.TempDir(), "upstream")
	upstream, err := gitlib.PlainInit(upstreamPath, false)
	require.NoError(t, err)
	worktree, err := upstream.Worktree()
	require.NoError(t, err)
	signature := &object.Signature{Name: "cache-proxy test", Email: "test@example.invalid"}
	commit := func(content string, when time.Time) plumbing.Hash {
		require.NoError(t, os.WriteFile(filepath.Join(upstreamPath, "README.md"), []byte(content), 0o644))
		_, err := worktree.Add("README.md")
		require.NoError(t, err)
		signature.When = when
		hash, err := worktree.Commit(content, &gitlib.CommitOptions{Author: signature, Committer: signature})
		require.NoError(t, err)
		return hash
	}
	firstHash := commit("version 1", time.Unix(1, 0).UTC())

	handler := newGitHandler(gitConfig{
		name: "test", upstream: upstreamPath,
		billyFs: newBillyAdapter(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()), ""),
	})
	t.Cleanup(func() { require.NoError(t, handler.Stop(context.Background())) })
	require.NoError(t, handler.Sync(context.Background()))
	head, err := handler.repository.Head()
	require.NoError(t, err)
	require.Equal(t, firstHash, head.Hash())

	secondHash := commit("version 2", time.Unix(2, 0).UTC())
	require.NoError(t, handler.Sync(context.Background()))
	head, err = handler.repository.Head()
	require.NoError(t, err)
	require.Equal(t, secondHash, head.Hash())
	latest, err := handler.repository.CommitObject(secondHash)
	require.NoError(t, err)
	file, err := latest.File("README.md")
	require.NoError(t, err)
	contents, err := file.Contents()
	require.NoError(t, err)
	require.Equal(t, "version 2", contents)
}

func TestBuildAuthExpandsEnvironmentAndRejectsEmptyCredentials(t *testing.T) {
	t.Setenv("GIT_TEST_USER", "git-user")
	t.Setenv("GIT_TEST_PASSWORD", "git-password")
	auth, err := buildAuth(&AuthConfig{Type: "basic", Username: "${GIT_TEST_USER}", Password: "${GIT_TEST_PASSWORD}"})
	require.NoError(t, err)
	basic, ok := auth.(*githttp.BasicAuth)
	require.True(t, ok)
	require.Equal(t, "git-user", basic.Username)
	require.Equal(t, "git-password", basic.Password)

	_, err = buildAuth(&AuthConfig{Type: "token", Password: "${GIT_TEST_MISSING}"})
	require.ErrorContains(t, err, "requires password")
}

func TestGitHandlerAppliesDefaultOperationTimeout(t *testing.T) {
	handler := newGitHandler(gitConfig{billyFs: newBillyAdapter(afero.NewMemMapFs(), "")})
	require.Equal(t, defaultOperationTimeout, handler.operationTimeout)
	require.Equal(t, defaultOperationTimeout, handler.bootstrapClient.Timeout)
}

func TestGitHandlerRejectsRequestsAfterStop(t *testing.T) {
	handler := newGitHandler(gitConfig{billyFs: newBillyAdapter(afero.NewMemMapFs(), "")})
	require.NoError(t, handler.Stop(context.Background()))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/info/refs?service=git-upload-pack", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, "1", recorder.Header().Get("Retry-After"))
}
