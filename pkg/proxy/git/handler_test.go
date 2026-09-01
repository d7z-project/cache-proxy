package git

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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

func TestSyncingMirrorPassesThroughWithSharedAdmission(t *testing.T) {
	upstreamRequest := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequest <- struct{}{}
		_, _ = io.WriteString(w, "advertisement")
	}))
	defer upstream.Close()

	gate := httpcache.NewUpstreamGate(httpcache.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
	release, err := gate.Acquire(context.Background(), upstream.URL, httpcache.AdmissionForeground)
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
