package filerepo

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func newValidationManager(t *testing.T, fetch FetchFunc) *GenerationManager {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	h, err := New(Config{Instance: "test", Mode: "test", Tenant: "metadata", Upstream: "https://example.test", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store, Fetch: fetch, Build: func(context.Context, *RefreshSession, Anchor) error { return nil }})
	require.NoError(t, err)
	require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("old")))
	_, err = h.Refresh(context.Background(), 1)
	require.NoError(t, err)
	return h
}

func TestGenerationStrictValidationPublishesBeforeServing(t *testing.T) {
	var requests atomic.Int32
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		requests.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Cache-Control": {"max-age=60"}}, ContentLength: 3, Body: io.NopCloser(strings.NewReader("new"))}, nil
	})
	req := httptest.NewRequest(http.MethodHead, "/repo/Release", nil)
	req.Header.Set("Cache-Control", "no-cache")
	w := httptest.NewRecorder()
	handled, status, _ := h.ServeCurrent(w, req, "repo/Release", true)
	require.True(t, handled)
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, w.Body.String())
	require.Equal(t, digestString([]byte("new")), h.Current("repo").Generation)
	require.Equal(t, int32(1), requests.Load())
}

func TestGenerationNoStoreRetiresRouting(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Cache-Control": {"no-store"}}, Body: http.NoBody}, nil
	})
	req := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
	req.Header.Set("Cache-Control", "no-cache")
	handled, _, _ := h.ServeCurrent(httptest.NewRecorder(), req, "repo/Release", true)
	require.False(t, handled)
	require.Nil(t, h.Current("repo"))
	require.Empty(t, h.pending)
	restored, err := New(h.config)
	require.NoError(t, err)
	require.Nil(t, restored.Current("repo"))
}

func TestGenerationErrorPolicyPreservesCommittedSnapshot(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
				return &http.Response{StatusCode: status, Header: http.Header{"Cache-Control": {"no-store"}}, Body: http.NoBody}, nil
			})
			req := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
			req.Header.Set("Cache-Control", "no-cache")
			w := httptest.NewRecorder()
			handled, code, _ := h.ServeCurrent(w, req, "repo/Release", true)
			require.True(t, handled)
			require.Equal(t, http.StatusBadGateway, code)
			require.Equal(t, digestString([]byte("old")), h.Current("repo").Generation)
			restored, err := New(h.config)
			require.NoError(t, err)
			require.Equal(t, h.Current("repo").CandidateID, restored.Current("repo").CandidateID)
		})
	}
}

func TestGenerationValidationDoesNotExtendProtocolExpiry(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
	})
	h.config.Build = func(_ context.Context, session *RefreshSession, _ Anchor) error {
		session.ValidUntil = time.Now().Add(-time.Hour)
		return nil
	}
	require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("expired")))
	_, err := h.Refresh(context.Background(), 1)
	require.NoError(t, err)
	w := httptest.NewRecorder()
	_, status, _ := h.ServeCurrent(w, httptest.NewRequest(http.MethodGet, "/repo/Release", nil), "repo/Release", true)
	require.Equal(t, http.StatusBadGateway, status)
	require.Equal(t, digestString([]byte("expired")), h.Current("repo").Generation)
}

func TestGenerationRetirementPreventsCandidatePublication(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	h.config.Build = func(ctx context.Context, _ *RefreshSession, _ Anchor) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, h.StageAnchor(ctx, "repo", "repo/Release", nil, strings.NewReader("new")))
	done := make(chan error, 1)
	go func() { _, err := h.Refresh(ctx, 1); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("candidate build did not start")
	}
	require.NoError(t, h.retireCachedRoot("repo"))
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		require.ErrorContains(t, err, "anchor changed")
	case <-ctx.Done():
		t.Fatal("candidate build did not complete")
	}
	require.Nil(t, h.Current("repo"))
	require.Empty(t, h.pending)
	restored, err := New(h.config)
	require.NoError(t, err)
	require.Nil(t, restored.Current("repo"))
}

func TestGenerationSchedulingAndValidationFailure(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.ErrUnexpectedEOF })
	now := time.Now()
	for _, root := range []string{"a", "b", "c", "long-root"} {
		delay := h.nextCheckAt(root, now, nil).Sub(now)
		require.GreaterOrEqual(t, delay, 12*time.Minute)
		require.LessOrEqual(t, delay, 15*time.Minute)
		require.Equal(t, delay, h.nextCheckAt(root, now, nil).Sub(now))
	}
	more, err := h.refresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	req := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
	req.Header.Set("Cache-Control", "no-cache")
	w := httptest.NewRecorder()
	handled, status, _ := h.ServeCurrent(w, req, "repo/Release", true)
	require.True(t, handled)
	require.Equal(t, http.StatusBadGateway, status)
	require.Equal(t, digestString([]byte("old")), h.Current("repo").Generation)
}

func TestGenerationWaitCancellationKeepsScheduledPublication(t *testing.T) {
	var fetches atomic.Int32
	base := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		fetches.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), ContentLength: 3, Body: io.NopCloser(strings.NewReader("new"))}, nil
	})
	sched, err := scheduler.NewPersistent(filepath.Join(t.TempDir(), "scheduler.json"))
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	cfg := base.config
	cfg.Scheduler = sched
	cfg.Build = func(ctx context.Context, _ *RefreshSession, _ Anchor) error {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h, err := New(cfg)
	require.NoError(t, err)
	sched.Start(context.Background())
	t.Cleanup(func() { require.NoError(t, sched.Stop(context.Background())) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/repo/Release", nil).WithContext(ctx)
		req.Header.Set("Cache-Control", "no-cache")
		w := httptest.NewRecorder()
		_, status, _ := h.ServeCurrent(w, req, "repo/Release", true)
		done <- status
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("candidate build did not start")
	}
	cancel()
	select {
	case status := <-done:
		require.Equal(t, http.StatusGatewayTimeout, status)
	case <-time.After(time.Second):
		t.Fatal("request did not cancel")
	}
	require.Equal(t, digestString([]byte("old")), h.Current("repo").Generation)
	close(release)
	require.Eventually(t, func() bool { return h.Current("repo").Generation == digestString([]byte("new")) }, 3*time.Second, time.Millisecond)
	require.Equal(t, int32(1), fetches.Load())
}

func FuzzGenerationRefreshCancellation(f *testing.F) {
	f.Add(uint8(2), true, false, "new")
	f.Add(uint8(4), false, false, "old")
	f.Add(uint8(3), false, true, "new")
	f.Fuzz(func(t *testing.T, workers uint8, cancelEarly, noStore bool, body string) {
		if len(body) > 1024 {
			t.Skip()
		}
		h := newValidationManager(t, func(ctx context.Context, _ string, _ http.Header) (*http.Response, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			header := make(http.Header)
			if noStore {
				header.Set("Cache-Control", "no-store")
			}
			return &http.Response{StatusCode: http.StatusOK, Header: header, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body))}, nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if cancelEarly {
			cancel()
		}
		var group sync.WaitGroup
		for range int(workers%4) + 1 {
			group.Go(func() { _, _ = h.Refresh(ctx, 1) })
		}
		group.Wait()
		if noStore && !cancelEarly {
			require.Nil(t, h.Current("repo"))
		} else {
			require.NotNil(t, h.Current("repo"))
		}
		h.mu.RLock()
		require.Empty(t, h.readers)
		require.LessOrEqual(t, len(h.pending), 1)
		h.mu.RUnlock()
	})
}
