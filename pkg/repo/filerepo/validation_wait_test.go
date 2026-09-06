package filerepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

func TestRetryWindowTracksCandidateFailures(t *testing.T) {
	failure := fmt.Errorf("metadata download: %w", io.ErrUnexpectedEOF)
	var previous retryWindow
	for attempt := 1; attempt <= 30; attempt++ {
		started := time.Now()
		next := previous.afterFailure("candidate", failure)
		require.Equal(t, attempt, next.failures)
		require.Equal(t, "candidate", next.candidateID)
		require.ErrorIs(t, next.err, io.ErrUnexpectedEOF)
		require.True(t, next.notBefore.After(started))
		require.LessOrEqual(t, time.Until(next.notBefore), 15*time.Minute)
		require.Equal(t, attempt-1, previous.failures)
		previous = next
	}
	replacementError := errors.New("replacement unavailable")
	replacement := previous.afterFailure("replacement", replacementError)
	require.Equal(t, 1, replacement.failures)
	require.Equal(t, "replacement", replacement.candidateID)
	require.ErrorIs(t, replacement.err, replacementError)
	require.Equal(t, 30, previous.failures)
}

func TestValidationBackoffReturnsCandidateError(t *testing.T) {
	upstreamErr := fmt.Errorf("upstream request: %w", &net.DNSError{Err: "headers stalled", IsTimeout: true})
	var fetches atomic.Int32
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		fetches.Add(1)
		return nil, upstreamErr
	})
	baseline := h.current["repo"].validatedAt
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := h.waitForValidation(ctx, "repo", baseline, scheduler.ValidationDetails{})
		cancel()
		require.ErrorIs(t, err, upstreamErr)
		require.NotErrorIs(t, err, errValidationWait)
	}
	require.Equal(t, int32(1), fetches.Load())
	req := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
	req.Header.Set("Cache-Control", "no-cache")
	w := httptest.NewRecorder()
	_, status, _ := h.ServeCurrent(w, req, "repo/Release", true)
	require.Equal(t, http.StatusGatewayTimeout, status)
	require.Equal(t, "upstream_timeout", w.Header().Get("X-Cache-Error"))
	require.Equal(t, int32(1), fetches.Load())
	require.Equal(t, digestString([]byte("old")), h.Current("repo").Generation)
}

func TestPendingBackoffPreservesOriginalErrorAndReplacement(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
	})
	buildErr := errors.New("metadata checksum mismatch")
	h.config.Build = func(context.Context, *RefreshSession, Anchor) error { return &retryableRefreshError{err: buildErr} }
	require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("new")))
	_, err := h.Refresh(context.Background(), 1)
	require.ErrorIs(t, err, buildErr)
	baseline := h.current["repo"].validatedAt
	err = h.waitForValidation(context.Background(), "repo", baseline, scheduler.ValidationDetails{})
	require.ErrorIs(t, err, buildErr)
	failedCandidate := h.pending["repo"].CandidateID
	h.config.Build = func(context.Context, *RefreshSession, Anchor) error { return nil }
	require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("replacement")))
	// A delayed failure belonging to the superseded candidate cannot block its replacement.
	h.recordPendingFailure("repo", failedCandidate, buildErr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, h.waitForValidation(ctx, "repo", baseline, scheduler.ValidationDetails{}))
	require.Equal(t, digestString([]byte("replacement")), h.Current("repo").Generation)
	require.Empty(t, h.retryWindows)
}

func TestValidationCompletionPrecedesWaitDeadline(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return nil, errors.New("unexpected fetch")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	baseline := h.current["repo"].validatedAt
	require.NoError(t, h.waitForValidation(ctx, "repo", baseline.Add(-time.Second), scheduler.ValidationDetails{}))
	err := h.waitForValidation(ctx, "repo", baseline, scheduler.ValidationDetails{})
	require.ErrorIs(t, err, errValidationWait)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDiscardedCandidateFailureStillBlocksStrictReuse(t *testing.T) {
	failure := errors.New("invalid metadata structure")
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		t.Error("backoff reached upstream")
		return nil, failure
	})
	h.config.Build = func(context.Context, *RefreshSession, Anchor) error { return failure }
	require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("invalid")))
	_, _, err := h.runRefresh(context.Background(), 1)
	require.ErrorIs(t, err, failure)
	require.Empty(t, h.pending)
	require.ErrorIs(t, h.waitForValidation(context.Background(), "repo", h.current["repo"].validatedAt, scheduler.ValidationDetails{}), failure)
	require.Equal(t, h.Current("repo").CandidateID, h.retryWindows["repo"].candidateID)
	require.Equal(t, digestString([]byte("old")), h.Current("repo").Generation)
}

func TestConcurrentValidationPublishesOnceAndCarriesBuildCauses(t *testing.T) {
	var fetches atomic.Int32
	base := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		fetches.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), ContentLength: 3, Body: io.NopCloser(strings.NewReader("new"))}, nil
	})
	sched, err := scheduler.NewPersistent(filepath.Join(t.TempDir(), "scheduler.json"))
	require.NoError(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	cfg := base.config
	cfg.Scheduler = sched
	cfg.Build = func(ctx context.Context, _ *RefreshSession, _ Anchor) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h, err := New(cfg)
	require.NoError(t, err)
	events := make(chan scheduler.TaskRun, 8)
	sched.SetRunObserver(func(run scheduler.TaskRun) { events <- run })
	sched.Start(context.Background())
	t.Cleanup(func() { once.Do(func() { close(release) }); require.NoError(t, sched.Stop(context.Background())) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan *httptest.ResponseRecorder, 2)
	request := func(policy string) {
		req := httptest.NewRequest(http.MethodGet, "/repo/Release", nil).WithContext(ctx)
		req.Header.Set("Cache-Control", policy)
		w := httptest.NewRecorder()
		h.ServeCurrent(w, req, "repo/Release", true)
		done <- w
	}
	go request("no-cache")
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("build did not start")
	}
	go request("max-age=0")
	require.Eventually(t, func() bool { h.mu.RLock(); defer h.mu.RUnlock(); return h.pending["repo"].validation.ClientMaxAgeZero }, time.Second, time.Millisecond)
	once.Do(func() { close(release) })
	for range 2 {
		select {
		case w := <-done:
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "new", w.Body.String())
		case <-ctx.Done():
			t.Fatal("waiter did not finish")
		}
	}
	require.Equal(t, int32(1), fetches.Load())
	for {
		select {
		case event := <-events:
			if event.Result == "published" {
				require.True(t, event.Validation.ClientNoCache)
				require.True(t, event.Validation.ClientMaxAgeZero)
				return
			}
		case <-ctx.Done():
			t.Fatal("publication event missing")
		}
	}
}

func TestFreshSecondAnchorRequestDoesNotValidate(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		t.Error("fresh ordinary request reached upstream")
		return nil, errors.New("unexpected fetch")
	})
	for range 2 {
		w := httptest.NewRecorder()
		_, status, cache := h.ServeCurrent(w, httptest.NewRequest(http.MethodGet, "/repo/Release", nil), "repo/Release", true)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "HIT", cache)
		require.Equal(t, "old", w.Body.String())
	}
	require.Empty(t, h.pollQueue)
}

func TestValidationTriggerDetails(t *testing.T) {
	for _, tc := range []struct {
		name              string
		request, response http.Header
		expired           bool
		check             func(*testing.T, scheduler.ValidationDetails)
	}{
		{"client no-cache", http.Header{"Cache-Control": {"no-cache"}}, nil, false, func(t *testing.T, v scheduler.ValidationDetails) { require.True(t, v.ClientNoCache) }},
		{"client pragma", http.Header{"Pragma": {"no-cache"}}, nil, false, func(t *testing.T, v scheduler.ValidationDetails) { require.True(t, v.ClientNoCache) }},
		{"client zero age", http.Header{"Cache-Control": {"max-age=0"}}, nil, false, func(t *testing.T, v scheduler.ValidationDetails) { require.True(t, v.ClientMaxAgeZero) }},
		{"response no-cache", nil, http.Header{"Cache-Control": {"no-cache"}}, false, func(t *testing.T, v scheduler.ValidationDetails) { require.True(t, v.ResponseNoCache) }},
		{"expired freshness", nil, http.Header{"Cache-Control": {"max-age=1"}, "Age": {"60"}}, false, func(t *testing.T, v scheduler.ValidationDetails) {
			require.True(t, v.FreshnessExpired)
			require.GreaterOrEqual(t, v.Age, time.Minute)
			require.Equal(t, time.Second, v.Lifetime)
		}},
		{"protocol expiry", nil, nil, true, func(t *testing.T, v scheduler.ValidationDetails) { require.True(t, v.ProtocolExpired) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) { return nil, errors.New("unused") })
			sched, err := scheduler.NewPersistent(filepath.Join(t.TempDir(), "scheduler.json"))
			require.NoError(t, err)
			h.config.Scheduler = sched
			h.current["repo"].header = tc.response
			if tc.expired {
				h.current["repo"].snapshot.ValidUntil = time.Now().Add(-time.Second)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				req := httptest.NewRequest(http.MethodGet, "/repo/Release", nil).WithContext(ctx)
				req.Header = tc.request
				h.ServeCurrent(httptest.NewRecorder(), req, "repo/Release", true)
			}()
			require.Eventually(t, func() bool {
				h.mu.RLock()
				defer h.mu.RUnlock()
				return !h.pollRequests["repo"].validation.ValidatedAt.IsZero()
			}, time.Second, time.Millisecond)
			h.mu.RLock()
			v := h.pollRequests["repo"].validation
			h.mu.RUnlock()
			tc.check(t, v)
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("waiter did not stop")
			}
		})
	}
}

func TestActiveRetryMergesWaitingCauses(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	h := newValidationManager(t, func(ctx context.Context, _ string, _ http.Header) (*http.Response, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
	})
	baseline := h.current["repo"].validatedAt
	h.retryWindows["repo"] = retryWindow{candidateID: h.Current("repo").CandidateID, failures: 1, notBefore: time.Now().Add(-time.Second), err: errors.New("previous failure")}
	h.requestCurrentPoll("repo", false, baseline, scheduler.ValidationDetails{ClientNoCache: true, ValidatedAt: baseline})
	done := make(chan *scheduler.TaskOutcome, 1)
	go func() { _, result, _ := h.runRefresh(context.Background(), 1); done <- result }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("retry did not start")
	}
	// Attach a different cause while the same conditional request is in flight.
	h.requestCurrentPoll("repo", false, baseline, scheduler.ValidationDetails{ClientMaxAgeZero: true, ValidatedAt: baseline})
	h.mu.RLock()
	require.True(t, h.current["repo"].polling)
	h.mu.RUnlock()
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		require.True(t, result.Validation.ClientNoCache)
		require.True(t, result.Validation.ClientMaxAgeZero)
		require.Equal(t, "unchanged", result.Result)
	case <-time.After(time.Second):
		t.Fatal("retry did not complete")
	}
	require.NoError(t, h.waitForValidation(context.Background(), "repo", baseline, scheduler.ValidationDetails{}))
	require.Empty(t, h.retryWindows)
}

func FuzzValidationFailureAndReplacement(f *testing.F) {
	f.Add(uint8(3), true, false)
	f.Add(uint8(1), false, true)
	f.Fuzz(func(t *testing.T, workers uint8, replace, cancelEarly bool) {
		failure := errors.New("upstream interrupted")
		h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) { return nil, failure })
		baseline := h.current["repo"].validatedAt
		require.ErrorIs(t, h.waitForValidation(context.Background(), "repo", baseline, scheduler.ValidationDetails{}), failure)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if cancelEarly {
			cancel()
		}
		var wg sync.WaitGroup
		for range int(workers%4) + 1 {
			wg.Go(func() {
				err := h.waitForValidation(ctx, "repo", baseline, scheduler.ValidationDetails{ClientNoCache: true, ValidatedAt: baseline})
				if err != nil {
					require.True(t, errors.Is(err, failure) || errors.Is(err, context.Canceled))
				}
			})
		}
		if replace {
			require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("new")))
			_, _, err := h.runRefresh(context.Background(), 1)
			require.NoError(t, err)
		}
		wg.Wait()
		if replace {
			require.Equal(t, digestString([]byte("new")), h.Current("repo").Generation)
		} else {
			require.Equal(t, digestString([]byte("old")), h.Current("repo").Generation)
		}
		h.mu.RLock()
		require.Empty(t, h.readers)
		require.LessOrEqual(t, len(h.pollQueue), 1)
		h.mu.RUnlock()
	})
}
