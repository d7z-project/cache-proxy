package filerepo

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackgroundPollDoesNotUseHTTPFreshness(t *testing.T) {
	cases := []struct {
		name   string
		header http.Header
	}{
		{name: "short max age", header: http.Header{"Cache-Control": {"max-age=1"}}},
		{name: "no cache", header: http.Header{"Cache-Control": {"no-cache"}}},
		{name: "old age and date", header: http.Header{"Age": {"86400"}, "Date": {"Thu, 01 Jan 1970 00:00:00 GMT"}}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var requests atomic.Int32
			h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
				requests.Add(1)
				return &http.Response{StatusCode: http.StatusNotModified, Header: testCase.header, Body: http.NoBody}, nil
			})
			h.config.RefreshInterval = time.Hour
			h.mu.Lock()
			current := h.current["repo"]
			current.nextPoll = time.Now().Add(30 * time.Minute)
			current.header = testCase.header
			anchor := current.snapshot.byPath[current.snapshot.Anchor]
			anchor.Header = testCase.header
			current.snapshot.byPath[current.snapshot.Anchor] = anchor
			h.mu.Unlock()

			more, outcome, err := h.runRefresh(context.Background(), 1)
			require.NoError(t, err)
			require.False(t, more)
			require.True(t, outcome.NoWork)
			require.Zero(t, requests.Load())
		})
	}
}

func TestNextPollAtUsesLatestValidatedTimestampAndStableBound(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return nil, io.EOF
	})
	h.config.RefreshInterval = time.Hour
	validated := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	published := validated.Add(10 * time.Minute)
	bound := min(h.config.RefreshInterval/20, time.Minute)

	first := h.nextPollAt("repo", validated, published)
	second := h.nextPollAt("repo", validated, published)
	require.Equal(t, first, second)
	require.GreaterOrEqual(t, first.Sub(published), h.config.RefreshInterval)
	require.LessOrEqual(t, first.Sub(published), h.config.RefreshInterval+bound)
	require.GreaterOrEqual(t, h.pollOffset("repo"), time.Duration(0))
	require.LessOrEqual(t, h.pollOffset("repo"), bound)

	olderPublished := validated.Add(-time.Hour)
	fromValidated := h.nextPollAt("repo", validated, olderPublished)
	require.Equal(t, h.config.RefreshInterval+h.pollOffset("repo"), fromValidated.Sub(validated))
}

func TestRequestValidationRunsBeforeFutureBackgroundPoll(t *testing.T) {
	cases := []struct {
		name   string
		forced bool
	}{
		{name: "explicit force", forced: true},
		{name: "expired anchor", forced: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var requests atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				w.Header().Set("Cache-Control", "max-age=3600")
				w.WriteHeader(http.StatusNotModified)
			}))
			t.Cleanup(upstream.Close)
			h := newValidationManager(t, func(ctx context.Context, requestPath string, header http.Header) (*http.Response, error) {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL+"/"+requestPath, nil)
				require.NoError(t, err)
				for key, values := range header {
					for _, value := range values {
						request.Header.Add(key, value)
					}
				}
				return http.DefaultClient.Do(request)
			})
			h.config.RefreshInterval = time.Hour
			h.mu.Lock()
			current := h.current["repo"]
			candidateID := current.snapshot.CandidateID
			validatedAt := current.validatedAt
			current.nextPoll = time.Now().Add(30 * time.Minute)
			if !testCase.forced {
				current.validatedAt = time.Now().Add(-time.Hour)
				current.header = http.Header{"Cache-Control": {"max-age=1"}}
				anchor := current.snapshot.byPath[current.snapshot.Anchor]
				anchor.Header = current.header
				current.snapshot.byPath[current.snapshot.Anchor] = anchor
			}
			h.mu.Unlock()

			request := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
			if testCase.forced {
				request.Header.Set("Cache-Control", "no-cache")
			}
			response := httptest.NewRecorder()
			handled, status, _ := h.ServeCurrent(response, request, "repo/Release", true)
			require.True(t, handled)
			require.Equal(t, http.StatusOK, status)
			require.Equal(t, int32(1), requests.Load())

			h.mu.RLock()
			current = h.current["repo"]
			require.Equal(t, candidateID, current.snapshot.CandidateID)
			require.True(t, current.validatedAt.After(validatedAt))
			require.Equal(t, time.Hour+h.pollOffset("repo"), current.nextPoll.Sub(current.validatedAt))
			h.mu.RUnlock()
		})
	}
}

func TestUnchangedAnchorOnlyAdvancesFreshness(t *testing.T) {
	var requests atomic.Int32
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		requests.Add(1)
		return &http.Response{StatusCode: http.StatusNotModified, Header: http.Header{"Cache-Control": {"max-age=1"}}, Body: http.NoBody}, nil
	})
	h.config.RefreshInterval = time.Hour
	h.mu.Lock()
	current := h.current["repo"]
	current.nextPoll = time.Now().Add(-time.Minute)
	candidateID := current.snapshot.CandidateID
	generation := current.snapshot.Generation
	validatedAt := current.validatedAt
	h.mu.Unlock()

	more, outcome, err := h.runRefresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, "unchanged", outcome.Result)
	require.False(t, outcome.NoWork)
	require.Equal(t, int32(1), requests.Load())
	h.mu.RLock()
	current = h.current["repo"]
	require.Equal(t, candidateID, current.snapshot.CandidateID)
	require.Equal(t, generation, current.snapshot.Generation)
	require.True(t, current.validatedAt.After(validatedAt))
	require.Equal(t, time.Hour+h.pollOffset("repo"), current.nextPoll.Sub(current.validatedAt))
	h.mu.RUnlock()
}

func TestRestoreUsesCurrentIntervalAndSpreadsOverdueRoots(t *testing.T) {
	anchorFetch := func(context.Context, string, http.Header) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
	}
	h := newValidationManager(t, anchorFetch)
	h.config.RefreshInterval = time.Hour
	roots := []string{"repo", "overdue-a", "overdue-b", "future"}
	for _, rootID := range roots[1:] {
		require.NoError(t, h.StageAnchorID(context.Background(), rootID, rootID, rootID+"/Release", nil, strings.NewReader(rootID)))
	}
	_, err := h.Refresh(context.Background(), 10)
	require.NoError(t, err)

	now := time.Now().UTC()
	rewriteSnapshotTimes(t, h, roots[0], now.Add(-10*time.Minute), now.Add(-10*time.Minute))
	rewriteSnapshotTimes(t, h, roots[1], now.Add(-2*time.Hour), now.Add(-2*time.Hour))
	rewriteSnapshotTimes(t, h, roots[2], now.Add(-2*time.Hour), now.Add(-2*time.Hour))
	rewriteSnapshotTimes(t, h, roots[3], now.Add(time.Hour), now.Add(time.Hour))
	restoreStarted := time.Now()
	restored, err := New(h.config)
	require.NoError(t, err)

	restoredNow := time.Now()
	restored.mu.RLock()
	recent := restored.current[roots[0]]
	overdueA := restored.current[roots[1]]
	overdueB := restored.current[roots[2]]
	require.NotNil(t, recent)
	require.NotNil(t, overdueA)
	require.NotNil(t, overdueB)
	require.Greater(t, recent.nextPoll.Sub(restoredNow), 40*time.Minute)
	for _, current := range []*liveSnapshot{overdueA, overdueB, restored.current["future"]} {
		require.NotNil(t, current)
		offset := restored.pollOffset(current.snapshot.RootID)
		require.False(t, current.nextPoll.Before(restoreStarted.Add(offset)))
		require.False(t, current.nextPoll.After(restoredNow.Add(offset)))
	}
	restored.mu.RUnlock()
}

func TestPendingBackoffDoesNotHideDueRoot(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
	})
	require.NoError(t, h.StageAnchorID(context.Background(), "other", "other", "other/Release", nil, strings.NewReader("other")))
	_, err := h.Refresh(context.Background(), 10)
	require.NoError(t, err)
	h.requestCurrentPoll("repo", true, time.Time{})
	require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("new")))
	h.mu.Lock()
	h.retryWindows["repo"] = retryWindow{candidateID: h.pending["repo"].CandidateID, failures: 1, notBefore: time.Now().Add(time.Hour)}
	h.current["other"].nextPoll = time.Now().Add(-time.Minute)
	h.mu.Unlock()
	more, outcome, err := h.runRefresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, "other", outcome.Target)
	require.Equal(t, "periodic", outcome.Reason)
	require.Equal(t, "unchanged", outcome.Result)
	more, outcome, err = h.runRefresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	require.True(t, outcome.NoWork)
}

func TestReadyRefreshAlternatesQueuedPollsAndPendingBuilds(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
	})
	for _, root := range []string{"a", "b", "c"} {
		require.NoError(t, h.StageAnchor(context.Background(), root, root+"/Release", nil, strings.NewReader(root)))
	}
	for _, root := range []string{"a", "b", "c"} {
		h.requestCurrentPoll("repo", false, time.Time{})
		more, outcome, err := h.runRefresh(context.Background(), 1)
		require.NoError(t, err)
		require.True(t, more)
		require.Equal(t, "repo", outcome.Target)
		require.Equal(t, "check", outcome.Phase)
		require.Equal(t, "unchanged", outcome.Result)
		_, outcome, err = h.runRefresh(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, root, outcome.Target)
		require.Equal(t, "build", outcome.Phase)
		require.Equal(t, "published", outcome.Result)
		require.NotNil(t, h.Current(root))
	}
	more, outcome, err := h.runRefresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	require.True(t, outcome.NoWork)
}

func TestRecoveryDuringActivePollPublishesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var requests, builds atomic.Int32
	h := newValidationManager(t, func(ctx context.Context, _ string, _ http.Header) (*http.Response, error) {
		if requests.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
	})
	h.config.Build = func(context.Context, *RefreshSession, Anchor) error { builds.Add(1); return nil }
	previous := h.Current("repo").CandidateID
	h.requestCurrentPoll("repo", false, time.Time{})
	go func() { _, _, err := h.runRefresh(ctx, 1); finished <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var triggers sync.WaitGroup
	for index := range 8 {
		triggers.Go(func() { h.requestCurrentPoll("repo", index%2 == 0, time.Time{}) })
	}
	triggers.Wait()
	close(release)
	require.NoError(t, <-finished)
	more, outcome, err := h.runRefresh(ctx, 1)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, "recovery", outcome.Reason)
	require.Equal(t, "staged", outcome.Result)
	_, outcome, err = h.runRefresh(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, "recovery", outcome.Reason)
	require.Equal(t, "build", outcome.Phase)
	require.Equal(t, "published", outcome.Result)
	require.NotEqual(t, previous, h.Current("repo").CandidateID)
	more, outcome, err = h.runRefresh(ctx, 1)
	require.NoError(t, err)
	require.False(t, more)
	require.True(t, outcome.NoWork)
	require.Equal(t, int32(2), requests.Load())
	require.Equal(t, int32(1), builds.Load())
}

func TestRequestCurrentPollCoalescesAndRetainsRecovery(t *testing.T) {
	h := newValidationManager(t, func(context.Context, string, http.Header) (*http.Response, error) {
		return nil, io.EOF
	})
	h.requestCurrentPoll("repo", false, time.Time{})
	h.requestCurrentPoll("repo", false, time.Time{})
	h.mu.RLock()
	require.Len(t, h.pollQueue, 1)
	require.False(t, h.pollRequests["repo"].forceRebuild)
	h.mu.RUnlock()

	h.requestCurrentPoll("repo", true, time.Time{})
	h.requestCurrentPoll("repo", false, time.Time{})
	h.mu.RLock()
	require.Len(t, h.pollQueue, 1)
	require.True(t, h.pollRequests["repo"].forceRebuild)
	require.Equal(t, "recovery", h.pollRequests["repo"].reason)
	h.mu.RUnlock()

	h.mu.Lock()
	h.removePollLocked("repo")
	h.current["repo"].polling = true
	h.current["repo"].pollingRebuild = false
	h.mu.Unlock()
	h.requestCurrentPoll("repo", false, time.Time{})
	h.mu.RLock()
	require.Empty(t, h.pollQueue)
	h.mu.RUnlock()
	h.requestCurrentPoll("repo", true, time.Time{})
	h.mu.RLock()
	require.Len(t, h.pollQueue, 1)
	require.True(t, h.pollRequests["repo"].forceRebuild)
	h.mu.RUnlock()
}

func rewriteSnapshotTimes(t *testing.T, h *GenerationManager, rootID string, validatedAt, publishedAt time.Time) {
	t.Helper()
	current := h.Current(rootID)
	require.NotNil(t, current)
	var snapshot Snapshot
	require.NoError(t, readJSON(h.config.StateDir, snapshotName(rootID, current.Generation, current.CandidateID), maxSnapshotStateSize, &snapshot))
	snapshot.PublishedAt = publishedAt
	require.NoError(t, prepareSnapshot(&snapshot))
	encoded, digest, err := encodeJSONDigest(snapshot)
	require.NoError(t, err)
	require.NoError(t, writeBytes(h.config.StateDir, snapshotName(rootID, current.Generation, current.CandidateID), encoded))
	var marker currentMarker
	require.NoError(t, readYAML(h.config.StateDir, currentName(rootID), &marker))
	marker.ValidatedAt = validatedAt
	marker.SnapshotSHA256 = digest
	prepared, err := prepareYAML(h.config.StateDir, currentName(rootID), marker)
	require.NoError(t, err)
	defer prepared.discard()
	require.NoError(t, prepared.commit())
}
