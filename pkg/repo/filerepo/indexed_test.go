package filerepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

func TestGenerationManagerPublishesCompleteGeneration(t *testing.T) {
	metadata := []byte("complete metadata\n")
	digest := sha256.Sum256(metadata)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/repo/index", request.URL.Path)
		_, _ = w.Write(metadata)
	}))
	t.Cleanup(upstream.Close)

	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "test", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(ctx context.Context, origin, requestPath string, _ http.Header) (*http.Response, error) {
			require.Equal(t, upstream.URL, origin)
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/"+requestPath, nil)
			if err != nil {
				return nil, err
			}
			return http.DefaultClient.Do(request)
		},
		Build: func(ctx context.Context, session *RefreshSession, _ Anchor) error {
			blob, err := session.Fetch(ctx, ObjectSpec{Path: "repo/index", ExpectedSize: int64Pointer(len(metadata)), Checksums: []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(digest[:])}}})
			if err != nil {
				return err
			}
			return session.Alias("repo/by-hash/"+hex.EncodeToString(digest[:]), blob)
		},
	})
	require.NoError(t, err)

	anchor := bytes.NewReader([]byte("anchor\n"))
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", upstream.URL, http.Header{"Content-Type": {"text/plain"}}, anchor))
	more, err := handler.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.False(t, more)
	require.NotNil(t, handler.Current("repo"))

	request := httptest.NewRequest(http.MethodGet, "/repo/index", nil)
	response := httptest.NewRecorder()
	handled, status, cache := handler.ServeCurrent(response, request, "repo/index", true)
	require.True(t, handled)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", cache)
	require.Equal(t, metadata, response.Body.Bytes())

	missingResponse := httptest.NewRecorder()
	handled, status, _ = handler.ServeCurrent(missingResponse, httptest.NewRequest(http.MethodGet, "/repo/missing", nil), "repo/missing", true)
	require.True(t, handled)
	require.Equal(t, http.StatusServiceUnavailable, status)

	restarted, err := New(Config{
		Instance: "test", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.NotNil(t, restarted.Current("repo"))
}

func TestGenerationManagerResumesVerifiedSHA256Objects(t *testing.T) {
	firstBody := []byte("first")
	secondBody := []byte("second")
	firstDigest := sha256.Sum256(firstBody)
	secondDigest := sha256.Sum256(secondBody)
	var firstRequests, secondRequests atomic.Int32
	var failSecond atomic.Bool
	failSecond.Store(true)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repo/first":
			firstRequests.Add(1)
			_, _ = w.Write(firstBody)
		case "/repo/second":
			secondRequests.Add(1)
			if failSecond.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write(secondBody)
		}
	}))
	t.Cleanup(upstream.Close)
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "resume", Mode: "deb", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(ctx context.Context, origin, requestPath string, _ http.Header) (*http.Response, error) {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/"+requestPath, nil)
			if err != nil {
				return nil, err
			}
			return http.DefaultClient.Do(request)
		},
		Build: func(ctx context.Context, session *RefreshSession, _ Anchor) error {
			if _, err := session.Fetch(ctx, ObjectSpec{Path: "repo/first", ExpectedSize: int64Pointer(len(firstBody)), Checksums: []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(firstDigest[:])}}}); err != nil {
				return err
			}
			_, err := session.Fetch(ctx, ObjectSpec{Path: "repo/second", ExpectedSize: int64Pointer(len(secondBody)), Checksums: []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(secondDigest[:])}}})
			return err
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", upstream.URL, nil, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 10)
	require.Error(t, err)
	failSecond.Store(false)
	handler.mu.Lock()
	delete(handler.retryWindows, "repo")
	handler.mu.Unlock()
	_, err = handler.Refresh(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, int32(1), firstRequests.Load())
	require.Equal(t, int32(2), secondRequests.Load())
}

func TestRefreshSessionRejectsFetchPathOutsideRepositoryRoot(t *testing.T) {
	var requests atomic.Int32
	handler := &GenerationManager{config: Config{Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) {
		requests.Add(1)
		return nil, errors.New("unexpected fetch")
	}}}
	session := &RefreshSession{handler: handler, root: "repository", objects: map[string]Object{}}
	_, err := session.Fetch(context.Background(), ObjectSpec{Path: "repository/index", FetchPath: "other/index"})
	require.ErrorContains(t, err, "invalid metadata fetch path")
	require.Zero(t, requests.Load())
}

func TestGenerationManagerRejectsSupersededCandidate(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	handler, err := New(Config{
		Instance: "race", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(_ context.Context, _ *RefreshSession, anchor Anchor) error {
			if anchor.Generation == digestString([]byte("first")) {
				once.Do(func() { close(started) })
				<-release
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("first"))))
	handler.mu.RLock()
	firstCandidate := handler.pending["repo"].CandidateID
	handler.mu.RUnlock()
	refreshDone := make(chan error, 1)
	go func() {
		_, refreshErr := handler.Refresh(context.Background(), 1)
		refreshDone <- refreshErr
	}()
	<-started
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("first"))))
	close(release)
	require.Error(t, <-refreshDone)
	require.Nil(t, handler.Current("repo"))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, digestString([]byte("first")), handler.Current("repo").Generation)
	require.NotEqual(t, firstCandidate, handler.Current("repo").CandidateID)
}

func TestGenerationManagerConditionalPollAdvancesFreshness(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	var validators http.Header
	config := Config{
		Instance: "poll", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, _, _ string, header http.Header) (*http.Response, error) {
			validators = header.Clone()
			return &http.Response{StatusCode: http.StatusNotModified, Header: http.Header{"Etag": {`"v2"`}}, Body: io.NopCloser(strings.NewReader(""))}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	handler, err := New(config)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", http.Header{"ETag": {`"v1"`}}, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	committed := handler.Current("repo")
	snapshotFile := statePath(stateDir, snapshotName("repo", committed.Generation, committed.CandidateID))
	snapshotBefore, err := os.ReadFile(snapshotFile)
	require.NoError(t, err)
	markerBefore, err := os.ReadFile(statePath(stateDir, currentName("repo")))
	require.NoError(t, err)
	before := handler.Current("repo").PublishedAt
	time.Sleep(time.Millisecond)
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, `"v1"`, validators.Get("If-None-Match"))
	require.True(t, handler.Current("repo").PublishedAt.After(before))
	require.Equal(t, `"v2"`, handler.Current("repo").byPath["repo/Release"].Header.Get("ETag"))
	snapshotAfter, err := os.ReadFile(snapshotFile)
	require.NoError(t, err)
	markerAfter, err := os.ReadFile(statePath(stateDir, currentName("repo")))
	require.NoError(t, err)
	require.Equal(t, snapshotBefore, snapshotAfter)
	require.Equal(t, markerBefore, markerAfter)

	restarted, err := New(config)
	require.NoError(t, err)
	require.Equal(t, committed.CandidateID, restarted.Current("repo").CandidateID)
	require.Equal(t, `"v1"`, restarted.Current("repo").byPath["repo/Release"].Header.Get("ETag"))
}

func TestGenerationManagerUnchangedPollAdvancesFreshnessWithoutCandidate(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	var validators http.Header
	handler, err := New(Config{
		Instance: "unchanged-poll", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, _, _ string, header http.Header) (*http.Response, error) {
			validators = header.Clone()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": {`"v2"`}}, Body: io.NopCloser(strings.NewReader("anchor")), ContentLength: 6}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", http.Header{"ETag": {`"v1"`}}, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	committed := handler.Current("repo")
	time.Sleep(time.Millisecond)
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := handler.Current("repo")
	require.Equal(t, committed.CandidateID, current.CandidateID)
	require.True(t, current.PublishedAt.After(committed.PublishedAt))
	require.Equal(t, `"v1"`, validators.Get("If-None-Match"))
	require.Equal(t, `"v2"`, current.byPath["repo/Release"].Header.Get("ETag"))
	handler.mu.RLock()
	require.Empty(t, handler.pending)
	handler.mu.RUnlock()
}

func TestGenerationManagerPollUsesConfiguredLargeAnchorLimit(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	largeAnchor := bytes.Repeat([]byte("x"), 16<<20+1)
	handler, err := New(Config{
		Instance: "large-anchor", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		AnchorMaxBytes: 17 << 20,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(largeAnchor)), ContentLength: int64(len(largeAnchor))}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("small"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := handler.Current("repo")
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := handler.Current("repo")
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	require.Equal(t, digestString(largeAnchor), current.Generation)
}

func TestLastSeenPersistenceDoesNotRetryUnderContinuousTraffic(t *testing.T) {
	rootID := "busy-repository"
	now := time.Now().UTC()
	handler := &GenerationManager{
		config:            Config{StateDir: t.TempDir()},
		lastSeen:          map[string]time.Time{rootID: now},
		lastSeenPersisted: map[string]time.Time{rootID: now.Add(-2 * time.Hour)},
	}
	stop := make(chan struct{})
	started := make(chan struct{})
	updatesDone := make(chan struct{})
	go func() {
		defer close(updatesDone)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				handler.mu.Lock()
				handler.lastSeen[rootID] = time.Now().UTC()
				handler.mu.Unlock()
			}
		}
	}()
	<-started
	done := make(chan error, 1)
	go func() { done <- handler.touchLastSeen(rootID, time.Now().UTC()) }()
	var persistErr error
	select {
	case persistErr = <-done:
	case <-time.After(time.Second):
		close(stop)
		<-updatesDone
		t.Fatal("last-seen persistence did not complete while requests advanced the timestamp")
	}
	close(stop)
	<-updatesDone
	require.NoError(t, persistErr)

	var marker lastSeenMarker
	_, err := readJSON(handler.config.StateDir, lastSeenName(rootID), maxRepositoryMarkerSize, &marker)
	require.NoError(t, err)
	require.Equal(t, rootID, marker.RootID)
	require.False(t, marker.SeenAt.IsZero())
}

func TestGenerationManagerGCProtectsActiveReader(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "gc", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:        func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:        func(context.Context, *RefreshSession, Anchor) error { return nil },
		KeepPrevious: 1, GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	publish := func(body string) *Snapshot {
		require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte(body))))
		_, refreshErr := handler.Refresh(context.Background(), 1)
		require.NoError(t, refreshErr)
		return handler.Current("repo")
	}
	first := publish("first")
	writer := &blockingResponseWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	served := make(chan struct{})
	go func() {
		handler.ServeCurrent(writer, httptest.NewRequest(http.MethodGet, "/repo/Release", nil), "repo/Release", true)
		close(served)
	}()
	<-writer.started
	publish("second")
	publish("third")
	drainGenerationGC(t, handler)
	_, err = store.StatObject(context.Background(), "metadata", first.byPath["repo/Release"].Key)
	require.NoError(t, err)
	close(writer.release)
	<-served
	drainGenerationGC(t, handler)
	_, err = store.StatObject(context.Background(), "metadata", first.byPath["repo/Release"].Key)
	require.Error(t, err)
}

func TestGenerationManagerGCProtectsPersistedCurrentMarker(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "gc-marker", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:       func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:       func(context.Context, *RefreshSession, Anchor) error { return nil },
		GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	snapshot := handler.Current("repo")
	key := snapshot.byPath[snapshot.Anchor].Key

	// Simulate a process state fault after the durable marker became visible.
	handler.mu.Lock()
	delete(handler.current, "repo")
	delete(handler.lastSeen, "repo")
	handler.mu.Unlock()
	drainGenerationGC(t, handler)
	_, err = store.StatObject(context.Background(), "metadata", key)
	require.NoError(t, err)
}

func TestGenerationManagerIsolatesInvalidCurrentState(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	config := Config{
		Instance: "state", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	handler, err := New(config)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	currentPath := filepath.Join(stateDir, filepath.FromSlash(currentName("repo")))
	var marker map[string]any
	data, err := os.ReadFile(currentPath)
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(data, &marker))
	marker["unknown"] = true
	data, err = yaml.Marshal(marker)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(currentPath, data, 0o644))
	restored, err := New(config)
	require.NoError(t, err)
	require.Nil(t, restored.Current("repo"))
}

func TestGenerationManagerRestoresCurrentDespiteInvalidAncillaryState(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	config := Config{
		Instance: "ancillary", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	handler, err := New(config)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor"))))
	pendingData, err := os.ReadFile(statePath(stateDir, pendingName("repo")))
	require.NoError(t, err)
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := handler.Current("repo")
	require.NotNil(t, current)

	require.NoError(t, os.WriteFile(statePath(stateDir, lastSeenName("repo")), []byte("not-json"), 0o600))
	require.NoError(t, os.WriteFile(statePath(stateDir, pendingName("repo")), pendingData, 0o600))
	restarted, err := New(config)
	require.NoError(t, err)
	require.Equal(t, current.CandidateID, restarted.Current("repo").CandidateID)
	restarted.mu.RLock()
	_, pending := restarted.pending["repo"]
	restarted.mu.RUnlock()
	require.False(t, pending)
	_, err = os.Stat(statePath(stateDir, pendingName("repo")))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestGenerationManagerRetiresInactiveRoot(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "inactive", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch:         func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:         func(context.Context, *RefreshSession, Anchor) error { return nil },
		InactiveAfter: time.Nanosecond, GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	snapshot := handler.Current("repo")
	key := snapshot.byPath["repo/Release"].Key
	handler.mu.Lock()
	handler.lastSeen["repo"] = time.Now().Add(-time.Hour)
	handler.mu.Unlock()
	drainGenerationGC(t, handler)
	require.Nil(t, handler.Current("repo"))
	_, err = store.StatObject(context.Background(), "metadata", key)
	require.Error(t, err)
	_, err = os.Stat(statePath(stateDir, currentName("repo")))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(statePath(stateDir, repositoryDirectory("repo")))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestGenerationManagerGCRemovesInvalidOrphanCandidate(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "invalid-candidate", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch:       func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:       func(context.Context, *RefreshSession, Anchor) error { return nil },
		GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	rootHash := digestString([]byte("orphan"))
	generation := strings.Repeat("a", 64)
	candidateID := strings.Repeat("b", 32)
	prefix := "generations/" + rootHash + "/" + generation + "/" + candidateID
	snapshotPath := statePath(stateDir, "repositories/"+rootHash+"/generations/"+generation+"/"+candidateID+"/snapshot.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(snapshotPath), 0o755))
	require.NoError(t, os.WriteFile(snapshotPath, []byte("not-json"), 0o600))
	past := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(snapshotPath, past, past))
	require.NoError(t, store.MkdirAll("metadata/"+prefix, 0o755))
	_, err = store.Put(context.Background(), "metadata", prefix+"/anchor", strings.NewReader("orphan"), nil)
	require.NoError(t, err)

	drainGenerationGC(t, handler)
	_, err = os.Stat(snapshotPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Dir(snapshotPath))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = store.StatObject(context.Background(), "metadata", prefix+"/anchor")
	require.Error(t, err)
}

func TestGenerationManagerRetiresPendingOnlyRoot(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "pending", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil }, InactiveAfter: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor"))))
	handler.mu.Lock()
	handler.lastSeen["repo"] = time.Now().Add(-time.Hour)
	handler.mu.Unlock()
	drainGenerationGC(t, handler)
	handler.mu.RLock()
	_, pending := handler.pending["repo"]
	handler.mu.RUnlock()
	require.False(t, pending)
	_, err = os.Stat(statePath(stateDir, pendingName("repo")))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestGenerationManagerRetriesFailedRootRetirement(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "retire-retry", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil }, InactiveAfter: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor"))))
	handler.mu.Lock()
	handler.lastSeen["repo"] = time.Now().Add(-time.Hour)
	handler.mu.Unlock()

	pendingPath := statePath(stateDir, pendingName("repo"))
	require.NoError(t, os.Remove(pendingPath))
	require.NoError(t, os.MkdirAll(pendingPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pendingPath, "blocker"), []byte("x"), 0o600))
	_, err = handler.GC(context.Background(), 1)
	require.Error(t, err)
	require.Empty(t, handler.gcCursor)
	handler.mu.RLock()
	_, stillPending := handler.pending["repo"]
	handler.mu.RUnlock()
	require.True(t, stillPending)

	require.NoError(t, os.RemoveAll(pendingPath))
	drainGenerationGC(t, handler)
	handler.mu.RLock()
	_, stillPending = handler.pending["repo"]
	handler.mu.RUnlock()
	require.False(t, stillPending)
}

func TestGenerationManagerRetiresLastSeenOnlyRoot(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	data, _, err := encodeJSONDigest(lastSeenMarker{
		Version: StateVersion,
		RootID:  "orphan",
		SeenAt:  time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	require.NoError(t, writeBytes(stateDir, lastSeenName("orphan"), data))
	handler, err := New(Config{
		Instance: "last-seen", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch:         func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:         func(context.Context, *RefreshSession, Anchor) error { return nil },
		InactiveAfter: time.Nanosecond,
	})
	require.NoError(t, err)
	drainGenerationGC(t, handler)
	handler.mu.RLock()
	_, exists := handler.lastSeen["orphan"]
	handler.mu.RUnlock()
	require.False(t, exists)
	_, err = os.Stat(statePath(stateDir, lastSeenName("orphan")))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestGenerationManagerRotatesPastFailedRoot(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "rotation", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(_ context.Context, _ *RefreshSession, anchor Anchor) error {
			if anchor.RootID == "a" {
				return &retryableRefreshError{err: io.ErrUnexpectedEOF}
			}
			return nil
		},
	})
	require.NoError(t, err)
	for _, rootID := range []string{"a", "b"} {
		require.NoError(t, handler.StageAnchorID(context.Background(), rootID, rootID, rootID+"/Release", "https://upstream.example", nil, bytes.NewReader([]byte(rootID))))
	}
	more, err := handler.Refresh(context.Background(), 1)
	require.Error(t, err)
	require.True(t, more)
	more, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	require.NotNil(t, handler.Current("b"))
	handler.mu.RLock()
	retry := handler.retryWindows["a"]
	handler.mu.RUnlock()
	require.True(t, retry.notBefore.After(time.Now()))

	// A newly observed anchor supersedes the failed attempt and must be eligible
	// immediately instead of inheriting its retry delay.
	require.NoError(t, handler.StageAnchorID(context.Background(), "a", "a", "a/Release", "https://upstream.example", nil, bytes.NewReader([]byte("replacement"))))
	handler.mu.RLock()
	_, delayed := handler.retryWindows["a"]
	handler.mu.RUnlock()
	require.False(t, delayed)
}

func TestGenerationManagerRejectsOversizedAnchorBeforeStaging(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "anchor-limit", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		AnchorMaxBytes: 4,
		Fetch:          func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:          func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.ErrorIs(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("large"))), storeio.ErrObjectTooLarge)
	handler.mu.RLock()
	_, pending := handler.pending["repo"]
	handler.mu.RUnlock()
	require.False(t, pending)
}

func TestGenerationManagerRejectsOversizedPendingState(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "pending-limit", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	header := http.Header{"X-Oversized": {strings.Repeat("x", maxRepositoryMarkerSize)}}
	err = handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", header, bytes.NewReader([]byte("anchor")))
	require.ErrorContains(t, err, "state document exceeds")
	require.Nil(t, handler.Current("repo"))
	handler.mu.RLock()
	_, pending := handler.pending["repo"]
	handler.mu.RUnlock()
	require.False(t, pending)
	_, err = os.Stat(statePath(stateDir, pendingName("repo")))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestGenerationManagerPublicationFaultsRemainRetryable(t *testing.T) {
	newManager := func(t *testing.T) (*GenerationManager, string) {
		t.Helper()
		store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		stateDir := t.TempDir()
		handler, err := New(Config{
			Instance: "fault", Mode: "test", Tenant: "metadata", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
			Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
			Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
		})
		require.NoError(t, err)
		return handler, stateDir
	}

	t.Run("pending marker", func(t *testing.T) {
		handler, stateDir := newManager(t)
		destination := statePath(stateDir, pendingName("repo"))
		require.NoError(t, os.MkdirAll(destination, 0o755))
		err := handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor")))
		require.Error(t, err)
		require.Nil(t, handler.Current("repo"))
		handler.mu.RLock()
		_, pending := handler.pending["repo"]
		handler.mu.RUnlock()
		require.False(t, pending)
	})

	for _, fault := range []string{"snapshot", "current marker"} {
		t.Run(fault, func(t *testing.T) {
			handler, stateDir := newManager(t)
			require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte("anchor"))))
			handler.mu.RLock()
			pending := handler.pending["repo"]
			handler.mu.RUnlock()
			destination := statePath(stateDir, currentName("repo"))
			if fault == "snapshot" {
				destination = statePath(stateDir, snapshotName("repo", pending.Generation, pending.CandidateID))
			}
			require.NoError(t, os.MkdirAll(destination, 0o755))
			_, err := handler.Refresh(context.Background(), 1)
			require.Error(t, err)
			require.Nil(t, handler.Current("repo"))
			handler.mu.RLock()
			latest, stillPending := handler.pending["repo"]
			handler.mu.RUnlock()
			require.True(t, stillPending)
			require.Equal(t, pending.CandidateID, latest.CandidateID)

			require.NoError(t, os.RemoveAll(destination))
			handler.mu.Lock()
			delete(handler.retryWindows, "repo")
			handler.mu.Unlock()
			_, err = handler.Refresh(context.Background(), 1)
			require.NoError(t, err)
			require.Equal(t, pending.CandidateID, handler.Current("repo").CandidateID)
		})
	}
}

func TestGenerationManagerGCBoundsInactiveRootInspection(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "gc-batch", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:         func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:         func(context.Context, *RefreshSession, Anchor) error { return nil },
		InactiveAfter: time.Nanosecond,
	})
	require.NoError(t, err)
	for _, rootID := range []string{"a", "b", "c"} {
		require.NoError(t, handler.StageAnchorID(context.Background(), rootID, rootID, rootID+"/Release", "https://upstream.example", nil, bytes.NewReader([]byte(rootID))))
	}
	handler.mu.Lock()
	for rootID := range handler.pending {
		handler.lastSeen[rootID] = time.Now().Add(-time.Hour)
	}
	handler.mu.Unlock()

	for remaining := 2; remaining >= 0; remaining-- {
		more, err := handler.GC(context.Background(), 1)
		require.NoError(t, err)
		require.True(t, more)
		handler.mu.RLock()
		require.Len(t, handler.pending, remaining)
		handler.mu.RUnlock()
	}
}

func TestGenerationManagerGCRetriesInterruptedCandidateDeletion(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "gc-cancel", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:        func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:        func(context.Context, *RefreshSession, Anchor) error { return nil },
		KeepPrevious: 1, GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	var oldestKey string
	for _, body := range []string{"first", "second", "third"} {
		require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", "https://upstream.example", nil, bytes.NewReader([]byte(body))))
		_, err = handler.Refresh(context.Background(), 1)
		require.NoError(t, err)
		if oldestKey == "" {
			oldestKey = handler.Current("repo").byPath["repo/Release"].Key
		}
		time.Sleep(time.Millisecond)
	}

	more, err := handler.GC(context.Background(), 100)
	require.NoError(t, err)
	require.True(t, more)
	more, err = handler.GC(context.Background(), 100)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, generationGCDelete, handler.gcPhase)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = handler.GC(canceled, 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.StatObject(context.Background(), "metadata", oldestKey)
	require.NoError(t, err)

	drainGenerationGC(t, handler)
	_, err = store.StatObject(context.Background(), "metadata", oldestKey)
	require.Error(t, err)
}

func TestGenerationManagerCoalescesScheduledDiscovery(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "discovery", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	lifecycle := storeio.NewLifecycle()
	for range 20 {
		handler.ScheduleDiscovery(lifecycle, "repo", "repo", "repo/Release", "https://upstream.example")
	}
	handler.discoveryMu.Lock()
	require.Len(t, handler.discoveryPending, 1)
	handler.discoveryMu.Unlock()
	require.NoError(t, lifecycle.Close(context.Background()))
	handler.discoveryMu.Lock()
	require.Empty(t, handler.discoveryPending)
	handler.discoveryMu.Unlock()
}

type blockingResponseWriter struct {
	header  http.Header
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingResponseWriter) Header() http.Header { return w.header }
func (*blockingResponseWriter) WriteHeader(int)       {}
func (w *blockingResponseWriter) Write(body []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(body), nil
}

func digestString(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func int64Pointer(value int) *int64 {
	converted := int64(value)
	return &converted
}

func drainGenerationGC(t *testing.T, handler *GenerationManager) {
	t.Helper()
	for range 20 {
		more, err := handler.GC(context.Background(), 100)
		require.NoError(t, err)
		if !more {
			return
		}
	}
	t.Fatal("generation GC did not complete")
}

func FuzzCleanPath(f *testing.F) {
	for _, seed := range []string{"dists/bookworm/Release", "repo/repodata/repomd.xml", "../escape", "a\\b", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		cleaned, err := CleanPath(value)
		if err == nil {
			require.Equal(t, value, cleaned)
			require.NotEmpty(t, cleaned)
			require.NotContains(t, cleaned, "\\")
			for _, segment := range strings.Split(cleaned, "/") {
				require.NotEqual(t, "..", segment)
			}
		}
	})
}

func FuzzStateDecoders(f *testing.F) {
	f.Add([]byte("version: 4\nroot_id: repo\nroot: repo\ngeneration: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\ncandidate_id: 0123456789abcdef0123456789abcdef\nsnapshot_sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nupstream: https://example.test\n"))
	f.Add([]byte(`{"version":4,"root_id":"repo","root":"repo"}`))
	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}
		var marker currentMarker
		_ = decodeYAML(data, &marker)
		var snapshot Snapshot
		if decodeJSON(data, &snapshot) == nil {
			_ = prepareSnapshot(&snapshot)
		}
	})
}
