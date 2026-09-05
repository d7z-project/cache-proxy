package filerepo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
)

func TestGenerationManagerGCProtectsActiveReader(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "gc", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:        func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:        func(context.Context, *RefreshSession, Anchor) error { return nil },
		KeepPrevious: 1, GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	publish := func(body string) *Snapshot {
		require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte(body))))
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
		Instance: "gc-marker", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:       func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:       func(context.Context, *RefreshSession, Anchor) error { return nil },
		GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
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

func TestGenerationManagerRetiresInactiveRoot(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "inactive", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch:         func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:         func(context.Context, *RefreshSession, Anchor) error { return nil },
		InactiveAfter: time.Nanosecond, GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	snapshot := handler.Current("repo")
	key := snapshot.byPath["repo/Release"].Key
	handler.requestCurrentPoll("repo", true)
	handler.mu.Lock()
	handler.lastSeen["repo"] = time.Now().Add(-time.Hour)
	handler.mu.Unlock()
	drainGenerationGC(t, handler)
	require.Nil(t, handler.Current("repo"))
	require.Empty(t, handler.forceRebuildQueued)
	require.Empty(t, handler.pollQueued)
	require.Empty(t, handler.pollQueue)
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
		Instance: "invalid-candidate", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch:       func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
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
		Instance: "pending", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil }, InactiveAfter: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
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
		Instance: "retire-retry", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil }, InactiveAfter: time.Nanosecond,
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
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
		RootID: "orphan",
		SeenAt: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	require.NoError(t, writeBytes(stateDir, lastSeenName("orphan"), data))
	handler, err := New(Config{
		Instance: "last-seen", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch:         func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
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

func TestGenerationManagerGCBoundsInactiveRootInspection(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "gc-batch", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:         func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:         func(context.Context, *RefreshSession, Anchor) error { return nil },
		InactiveAfter: time.Nanosecond,
	})
	require.NoError(t, err)
	for _, rootID := range []string{"a", "b", "c"} {
		require.NoError(t, handler.StageAnchorID(context.Background(), rootID, rootID, rootID+"/Release", nil, bytes.NewReader([]byte(rootID))))
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

func TestGenerationManagerGCProcessesHistoryInSmallBatches(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	cfg := Config{
		Instance: "history", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:        func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:        func(context.Context, *RefreshSession, Anchor) error { return nil },
		KeepPrevious: 1, GracePeriod: time.Nanosecond,
	}
	h, err := New(cfg)
	require.NoError(t, err)
	var keys []string
	publish := func(body string) {
		require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader(body)))
		_, err := h.Refresh(context.Background(), 1)
		require.NoError(t, err)
		keys = append(keys, h.Current("repo").byPath["repo/Release"].Key)
	}
	for i := range 12 {
		publish(fmt.Sprint(i))
	}
	for range 2 {
		_, err = h.GC(context.Background(), 1)
		require.NoError(t, err)
	}
	require.Equal(t, generationGCSnapshots, h.gcPhase)
	publish("updated during GC")
	for batch := 0; ; batch++ {
		require.Less(t, batch, 100)
		before := 0
		for _, key := range keys {
			if _, err := store.StatObject(context.Background(), "metadata", key); err == nil {
				before++
			}
		}
		more, err := h.GC(context.Background(), 1)
		require.NoError(t, err)
		require.LessOrEqual(t, len(h.gcRetained), 2)
		after := 0
		for _, key := range keys {
			if _, err := store.StatObject(context.Background(), "metadata", key); err == nil {
				after++
			}
		}
		require.LessOrEqual(t, before-after, 1)
		if !more {
			break
		}
	}
	for _, key := range keys[len(keys)-2:] {
		_, err := store.StatObject(context.Background(), "metadata", key)
		require.NoError(t, err)
	}
	// Restart midway through a new cycle; only the committed marker restores state.
	_, err = h.GC(context.Background(), 1)
	require.NoError(t, err)
	h, err = New(cfg)
	require.NoError(t, err)
	drainGenerationGC(t, h)
	for _, key := range keys[:len(keys)-2] {
		_, err := store.StatObject(context.Background(), "metadata", key)
		require.Error(t, err)
	}
	for _, key := range keys[len(keys)-2:] {
		_, err := store.StatObject(context.Background(), "metadata", key)
		require.NoError(t, err)
	}
	require.NoError(t, h.StageAnchor(context.Background(), "repo", "repo/Release", nil, strings.NewReader("pending")))
	pendingKey := h.pending["repo"].Key
	drainGenerationGC(t, h)
	_, err = store.StatObject(context.Background(), "metadata", pendingKey)
	require.NoError(t, err)
}

func TestGenerationManagerGCRetriesInterruptedCandidateDeletion(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "gc-cancel", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch:        func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:        func(context.Context, *RefreshSession, Anchor) error { return nil },
		KeepPrevious: 1, GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	var oldestKey string
	for _, body := range []string{"first", "second", "third"} {
		require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte(body))))
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
	require.Equal(t, generationGCSnapshots, handler.gcPhase)

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
