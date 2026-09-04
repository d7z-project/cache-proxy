package filerepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
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
		Instance: "test", Mode: "test", Tenant: "metadata", Upstream: upstream.URL, StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(ctx context.Context, requestPath string, _ http.Header) (*http.Response, error) {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL+"/"+requestPath, nil)
			if err != nil {
				return nil, err
			}
			return http.DefaultClient.Do(request)
		},
		Build: func(ctx context.Context, session *RefreshSession, _ Anchor) error {
			_, err := session.Fetch(ctx, ObjectSpec{
				Path:         "repo/index",
				Aliases:      []string{"repo/by-hash/" + hex.EncodeToString(digest[:])},
				ExpectedSize: int64Pointer(len(metadata)),
				Checksums:    []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(digest[:])}},
			})
			return err
		},
	})
	require.NoError(t, err)

	anchor := bytes.NewReader([]byte("anchor\n"))
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", http.Header{"Content-Type": {"text/plain"}}, anchor))
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
	require.False(t, handled)
	require.Zero(t, status)
	require.Empty(t, missingResponse.Body.String())

	restarted, err := New(Config{
		Instance: "test", Mode: "test", Tenant: "metadata", Upstream: upstream.URL, StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.NotNil(t, restarted.Current("repo"))
}

func TestGenerationManagerMissingCurrentBlobFallsThroughAndQueuesPoll(t *testing.T) {
	metadata := []byte("metadata\n")
	digest := sha256.Sum256(metadata)
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	manager, err := New(Config{
		Instance: "missing-blob", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(metadata)), ContentLength: int64(len(metadata))}, nil
		},
		Build: func(ctx context.Context, session *RefreshSession, _ Anchor) error {
			_, err := session.Fetch(ctx, ObjectSpec{Path: "repo/index", ExpectedSize: int64Pointer(len(metadata)), Checksums: []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(digest[:])}}})
			return err
		},
	})
	require.NoError(t, err)
	require.NoError(t, manager.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
	_, err = manager.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := manager.Current("repo")
	require.NotNil(t, current)
	object := current.byPath["repo/index"]
	require.NoError(t, store.DeleteObject(context.Background(), "metadata", object.Key))

	response := httptest.NewRecorder()
	handled, status, cache := manager.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/index", nil), "repo/index", true)
	require.False(t, handled)
	require.Zero(t, status)
	require.Empty(t, cache)
	require.Empty(t, response.Body.String())
	manager.mu.RLock()
	require.True(t, manager.pollQueued["repo"])
	require.Empty(t, manager.readers)
	manager.mu.RUnlock()
}

func TestGenerationManagerServesExactRetainedClosureAndRestoresIt(t *testing.T) {
	type versionObject struct {
		path             string
		body             []byte
		allowUnavailable bool
		retain           bool
	}
	type version struct {
		anchor  []byte
		objects []versionObject
	}
	versions := []version{
		{anchor: []byte("release-a"), objects: []versionObject{
			{path: "repo/shared", body: []byte("shared-a"), retain: true},
			{path: "repo/historical", body: []byte("historical-a"), retain: true},
			{path: "repo/removed", body: []byte("removed-a"), retain: true},
			{path: "repo/unmarked", body: []byte("unmarked-a")},
		}},
		{anchor: []byte("release-b"), objects: []versionObject{
			{path: "repo/shared", body: []byte("shared-b"), retain: true},
			{path: "repo/removed", allowUnavailable: true},
		}},
	}
	active := 0
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	config := Config{
		Instance: "retained", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		GracePeriod: time.Nanosecond,
		Fetch: func(_ context.Context, requestPath string, _ http.Header) (*http.Response, error) {
			for _, object := range versions[active].objects {
				if object.path != requestPath {
					continue
				}
				if object.body == nil {
					return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody}, nil
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(object.body)), ContentLength: int64(len(object.body))}, nil
			}
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody}, nil
		},
	}
	config.Build = func(ctx context.Context, session *RefreshSession, anchor Anchor) error {
		selected := -1
		for index := range versions {
			if digestString(versions[index].anchor) == anchor.Generation {
				selected = index
				break
			}
		}
		if selected < 0 {
			return errors.New("unknown test generation")
		}
		for _, object := range versions[selected].objects {
			blob, err := session.Fetch(ctx, ObjectSpec{Path: object.path, AllowUnavailable: object.allowUnavailable})
			if err != nil {
				return err
			}
			if object.retain && blob != nil {
				if err := session.RetainObject(object.path); err != nil {
					return err
				}
			}
		}
		return nil
	}
	handler, err := New(config)
	require.NoError(t, err)
	publish := func(index int) *Snapshot {
		active = index
		require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader(versions[index].anchor)))
		_, refreshErr := handler.Refresh(context.Background(), 1)
		require.NoError(t, refreshErr)
		return handler.Current("repo")
	}
	first := publish(0)
	firstAnchorKey := first.byPath[first.Anchor].Key
	second := publish(1)

	for requestPath, expected := range map[string]struct {
		handled bool
		status  int
		body    string
	}{
		"repo/shared":     {handled: true, status: http.StatusOK, body: "shared-b"},
		"repo/historical": {handled: true, status: http.StatusOK, body: "historical-a"},
		"repo/removed":    {handled: true, status: http.StatusOK, body: "removed-a"},
		"repo/unmarked":   {},
	} {
		response := httptest.NewRecorder()
		handled, status, _ := handler.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/"+requestPath, nil), requestPath, true)
		require.Equal(t, expected.handled, handled, requestPath)
		require.Equal(t, expected.status, status, requestPath)
		if expected.body != "" {
			require.Equal(t, expected.body, response.Body.String(), requestPath)
		}
	}

	var marker currentMarker
	err = readYAML(stateDir, currentName("repo"), &marker)
	require.NoError(t, err)
	require.Equal(t, second.CandidateID, marker.CandidateID)
	require.Len(t, marker.Previous, 1)
	require.Equal(t, first.Generation, marker.Previous[0].Generation)
	require.Equal(t, first.CandidateID, marker.Previous[0].CandidateID)
	require.True(t, validSHA256(marker.Previous[0].SnapshotSHA256))

	restarted, err := New(config)
	require.NoError(t, err)
	response := httptest.NewRecorder()
	handled, status, _ := restarted.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/historical", nil), "repo/historical", true)
	require.True(t, handled)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "historical-a", response.Body.String())
	drainGenerationGC(t, restarted)
	response = httptest.NewRecorder()
	_, status, _ = restarted.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/historical", nil), "repo/historical", true)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "historical-a", response.Body.String())

	marker.Previous[0].SnapshotSHA256 = strings.Repeat("0", sha256.Size*2)
	prepared, err := prepareYAML(stateDir, currentName("repo"), marker)
	require.NoError(t, err)
	require.NoError(t, prepared.commit())
	prepared.discard()
	withoutPrevious, err := New(config)
	require.NoError(t, err)
	require.Equal(t, second.CandidateID, withoutPrevious.Current("repo").CandidateID)
	response = httptest.NewRecorder()
	handled, status, _ = withoutPrevious.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/historical", nil), "repo/historical", true)
	require.False(t, handled)
	require.Zero(t, status)
	drainGenerationGC(t, withoutPrevious)
	_, err = store.StatObject(context.Background(), "metadata", firstAnchorKey)
	require.Error(t, err)
}

func TestGenerationManagerRejectsConflictingRetainedObjects(t *testing.T) {
	bodies := [][]byte{[]byte("first"), []byte("second"), nil}
	active := 0
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "retained-conflict", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		KeepPrevious: 2,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(bodies[active])), ContentLength: int64(len(bodies[active]))}, nil
		},
		Build: func(ctx context.Context, session *RefreshSession, _ Anchor) error {
			if bodies[active] == nil {
				return nil
			}
			if _, err := session.Fetch(ctx, ObjectSpec{Path: "repo/versioned"}); err != nil {
				return err
			}
			return session.RetainObject("repo/versioned")
		},
	})
	require.NoError(t, err)
	for index := range bodies {
		active = index
		require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte{byte(index)})))
		_, err = handler.Refresh(context.Background(), 1)
		require.NoError(t, err)
	}
	response := httptest.NewRecorder()
	handled, status, _ := handler.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/versioned", nil), "repo/versioned", true)
	require.False(t, handled)
	require.Zero(t, status)
}

func TestGenerationManagerRequiresAbsoluteHTTPUpstream(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	config := Config{
		Instance: "upstream-validation", Mode: "test", Tenant: "metadata", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	for _, upstream := range []string{"", "relative/path", "ftp://upstream.example/repo", "https:///missing-host"} {
		t.Run(upstream, func(t *testing.T) {
			config.Upstream = upstream
			_, err := New(config)
			require.Error(t, err)
		})
	}
}

func TestGenerationManagerInvalidatesStateFromAnotherUpstream(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	config := Config{
		Instance: "upstream-change", Mode: "test", Tenant: "metadata", Upstream: "https://old.example/repository", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	handler, err := New(config)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "published", "published/Release", http.Header{"ETag": {`"old-current"`}}, bytes.NewReader([]byte("old current"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "pending", "pending/Release", http.Header{"ETag": {`"old-pending"`}}, bytes.NewReader([]byte("old pending"))))

	sameUpstream, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, sameUpstream.Current("published"))
	sameUpstream.mu.RLock()
	_, pendingRestored := sameUpstream.pending["pending"]
	sameUpstream.mu.RUnlock()
	require.True(t, pendingRestored)

	var discoveryHeader http.Header
	config.Upstream = "https://new.example/repository"
	config.Fetch = func(_ context.Context, requestPath string, header http.Header) (*http.Response, error) {
		require.Equal(t, "published/Release", requestPath)
		discoveryHeader = header.Clone()
		body := []byte("new current")
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"ETag": {`"new-current"`}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}, nil
	}
	restarted, err := New(config)
	require.NoError(t, err)
	require.Nil(t, restarted.Current("published"))
	restarted.mu.RLock()
	require.Empty(t, restarted.pending)
	restarted.mu.RUnlock()
	for _, marker := range []string{currentName("published"), pendingName("pending")} {
		_, err := os.Stat(statePath(stateDir, marker))
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	response := httptest.NewRecorder()
	handled, _, _ := restarted.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/published/Release", nil), "published/Release", true)
	require.False(t, handled)
	require.NoError(t, restarted.Discover(context.Background(), "published", "published", "published/Release"))
	require.Empty(t, discoveryHeader.Get("If-None-Match"))
	require.Empty(t, discoveryHeader.Get("If-Modified-Since"))
}

func TestGenerationManagerResumesVerifiedSHA256ObjectsAfterChecksumMismatch(t *testing.T) {
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
				_, _ = w.Write(bytes.Repeat([]byte("x"), len(secondBody)))
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
		Instance: "resume", Mode: "deb", Tenant: "metadata", Upstream: upstream.URL, StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(ctx context.Context, requestPath string, _ http.Header) (*http.Response, error) {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL+"/"+requestPath, nil)
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
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
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

func TestRefreshSessionFallbackAndAliases(t *testing.T) {
	body := []byte("verified metadata")
	digest := sha256.Sum256(body)
	var requests []string
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "fallback", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, requestPath string, _ http.Header) (*http.Response, error) {
			requests = append(requests, requestPath)
			if requestPath == "repo/by-hash/value" {
				return &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header), Body: http.NoBody}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}, nil
		},
		Build: func(ctx context.Context, session *RefreshSession, _ Anchor) error {
			size := int64(len(body))
			_, fetchErr := session.Fetch(ctx, ObjectSpec{
				Path: "repo/index", FetchPath: "repo/by-hash/value", FallbackFetchPath: "repo/index",
				Aliases: []string{"repo/by-hash/value", "repo/by-hash/second"}, ExpectedSize: &size,
				Checksums: []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(digest[:])}},
			})
			return fetchErr
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []string{"repo/by-hash/value", "repo/index"}, requests)
	current := handler.Current("repo")
	require.NotNil(t, current)
	for _, objectPath := range []string{"repo/index", "repo/by-hash/value", "repo/by-hash/second"} {
		object := current.byPath[objectPath]
		require.NotEmpty(t, object.Key)
		require.Equal(t, current.byPath["repo/index"].Key, object.Key)
	}
}

func TestRefreshSessionDoesNotFallbackOnServerError(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests []string
			handler := &GenerationManager{config: Config{Fetch: func(_ context.Context, requestPath string, _ http.Header) (*http.Response, error) {
				requests = append(requests, requestPath)
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: http.NoBody}, nil
			}}}
			session := &RefreshSession{handler: handler, root: "repo", objects: map[string]Object{}}
			_, err := session.Fetch(context.Background(), ObjectSpec{Path: "repo/index", FetchPath: "repo/by-hash/value", FallbackFetchPath: "repo/index"})
			require.ErrorContains(t, err, fmt.Sprintf("returned %d", status))
			require.Equal(t, []string{"repo/by-hash/value"}, requests)
		})
	}
}

func TestRefreshSessionRejectsFetchPathOutsideRepositoryRoot(t *testing.T) {
	var requests atomic.Int32
	handler := &GenerationManager{config: Config{Fetch: func(context.Context, string, http.Header) (*http.Response, error) {
		requests.Add(1)
		return nil, errors.New("unexpected fetch")
	}}}
	session := &RefreshSession{handler: handler, root: "repository", objects: map[string]Object{}}
	_, err := session.Fetch(context.Background(), ObjectSpec{Path: "repository/index", FetchPath: "other/index"})
	require.ErrorContains(t, err, "invalid metadata fetch path")
	require.Zero(t, requests.Load())
	_, err = session.Fetch(context.Background(), ObjectSpec{Path: "repository/index", FallbackFetchPath: "other/index"})
	require.ErrorContains(t, err, "invalid metadata fallback path")
	_, err = session.Fetch(context.Background(), ObjectSpec{Path: "repository/index", Aliases: []string{"other/index"}})
	require.ErrorContains(t, err, "invalid metadata alias")
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
		Instance: "race", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(_ context.Context, _ *RefreshSession, anchor Anchor) error {
			if anchor.Generation == digestString([]byte("first")) {
				once.Do(func() { close(started) })
				<-release
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("first"))))
	handler.mu.RLock()
	firstCandidate := handler.pending["repo"].CandidateID
	handler.mu.RUnlock()
	refreshDone := make(chan error, 1)
	go func() {
		_, refreshErr := handler.Refresh(context.Background(), 1)
		refreshDone <- refreshErr
	}()
	<-started
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("first"))))
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
		Instance: "poll", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, _ string, header http.Header) (*http.Response, error) {
			validators = header.Clone()
			return &http.Response{StatusCode: http.StatusNotModified, Header: http.Header{"Etag": {`"v2"`}}, Body: io.NopCloser(strings.NewReader(""))}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	handler, err := New(config)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", http.Header{"ETag": {`"v1"`}}, bytes.NewReader([]byte("anchor"))))
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

func TestGenerationManagerMissingObjectRebuildsAfterNotModifiedPoll(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	var builds atomic.Int32
	var validators http.Header
	handler, err := New(Config{
		Instance: "missing-rebuild", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, _ string, header http.Header) (*http.Response, error) {
			validators = header.Clone()
			return &http.Response{StatusCode: http.StatusNotModified, Body: io.NopCloser(strings.NewReader(""))}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error {
			builds.Add(1)
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", http.Header{"ETag": {`"v1"`}}, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := handler.Current("repo")
	require.NotNil(t, previous)
	require.Equal(t, int32(1), builds.Load())

	response := httptest.NewRecorder()
	handled, _, _ := handler.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/missing", nil), "repo/missing", true)
	require.False(t, handled)
	more, err := handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, `"v1"`, validators.Get("If-None-Match"))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)

	current := handler.Current("repo")
	require.NotNil(t, current)
	require.Equal(t, previous.Generation, current.Generation)
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	require.Equal(t, int32(2), builds.Load())
}

func TestGenerationManagerUsesSourceLastModifiedForConditionalResponses(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	modified := time.Date(2026, time.August, 24, 1, 12, 10, 0, time.UTC).Format(http.TimeFormat)
	handler, err := New(Config{
		Instance: "conditional", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	header := http.Header{"ETag": {`"anchor"`}, "Last-Modified": {modified}}
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", header, bytes.NewReader([]byte("anchor"))))

	for name, requestHeader := range map[string]http.Header{
		"date": {"If-Modified-Since": {modified}},
		"etag": {"If-None-Match": {`"anchor"`}},
	} {
		t.Run("staged-"+name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
			request.Header = requestHeader
			response := httptest.NewRecorder()
			require.True(t, handler.ServeStagedAnchorFor(response, request, "repo/Release", "repo"))
			require.Equal(t, http.StatusNotModified, response.Code)
			require.Empty(t, response.Body.Bytes())
		})
	}
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	for name, requestHeader := range map[string]http.Header{
		"date": {"If-Modified-Since": {modified}},
		"etag": {"If-None-Match": {`"anchor"`}},
	} {
		t.Run("current-"+name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
			request.Header = requestHeader
			response := httptest.NewRecorder()
			handled, _, _ := handler.ServeCurrent(response, request, "repo/Release", true)
			require.True(t, handled)
			require.Equal(t, http.StatusNotModified, response.Code)
			require.Empty(t, response.Body.Bytes())
		})
	}
}

func TestGenerationManagerServeCurrentDoesNotPersistLastSeen(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "hot-path", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	markerPath := statePath(stateDir, lastSeenName("repo"))
	before, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	handler.mu.Lock()
	handler.lastSeenPersisted["repo"] = time.Now().Add(-2 * time.Hour)
	handler.mu.Unlock()

	type result struct {
		handled bool
		status  int
	}
	response := httptest.NewRecorder()
	done := make(chan result, 1)
	handler.commitMu.Lock()
	go func() {
		handled, status, _ := handler.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/Release", nil), "repo/Release", true)
		done <- result{handled: handled, status: status}
	}()
	var served result
	select {
	case served = <-done:
		handler.commitMu.Unlock()
	case <-time.After(time.Second):
		handler.commitMu.Unlock()
		t.Fatal("current metadata response blocked on repository persistence")
	}
	require.True(t, served.handled)
	require.Equal(t, http.StatusOK, served.status)
	after, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestGenerationManagerUnchangedPollAdvancesFreshnessWithoutCandidate(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	var validators http.Header
	handler, err := New(Config{
		Instance: "unchanged-poll", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, _ string, header http.Header) (*http.Response, error) {
			validators = header.Clone()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": {`"v2"`}}, Body: io.NopCloser(strings.NewReader("anchor")), ContentLength: 6}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", http.Header{"ETag": {`"v1"`}}, bytes.NewReader([]byte("anchor"))))
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
		Instance: "large-anchor", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		AnchorMaxBytes: 17 << 20,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(largeAnchor)), ContentLength: int64(len(largeAnchor))}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("small"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := handler.Current("repo")
	more, err := handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := handler.Current("repo")
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	require.Equal(t, digestString(largeAnchor), current.Generation)
}

func TestGenerationManagerAtomicallyPublishesChangedClosureAndRestoresIt(t *testing.T) {
	anchors := [][]byte{[]byte("release-initial\n"), []byte("release-updated\n")}
	metadata := [][]byte{[]byte("metadata-initial\n"), []byte("metadata-updated\n")}
	digests := [2][sha256.Size]byte{sha256.Sum256(metadata[0]), sha256.Sum256(metadata[1])}
	var revision atomic.Int32
	revision.Store(1)
	updatedFetchStarted := make(chan struct{})
	releaseUpdatedFetch := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseUpdatedFetch) }) })
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	config := Config{
		Instance: "atomic-update", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, requestPath string, headers http.Header) (*http.Response, error) {
			current := int(revision.Load() - 1)
			switch requestPath {
			case "repo/Release":
				require.Equal(t, `"release-initial"`, headers.Get("If-None-Match"))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"ETag": {`"release-updated"`}},
					Body:       io.NopCloser(bytes.NewReader(anchors[current])), ContentLength: int64(len(anchors[current])),
				}, nil
			case "repo/index":
				if current == 1 {
					startOnce.Do(func() { close(updatedFetchStarted) })
					<-releaseUpdatedFetch
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(metadata[current])), ContentLength: int64(len(metadata[current]))}, nil
			default:
				return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody}, nil
			}
		},
		Build: func(ctx context.Context, session *RefreshSession, anchor Anchor) error {
			current := 0
			if anchor.Generation == digestString(anchors[1]) {
				current = 1
			}
			size := int64(len(metadata[current]))
			_, err := session.Fetch(ctx, ObjectSpec{
				Path: "repo/index", ExpectedSize: &size,
				Checksums: []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(digests[current][:])}},
			})
			return err
		},
	}
	manager, err := New(config)
	require.NoError(t, err)
	require.NoError(t, manager.StageAnchor(context.Background(), "repo", "repo/Release", http.Header{"ETag": {`"release-initial"`}}, bytes.NewReader(anchors[0])))
	_, err = manager.Refresh(context.Background(), 1)
	require.NoError(t, err)
	previous := manager.Current("repo")
	require.NotNil(t, previous)

	revision.Store(2)
	refreshDone := make(chan error, 1)
	go func() {
		more, refreshErr := manager.Refresh(context.Background(), 1)
		if refreshErr == nil && more {
			_, refreshErr = manager.Refresh(context.Background(), 1)
		}
		refreshDone <- refreshErr
	}()
	select {
	case <-updatedFetchStarted:
	case <-time.After(time.Second):
		t.Fatal("updated metadata fetch did not start")
	}
	response := httptest.NewRecorder()
	handled, status, cache := manager.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/index", nil), "repo/index", true)
	require.True(t, handled)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", cache)
	require.Equal(t, metadata[0], response.Body.Bytes())
	require.Equal(t, previous.CandidateID, manager.Current("repo").CandidateID)

	releaseOnce.Do(func() { close(releaseUpdatedFetch) })
	select {
	case refreshErr := <-refreshDone:
		require.NoError(t, refreshErr)
	case <-time.After(time.Second):
		t.Fatal("updated metadata refresh did not finish")
	}
	current := manager.Current("repo")
	require.NotEqual(t, previous.CandidateID, current.CandidateID)
	require.Equal(t, digestString(anchors[1]), current.Generation)
	response = httptest.NewRecorder()
	handled, status, cache = manager.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/index", nil), "repo/index", true)
	require.True(t, handled)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", cache)
	require.Equal(t, metadata[1], response.Body.Bytes())

	restarted, err := New(config)
	require.NoError(t, err)
	require.Equal(t, current.CandidateID, restarted.Current("repo").CandidateID)
	response = httptest.NewRecorder()
	handled, status, _ = restarted.ServeCurrent(response, httptest.NewRequest(http.MethodGet, "/repo/index", nil), "repo/index", true)
	require.True(t, handled)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, metadata[1], response.Body.Bytes())
}

func TestGenerationManagerSchedulerPublishesChangedAnchor(t *testing.T) {
	anchors := [][]byte{[]byte("anchor-initial"), []byte("anchor-updated")}
	var revision atomic.Int32
	revision.Store(1)
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	sched, err := scheduler.NewPersistent(filepath.Join(t.TempDir(), "scheduler.json"))
	require.NoError(t, err)
	manager, err := New(Config{
		Instance: "scheduled", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store, Scheduler: sched,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) {
			body := anchors[int(revision.Load()-1)]
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, sched.Stop(context.Background()))
	})
	require.NoError(t, manager.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader(anchors[0])))
	require.Eventually(t, func() bool {
		current := manager.Current("repo")
		return current != nil && current.Generation == digestString(anchors[0])
	}, time.Second, time.Millisecond)

	revision.Store(2)
	request := httptest.NewRequest(http.MethodGet, "/repo/Release", nil)
	request.Header.Set("Cache-Control", "no-cache")
	response := httptest.NewRecorder()
	handled, status, cache := manager.ServeCurrent(response, request, "repo/Release", true)
	require.True(t, handled)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", cache)
	require.Equal(t, anchors[0], response.Body.Bytes())
	require.Eventually(t, func() bool {
		current := manager.Current("repo")
		return current != nil && current.Generation == digestString(anchors[1])
	}, time.Second, time.Millisecond)
}

func TestLastSeenPersistenceFlushesConcurrentUpdates(t *testing.T) {
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
	go func() { done <- handler.flushLastSeen(context.Background()) }()
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
	err := readJSON(handler.config.StateDir, lastSeenName(rootID), maxRepositoryMarkerSize, &marker)
	require.NoError(t, err)
	require.Equal(t, rootID, marker.RootID)
	require.False(t, marker.SeenAt.IsZero())
}

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

func TestGenerationManagerIsolatesInvalidCurrentState(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	config := Config{
		Instance: "state", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	handler, err := New(config)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
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
		Instance: "ancillary", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	}
	handler, err := New(config)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
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

func TestGenerationManagerRotatesPastFailedRoot(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "rotation", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(_ context.Context, _ *RefreshSession, anchor Anchor) error {
			if anchor.RootID == "a" {
				return &retryableRefreshError{err: io.ErrUnexpectedEOF}
			}
			return nil
		},
	})
	require.NoError(t, err)
	for _, rootID := range []string{"a", "b"} {
		require.NoError(t, handler.StageAnchorID(context.Background(), rootID, rootID, rootID+"/Release", nil, bytes.NewReader([]byte(rootID))))
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
	require.NoError(t, handler.StageAnchorID(context.Background(), "a", "a", "a/Release", nil, bytes.NewReader([]byte("replacement"))))
	handler.mu.RLock()
	_, delayed := handler.retryWindows["a"]
	handler.mu.RUnlock()
	require.False(t, delayed)
}

func TestGenerationManagerReplacesFailedPendingCandidateWhenAnchorChanges(t *testing.T) {
	anchors := [][]byte{[]byte("anchor-one"), []byte("anchor-two")}
	metadata := [][]byte{[]byte("metadata-one"), []byte("metadata-two")}
	digests := [2][sha256.Size]byte{sha256.Sum256(metadata[0]), sha256.Sum256(metadata[1])}
	var revision atomic.Int32
	var metadataRequests atomic.Int32
	revision.Store(1)
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "pending-update", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, requestPath string, header http.Header) (*http.Response, error) {
			current := int(revision.Load() - 1)
			switch requestPath {
			case "repo/Release":
				require.Equal(t, `"one"`, header.Get("If-None-Match"))
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"ETag": {`"two"`}}, Body: io.NopCloser(bytes.NewReader(anchors[current])), ContentLength: int64(len(anchors[current]))}, nil
			case "repo/index":
				metadataRequests.Add(1)
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(metadata[current])), ContentLength: int64(len(metadata[current]))}, nil
			default:
				return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: http.NoBody}, nil
			}
		},
		Build: func(ctx context.Context, session *RefreshSession, anchor Anchor) error {
			current := 0
			if anchor.Generation == digestString(anchors[1]) {
				current = 1
			}
			size := int64(len(metadata[current]))
			if _, fetchErr := session.Fetch(ctx, ObjectSpec{Path: "repo/index", ExpectedSize: &size, Checksums: []Checksum{{Algorithm: "sha256", Digest: hex.EncodeToString(digests[current][:])}}}); fetchErr != nil {
				return fetchErr
			}
			if current == 0 {
				return Retryable(io.ErrUnexpectedEOF)
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", http.Header{"ETag": {`"one"`}}, bytes.NewReader(anchors[0])))
	_, err = handler.Refresh(context.Background(), 1)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	handler.mu.Lock()
	firstCandidate := handler.pending["repo"].CandidateID
	retry := handler.retryWindows["repo"]
	retry.notBefore = time.Now().Add(-time.Second)
	handler.retryWindows["repo"] = retry
	handler.mu.Unlock()

	revision.Store(2)
	more, err := handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, more)
	handler.mu.RLock()
	secondCandidate := handler.pending["repo"].CandidateID
	handler.mu.RUnlock()
	require.NotEqual(t, firstCandidate, secondCandidate)
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	current := handler.Current("repo")
	require.NotNil(t, current)
	require.Equal(t, digestString(anchors[1]), current.Generation)
	reader, err := store.OpenObject(context.Background(), "metadata", current.byPath["repo/index"].Key)
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, metadata[1], content)
	require.Equal(t, int32(2), metadataRequests.Load())
}

func TestGenerationManagerBlockedPendingDoesNotPollOtherRoots(t *testing.T) {
	var currentPolls atomic.Int32
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "isolated-retry", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, requestPath string, _ http.Header) (*http.Response, error) {
			if requestPath == "b/Release" {
				currentPolls.Add(1)
			}
			return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
		},
		Build: func(_ context.Context, _ *RefreshSession, anchor Anchor) error {
			if anchor.RootID == "a" {
				return Retryable(io.ErrUnexpectedEOF)
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchorID(context.Background(), "b", "b", "b/Release", nil, bytes.NewReader([]byte("b"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchorID(context.Background(), "a", "a", "a/Release", nil, bytes.NewReader([]byte("a"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	more, err := handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Zero(t, currentPolls.Load())
	handler.mu.Lock()
	retry := handler.retryWindows["a"]
	retry.notBefore = time.Now().Add(-time.Second)
	handler.retryWindows["a"] = retry
	handler.mu.Unlock()
	more, err = handler.Refresh(context.Background(), 1)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.True(t, more)
	more, err = handler.Refresh(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, int32(1), currentPolls.Load())
}

func TestGenerationManagerBacksOffAndRevalidatesSamePendingCandidate(t *testing.T) {
	var builds, anchorPolls atomic.Int32
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "retry", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, requestPath string, header http.Header) (*http.Response, error) {
			require.Equal(t, "repo/Release", requestPath)
			require.Equal(t, `"anchor"`, header.Get("If-None-Match"))
			anchorPolls.Add(1)
			return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error {
			builds.Add(1)
			return Retryable(io.ErrUnexpectedEOF)
		},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", http.Header{"ETag": {`"anchor"`}}, bytes.NewReader([]byte("anchor"))))
	_, err = handler.Refresh(context.Background(), 1)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	for range 5 {
		more, refreshErr := handler.Refresh(context.Background(), 1)
		require.NoError(t, refreshErr)
		require.False(t, more)
	}
	require.Equal(t, int32(1), builds.Load())
	require.Zero(t, anchorPolls.Load())

	handler.mu.Lock()
	retry := handler.retryWindows["repo"]
	retry.notBefore = time.Now().Add(-time.Second)
	handler.retryWindows["repo"] = retry
	handler.mu.Unlock()
	_, err = handler.Refresh(context.Background(), 1)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, int32(2), builds.Load())
	require.Equal(t, int32(1), anchorPolls.Load())
	handler.mu.RLock()
	retry = handler.retryWindows["repo"]
	handler.mu.RUnlock()
	require.Equal(t, 2, retry.failures)
	require.Greater(t, time.Until(retry.notBefore), 90*time.Second)
	require.LessOrEqual(t, time.Until(retry.notBefore), 2*time.Minute+15*time.Second)
}

func TestGenerationManagerPollCycleVisitsEveryCurrentRoot(t *testing.T) {
	var mu sync.Mutex
	polled := make(map[string]int)
	var pollOrder []string
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "poll-cycle", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(_ context.Context, requestPath string, _ http.Header) (*http.Response, error) {
			mu.Lock()
			polled[requestPath]++
			pollOrder = append(pollOrder, requestPath)
			mu.Unlock()
			return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
		},
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	for _, rootID := range []string{"a", "b", "c"} {
		require.NoError(t, handler.StageAnchorID(context.Background(), rootID, rootID, rootID+"/Release", nil, bytes.NewReader([]byte(rootID))))
	}
	_, err = handler.Refresh(context.Background(), 3)
	require.NoError(t, err)
	for index := 0; index < 3; index++ {
		more, refreshErr := handler.Refresh(context.Background(), 1)
		require.NoError(t, refreshErr)
		require.Equal(t, index < 2, more)
		if index == 0 {
			handler.requestCurrentPoll("c", false)
		}
	}
	require.Equal(t, map[string]int{"a/Release": 1, "b/Release": 1, "c/Release": 1}, polled)
	require.Equal(t, []string{"a/Release", "c/Release", "b/Release"}, pollOrder)
}

func TestGenerationManagerRejectsOversizedAnchorBeforeStaging(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	handler, err := New(Config{
		Instance: "anchor-limit", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		AnchorMaxBytes: 4,
		Fetch:          func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build:          func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	require.ErrorIs(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("large"))), storeio.ErrObjectTooLarge)
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
		Instance: "pending-limit", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	header := http.Header{"X-Oversized": {strings.Repeat("x", maxRepositoryMarkerSize)}}
	err = handler.StageAnchor(context.Background(), "repo", "repo/Release", header, bytes.NewReader([]byte("anchor")))
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
			Instance: "fault", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
			Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
			Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
		})
		require.NoError(t, err)
		return handler, stateDir
	}

	t.Run("pending marker", func(t *testing.T) {
		handler, stateDir := newManager(t)
		destination := statePath(stateDir, pendingName("repo"))
		require.NoError(t, os.MkdirAll(destination, 0o755))
		err := handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor")))
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
			require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("anchor"))))
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

func TestGenerationManagerPublicationFailureKeepsCommittedSnapshots(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	stateDir := t.TempDir()
	handler, err := New(Config{
		Instance: "publication-state", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: stateDir, WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	for _, body := range []string{"first", "second"} {
		require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte(body))))
		_, err = handler.Refresh(context.Background(), 1)
		require.NoError(t, err)
	}
	committed := handler.Current("repo")
	handler.mu.RLock()
	require.Len(t, handler.retained["repo"], 1)
	retainedCandidate := handler.retained["repo"][0].snapshot.CandidateID
	handler.mu.RUnlock()

	require.NoError(t, handler.StageAnchor(context.Background(), "repo", "repo/Release", nil, bytes.NewReader([]byte("third"))))
	markerPath := statePath(stateDir, currentName("repo"))
	require.NoError(t, os.Remove(markerPath))
	require.NoError(t, os.Mkdir(markerPath, 0o755))
	_, err = handler.Refresh(context.Background(), 1)
	require.Error(t, err)
	require.Equal(t, committed.CandidateID, handler.Current("repo").CandidateID)
	handler.mu.RLock()
	require.Len(t, handler.retained["repo"], 1)
	require.Equal(t, retainedCandidate, handler.retained["repo"][0].snapshot.CandidateID)
	handler.mu.RUnlock()
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
		Instance: "discovery", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) { return nil, io.EOF },
		Build: func(context.Context, *RefreshSession, Anchor) error { return nil },
	})
	require.NoError(t, err)
	lifecycle := storeio.NewLifecycle()
	for range 20 {
		handler.ScheduleDiscovery(lifecycle, "repo", "repo", "repo/Release")
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
	f.Add([]byte("root_id: repo\nroot: repo\ngeneration: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\ncandidate_id: 0123456789abcdef0123456789abcdef\nsnapshot_sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nupstream: https://example.test\n"))
	f.Add([]byte("root_id: repo\nroot: repo\ngeneration: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\ncandidate_id: 0123456789abcdef0123456789abcdef\nsnapshot_sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nupstream: https://example.test\nprevious:\n  - generation: abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\n    candidate_id: abcdef0123456789abcdef0123456789\n    snapshot_sha256: abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789\n"))
	f.Add([]byte(`{"root_id":"repo","root":"repo"}`))
	generation := strings.Repeat("a", 64)
	candidateID := strings.Repeat("b", 32)
	seed, err := json.Marshal(Snapshot{
		RootID: "repo", Root: "repo", Anchor: "repo/Release", Generation: generation, CandidateID: candidateID,
		Upstream: "https://example.test", PublishedAt: time.Unix(1, 0).UTC(),
		Objects: []Object{{Path: "repo/Release", Key: candidatePrefix("repo", generation, candidateID) + "/anchor", Size: 0, SHA256: generation}},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
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
