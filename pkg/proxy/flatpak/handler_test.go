package flatpak

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/health"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func TestRefreshUsesOneUpstreamForMetadataGeneration(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "failed", http.StatusServiceUnavailable)
	}))
	defer first.Close()

	var requests []string
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		switch r.URL.Path {
		case "/summary":
			_, _ = w.Write([]byte("summary-data"))
		case "/summary.sig":
			_, _ = w.Write([]byte("signature-data"))
		case "/config":
			_, _ = w.Write([]byte("config-data"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer second.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{first.URL, second.URL})
	require.NoError(t, handler.Refresh(context.Background()))

	current := handler.currentSnapshot()
	require.Equal(t, second.URL, current.Upstream)
	require.NotEmpty(t, current.Generation)
	require.ElementsMatch(t, []string{
		"/summary", "/summary.sig", "/config",
		"/summary", "/summary.sig", "/config",
	}, requests)
	requireStoreObject(t, store, "flatpak-test", "flatpak/metadata/"+current.Generation+"/summary")
	requireStoreObject(t, store, "flatpak-test", "flatpak/metadata/"+current.Generation+"/summary.sig")
	requireStoreObject(t, store, "flatpak-test", "flatpak/metadata/"+current.Generation+"/config")
}

func TestRefreshTaskReportsUnchangedWithoutPublishingGeneration(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/summary":
			_, _ = w.Write([]byte("summary-data"))
		case "/summary.sig":
			_, _ = w.Write([]byte("signature-data"))
		case "/config":
			_, _ = w.Write([]byte("config-data"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	outcome, err := handler.RefreshTask(context.Background())
	require.NoError(t, err)
	require.Equal(t, "updated", outcome.Result)
	require.Equal(t, "published", outcome.ReasonCode)
	current := handler.currentSnapshot()
	require.NotEmpty(t, current.Generation)
	require.NotEmpty(t, current.Fingerprint)

	outcome, err = handler.RefreshTask(context.Background())
	require.NoError(t, err)
	require.Equal(t, "unchanged", outcome.Result)
	require.Equal(t, "same_as_current", outcome.ReasonCode)
	require.Equal(t, current.Generation, handler.currentSnapshot().Generation)
}

func TestSummarySigRequestPassesThroughWithoutCreatingGeneration(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/summary.sig", r.URL.Path)
		_, _ = w.Write([]byte("signature-data"))
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	req := httptest.NewRequest(http.MethodGet, "/summary.sig", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "signature-data", rec.Body.String())
	require.Empty(t, handler.currentSnapshot().Generation)
	_, err := store.StatObject(context.Background(), "flatpak-test", currentMetadataObject)
	require.Error(t, err)
}

func TestSummaryBootstrapDoesNotWaitForRefreshLock(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/summary", r.URL.Path)
		_, _ = io.WriteString(w, "summary-data")
	}))
	defer upstream.Close()

	handler := newTestHandler(t, openTestStore(t), []string{upstream.URL})
	refreshTriggered := make(chan struct{}, 1)
	handler.SetRefreshTrigger(func() { refreshTriggered <- struct{}{} })
	handler.refreshMu.Lock()
	defer handler.refreshMu.Unlock()

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/summary", nil))
		done <- recorder
	}()

	select {
	case recorder := <-done:
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, "summary-data", recorder.Body.String())
	case <-time.After(time.Second):
		require.FailNow(t, "bootstrap summary waited for metadata refresh")
	}
	select {
	case <-refreshTriggered:
	default:
		require.FailNow(t, "bootstrap summary did not request background refresh")
	}
}

func TestCommittedMissingCompanionDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/summary" {
			_, _ = io.WriteString(w, "summary-data")
			return
		}
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	handler := newTestHandler(t, openTestStore(t), []string{upstream.URL})
	require.NoError(t, handler.Refresh(context.Background()))
	before := requests.Load()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/summary.sig", nil))
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Equal(t, before, requests.Load())
}

func TestCorruptCurrentDoesNotRestorePreviousGeneration(t *testing.T) {
	var version atomic.Int32
	version.Store(1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/summary" {
			_, _ = fmt.Fprintf(w, "summary-%d", version.Load())
			return
		}
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	ctx := context.Background()
	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	require.NoError(t, handler.Refresh(ctx))
	previous := handler.currentSnapshot().Generation
	version.Store(2)
	require.NoError(t, handler.Refresh(ctx))
	current := handler.currentSnapshot().Generation
	require.NotEqual(t, previous, current)

	manifestPath := path.Join(metadataRoot, current, "snapshot.yaml")
	_, err := store.Put(ctx, "flatpak-test", manifestPath, strings.NewReader("corrupt: ["), nil)
	require.NoError(t, err)

	restored := newTestHandler(t, store, []string{upstream.URL})
	require.NoError(t, restored.restoreCurrent(ctx))
	require.Empty(t, restored.currentSnapshot().Generation)
	_, err = store.StatObject(ctx, "flatpak-test", currentMetadataObject)
	require.Error(t, err)
	requireStoreObject(t, store, "flatpak-test", path.Join(metadataRoot, previous, "snapshot.yaml"))
}

func TestCleanCurrentTempRemovesStalePublishRef(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{"https://upstream.example"})
	tmpPath := currentMetadataObject + ".tmp.old"
	require.NoError(t, store.MkdirAll(path.Join("flatpak-test", path.Dir(tmpPath)), 0o755))
	_, err := store.Put(ctx, "flatpak-test", tmpPath, strings.NewReader("old"), nil)
	require.NoError(t, err)

	handler.cleanCurrentTemp(ctx)

	_, err = store.StatObject(ctx, "flatpak-test", tmpPath)
	require.Error(t, err)
}

func TestFailedRefreshKeepsCurrentGeneration(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/summary" {
			_, _ = w.Write([]byte("summary-data"))
			return
		}
		http.NotFound(w, r)
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer bad.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{good.URL})
	require.NoError(t, handler.Refresh(context.Background()))
	current := handler.currentSnapshot()
	require.NotEmpty(t, current.Generation)

	handler.upstreams = []string{bad.URL}
	require.Error(t, handler.Refresh(context.Background()))
	require.Equal(t, current.Generation, handler.currentSnapshot().Generation)
}

func TestRefreshUsesConfiguredOrderDespitePassiveFailures(t *testing.T) {
	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		http.Error(w, "failed", http.StatusServiceUnavailable)
	}))
	defer first.Close()

	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		if r.URL.Path == "/summary" {
			_, _ = w.Write([]byte("summary-data"))
			return
		}
		http.NotFound(w, r)
	}))
	defer second.Close()

	store := openTestStore(t)
	handler, _, serviceHealth := newTestHandlerWithHealth(t, store, []string{first.URL, second.URL})
	for range 10 {
		serviceHealth.RecordResult(first.URL, http.StatusInternalServerError, time.Millisecond)
	}

	require.NoError(t, handler.Refresh(context.Background()))
	require.Greater(t, firstHits.Load(), int32(0))
	require.Greater(t, secondHits.Load(), int32(0))
	require.Equal(t, second.URL, handler.currentSnapshot().Upstream)
}

func TestRefreshFailureUpdatesRepositoryHealthStatus(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "failed", http.StatusInternalServerError)
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler, _, _ := newTestHandlerWithHealth(t, store, []string{upstream.URL})

	require.Error(t, handler.Refresh(context.Background()))

	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, "suspect", statuses[0].State)
	require.NotEmpty(t, statuses[0].LastError)
}

func TestRepositoryStatusesReportCurrentMetadataGeneration(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/summary":
			_, _ = w.Write([]byte("summary-data"))
		case "/summary.sig":
			_, _ = w.Write([]byte("signature-data"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	before := handler.RepositoryStatuses()
	require.Len(t, before, 1)
	require.False(t, before[0].HasCurrent)
	require.Equal(t, "pending", before[0].State)
	require.Equal(t, "flatpak", before[0].Layout)

	require.NoError(t, handler.Refresh(context.Background()))
	statuses := handler.RepositoryStatuses()
	require.Len(t, statuses, 1)
	status := statuses[0]
	require.True(t, status.HasCurrent)
	require.NotEmpty(t, status.Generation)
	require.Equal(t, "active", status.State)
	require.Equal(t, upstream.URL, status.Upstream)
	require.Equal(t, []string{"summary"}, status.PrimaryMetadata)
	require.Equal(t, 3, status.MetadataCount)
	require.Equal(t, status.Published, status.LastSuccessAt)
	require.Equal(t, status.Published, status.LastRefreshAt)
}

func TestObjectChecksumMismatchIsNotCached(t *testing.T) {
	const body = "not the expected object"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	req := httptest.NewRequest(http.MethodGet, "/objects/00/00000000000000000000000000000000000000000000000000000000000000.commit", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	require.Never(t, func() bool {
		_, err := store.StatObject(context.Background(), "flatpak-test", "flatpak"+req.URL.Path)
		return err == nil
	}, 200*time.Millisecond, 20*time.Millisecond)
}

func TestConcurrentObjectRequestsUseOneCacheFill(t *testing.T) {
	payload := []byte("shared immutable object")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	objectPath := fmt.Sprintf("/objects/%s/%s.commit", digest[:2], digest[2:])
	var hits atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		_, _ = w.Write(payload)
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	var wg sync.WaitGroup
	responses := make([]*httptest.ResponseRecorder, 2)
	for i := range responses {
		responses[i] = httptest.NewRecorder()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.ServeHTTP(responses[0], httptest.NewRequest(http.MethodGet, objectPath, nil))
	}()
	<-started
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.ServeHTTP(responses[1], httptest.NewRequest(http.MethodGet, objectPath, nil))
	}()
	close(release)
	wg.Wait()

	require.Equal(t, int32(1), hits.Load())
	for _, rec := range responses {
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, payload, rec.Body.Bytes())
	}
}

func TestValidObjectIsCachedOnce(t *testing.T) {
	payload := []byte("valid metadata object")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	objectPath := fmt.Sprintf("/objects/%s/%s.commit", digest[:2], digest[2:])
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		require.Equal(t, objectPath, r.URL.Path)
		_, _ = w.Write(payload)
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	req := httptest.NewRequest(http.MethodGet, objectPath, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, payload, rec.Body.Bytes())
	require.Eventually(t, func() bool {
		_, err := store.StatObject(context.Background(), "flatpak-test", "flatpak"+objectPath)
		return err == nil
	}, time.Second, 20*time.Millisecond)

	req = httptest.NewRequest(http.MethodGet, objectPath, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, payload, rec.Body.Bytes())
	require.Equal(t, int32(1), hits.Load())
}

func TestValidFilezObjectIsCached(t *testing.T) {
	payload := []byte("decompressed file content")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	var compressed strings.Builder
	gzipWriter := gzip.NewWriter(&compressed)
	_, err := gzipWriter.Write(payload)
	require.NoError(t, err)
	require.NoError(t, gzipWriter.Close())

	objectPath := fmt.Sprintf("/objects/%s/%s.filez", digest[:2], digest[2:])
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, compressed.String())
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	req := httptest.NewRequest(http.MethodGet, objectPath, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Eventually(t, func() bool {
		_, err := store.StatObject(context.Background(), "flatpak-test", "flatpak"+objectPath)
		return err == nil
	}, time.Second, 20*time.Millisecond)
}

func TestDeltaCachesAsOpaqueImmutableFile(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("delta"))
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "/deltas/aa/bb/superblock", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "delta", rec.Body.String())
		require.Eventually(t, func() bool {
			_, err := store.StatObject(context.Background(), "flatpak-test", "flatpak/deltas/aa/bb/superblock")
			return err == nil
		}, time.Second, 20*time.Millisecond)
	}
	require.Equal(t, int32(1), hits.Load())
}

func TestDeltaBypassesCacheWhenDisabled(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("delta"))
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	disabled := false
	handler.base = httpcache.NewHandler(
		handler.name,
		httpcache.RuntimeConfig{
			Mode:            config.ModeFlatpak,
			ExpireAfter:     config.DefaultExpireAfter,
			Upstreams:       []string{upstream.URL},
			BusyPolicy:      config.BusyPolicyStale,
			DefaultFreshFor: config.Freshness(defaultDescriptorFreshFor),
			UpstreamGate: httpcache.NewUpstreamGate(httpcache.UpstreamGateConfig{
				MaxActive: 8, MaxActivePerHost: 4,
			}),
			VerifyFunc: handler.verifyCacheObject,
		},
		store,
		resolver{policy: &Policy{CacheDeltas: &disabled}},
		handler.stats,
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/deltas/aa/bb/superblock", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	_, err := store.StatObject(context.Background(), "flatpak-test", "flatpak/deltas/aa/bb/superblock")
	require.Error(t, err)
}

func TestMetadataGCDoesNotDeleteObjectOrDeltaCache(t *testing.T) {
	payload := []byte("object kept outside metadata generations")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	objectPath := "flatpak/objects/" + digest[:2] + "/" + digest[2:] + ".commit"
	deltaPath := "flatpak/deltas/aa/bb/superblock"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("summary-data"))
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	for range 5 {
		require.NoError(t, handler.Refresh(context.Background()))
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, store.MkdirAll("flatpak-test/flatpak/objects/"+digest[:2], 0o755))
	_, err := store.Put(context.Background(), "flatpak-test", objectPath, strings.NewReader(string(payload)), map[string]string{
		"fetched-at": time.Now().UTC().Format(time.RFC3339Nano),
		"mode":       config.ModeFlatpak,
	})
	require.NoError(t, err)
	require.NoError(t, store.MkdirAll("flatpak-test/flatpak/deltas/aa/bb", 0o755))
	_, err = store.Put(context.Background(), "flatpak-test", deltaPath, strings.NewReader("delta"), map[string]string{
		"fetched-at": time.Now().UTC().Format(time.RFC3339Nano),
		"mode":       config.ModeFlatpak,
	})
	require.NoError(t, err)

	require.NoError(t, handler.CleanupMetadata(context.Background()))
	requireStoreObject(t, store, "flatpak-test", objectPath)
	requireStoreObject(t, store, "flatpak-test", deltaPath)
}

func TestCleanupUsesDeltaExpireAfter(t *testing.T) {
	store := openTestStore(t)
	deltaPath := "flatpak/deltas/aa/bb/superblock"
	require.NoError(t, store.MkdirAll("flatpak-test/flatpak/deltas/aa/bb", 0o755))
	_, err := store.Put(context.Background(), "flatpak-test", deltaPath, strings.NewReader("delta"), map[string]string{
		"fetched-at": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano),
		"mode":       config.ModeFlatpak,
	})
	require.NoError(t, err)

	handler := &Handler{
		name:             "flatpak-test",
		store:            store,
		expireAfter:      config.DefaultExpireAfter,
		deltaExpireAfter: config.Expiration(time.Millisecond),
	}
	require.NoError(t, handler.Cleanup(context.Background(), config.CleanupConfig{BatchSize: 10}))
	_, err = store.StatObject(context.Background(), "flatpak-test", deltaPath)
	require.Error(t, err)
}

func TestFlatpakRepoDescriptorRewrite(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("[Flatpak Repo]\nTitle=Test\nUrl=https://dl.example/repo\nGPGKey=abc\n"))
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	req := httptest.NewRequest(http.MethodGet, "/test.flatpakrepo", nil)
	req.Host = "cache.local"
	req.Header.Set("X-Cache-Proxy-Prefix", "/flathub")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "Url=http://cache.local/flathub\n")
	require.Contains(t, rec.Body.String(), "GPGKey=abc\n")
}

func TestFlatpakDescriptorCacheFailurePreservesRewrittenResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "[Flatpak Repo]\nTitle=Test\nUrl=https://dl.example/repo\n")
	}))
	defer upstream.Close()

	store := openTestStore(t)
	_, err := store.Put(context.Background(), "flatpak-test", "flatpak", strings.NewReader("blocks descriptor directory"), nil)
	require.NoError(t, err)
	handler := newTestHandler(t, store, []string{upstream.URL})
	req := httptest.NewRequest(http.MethodGet, "/test.flatpakrepo", nil)
	req.Host = "cache.local"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "BYPASS", recorder.Header().Get("X-Cache"))
	require.Contains(t, recorder.Body.String(), "Url=http://cache.local\n")
}

func TestFlatpakDescriptorRateLimitPreservesUpstreamResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer upstream.Close()

	handler := newTestHandler(t, openTestStore(t), []string{upstream.URL})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/test.flatpakrepo", nil))

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.Equal(t, "60", recorder.Header().Get("Retry-After"))
}

func TestFlatpakBrowserDescriptorVaryIsNotCached(t *testing.T) {
	const browserUserAgent = "Mozilla/5.0 Firefox/127.0"
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		require.Equal(t, browserUserAgent, req.UserAgent())
		w.Header().Set("Vary", "User-Agent")
		_, _ = io.WriteString(w, "[Flatpak Repo]\nTitle=Test\nUrl=https://dl.example/repo\n")
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "/test.flatpakrepo", nil)
		req.Header.Set("User-Agent", browserUserAgent)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "BYPASS", rec.Header().Get("X-Cache"))
		require.Equal(t, "User-Agent", rec.Header().Get("Vary"))
	}
	require.Equal(t, int32(2), requests.Load())
}

func TestFlatpakMetadataRefreshUsesInternalUserAgent(t *testing.T) {
	var userAgents []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		userAgents = append(userAgents, req.UserAgent())
		switch req.URL.Path {
		case "/summary":
			_, _ = io.WriteString(w, "summary-data")
		case "/summary.sig", "/config":
			http.NotFound(w, req)
		default:
			http.NotFound(w, req)
		}
	}))
	defer upstream.Close()

	store := openTestStore(t)
	handler := newTestHandler(t, store, []string{upstream.URL})
	require.NoError(t, handler.Refresh(context.Background()))
	require.NotEmpty(t, userAgents)
	for _, userAgent := range userAgents {
		require.Equal(t, httpcache.DefaultUserAgent, userAgent)
	}
}

func openTestStore(t *testing.T) *blobfs.Store {
	t.Helper()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func newTestHandler(t *testing.T, store *blobfs.Store, upstreams []string) *Handler {
	t.Helper()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	return newTestHandlerWithStatsAndHealth(t, store, upstreams, stats, nil)
}

func newTestHandlerWithHealth(
	t *testing.T,
	store *blobfs.Store,
	upstreams []string,
) (*Handler, *httpcache.Stats, *health.ServiceHealth) {
	t.Helper()
	stats := httpcache.NewStats(prometheus.NewRegistry())
	serviceHealth := health.New("flatpak-test", config.ModeFlatpak, health.DefaultConfig(), upstreams, stats)
	handler := newTestHandlerWithStatsAndHealth(t, store, upstreams, stats, serviceHealth)
	return handler, stats, serviceHealth
}

func newTestHandlerWithStatsAndHealth(
	t *testing.T,
	store *blobfs.Store,
	upstreams []string,
	stats *httpcache.Stats,
	serviceHealth *health.ServiceHealth,
) *Handler {
	t.Helper()
	policy := &Policy{}
	applyDefaults(policy)
	upstreamGate := httpcache.NewUpstreamGate(httpcache.UpstreamGateConfig{
		MaxActive: 8, MaxActivePerHost: 4,
	})
	runtimeCfg := httpcache.RuntimeConfig{
		Mode:            config.ModeFlatpak,
		ExpireAfter:     config.DefaultExpireAfter,
		Upstreams:       upstreams,
		BusyPolicy:      config.BusyPolicyJoin,
		DefaultFreshFor: config.Freshness(defaultDescriptorFreshFor),
		UpstreamGate:    upstreamGate,
	}
	return NewHandler(
		"flatpak-test",
		upstreams,
		nil,
		config.DefaultExpireAfter,
		time.Minute,
		policy,
		store,
		stats,
		serviceHealth,
		upstreamGate,
		runtimeCfg,
	)
}

func requireStoreObject(t *testing.T, store *blobfs.Store, tenant, objectPath string) {
	t.Helper()
	_, err := store.StatObject(context.Background(), tenant, objectPath)
	require.NoError(t, err)
}
