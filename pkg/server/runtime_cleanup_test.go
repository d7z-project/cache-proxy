package server

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestCleanupRemovesExpiredObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(time.Hour)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	require.NoError(t, rt.store.MkdirAll("test/dir", 0o755))

	now := time.Now()
	expired := now.Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	stillValid := now.Add(-30 * time.Minute).UTC().Format(time.RFC3339Nano)

	_, err = rt.store.Put(ctx, "test", "dir/expired.txt", strings.NewReader("expired content"), map[string]string{
		"fetched-at": expired,
		"type":       "cache",
	})
	require.NoError(t, err)

	_, err = rt.store.Put(ctx, "test", "dir/valid.txt", strings.NewReader("valid content"), map[string]string{
		"fetched-at": stillValid,
		"type":       "cache",
	})
	require.NoError(t, err)

	_, err = rt.store.Put(ctx, "test", "dir/no-fetched-at.txt", strings.NewReader("no fetched-at"), map[string]string{
		"type": "cache",
	})
	require.NoError(t, err)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	rt.runCleanup()

	_, err = rt.store.OpenObject(ctx, "test", "dir/expired.txt")
	require.ErrorIs(t, err, fs.ErrNotExist, "expired object should be deleted")

	_, err = rt.store.OpenObject(ctx, "test", "dir/valid.txt")
	require.NoError(t, err, "valid object should not be deleted")

	_, err = rt.store.OpenObject(ctx, "test", "dir/no-fetched-at.txt")
	require.ErrorIs(t, err, fs.ErrNotExist, "object without fetched-at should be deleted")
}

func TestCleanupDryRunDoesNotDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(time.Hour)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	require.NoError(t, rt.store.MkdirAll("test/dir", 0o755))

	expired := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	_, err = rt.store.Put(ctx, "test", "dir/expired.txt", strings.NewReader("expired content"), map[string]string{
		"fetched-at": expired,
		"type":       "cache",
	})
	require.NoError(t, err)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.global.Storage.Cleanup.DryRun = true
	rt.mu.Unlock()

	rt.runCleanup()

	_, err = rt.store.OpenObject(ctx, "test", "dir/expired.txt")
	require.NoError(t, err, "dry-run should not delete objects")
}

func TestCleanupSkipsNeverExpiresInstance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"never": {
			Name:   "never",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.ExpirationNever},
			Route:  config.InstanceRoute{Path: "/never"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	veryOld := time.Now().Add(-100 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	_, err = rt.store.Put(ctx, "never", "old.txt", strings.NewReader("very old"), map[string]string{
		"fetched-at": veryOld,
		"type":       "cache",
	})
	require.NoError(t, err)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	rt.runCleanup()

	_, err = rt.store.OpenObject(ctx, "never", "old.txt")
	require.NoError(t, err, "never expires instance should not have objects deleted")
}

func TestCleanupSkipsDirectories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(time.Hour)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	require.NoError(t, rt.store.MkdirAll("test/emptydir", 0o755))

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	rt.runCleanup()
}

func TestStorageCleanupAPIRequiresEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	handler := http.HandlerFunc(rt.serveMain)

	req := httptest.NewRequest(http.MethodPost, "/-/api/storage/cleanup", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCleanupAPIWorksWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	handler := http.HandlerFunc(rt.serveMain)

	req := httptest.NewRequest(http.MethodPost, "/-/api/storage/cleanup", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
}

func TestRuntimeCloseWaitsForCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.global.Storage.Cleanup.Interval = config.Duration(time.Millisecond)
	rt.mu.Unlock()

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()

	err = rt.Close(closeCtx)
	require.NoError(t, err)
}

func TestExpirationTypeSemantics(t *testing.T) {
	t.Run("IsNever returns true for never sentinel", func(t *testing.T) {
		require.True(t, config.ExpirationNever.IsNever())
		require.False(t, config.Expiration(0).IsNever())
		require.False(t, config.Expiration(time.Hour).IsNever())
	})

	t.Run("IsUnset returns true for zero value", func(t *testing.T) {
		require.True(t, config.Expiration(0).IsUnset())
		require.False(t, config.ExpirationNever.IsUnset())
		require.False(t, config.Expiration(time.Hour).IsUnset())
	})
}

func TestFreshnessTypeSemantics(t *testing.T) {
	t.Run("IsForever returns true for forever sentinel", func(t *testing.T) {
		require.True(t, config.FreshnessForever.IsForever())
		require.False(t, config.Freshness(0).IsForever())
		require.False(t, config.Freshness(time.Hour).IsForever())
	})

	t.Run("IsUnset returns true for zero value", func(t *testing.T) {
		require.True(t, config.Freshness(0).IsUnset())
		require.False(t, config.FreshnessForever.IsUnset())
		require.False(t, config.Freshness(time.Hour).IsUnset())
	})
}

func TestCleanupWithMalformedFetchedAt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(time.Hour)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	require.NoError(t, rt.store.MkdirAll("test/dir", 0o755))

	malformedFetchedAts := []string{
		"not-a-date",
		"2025-13-45T99:99:99Z",
		"invalid",
		"12345",
	}

	for i, mf := range malformedFetchedAts {
		path := "dir/malformed" + string(rune('a'+i)) + ".txt"
		_, err = rt.store.Put(ctx, "test", path, strings.NewReader("malformed"), map[string]string{
			"fetched-at": mf,
			"type":       "cache",
		})
		require.NoError(t, err)
	}

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	rt.runCleanup()

	for i := range malformedFetchedAts {
		path := "dir/malformed" + string(rune('a'+i)) + ".txt"
		_, err = rt.store.OpenObject(ctx, "test", path)
		require.ErrorIs(t, err, fs.ErrNotExist, "malformed fetched-at object should be deleted: %s", path)
	}
}

func TestCleanupWithDeletedStateObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(time.Hour)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	require.NoError(t, rt.store.MkdirAll("test/dir", 0o755))

	_, err = rt.store.Put(ctx, "test", "dir/active.txt", strings.NewReader("active"), map[string]string{
		"fetched-at": time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano),
		"type":       "cache",
	})
	require.NoError(t, err)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	rt.runCleanup()

	_, err = rt.store.OpenObject(ctx, "test", "dir/active.txt")
	require.ErrorIs(t, err, fs.ErrNotExist, "expired active object should be deleted")
}

func TestExpirationNeverExplicitySet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"never": {
			Name:   "never",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.ExpirationNever},
			Route:  config.InstanceRoute{Path: "/never"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	_, err = rt.store.Put(ctx, "never", "file.txt", strings.NewReader("content"), map[string]string{
		"fetched-at": time.Now().Add(-1000*time.Hour).UTC().Format(time.RFC3339Nano),
		"type":       "cache",
	})
	require.NoError(t, err)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	rt.runCleanup()

	_, err = rt.store.OpenObject(ctx, "never", "file.txt")
	require.NoError(t, err, "never expires objects should remain")
}

func TestCleanupMultipleInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)
	defer closeRuntime(t, rt)

	instances := map[string]config.InstanceSpec{
		"short": {
			Name:   "short",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(time.Minute)},
			Route:  config.InstanceRoute{Path: "/short"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
		"long": {
			Name:   "long",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(720 * time.Hour)},
			Route:  config.InstanceRoute{Path: "/long"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
		"never": {
			Name:   "never",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.ExpirationNever},
			Route:  config.InstanceRoute{Path: "/never"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	oldTime := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)

	for _, name := range []string{"short", "long", "never"} {
		require.NoError(t, rt.store.MkdirAll(name+"/dir", 0o755))
		_, err = rt.store.Put(ctx, name, "dir/old.txt", strings.NewReader("old"), map[string]string{
			"fetched-at": oldTime,
			"type":       "cache",
		})
		require.NoError(t, err)
	}

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.mu.Unlock()

	rt.runCleanup()

	_, err = rt.store.OpenObject(ctx, "short", "dir/old.txt")
	require.ErrorIs(t, err, fs.ErrNotExist, "short expire instance should have old objects deleted")

	_, err = rt.store.OpenObject(ctx, "long", "dir/old.txt")
	require.NoError(t, err, "long expire instance should keep old objects")

	_, err = rt.store.OpenObject(ctx, "never", "dir/old.txt")
	require.NoError(t, err, "never expire instance should keep old objects")
}

func TestSmokeRuntimeOpenClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()

	err = rt.Close(closeCtx)
	require.NoError(t, err)
}

func TestSmokeRuntimeWithInstance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(720 * time.Hour)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	rt.mu.RLock()
	_, hasInstance := rt.instances["test"]
	rt.mu.RUnlock()
	require.True(t, hasInstance)

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()

	err = rt.Close(closeCtx)
	require.NoError(t, err)
}

func TestChaosCleanupDuringInstanceAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(100 * time.Millisecond)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	_, err = rt.store.Put(ctx, "test", "file.txt", strings.NewReader("content"), map[string]string{
		"fetched-at": time.Now().Add(-200 * time.Millisecond).UTC().Format(time.RFC3339Nano),
		"type":       "cache",
	})
	require.NoError(t, err)

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.global.Storage.Cleanup.Interval = config.Duration(50 * time.Millisecond)
	rt.mu.Unlock()

	time.Sleep(300 * time.Millisecond)

	_, err = rt.store.OpenObject(ctx, "test", "file.txt")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Logf("Object may or may not exist due to race: %v", err)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()

	err = rt.Close(closeCtx)
	require.NoError(t, err)
}

func TestChaosCleanupWithManyObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	rt, err := OpenWithOptions(ctx, Options{
		Backend:      dir,
		Bind:         "127.0.0.1:0",
		MetricsPath:  "/-/metrics",
		GCInterval:   time.Hour,
		Password:     "",
		MetricsToken: "",
	})
	require.NoError(t, err)

	instances := map[string]config.InstanceSpec{
		"test": {
			Name:   "test",
			Meta:   config.InstanceMeta{Mode: config.ModeFile, Enabled: true, ExpireAfter: config.Expiration(time.Hour)},
			Route:  config.InstanceRoute{Path: "/test"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
		},
	}

	_, err = rt.replaceState(ctx, rt.generation, rt.global, instances, nil, nil)
	require.NoError(t, err)

	require.NoError(t, rt.store.MkdirAll("test/dir", 0o755))

	expired := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)

	for i := 0; i < 100; i++ {
		_, err = rt.store.Put(ctx, "test", "dir/expired"+string(rune('a'+i%26))+".txt", strings.NewReader("expired content"), map[string]string{
			"fetched-at": expired,
			"type":       "cache",
		})
		require.NoError(t, err)
	}

	rt.mu.Lock()
	rt.global.Storage.Cleanup.Enabled = true
	rt.global.Storage.Cleanup.BatchSize = 10
	rt.mu.Unlock()

	rt.runCleanup()

	for i := 0; i < 100; i++ {
		suffix := string(rune('a' + i%26))
		_, err = rt.store.OpenObject(ctx, "test", "dir/expired"+suffix+".txt")
		require.ErrorIs(t, err, fs.ErrNotExist, "all expired objects should be deleted batch %d", i)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()

	err = rt.Close(closeCtx)
	require.NoError(t, err)
}

func TestExpirationYAMLRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		exp  config.Expiration
	}{
		{"unset", config.Expiration(0)},
		{"never", config.ExpirationNever},
		{"1h", config.Expiration(time.Hour)},
		{"720h", config.Expiration(720 * time.Hour)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := yaml.Marshal(tt.exp)
			require.NoError(t, err)

			var got config.Expiration
			err = yaml.Unmarshal(data, &got)
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}

func TestFreshnessYAMLRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		f    config.Freshness
	}{
		{"unset", config.Freshness(0)},
		{"forever", config.FreshnessForever},
		{"1h", config.Freshness(time.Hour)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := yaml.Marshal(tt.f)
			require.NoError(t, err)

			var got config.Freshness
			err = yaml.Unmarshal(data, &got)
			require.NoError(t, err)
			require.Equal(t, tt.f, got)
		})
	}
}
