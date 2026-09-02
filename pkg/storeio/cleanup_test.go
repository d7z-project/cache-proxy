package storeio

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestResponseCleanupCursorProgressesWithoutDeletions(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, key := range []string{"first", "second", "third"} {
		require.NoError(t, putResponseRetained(context.Background(), store, "responses", key, "https://example.test", 200, nil, "", time.Hour, strings.NewReader(key)))
	}
	cleaner := &responseCleaner{store: store, tenant: "responses", opts: config.CleanupConfig{BatchSize: 1}}
	seen := make(map[string]struct{})
	for range 4 {
		more, err := cleaner.run(context.Background())
		require.NoError(t, err)
		if cleaner.cursor != "" {
			seen[cleaner.cursor] = struct{}{}
		}
		if !more {
			break
		}
	}
	require.Len(t, seen, 3)
	require.Empty(t, cleaner.cursor)
	for _, key := range []string{"first", "second", "third"} {
		response, err := OpenResponse(context.Background(), store, "responses", key)
		require.NoError(t, err)
		require.NoError(t, response.Reader.Close())
	}
}

func TestResponseCleanupDeletesCorruptOwnedMetadata(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	key := "corrupt"
	objectPath := responsePath(key)
	require.NoError(t, store.MkdirAll("responses/"+path.Dir(objectPath), 0o755))
	_, err = store.Put(ctx, "responses", objectPath, strings.NewReader("body"), map[string]string{"metadata": "not-json"})
	require.NoError(t, err)

	more, err := (&responseCleaner{store: store, tenant: "responses", opts: config.CleanupConfig{BatchSize: 1}}).run(ctx)
	require.NoError(t, err)
	require.True(t, more)
	_, err = store.StatObject(ctx, "responses", objectPath)
	require.Error(t, err)
}
