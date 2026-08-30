package httpcache

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func TestCleanupContinuesFromInspectionCursor(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	fresh := time.Now().UTC().Format(time.RFC3339Nano)
	for i := range 20 {
		_, err := store.Put(ctx, "test", fmt.Sprintf("a-%02d", i), strings.NewReader("fresh"), map[string]string{"fetched-at": fresh})
		require.NoError(t, err)
	}
	_, err = store.Put(ctx, "test", "z-expired", strings.NewReader("expired"), map[string]string{
		"fetched-at": time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano),
	})
	require.NoError(t, err)

	handler := NewHandler("test", RuntimeConfig{
		Mode:        "test",
		ExpireAfter: config.Expiration(time.Hour),
	}, store, literalResolver{}, NewStats(prometheus.NewRegistry()), nil)
	opts := config.CleanupConfig{BatchSize: 1}
	require.NoError(t, handler.Cleanup(ctx, opts))
	_, err = store.StatObject(ctx, "test", "z-expired")
	require.NoError(t, err)

	require.NoError(t, handler.Cleanup(ctx, opts))
	require.NoError(t, handler.Cleanup(ctx, opts))
	_, err = store.StatObject(ctx, "test", "z-expired")
	require.Error(t, err)
}
