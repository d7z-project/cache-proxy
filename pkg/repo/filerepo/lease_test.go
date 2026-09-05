package filerepo

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
)

func TestSnapshotLeasePinsNearestRootThroughPublicationAndGC(t *testing.T) {
	ctx := context.Background()
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	manager, err := New(Config{
		Instance: "lease", Mode: "test", Tenant: "metadata", Upstream: "https://upstream.example", StateDir: t.TempDir(), WorkDir: t.TempDir(), Store: store,
		Fetch: func(context.Context, string, http.Header) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotModified, Header: make(http.Header), Body: http.NoBody}, nil
		},
		Build:        func(context.Context, *RefreshSession, Anchor) error { return nil },
		KeepPrevious: 1, GracePeriod: time.Nanosecond,
	})
	require.NoError(t, err)
	publish := func(root, body string) {
		require.NoError(t, manager.StageAnchor(ctx, root, root+"/Release", nil, strings.NewReader(body)))
		_, err := manager.Refresh(ctx, 10)
		require.NoError(t, err)
	}
	publish("repo", "outer")
	publish("repo/nested", "old")
	lease := manager.AcquireSnapshots("repo/nested/Packages.gz")
	require.NotNil(t, lease)
	defer lease.Close()
	require.Equal(t, "repo/nested", lease.Snapshots[0].Root)
	publish("repo/nested", "second")
	publish("repo/nested", "third")
	drainGenerationGC(t, manager)
	reader, err := lease.OpenAnchor(ctx, lease.Snapshots[0])
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "old", string(body))
	_, err = lease.OpenAnchor(ctx, manager.Current("repo"))
	require.Error(t, err)
	lease.Close()
	lease.Close()
	require.Empty(t, manager.readers)
	drainGenerationGC(t, manager)
	_, err = store.StatObject(ctx, "metadata", lease.Snapshots[0].byPath["repo/nested/Release"].Key)
	require.Error(t, err)
	require.Nil(t, manager.AcquireSnapshots("unrelated/Packages"))
}
