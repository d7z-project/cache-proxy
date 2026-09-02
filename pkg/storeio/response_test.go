package storeio

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/blobfs"
)

func TestResponseRoundTripAndTouch(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	require.NoError(t, PutResponse(ctx, store, "maven", "refs/a", "https://repo.test", http.StatusOK,
		http.Header{"Etag": {`"one"`}}, "abc", strings.NewReader("payload")))

	object, err := OpenResponse(ctx, store, "maven", "refs/a")
	require.NoError(t, err)
	firstFetch := object.Fetched
	require.Equal(t, `"one"`, object.Header.Get("ETag"))
	require.Equal(t, "7", object.Header.Get("Content-Length"))
	require.Equal(t, int64(7), object.WireSize)
	require.NoError(t, object.Reader.Close())

	time.Sleep(time.Millisecond)
	require.NoError(t, TouchResponse(ctx, store, "maven", "refs/a", http.Header{"Etag": {`"two"`}}))
	object, err = OpenResponse(ctx, store, "maven", "refs/a")
	require.NoError(t, err)
	require.Equal(t, `"two"`, object.Header.Get("ETag"))
	require.True(t, object.Fetched.After(firstFetch))
	require.NoError(t, object.Reader.Close())
}

func TestResponseTouchPreservesRetentionAndDeleteVerifiesLogicalKey(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	key := "refs/retained"
	require.NoError(t, putResponseRetained(ctx, store, "responses", key, "https://repo.test", http.StatusOK, nil, "", time.Hour, strings.NewReader("body")))
	for range 2 {
		require.NoError(t, TouchResponse(ctx, store, "responses", key, nil))
		object, err := OpenResponse(ctx, store, "responses", key)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now().Add(time.Hour), object.DeleteAt, time.Second)
		require.NoError(t, object.Reader.Close())
	}

	objectPath := responsePath(key)
	info, err := store.StatObject(ctx, "responses", objectPath)
	require.NoError(t, err)
	var metadata map[string]any
	require.NoError(t, json.Unmarshal([]byte(info.Options["metadata"]), &metadata))
	metadata["logical_key"] = "refs/other"
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	info.Options["metadata"] = string(encoded)
	_, err = store.UpdateMetadata(ctx, "responses", objectPath, info.Options)
	require.NoError(t, err)
	require.Error(t, DeleteResponse(ctx, store, "responses", key))
	_, err = store.StatObject(ctx, "responses", objectPath)
	require.NoError(t, err)
}
