package storeio

import (
	"context"
	"encoding/json"
	"io"
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
	firstFetch := object.ValidatedAt
	require.Equal(t, `"one"`, object.Header.Get("ETag"))
	require.Equal(t, "7", object.Header.Get("Content-Length"))
	require.Equal(t, int64(7), object.WireSize)
	require.NoError(t, object.Reader.Close())

	time.Sleep(time.Millisecond)
	require.NoError(t, TouchResponse(ctx, store, "maven", "refs/a", http.Header{"Etag": {`"two"`}}))
	object, err = OpenResponse(ctx, store, "maven", "refs/a")
	require.NoError(t, err)
	require.Equal(t, `"two"`, object.Header.Get("ETag"))
	require.True(t, object.ValidatedAt.After(firstFetch))
	require.NoError(t, object.Reader.Close())
}

func TestResponseTimingSurvivesPublicationAndValidation(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	now := time.Now().UTC().Truncate(time.Second)
	response := &http.Response{Header: http.Header{"Age": {"10"}, "Cache-Control": {"max-age=60"}}}
	RecordResponseTiming(response, now.Add(-25*time.Second), now.Add(-20*time.Second))
	ctx := WithResponseTiming(context.Background(), response)
	require.NoError(t, PutResponse(ctx, store, "test", "ref", "https://upstream.test", http.StatusOK, response.Header, "", strings.NewReader("body")))
	object, err := OpenResponse(ctx, store, "test", "ref")
	require.NoError(t, err)
	require.Equal(t, now.Add(-20*time.Second), object.ValidatedAt)
	require.Equal(t, "15", object.Header.Get("Age"))
	require.Contains(t, []string{"35", "36"}, object.ResponseHeader().Get("Age"))
	created := object.CreatedAt
	require.NoError(t, object.Reader.Close())
	response = &http.Response{Header: http.Header{"Cache-Control": {"no-cache"}}}
	RecordResponseTiming(response, now, now)
	require.NoError(t, TouchResponse(WithResponseTiming(ctx, response), store, "test", "ref", response.Header))
	object, err = OpenResponse(ctx, store, "test", "ref")
	require.NoError(t, err)
	defer object.Reader.Close()
	require.Equal(t, created, object.CreatedAt)
	require.Equal(t, "0", object.Header.Get("Age"))
	require.Equal(t, "no-cache", object.Header.Get("Cache-Control"))
}

func TestRestrictedValidationKeepsOnlyActiveReader(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	for _, header := range []http.Header{{"Cache-Control": {"no-store"}}, {"Cache-Control": {"private"}}, {"Vary": {"*"}}} {
		require.NoError(t, PutResponse(ctx, store, "test", "ref", "https://example.test", http.StatusOK, nil, "", strings.NewReader("verified")))
		object, err := RevalidateResponse(ctx, store, "test", "ref", header)
		require.NoError(t, err)
		require.NotNil(t, object)
		_, err = OpenResponse(ctx, store, "test", "ref")
		require.Error(t, err)
		body, err := io.ReadAll(object.Reader)
		require.NoError(t, err)
		require.NoError(t, object.Reader.Close())
		require.Equal(t, "verified", string(body))
	}
}

func TestResponseTouchPreservesRetentionAndDeleteVerifiesLogicalKey(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	key := "refs/retained"
	require.NoError(t, putResponseWithRetention(ctx, store, "responses", key, "https://repo.test", http.StatusOK, nil, "", time.Hour, strings.NewReader("body")))
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

func TestResponseMetadataValidation(t *testing.T) {
	_, err := decodeResponseMetadata(strings.Repeat(" ", 64<<10+1))
	require.Error(t, err)
	for _, tc := range []struct {
		name, field string
		value       any
	}{
		{"unknown field", "unexpected", true},
		{"validation time", "validated_at", time.Time{}},
		{"creation time", "created_at", time.Time{}},
		{"retention", "retention", -1},
		{"identity", "logical_key", "another"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			ctx := context.Background()
			require.NoError(t, PutResponse(ctx, store, "test", "ref", "https://example.test", http.StatusOK, nil, "", strings.NewReader("body")))
			info, err := store.StatObject(ctx, "test", responsePath("ref"))
			require.NoError(t, err)
			var metadata map[string]any
			require.NoError(t, json.Unmarshal([]byte(info.Options["metadata"]), &metadata))
			metadata[tc.field] = tc.value
			encoded, err := json.Marshal(metadata)
			require.NoError(t, err)
			_, err = store.UpdateMetadata(ctx, "test", responsePath("ref"), map[string]string{"metadata": string(encoded)})
			require.NoError(t, err)
			object, err := OpenResponse(ctx, store, "test", "ref")
			if object != nil {
				_ = object.Reader.Close()
			}
			require.Error(t, err)
			require.Error(t, TouchResponse(ctx, store, "test", "ref", nil))
			require.Error(t, DeleteResponse(ctx, store, "test", "ref"))
		})
	}
}

func TestResponseStorageBoundsMetadata(t *testing.T) {
	store, err := blobfs.Open(t.TempDir(), blobfs.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	key := strings.Repeat("x", maxResponseMetadataSize)
	err = PutResponse(ctx, store, "test", key, "https://example.test", http.StatusOK, nil, "", strings.NewReader("body"))
	require.ErrorContains(t, err, "metadata exceeds")
	_, err = store.StatObject(ctx, "test", responsePath(key))
	require.Error(t, err)
}

func TestResponseHeaderIsIndependent(t *testing.T) {
	object := ResponseObject{ValidatedAt: time.Now()}
	header := object.ResponseHeader()
	require.NotEmpty(t, header.Get("Age"))
	require.Nil(t, object.Header)
	object.Header = http.Header{"Cache-Control": {"no-cache"}}
	header = object.ResponseHeader()
	header.Set("Cache-Control", "max-age=60")
	require.Equal(t, "no-cache", object.Header.Get("Cache-Control"))
}

func FuzzResponseMetadata(f *testing.F) {
	f.Add(`{"status":200,"origin":"https://example.test","validated_at":"2026-01-01T00:00:00Z","created_at":"2026-01-01T00:00:00Z","delete_at":"2026-02-01T00:00:00Z","retention":3600000000000,"logical_key":"ref"}`)
	f.Add(`null`)
	f.Add(`{} {}`)
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		metadata, err := decodeResponseMetadata(raw)
		if err != nil {
			return
		}
		require.NotEmpty(t, metadata.LogicalKey)
		require.False(t, metadata.ValidatedAt.IsZero())
		require.False(t, metadata.CreatedAt.IsZero())
		require.Positive(t, metadata.Retention)
		encoded, err := json.Marshal(metadata)
		require.NoError(t, err)
		roundTrip, err := decodeResponseMetadata(string(encoded))
		require.NoError(t, err)
		require.Equal(t, metadata, roundTrip)
	})
}
