package storeio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

// ResponseObject is a stored wire representation. Protocol packages own its
// identity and freshness; storeio only preserves bytes and response metadata.
type ResponseObject struct {
	Reader      *blobfs.ObjectReader
	Header      http.Header
	Status      int
	Origin      string
	ValidatedAt time.Time
	CreatedAt   time.Time
	SHA256      string
	WireSize    int64
	DeleteAt    time.Time
}

type storedResponseMetadata struct {
	Status      int           `json:"status"`
	Origin      string        `json:"origin"`
	ValidatedAt time.Time     `json:"validated_at"`
	CreatedAt   time.Time     `json:"created_at"`
	SHA256      string        `json:"sha256,omitempty"`
	DeleteAt    time.Time     `json:"delete_at"`
	Retention   time.Duration `json:"retention"`
	LogicalKey  string        `json:"logical_key"`
}

type responseTimingKey struct{}

type responseTiming struct {
	Received time.Time
	Age      time.Duration
}

// RecordResponseTiming attaches transport timing without introducing wire headers.
func RecordResponseTiming(response *http.Response, started, received time.Time) {
	request := response.Request
	if request == nil {
		request = &http.Request{}
	}
	timing := responseTiming{Received: received, Age: proxyruntime.ResponseAge(response.Header, started, received)}
	response.Request = request.WithContext(context.WithValue(request.Context(), responseTimingKey{}, timing))
}

// WithResponseTiming carries upstream timing through detached cache publication.
func WithResponseTiming(ctx context.Context, response *http.Response) context.Context {
	if response != nil && response.Request != nil {
		if timing, ok := response.Request.Context().Value(responseTimingKey{}).(responseTiming); ok {
			return context.WithValue(ctx, responseTimingKey{}, timing)
		}
	}
	return ctx
}

// ResponseTimingHeader returns a private header copy with age at receipt.
func ResponseTimingHeader(ctx context.Context, header http.Header) (time.Time, http.Header) {
	received := time.Now().UTC()
	header = header.Clone()
	if header == nil {
		header = make(http.Header)
	}
	if timing, ok := ctx.Value(responseTimingKey{}).(responseTiming); ok {
		received = timing.Received.UTC()
		header.Set("Age", strconv.FormatInt(int64(timing.Age/time.Second), 10))
	}
	return received, header
}

func (o *ResponseObject) ResponseHeader() http.Header {
	header := o.Header.Clone()
	if header == nil {
		header = make(http.Header)
	}
	header.Set("Age", strconv.FormatInt(int64(proxyruntime.ResponseAge(o.Header, o.ValidatedAt, time.Now())/time.Second), 10))
	return header
}

const (
	defaultReferenceRetention = 30 * 24 * time.Hour
	maxResponseMetadataSize   = 64 << 10
)

func OpenResponse(ctx context.Context, store *blobfs.Store, tenant, key string) (*ResponseObject, error) {
	objectPath := responsePath(key)
	reader, err := store.OpenObject(ctx, tenant, objectPath)
	if err != nil {
		return nil, err
	}
	options := reader.Info().Options
	var header http.Header
	if err := json.Unmarshal([]byte(options["header"]), &header); err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("decode stored response header: %w", err)
	}
	metadata, err := decodeResponseMetadata(options["metadata"])
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	if metadata.LogicalKey != key {
		_ = reader.Close()
		return nil, errors.New("stored response logical key mismatch")
	}
	return &ResponseObject{
		Reader:      reader,
		Header:      header,
		Status:      metadata.Status,
		Origin:      metadata.Origin,
		ValidatedAt: metadata.ValidatedAt,
		CreatedAt:   metadata.CreatedAt,
		SHA256:      metadata.SHA256,
		WireSize:    reader.Info().Size,
		DeleteAt:    metadata.DeleteAt,
	}, nil
}

func PutResponse(ctx context.Context, store *blobfs.Store, tenant, key, origin string, status int, header http.Header, sha256Digest string, body io.Reader) error {
	return putResponseWithRetention(ctx, store, tenant, key, origin, status, header, sha256Digest, defaultReferenceRetention, body)
}

func putResponseWithRetention(ctx context.Context, store *blobfs.Store, tenant, key, origin string, status int, header http.Header, sha256Digest string, retention time.Duration, body io.Reader) error {
	if key == "" || origin == "" || status < 100 || status > 599 || retention <= 0 || body == nil {
		return errors.New("response storage parameters are invalid")
	}
	canonicalHeader := make(http.Header, len(header))
	proxyruntime.CopyEndToEndHeaders(canonicalHeader, header)
	validatedAt, canonicalHeader := ResponseTimingHeader(ctx, canonicalHeader)
	if seeker, ok := body.(io.Seeker); ok {
		position, positionErr := seeker.Seek(0, io.SeekCurrent)
		if positionErr == nil {
			size, sizeErr := seeker.Seek(0, io.SeekEnd)
			if _, err := seeker.Seek(position, io.SeekStart); err != nil {
				return fmt.Errorf("restore response body position: %w", err)
			}
			if sizeErr == nil {
				canonicalHeader.Set("Content-Length", strconv.FormatInt(size, 10))
			}
		}
	} else if length, err := strconv.ParseInt(canonicalHeader.Get("Content-Length"), 10, 64); err == nil && length < 0 {
		canonicalHeader.Del("Content-Length")
	}
	encodedHeader, err := json.Marshal(canonicalHeader)
	if err != nil {
		return fmt.Errorf("encode response header: %w", err)
	}
	now := time.Now().UTC()
	metadata, err := json.Marshal(storedResponseMetadata{
		Status:      status,
		Origin:      origin,
		ValidatedAt: validatedAt,
		CreatedAt:   now,
		SHA256:      sha256Digest,
		DeleteAt:    now.Add(retention),
		Retention:   retention,
		LogicalKey:  key,
	})
	if err != nil {
		return fmt.Errorf("encode response metadata: %w", err)
	}
	if len(metadata) > maxResponseMetadataSize {
		return errors.New("response metadata exceeds 64 KiB")
	}
	objectPath := responsePath(key)
	if err := store.MkdirAll(tenant+"/"+path.Dir(objectPath), 0o755); err != nil {
		return fmt.Errorf("prepare response directory: %w", err)
	}
	_, err = store.Put(ctx, tenant, objectPath, body, map[string]string{
		"header":   string(encodedHeader),
		"metadata": string(metadata),
	})
	return err
}

func TouchResponse(ctx context.Context, store *blobfs.Store, tenant, key string, update http.Header) error {
	objectPath := responsePath(key)
	info, err := store.StatObject(ctx, tenant, objectPath)
	if err != nil {
		return err
	}
	metadata, err := decodeResponseMetadata(info.Options["metadata"])
	if err != nil {
		return err
	}
	if metadata.LogicalKey != key {
		return errors.New("stored response logical key mismatch")
	}
	var header http.Header
	if err := json.Unmarshal([]byte(info.Options["header"]), &header); err != nil {
		return fmt.Errorf("decode stored response header: %w", err)
	}
	header = proxyruntime.MergeRevalidationHeader(header, update)
	policy := proxyruntime.ParseCachePolicy(header, time.Now(), 0)
	if policy.NoStore || policy.Private || strings.Contains(header.Get("Vary"), "*") {
		return DeleteResponse(ctx, store, tenant, key)
	}
	metadata.ValidatedAt, header = ResponseTimingHeader(ctx, header)
	metadata.DeleteAt = metadata.ValidatedAt.Add(metadata.Retention)
	encodedHeader, err := json.Marshal(header)
	if err != nil {
		return err
	}
	encodedMetadata, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if len(encodedMetadata) > maxResponseMetadataSize {
		return errors.New("response metadata exceeds 64 KiB")
	}
	options := map[string]string{"header": string(encodedHeader), "metadata": string(encodedMetadata)}
	_, err = store.UpdateMetadata(ctx, tenant, objectPath, options)
	return err
}

func DeleteResponse(ctx context.Context, store *blobfs.Store, tenant, key string) error {
	objectPath := responsePath(key)
	info, err := store.StatObject(ctx, tenant, objectPath)
	if err != nil {
		return err
	}
	metadata, err := decodeResponseMetadata(info.Options["metadata"])
	if err != nil {
		return err
	}
	if metadata.LogicalKey != key {
		return errors.New("stored response logical key mismatch")
	}
	return store.DeleteObject(ctx, tenant, objectPath)
}

// RevalidateResponse holds the verified body while applying 304 metadata. Even
// if persistence fails, the returned representation is valid for this request.
// A no-store update removes its cache reference without disrupting that reader.
func RevalidateResponse(ctx context.Context, store *blobfs.Store, tenant, key string, update http.Header) (*ResponseObject, error) {
	object, err := OpenResponse(ctx, store, tenant, key)
	if err != nil {
		return nil, err
	}
	object.ValidatedAt, update = ResponseTimingHeader(ctx, update)
	object.Header = proxyruntime.MergeRevalidationHeader(object.Header, update)
	return object, TouchResponse(ctx, store, tenant, key, update)
}

func responsePath(logicalKey string) string {
	digest := sha256.Sum256([]byte(logicalKey))
	encoded := hex.EncodeToString(digest[:])
	return path.Join("responses", encoded[:2], encoded[2:4], encoded)
}

func decodeResponseMetadata(raw string) (storedResponseMetadata, error) {
	var metadata storedResponseMetadata
	if len(raw) > maxResponseMetadataSize {
		return metadata, errors.New("stored response metadata exceeds 64 KiB")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return metadata, fmt.Errorf("decode stored response metadata: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return metadata, fmt.Errorf("decode trailing response metadata: %w", err)
		}
		return metadata, errors.New("stored response metadata contains trailing data")
	}
	if metadata.Status < 100 || metadata.Status > 599 || metadata.Origin == "" ||
		metadata.ValidatedAt.IsZero() || metadata.CreatedAt.IsZero() || metadata.DeleteAt.IsZero() ||
		metadata.Retention <= 0 || metadata.LogicalKey == "" {
		return metadata, errors.New("stored response metadata is invalid")
	}
	return metadata, nil
}
