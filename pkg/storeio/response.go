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
	"time"

	"gopkg.d7z.net/blobfs"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

// ResponseObject is a stored wire representation. Protocol packages own its
// identity and freshness; storeio only preserves bytes and response metadata.
type ResponseObject struct {
	Reader   *blobfs.ObjectReader
	Header   http.Header
	Status   int
	Origin   string
	Fetched  time.Time
	SHA256   string
	WireSize int64
	DeleteAt time.Time
}

type storedResponseMetadata struct {
	Status     int           `json:"status"`
	Origin     string        `json:"origin"`
	Fetched    time.Time     `json:"fetched"`
	SHA256     string        `json:"sha256,omitempty"`
	DeleteAt   time.Time     `json:"delete_at"`
	Retention  time.Duration `json:"retention"`
	LogicalKey string        `json:"logical_key"`
}

const defaultReferenceRetention = 30 * 24 * time.Hour

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
	var metadata storedResponseMetadata
	if err := json.Unmarshal([]byte(options["metadata"]), &metadata); err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("decode stored response metadata: %w", err)
	}
	if metadata.Status < 100 || metadata.Status > 599 || metadata.Origin == "" || metadata.Fetched.IsZero() || metadata.DeleteAt.IsZero() || metadata.Retention <= 0 || metadata.LogicalKey != key {
		_ = reader.Close()
		return nil, fmt.Errorf("stored response metadata is invalid")
	}
	return &ResponseObject{
		Reader: reader, Header: header, Status: metadata.Status, Origin: metadata.Origin,
		Fetched: metadata.Fetched, SHA256: metadata.SHA256, WireSize: reader.Info().Size,
		DeleteAt: metadata.DeleteAt,
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
		Status: status, Origin: origin, Fetched: now, SHA256: sha256Digest,
		DeleteAt: now.Add(retention), Retention: retention, LogicalKey: key,
	})
	if err != nil {
		return fmt.Errorf("encode response metadata: %w", err)
	}
	objectPath := responsePath(key)
	if err := store.MkdirAll(tenant+"/"+path.Dir(objectPath), 0o755); err != nil {
		return fmt.Errorf("prepare response directory: %w", err)
	}
	_, err = store.Put(ctx, tenant, objectPath, body, map[string]string{
		"header": string(encodedHeader), "metadata": string(metadata),
	})
	return err
}

func TouchResponse(ctx context.Context, store *blobfs.Store, tenant, key string, update http.Header) error {
	objectPath := responsePath(key)
	info, err := store.StatObject(ctx, tenant, objectPath)
	if err != nil {
		return err
	}
	var header http.Header
	if err := json.Unmarshal([]byte(info.Options["header"]), &header); err != nil {
		return fmt.Errorf("decode stored response header: %w", err)
	}
	for _, name := range []string{"Cache-Control", "Content-Location", "ETag", "Expires", "Last-Modified", "Vary"} {
		if values := update.Values(name); len(values) > 0 {
			header[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
		}
	}
	var metadata storedResponseMetadata
	if err := json.Unmarshal([]byte(info.Options["metadata"]), &metadata); err != nil {
		return fmt.Errorf("decode stored response metadata: %w", err)
	}
	if metadata.LogicalKey != key {
		return errors.New("stored response logical key mismatch")
	}
	metadata.Fetched = time.Now().UTC()
	if !metadata.DeleteAt.IsZero() {
		retention := metadata.Retention
		if retention <= 0 {
			return errors.New("stored response retention is invalid")
		}
		metadata.DeleteAt = metadata.Fetched.Add(retention)
	}
	encodedHeader, err := json.Marshal(header)
	if err != nil {
		return err
	}
	encodedMetadata, err := json.Marshal(metadata)
	if err != nil {
		return err
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
	var metadata storedResponseMetadata
	if err := json.Unmarshal([]byte(info.Options["metadata"]), &metadata); err != nil {
		return fmt.Errorf("decode stored response metadata: %w", err)
	}
	if metadata.LogicalKey != key {
		return errors.New("stored response logical key mismatch")
	}
	return store.DeleteObject(ctx, tenant, objectPath)
}

func responsePath(logicalKey string) string {
	digest := sha256.Sum256([]byte(logicalKey))
	encoded := hex.EncodeToString(digest[:])
	return path.Join("responses", encoded[:2], encoded[2:4], encoded)
}
