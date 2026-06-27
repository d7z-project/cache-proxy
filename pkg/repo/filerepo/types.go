package filerepo

import (
	"bytes"
	"context"
	"errors"
	"path"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

const DefaultBlockedRetryInterval = time.Hour

var (
	errMetadataNotFound  = errors.New("metadata upstream not found")
	errMetadataTransient = errors.New("metadata upstream transient failure")
	errMetadataForbidden = errors.New("metadata upstream forbidden")
)

func ResolveMetadataRefreshInterval(value config.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value.Duration()
	}
	return fallback
}

type RefreshPolicy struct {
	Interval time.Duration
}

type MetadataFetchError struct {
	Path string
	Err  error
}

func (e MetadataFetchError) Error() string { return e.Path + ": " + e.Err.Error() }
func (e MetadataFetchError) Unwrap() error { return e.Err }

type MetadataTarget struct {
	URL        string
	Candidates []string
	Kind       string
	Repo       string
	Arch       string
}

type MetadataBlob struct {
	Path      string
	Body      []byte
	Headers   map[string]string
	FetchedAt time.Time
}

type LiveSnapshot struct {
	Metadata   map[string]struct{}
	Artifacts  map[string]string
	Auxiliary  map[string]string
	Companions map[string][]string
}

type SnapshotBuilder func(context.Context, *RefreshSession) (*LiveSnapshot, error)

type RefreshSession struct {
	handler *IndexedHandler
	blobs   map[string]MetadataBlob
	targets []MetadataTarget
}

func (s *RefreshSession) Targets() []MetadataTarget {
	return append([]MetadataTarget(nil), s.targets...)
}

func (s *RefreshSession) Fetch(ctx context.Context, target MetadataTarget) (MetadataBlob, error) {
	candidates := append([]string{target.URL}, target.Candidates...)
	for _, candidate := range candidates {
		if blob, ok := s.blobs[candidate]; ok {
			return blob, nil
		}
	}
	var lastErr error
	for _, candidate := range candidates {
		blob, err := s.handler.refreshMetadataObject(ctx, candidate)
		if err != nil {
			lastErr = err
			continue
		}
		for _, key := range candidates {
			s.blobs[key] = blob
		}
		return blob, nil
	}
	if lastErr == nil {
		lastErr = errors.New("metadata upstream fetch failed")
	}
	return MetadataBlob{}, MetadataFetchError{Path: target.URL, Err: lastErr}
}

func (s *RefreshSession) Store(ctx context.Context, cleanPath string, body []byte, meta map[string]string) error {
	storePath := path.Join(s.handler.objectRoot, cleanPath)
	_, err := s.handler.store.Put(ctx, s.handler.name, storePath, bytes.NewReader(body), meta)
	return err
}

type RootSpec interface {
	Key() string
	Targets() []MetadataTarget
	Merge(RootSpec) bool
}

type Discoverer interface {
	Discover(cleanPath string) (RootSpec, bool)
}

type staticRootSpec struct {
	key     string
	targets []MetadataTarget
}

func (s staticRootSpec) Key() string { return s.key }
func (s staticRootSpec) Targets() []MetadataTarget {
	return append([]MetadataTarget(nil), s.targets...)
}
func (s staticRootSpec) Merge(_ RootSpec) bool { return false }
