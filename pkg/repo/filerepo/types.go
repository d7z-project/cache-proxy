package filerepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
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
	Path    string
	Body    []byte
	Headers map[string]string
}

type MetadataObject struct {
	Path      string `yaml:"path"`
	Identity  string `yaml:"identity,omitempty"`
	Required  bool   `yaml:"required"`
	StorePath string `yaml:"store_path,omitempty"`
}

type RepoObject struct {
	Path        string `yaml:"path"`
	Identity    string `yaml:"identity,omitempty"`
	ContentHash string `yaml:"content_hash,omitempty"`
	Upstream    string `yaml:"upstream"`
}

type LiveSnapshot struct {
	RootKey    string                    `yaml:"root_key"`
	Generation string                    `yaml:"generation"`
	Upstream   string                    `yaml:"upstream"`
	Published  time.Time                 `yaml:"published"`
	Metadata   map[string]MetadataObject `yaml:"metadata"`
	Artifacts  map[string]RepoObject     `yaml:"artifacts"`
	Targets    []MetadataTarget          `yaml:"targets,omitempty"`
}

type SnapshotBuilder func(context.Context, *RefreshSession) (*LiveSnapshot, error)

type RefreshSession struct {
	handler    *IndexedHandler
	rootKey    string
	upstream   string
	generation string
	blobs      map[string]MetadataBlob
	targets    []MetadataTarget
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
		blob, err := s.handler.fetchMetadataObject(ctx, s.rootKey, s.generation, s.upstream, candidate)
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
	var mfe MetadataFetchError
	if errors.As(lastErr, &mfe) {
		lastErr = mfe.Err
	}
	return MetadataBlob{}, MetadataFetchError{Path: target.URL, Err: lastErr}
}

func (s *RefreshSession) Store(ctx context.Context, cleanPath string, body []byte, meta map[string]string) error {
	storePath := s.handler.generationMetadataPath(s.rootKey, s.generation, cleanPath)
	_, err := s.handler.store.Put(ctx, s.handler.name, storePath, bytes.NewReader(body), meta)
	return err
}

func (s *RefreshSession) FetchDerived(ctx context.Context, derivedPath string) (MetadataObject, error) {
	blob, err := s.Fetch(ctx, MetadataTarget{URL: derivedPath})
	if err != nil {
		var mfe MetadataFetchError
		if errors.As(err, &mfe) && (errors.Is(mfe.Err, errMetadataNotFound) || errors.Is(mfe.Err, errMetadataForbidden)) {
			slog.Debug("derived metadata not available", "path", derivedPath, "root", s.rootKey, "upstream", s.upstream)
			return MetadataObject{}, nil
		}
		return MetadataObject{}, err
	}
	return MetadataObject{Path: blob.Path, Required: false}, nil
}

func (s *RefreshSession) Release(target MetadataTarget) {
	for _, key := range append([]string{target.URL}, target.Candidates...) {
		delete(s.blobs, key)
	}
}

func DeduceCompanions(basePath string) []string {
	var companions []string
	for _, s := range []string{".sig", ".asc", ".gpg"} {
		companions = append(companions, basePath+s)
	}
	return companions
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

func metadataStorePath(root, rootKey, generation, cleanPath string) string {
	return path.Join(root, ".roots", pathEscapeKey(rootKey), "generations", generation, "metadata", cleanPath)
}

func pathEscapeKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
