package filerepo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

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
	file    *os.File
	temp    string
	Headers map[string]string
}

func (b MetadataBlob) Open() (io.ReadSeeker, error) {
	if b.file == nil {
		return nil, errors.New("metadata blob is closed")
	}
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return b.file, nil
}

func (b MetadataBlob) Close() {
	if b.file != nil {
		_ = b.file.Close()
	}
	if b.temp != "" {
		_ = os.Remove(b.temp)
	}
}

type MetadataObject struct {
	Path      string `yaml:"path"`
	Required  bool   `yaml:"required"`
	StorePath string `yaml:"store_path,omitempty"`
}

type LiveSnapshot struct {
	RootKey       string                    `yaml:"root_key"`
	Generation    string                    `yaml:"generation"`
	Upstream      string                    `yaml:"upstream"`
	Published     time.Time                 `yaml:"published"`
	Metadata      map[string]MetadataObject `yaml:"metadata"`
	ArtifactCount int                       `yaml:"artifact_count"`
	Targets       []MetadataTarget          `yaml:"targets,omitempty"`
}

type SnapshotBuilder func(context.Context, *RefreshSession, *PathIndexBuilder) (*LiveSnapshot, error)
type CleanupIndexBuilder func(context.Context, *LocalSession, *PathIndexBuilder) error

type PathIndexBuilder struct {
	paths []string
}

func (b *PathIndexBuilder) Add(path string) {
	path = strings.Trim(strings.TrimSpace(path), "/")
	if path == "" {
		return
	}
	b.paths = append(b.paths, path)
}

func (b *PathIndexBuilder) AddAuxiliary(basePath string) {
	basePath = strings.Trim(strings.TrimSpace(basePath), "/")
	if basePath == "" {
		return
	}
	for _, suffix := range []string{".sig", ".asc", ".gpg", ".sha256", ".sha512", ".md5", ".md5sum"} {
		b.paths = append(b.paths, basePath+suffix)
	}
}

func (b *PathIndexBuilder) Finalize() []string {
	if len(b.paths) == 0 {
		return nil
	}
	sort.Strings(b.paths)
	n := 1
	for i := 1; i < len(b.paths); i++ {
		if b.paths[i] == b.paths[n-1] {
			continue
		}
		b.paths[n] = b.paths[i]
		n++
	}
	return b.paths[:n]
}

type RefreshSession struct {
	handler    *IndexedHandler
	rootKey    string
	upstream   string
	generation string
	blobs      map[string]*MetadataBlob
	targets    []MetadataTarget
}

func (s *RefreshSession) Targets() []MetadataTarget {
	return append([]MetadataTarget(nil), s.targets...)
}

func (s *RefreshSession) Fetch(ctx context.Context, target MetadataTarget) (MetadataBlob, error) {
	candidates := append([]string{target.URL}, target.Candidates...)
	for _, candidate := range candidates {
		if blob, ok := s.blobs[candidate]; ok {
			return *blob, nil
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
			s.blobs[key] = &blob
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
	seen := map[string]*MetadataBlob{}
	for _, key := range append([]string{target.URL}, target.Candidates...) {
		if blob := s.blobs[key]; blob != nil {
			seen[blob.temp] = blob
		}
		delete(s.blobs, key)
	}
	for _, blob := range seen {
		if blob.file != nil {
			_ = blob.file.Close()
		}
		if blob.temp != "" {
			_ = os.Remove(blob.temp)
		}
	}
}

func (s *RefreshSession) Close() {
	seen := map[string]*MetadataBlob{}
	for key, blob := range s.blobs {
		if blob != nil {
			seen[blob.temp] = blob
		}
		delete(s.blobs, key)
	}
	for _, blob := range seen {
		if blob.file != nil {
			_ = blob.file.Close()
		}
		if blob.temp != "" {
			_ = os.Remove(blob.temp)
		}
	}
}

type LocalSession struct {
	handler  *IndexedHandler
	snapshot *LiveSnapshot
	ctx      context.Context
}

func (s *LocalSession) Targets() []MetadataTarget {
	if s.snapshot == nil {
		return nil
	}
	return append([]MetadataTarget(nil), s.snapshot.Targets...)
}

func (s *LocalSession) Fetch(target MetadataTarget) (MetadataBlob, error) {
	if s.snapshot == nil {
		return MetadataBlob{}, errors.New("snapshot is nil")
	}
	candidates := append([]string{target.URL}, target.Candidates...)
	for _, candidate := range candidates {
		obj, ok := s.snapshot.Metadata[candidate]
		if !ok {
			continue
		}
		ctx := s.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		reader, err := s.handler.store.OpenObject(ctx, s.handler.name, obj.StorePath)
		if err != nil {
			return MetadataBlob{}, err
		}
		tempFile, _, err := utils.TempFileFromReader(reader)
		reader.Close()
		if err != nil {
			return MetadataBlob{}, err
		}
		return MetadataBlob{Path: obj.Path, file: tempFile, temp: tempFile.Name()}, nil
	}
	return MetadataBlob{}, MetadataFetchError{Path: target.URL, Err: errMetadataNotFound}
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
	SubPath() string
	Targets() []MetadataTarget
	Merge(RootSpec) bool
}

type Discoverer interface {
	Discover(cleanPath string) (RootSpec, bool)
}

func metadataStorePath(root, rootKey, generation, cleanPath string) string {
	return path.Join(root, ".roots", pathEscapeKey(rootKey), "generations", generation, "metadata", cleanPath)
}

func pathEscapeKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
