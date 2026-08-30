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
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

var (
	errMetadataNotFound            = errors.New("metadata upstream not found")
	errMetadataTransient           = errors.New("metadata upstream transient failure")
	errMetadataForbidden           = errors.New("metadata upstream forbidden")
	errMetadataMirrorRetry         = errors.New("metadata upstream allows mirror retry")
	errMetadataRefreshContinuation = errors.New("metadata refresh slice is complete")
	errMetadataAnchorChanged       = errors.New("metadata refresh anchor changed")
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
	temp    string
	Headers map[string]string
	Size    int64
	Digest  string
}

func (b MetadataBlob) Open() (io.ReadSeekCloser, error) {
	if b.temp == "" {
		return nil, errors.New("metadata blob is closed")
	}
	return os.Open(b.temp)
}

type MetadataObject struct {
	Path         string `yaml:"path"`
	Required     bool   `yaml:"required"`
	StatusCode   int    `yaml:"status_code,omitempty"`
	StorePath    string `yaml:"store_path,omitempty"`
	Digest       string `yaml:"digest,omitempty"`
	Size         int64  `yaml:"size,omitempty"`
	ChecksumType string `yaml:"checksum_type,omitempty"`
	Checksum     string `yaml:"checksum,omitempty"`
}

type RepositoryAttribute struct {
	LabelKey string `yaml:"label_key"`
	Value    string `yaml:"value"`
}

type RepositoryRoot struct {
	ID              string                `yaml:"id"`
	Path            string                `yaml:"path"`
	DisplayName     string                `yaml:"display_name"`
	Layout          string                `yaml:"layout,omitempty"`
	PrimaryMetadata []string              `yaml:"primary_metadata,omitempty"`
	Attributes      []RepositoryAttribute `yaml:"attributes,omitempty"`
	Targets         []MetadataTarget      `yaml:"targets,omitempty"`
	Suite           string                `yaml:"suite,omitempty"`
	Components      []string              `yaml:"components,omitempty"`
	Architectures   []string              `yaml:"architectures,omitempty"`
	Source          bool                  `yaml:"source,omitempty"`
	Repo            string                `yaml:"repo,omitempty"`
	Arch            string                `yaml:"arch,omitempty"`
}

const (
	LayoutDebDistribution = "deb_distribution"
	LayoutDebFlat         = "deb_flat"
	LayoutAPK             = "apk"
	LayoutPacman          = "pacman"
	LayoutRPM             = "rpm"
)

func RepositoryID(layout, rootPath string) string {
	rootPath = strings.Trim(strings.TrimSpace(rootPath), "/")
	if layout == "" {
		if rootPath == "" {
			return "/"
		}
		return rootPath
	}
	if rootPath == "" {
		return layout + ":/"
	}
	return layout + ":" + rootPath
}

type LiveSnapshot struct {
	Version       int                       `yaml:"version"`
	RootID        string                    `yaml:"root_id"`
	RootPath      string                    `yaml:"root_path"`
	Generation    string                    `yaml:"generation"`
	Upstream      string                    `yaml:"upstream"`
	Published     time.Time                 `yaml:"published"`
	Metadata      map[string]MetadataObject `yaml:"metadata"`
	ArtifactCount int                       `yaml:"artifact_count"`
	Targets       []MetadataTarget          `yaml:"targets,omitempty"`
}

type SnapshotBuilder func(context.Context, *RefreshSession, *PathIndexBuilder) (*LiveSnapshot, error)

type PathIndexBuilder struct {
	paths []string
}

func (b *PathIndexBuilder) Add(rawPath string) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(rawPath)), "/")
	if cleanPath == "." || cleanPath == "" || !httpcache.SafePath(cleanPath) {
		return
	}
	b.paths = append(b.paths, cleanPath)
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
	handler              *IndexedHandler
	rootID               string
	upstream             string
	generation           string
	blobs                map[string]*MetadataBlob
	targets              []MetadataTarget
	maxTransfers         int
	transfers            int
	anchorPath           string
	anchorDigest         string
	expectedAnchorPath   string
	expectedAnchorDigest string
}

func (s *RefreshSession) Targets() []MetadataTarget {
	return append([]MetadataTarget(nil), s.targets...)
}

// IsMetadataAbsent reports whether err means upstream metadata is missing or forbidden.
func IsMetadataAbsent(err error) bool {
	var fetchErr MetadataFetchError
	if errors.As(err, &fetchErr) {
		err = fetchErr.Err
	}
	return errors.Is(err, errMetadataNotFound) || errors.Is(err, errMetadataForbidden)
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
		if err := s.reserveTransfer(); err != nil {
			return MetadataBlob{}, err
		}
		blob, err := s.handler.fetchMetadataObject(ctx, metadataFetchRequest{
			rootID: s.rootID, generation: s.generation, upstream: s.upstream,
			requestPath: candidate, objectPath: candidate,
		})
		if err != nil {
			lastErr = err
			continue
		}
		for _, key := range candidates {
			s.blobs[key] = &blob
		}
		if s.anchorPath == "" {
			s.anchorPath = blob.Path
			s.anchorDigest = blob.Digest
			if s.expectedAnchorPath != "" && s.expectedAnchorPath != blob.Path ||
				s.expectedAnchorDigest != "" && !strings.EqualFold(s.expectedAnchorDigest, blob.Digest) {
				return MetadataBlob{}, errMetadataAnchorChanged
			}
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

// FetchVerified downloads requestPath but persists it under objectPath only after the
// declared size and SHA256 have been verified. It is used for content-addressed
// repository metadata such as Debian by-hash resources.
func (s *RefreshSession) FetchVerified(
	ctx context.Context,
	requestPath, objectPath string,
	expectedSize int64,
	expectedSHA256 string,
) (MetadataBlob, error) {
	requestPath = strings.TrimPrefix(path.Clean("/"+requestPath), "/")
	objectPath = strings.TrimPrefix(path.Clean("/"+objectPath), "/")
	if requestPath == "." || objectPath == "." || !httpcache.SafePath(requestPath) || !httpcache.SafePath(objectPath) {
		return MetadataBlob{}, errors.New("invalid verified metadata path")
	}
	if expectedSize < 0 || len(expectedSHA256) != sha256.Size*2 {
		return MetadataBlob{}, errors.New("invalid verified metadata declaration")
	}
	if blob, ok := s.blobs[objectPath]; ok {
		return *blob, nil
	}
	if blob, ok := s.handler.openVerifiedStagingObject(ctx, s.rootID, s.generation, objectPath, expectedSize, expectedSHA256); ok {
		s.blobs[objectPath] = &blob
		return blob, nil
	}
	if err := s.reserveTransfer(); err != nil {
		return MetadataBlob{}, err
	}
	blob, err := s.handler.fetchMetadataObject(ctx, metadataFetchRequest{
		rootID: s.rootID, generation: s.generation, upstream: s.upstream,
		requestPath: requestPath, objectPath: objectPath,
		verify: true, expectedSize: expectedSize, expectedSHA256: expectedSHA256,
	})
	if err != nil {
		return MetadataBlob{}, err
	}
	s.blobs[objectPath] = &blob
	return blob, nil
}

func (s *RefreshSession) reserveTransfer() error {
	if s.maxTransfers > 0 && s.transfers >= s.maxTransfers {
		return errMetadataRefreshContinuation
	}
	s.transfers++
	return nil
}

func (s *RefreshSession) FetchDerived(ctx context.Context, derivedPath string) (MetadataObject, error) {
	blob, err := s.Fetch(ctx, MetadataTarget{URL: derivedPath})
	if err != nil {
		var mfe MetadataFetchError
		if errors.As(err, &mfe) && (errors.Is(mfe.Err, errMetadataNotFound) || errors.Is(mfe.Err, errMetadataForbidden)) {
			slog.Debug("derived metadata not available", "path", derivedPath, "root", s.rootID, "upstream", s.upstream)
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
		if blob.temp != "" {
			_ = os.Remove(blob.temp)
		}
	}
}

type DiscoveryRole string

const (
	DiscoveryCreateRoot DiscoveryRole = "create_root"
	DiscoveryUpdateRoot DiscoveryRole = "update_root"
	DiscoveryIgnore     DiscoveryRole = "ignore"
)

type DiscoveryResult struct {
	Class ResourceClass
	Role  DiscoveryRole
	Root  RepositoryRoot
}

type PathInspector interface {
	InspectPath(cleanPath string) DiscoveryResult
}

type SnapshotObjectOpener func(cleanPath string) (io.ReadCloser, error)

// SnapshotValidator verifies protocol references in an already persisted manifest.
type SnapshotValidator interface {
	ValidateSnapshot(context.Context, *LiveSnapshot, SnapshotObjectOpener) error
}

type RootFinalizer interface {
	FinalizeRoot(root RepositoryRoot) RepositoryRoot
}

func metadataStorePath(root, rootKey, generation, cleanPath string) string {
	return path.Join(root, ".roots", pathEscapeKey(rootKey), "generations", generation, "metadata", cleanPath)
}

func pathEscapeKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
