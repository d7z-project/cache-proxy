package filerepo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

var (
	// ErrMetadataClosurePending tells IndexedHandler that discovery has not yet
	// produced a protocol closure that is safe to publish as current.
	ErrMetadataClosurePending      = errors.New("metadata closure is pending discovery")
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

type MetadataObjectState string

const (
	MetadataPresent   MetadataObjectState = "present"
	MetadataNotFound  MetadataObjectState = "not_found"
	MetadataForbidden MetadataObjectState = "forbidden"
)

type MetadataAnchor struct {
	Path   string              `yaml:"path"`
	State  MetadataObjectState `yaml:"state"`
	Size   int64               `yaml:"size,omitempty"`
	Digest string              `yaml:"sha256,omitempty"`
}

type MetadataObject struct {
	Path         string              `yaml:"canonical_path"`
	State        MetadataObjectState `yaml:"state"`
	Required     bool                `yaml:"required"`
	Digest       string              `yaml:"sha256,omitempty"`
	Size         int64               `yaml:"size,omitempty"`
	ChecksumType string              `yaml:"checksum_type,omitempty"`
	Checksum     string              `yaml:"checksum,omitempty"`

	// StorePath and StatusCode are derived runtime fields and are never persisted.
	StorePath  string `yaml:"-"`
	StatusCode int    `yaml:"-"`
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
	Version            int                       `yaml:"version"`
	RootID             string                    `yaml:"root_id"`
	RootPath           string                    `yaml:"root_path"`
	Generation         string                    `yaml:"generation"`
	Upstream           string                    `yaml:"upstream"`
	Published          time.Time                 `yaml:"published_at"`
	AnchorSetDigest    string                    `yaml:"anchor_set_sha256"`
	Anchors            []MetadataAnchor          `yaml:"anchors"`
	Metadata           map[string]MetadataObject `yaml:"objects"`
	ArtifactCount      int                       `yaml:"artifact_count"`
	Targets            []MetadataTarget          `yaml:"targets"`
	CleanupIndexDigest string                    `yaml:"cleanup_index_sha256"`
}

type currentReference struct {
	Version            int    `yaml:"version"`
	RootID             string `yaml:"root_id"`
	Generation         string `yaml:"generation"`
	SnapshotDigest     string `yaml:"snapshot_sha256"`
	CleanupIndexDigest string `yaml:"cleanup_index_sha256"`
}

type SnapshotBuilder func(context.Context, *RefreshSession, *PathIndexBuilder) (*LiveSnapshot, error)

type PathIndexBuilder struct {
	file   *os.File
	writer *bufio.Writer
	err    error
}

func (b *PathIndexBuilder) Add(rawPath string) {
	if b.err != nil {
		return
	}
	cleanPath := strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(rawPath)), "/")
	if cleanPath == "." || cleanPath == "" || len(cleanPath) > 4096 || !httpcache.SafePath(cleanPath) {
		return
	}
	if b.file == nil {
		b.file, b.err = os.CreateTemp("", "cache-proxy-cleanup-index-*")
		if b.err != nil {
			return
		}
		b.writer = bufio.NewWriterSize(b.file, 64<<10)
	}
	_, b.err = b.writer.WriteString(cleanPath + "\n")
}

func (b *PathIndexBuilder) Finalize() []string {
	reader, err := b.rewind()
	if err != nil {
		return nil
	}
	var paths []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		paths = append(paths, scanner.Text())
	}
	if scanner.Err() != nil {
		b.err = scanner.Err()
		return nil
	}
	if len(paths) == 0 {
		return nil
	}
	sort.Strings(paths)
	n := 1
	for i := 1; i < len(paths); i++ {
		if paths[i] == paths[n-1] {
			continue
		}
		paths[n] = paths[i]
		n++
	}
	return paths[:n]
}

func (b *PathIndexBuilder) rewind() (io.Reader, error) {
	if b.err != nil {
		return nil, b.err
	}
	if b.file == nil {
		b.file, b.err = os.CreateTemp("", "cache-proxy-cleanup-index-*")
		if b.err != nil {
			return nil, b.err
		}
	}
	if b.writer != nil {
		if err := b.writer.Flush(); err != nil {
			b.err = err
			return nil, err
		}
	}
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		b.err = err
		return nil, err
	}
	return b.file, nil
}

func (b *PathIndexBuilder) Close() error {
	if b.file == nil {
		return b.err
	}
	name := b.file.Name()
	err := b.file.Close()
	if removeErr := os.Remove(name); err == nil && !errors.Is(removeErr, os.ErrNotExist) {
		err = removeErr
	}
	b.file = nil
	b.writer = nil
	return errors.Join(b.err, err)
}

type RefreshSession struct {
	handler         *IndexedHandler
	rootID          string
	upstream        string
	generation      string
	blobs           map[string]*MetadataBlob
	targets         []MetadataTarget
	maxTransfers    int
	transfers       int
	anchors         map[string]MetadataAnchor
	expectedAnchors map[string]MetadataAnchor
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

func (s *RefreshSession) fetch(ctx context.Context, target MetadataTarget) (MetadataBlob, error) {
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

// FetchAnchor downloads required metadata and binds it to the candidate anchor set.
func (s *RefreshSession) FetchAnchor(ctx context.Context, target MetadataTarget) (MetadataBlob, error) {
	var lastErr error
	for _, candidate := range append([]string{target.URL}, target.Candidates...) {
		blob, err := s.fetch(ctx, MetadataTarget{URL: candidate})
		if err == nil {
			anchor := MetadataAnchor{Path: candidate, State: MetadataPresent, Size: blob.Size, Digest: blob.Digest}
			if !s.acceptAnchor(anchor) {
				return MetadataBlob{}, errMetadataAnchorChanged
			}
			s.anchors[candidate] = anchor
			return blob, nil
		}
		var fetchErr MetadataFetchError
		if !errors.As(err, &fetchErr) {
			return MetadataBlob{}, err
		}
		switch {
		case errors.Is(fetchErr.Err, errMetadataNotFound):
			anchor := MetadataAnchor{Path: candidate, State: MetadataNotFound}
			if !s.acceptAnchor(anchor) {
				return MetadataBlob{}, errMetadataAnchorChanged
			}
			s.anchors[candidate] = anchor
		case errors.Is(fetchErr.Err, errMetadataForbidden):
			anchor := MetadataAnchor{Path: candidate, State: MetadataForbidden}
			if !s.acceptAnchor(anchor) {
				return MetadataBlob{}, errMetadataAnchorChanged
			}
			s.anchors[candidate] = anchor
		default:
			return MetadataBlob{}, err
		}
		lastErr = err
	}
	return MetadataBlob{}, lastErr
}

func (s *RefreshSession) acceptAnchor(anchor MetadataAnchor) bool {
	expected, ok := s.expectedAnchors[anchor.Path]
	return !ok || expected.State == anchor.State && expected.Size == anchor.Size && strings.EqualFold(expected.Digest, anchor.Digest)
}

func (s *RefreshSession) Anchor(cleanPath string) (MetadataAnchor, bool) {
	anchor, ok := s.anchors[cleanPath]
	return anchor, ok
}

// FetchRequired downloads metadata that is required by an already fetched anchor.
func (s *RefreshSession) FetchRequired(ctx context.Context, target MetadataTarget) (MetadataBlob, error) {
	return s.fetch(ctx, target)
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

// MaterializeVerified persists locally derived metadata only after validating
// it against a signed size and SHA256 declaration.
func (s *RefreshSession) MaterializeVerified(
	ctx context.Context,
	objectPath string,
	source io.Reader,
	expectedSize int64,
	expectedSHA256 string,
) (MetadataBlob, error) {
	objectPath = strings.TrimPrefix(path.Clean("/"+objectPath), "/")
	if objectPath == "." || !httpcache.SafePath(objectPath) || expectedSize < 0 || expectedSize > maxMetadataObjectSize ||
		len(expectedSHA256) != sha256.Size*2 {
		return MetadataBlob{}, errors.New("invalid derived metadata declaration")
	}
	if _, err := hex.DecodeString(expectedSHA256); err != nil {
		return MetadataBlob{}, errors.New("invalid derived metadata SHA256")
	}
	if blob, ok := s.blobs[objectPath]; ok {
		return *blob, nil
	}
	if blob, ok := s.handler.openVerifiedStagingObject(ctx, s.rootID, s.generation, objectPath, expectedSize, expectedSHA256); ok {
		s.blobs[objectPath] = &blob
		return blob, nil
	}
	tempFile, err := os.CreateTemp("", "cache-proxy-derived-metadata-*")
	if err != nil {
		return MetadataBlob{}, err
	}
	tempPath := tempFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
		}
	}()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(tempFile, hash), io.LimitReader(source, maxMetadataObjectSize+1))
	if err != nil {
		return MetadataBlob{}, err
	}
	if size != expectedSize {
		return MetadataBlob{}, fmt.Errorf("%s: derived metadata size mismatch: got %d, want %d", objectPath, size, expectedSize)
	}
	actualSHA256 := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(actualSHA256, expectedSHA256) {
		return MetadataBlob{}, fmt.Errorf("%s: derived metadata SHA256 mismatch", objectPath)
	}
	if err := s.handler.putMetadataObject(ctx, s.rootID, s.generation, objectPath, tempFile, size, nil); err != nil {
		return MetadataBlob{}, err
	}
	if err := tempFile.Close(); err != nil {
		return MetadataBlob{}, err
	}
	blob := MetadataBlob{Path: objectPath, temp: tempPath, Size: size, Digest: actualSHA256}
	s.blobs[objectPath] = &blob
	cleanup = false
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
	blob, err := s.FetchRequired(ctx, MetadataTarget{URL: derivedPath})
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

// FetchOptionalAnchor records both present and expected absent companion states.
func (s *RefreshSession) FetchOptionalAnchor(ctx context.Context, cleanPath string) (MetadataObject, error) {
	blob, err := s.FetchRequired(ctx, MetadataTarget{URL: cleanPath})
	if err == nil {
		anchor := MetadataAnchor{Path: cleanPath, State: MetadataPresent, Size: blob.Size, Digest: blob.Digest}
		if !s.acceptAnchor(anchor) {
			return MetadataObject{}, errMetadataAnchorChanged
		}
		s.anchors[cleanPath] = anchor
		return MetadataObject{Path: blob.Path, State: MetadataPresent}, nil
	}
	var fetchErr MetadataFetchError
	if !errors.As(err, &fetchErr) {
		return MetadataObject{}, err
	}
	state := MetadataObjectState("")
	switch {
	case errors.Is(fetchErr.Err, errMetadataNotFound):
		state = MetadataNotFound
	case errors.Is(fetchErr.Err, errMetadataForbidden):
		state = MetadataForbidden
	default:
		return MetadataObject{}, err
	}
	anchor := MetadataAnchor{Path: cleanPath, State: state}
	if !s.acceptAnchor(anchor) {
		return MetadataObject{}, errMetadataAnchorChanged
	}
	s.anchors[cleanPath] = anchor
	return MetadataObject{Path: cleanPath, State: state}, nil
}

func (s *RefreshSession) anchorSet() ([]MetadataAnchor, string, error) {
	anchors := make([]MetadataAnchor, 0, len(s.anchors))
	for _, anchor := range s.anchors {
		anchors = append(anchors, anchor)
	}
	digest, err := metadataAnchorsDigest(anchors)
	if err != nil {
		return nil, "", err
	}
	sort.Slice(anchors, func(i, j int) bool { return anchors[i].Path < anchors[j].Path })
	return anchors, digest, nil
}

func metadataAnchorsDigest(anchors []MetadataAnchor) (string, error) {
	if len(anchors) == 0 {
		return "", errors.New("metadata refresh has no anchors")
	}
	ordered := append([]MetadataAnchor(nil), anchors...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	data, err := yaml.Marshal(ordered)
	if err != nil {
		return "", err
	}
	return digestBytes(data), nil
}

// ConfirmAnchors fetches every anchor again from the same origin and rejects a
// candidate if bytes or optional presence state changed during construction.
func (s *RefreshSession) ConfirmAnchors(ctx context.Context) ([]MetadataAnchor, string, error) {
	initial, initialDigest, err := s.anchorSet()
	if err != nil {
		return nil, "", err
	}
	for cleanPath, expected := range s.expectedAnchors {
		actual, ok := s.anchors[cleanPath]
		if !ok || actual.State != expected.State || actual.Size != expected.Size || !strings.EqualFold(actual.Digest, expected.Digest) {
			return initial, initialDigest, errMetadataAnchorChanged
		}
	}
	for _, expected := range initial {
		if err := s.reserveTransfer(); err != nil {
			return initial, initialDigest, err
		}
		blob, fetchErr := s.handler.fetchMetadataObject(ctx, metadataFetchRequest{
			rootID: s.rootID, generation: s.generation, upstream: s.upstream,
			requestPath: expected.Path, objectPath: expected.Path,
		})
		if blob.temp != "" {
			_ = os.Remove(blob.temp)
		}
		if expected.State == MetadataPresent {
			if fetchErr != nil || blob.Size != expected.Size || !strings.EqualFold(blob.Digest, expected.Digest) {
				return initial, initialDigest, errMetadataAnchorChanged
			}
			continue
		}
		var metadataErr MetadataFetchError
		if !errors.As(fetchErr, &metadataErr) {
			return initial, initialDigest, errMetadataAnchorChanged
		}
		state := MetadataObjectState("")
		switch {
		case errors.Is(metadataErr.Err, errMetadataNotFound):
			state = MetadataNotFound
		case errors.Is(metadataErr.Err, errMetadataForbidden):
			state = MetadataForbidden
		}
		if state != expected.State {
			return initial, initialDigest, errMetadataAnchorChanged
		}
	}
	return initial, initialDigest, nil
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func strictYAML(data []byte, value any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple YAML documents")
		}
		return err
	}
	return nil
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
