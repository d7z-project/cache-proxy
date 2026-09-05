package filerepo

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type RefreshSession struct {
	ValidUntil  time.Time
	handler     *GenerationManager
	rootID      string
	root        string
	generation  string
	candidateID string
	objects     map[string]Object
}

func (s *RefreshSession) Fetch(ctx context.Context, spec ObjectSpec) (*Blob, error) {
	cleaned, err := CleanPath(spec.Path)
	if err != nil || !containsPath(s.root, cleaned) {
		return nil, fmt.Errorf("invalid metadata path %q", spec.Path)
	}
	spec.Path = cleaned
	fetchPath := spec.FetchPath
	if fetchPath == "" {
		fetchPath = cleaned
	}
	fetchPath, err = CleanPath(fetchPath)
	if err != nil || !containsPath(s.root, fetchPath) {
		return nil, fmt.Errorf("invalid metadata fetch path %q", spec.FetchPath)
	}
	fallbackFetchPath := spec.FallbackFetchPath
	if fallbackFetchPath != "" {
		fallbackFetchPath, err = CleanPath(fallbackFetchPath)
		if err != nil || !containsPath(s.root, fallbackFetchPath) {
			return nil, fmt.Errorf("invalid metadata fallback path %q", spec.FallbackFetchPath)
		}
	}
	aliases := make([]string, 0, len(spec.Aliases))
	registeredPaths := map[string]struct{}{spec.Path: {}}
	for _, alias := range spec.Aliases {
		cleanedAlias, aliasErr := CleanPath(alias)
		if aliasErr != nil || !containsPath(s.root, cleanedAlias) {
			return nil, fmt.Errorf("invalid metadata alias %q", alias)
		}
		if _, exists := registeredPaths[cleanedAlias]; !exists {
			registeredPaths[cleanedAlias] = struct{}{}
			aliases = append(aliases, cleanedAlias)
		}
	}
	recordObject := func(object Object) {
		object.Path = spec.Path
		s.objects[spec.Path] = object
		for _, alias := range aliases {
			object.Path = alias
			s.objects[alias] = object
		}
	}
	maxBytes := spec.MaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxObject
	}
	if spec.ExpectedSize != nil && (*spec.ExpectedSize < 0 || *spec.ExpectedSize > maxBytes) {
		return nil, fmt.Errorf("metadata %s declared invalid size %d", spec.Path, *spec.ExpectedSize)
	}
	checksums := make([]Checksum, 0, len(spec.Checksums))
	checksumHashers := make([]hash.Hash, 0, len(spec.Checksums))
	for _, checksum := range spec.Checksums {
		algorithm := strings.ToLower(strings.TrimSpace(checksum.Algorithm))
		digest := strings.ToLower(strings.TrimSpace(checksum.Digest))
		hasher, hashErr := checksumHash(algorithm)
		if hashErr != nil || len(digest) != hasher.Size()*2 {
			return nil, fmt.Errorf("metadata %s has invalid %s checksum", spec.Path, checksum.Algorithm)
		}
		if _, decodeErr := hex.DecodeString(digest); decodeErr != nil {
			return nil, fmt.Errorf("metadata %s has invalid %s checksum", spec.Path, checksum.Algorithm)
		}
		checksums = append(checksums, Checksum{Algorithm: algorithm, Digest: digest})
		checksumHashers = append(checksumHashers, hasher)
	}
	var objectKey string
	if len(checksums) > 0 {
		pathDigest := sha256.Sum256([]byte(spec.Path))
		objectKey = candidatePrefix(s.rootID, s.generation, s.candidateID) + "/objects/" + hex.EncodeToString(pathDigest[:]) + "/" + checksums[0].Algorithm + "/" + checksums[0].Digest
	}
	if objectKey != "" {
		if reader, openErr := s.handler.config.Store.OpenObject(ctx, s.handler.config.Tenant, objectKey); openErr == nil {
			writers := make([]io.Writer, 0, len(checksums)+1)
			internalDigest := sha256.New()
			writers = append(writers, internalDigest)
			for _, hasher := range checksumHashers {
				writers = append(writers, hasher)
			}
			size, hashErr := io.Copy(io.MultiWriter(writers...), reader)
			options := reader.Info().Options
			closeErr := reader.Close()
			valid := hashErr == nil && closeErr == nil && (spec.ExpectedSize == nil || size == *spec.ExpectedSize)
			for index, checksum := range checksums {
				valid = valid && hex.EncodeToString(checksumHashers[index].Sum(nil)) == checksum.Digest
			}
			if valid {
				var header http.Header
				_ = json.Unmarshal([]byte(options["header"]), &header)
				object := Object{Path: spec.Path, Key: objectKey, Size: size, SHA256: hex.EncodeToString(internalDigest.Sum(nil)), Header: header}
				recordObject(object)
				return &Blob{handler: s.handler, object: object}, nil
			}
			_ = s.handler.config.Store.DeleteObject(context.Background(), s.handler.config.Tenant, objectKey)
			for _, hasher := range checksumHashers {
				hasher.Reset()
			}
		}
	}
	response, err := s.handler.config.Fetch(ctx, fetchPath, nil)
	if err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	if fallbackFetchPath != "" && (response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusForbidden) {
		_ = response.Body.Close()
		response, err = s.handler.config.Fetch(ctx, fallbackFetchPath, nil)
		if err != nil {
			return nil, &retryableRefreshError{err: err}
		}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusForbidden {
		if !spec.AllowUnavailable {
			return nil, &retryableRefreshError{err: fmt.Errorf("required metadata %s returned %d", spec.Path, response.StatusCode)}
		}
		return nil, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, &retryableRefreshError{err: fmt.Errorf("metadata %s returned %d", spec.Path, response.StatusCode)}
	}
	policy := proxyruntime.ParseCachePolicy(response.Header, time.Now(), 0)
	if policy.NoStore || policy.Private {
		return nil, errUncacheableMetadata
	}
	writers := make([]io.Writer, 0, len(checksums))
	for _, hasher := range checksumHashers {
		writers = append(writers, hasher)
	}
	source := io.Reader(response.Body)
	if len(writers) > 0 {
		source = io.TeeReader(response.Body, io.MultiWriter(writers...))
	}
	expectedSize := response.ContentLength
	if spec.ExpectedSize != nil {
		expectedSize = *spec.ExpectedSize
	}
	spool, err := s.handler.config.Spooler.SpoolWithExpectedSize(ctx, source, maxBytes, expectedSize)
	if err != nil {
		if errors.Is(err, storeio.ErrObjectTooLarge) {
			return nil, err
		}
		return nil, &retryableRefreshError{err: err}
	}
	defer func() { _ = spool.Close() }()
	if spec.ExpectedSize != nil && spool.Size != *spec.ExpectedSize {
		return nil, &retryableRefreshError{err: fmt.Errorf("metadata %s size mismatch: got %d, want %d", spec.Path, spool.Size, *spec.ExpectedSize)}
	}
	for index, checksum := range checksums {
		if actual := hex.EncodeToString(checksumHashers[index].Sum(nil)); actual != checksum.Digest {
			return nil, &retryableRefreshError{err: fmt.Errorf("metadata %s %s mismatch", spec.Path, checksum.Algorithm)}
		}
	}
	if objectKey == "" {
		objectKey = candidatePrefix(s.rootID, s.generation, s.candidateID) + "/objects/sha256/" + spool.SHA256
	}
	if err := s.handler.config.Store.MkdirAll(s.handler.config.Tenant+"/"+path.Dir(objectKey), 0o755); err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	if _, err := spool.File.Seek(0, io.SeekStart); err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	encodedHeader, _ := json.Marshal(cloneHeader(response.Header))
	if _, err := s.handler.config.Store.Put(ctx, s.handler.config.Tenant, objectKey, spool.File, map[string]string{"header": string(encodedHeader)}); err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	object := Object{Path: spec.Path, Key: objectKey, Size: spool.Size, SHA256: spool.SHA256, Header: cloneHeader(response.Header)}
	recordObject(object)
	return &Blob{handler: s.handler, object: object}, nil
}

func (s *RefreshSession) RetainObject(objectPath string) error {
	cleaned, err := CleanPath(objectPath)
	if err != nil || !containsPath(s.root, cleaned) || cleaned == s.root || cleaned == "" {
		return fmt.Errorf("invalid retainable metadata path %q", objectPath)
	}
	object, exists := s.objects[cleaned]
	anchorKey := candidatePrefix(s.rootID, s.generation, s.candidateID) + "/anchor"
	if !exists || object.Key == "" || object.Key == anchorKey {
		return fmt.Errorf("retainable metadata %q is not a present closure object", objectPath)
	}
	object.Retainable = true
	s.objects[cleaned] = object
	return nil
}

type Blob struct {
	handler *GenerationManager
	object  Object
}

func (b *Blob) Open(ctx context.Context) (*blobfs.ObjectReader, error) {
	reader, err := b.handler.config.Store.OpenObject(ctx, b.handler.config.Tenant, b.object.Key)
	if err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	return reader, nil
}

func (b *Blob) Size() int64 { return b.object.Size }

func (a Anchor) Open(ctx context.Context) (*blobfs.ObjectReader, error) {
	return a.blob.Open(ctx)
}

func (a Anchor) Size() int64 { return a.blob.Size() }

func checksumHash(kind string) (hash.Hash, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "md5":
		return md5.New(), nil
	case "sha", "sha1":
		return sha1.New(), nil
	case "sha256":
		return sha256.New(), nil
	case "sha224":
		return sha256.New224(), nil
	case "sha384":
		return sha512.New384(), nil
	case "sha512":
		return sha512.New(), nil
	default:
		return nil, fmt.Errorf("unsupported metadata checksum %q", kind)
	}
}
