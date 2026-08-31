package deb

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

type releaseEntry struct {
	Path   string
	Size   int64
	SHA256 string
}

type releaseManifest struct {
	AcquireByHash bool
	Entries       []releaseEntry
}

func (inspector) ValidateSnapshot(_ context.Context, snapshot *filerepo.LiveSnapshot, open filerepo.SnapshotObjectOpener) error {
	validated := map[string]struct{}{}
	for _, rootTarget := range snapshot.Targets {
		if !isDistributionReleasePath(rootTarget.URL) {
			continue
		}
		release, ok := resolveManifestTarget(snapshot, rootTarget)
		if !ok || !release.Required || !isDistributionReleasePath(release.Path) {
			return fmt.Errorf("%s: required distribution Release is absent from manifest", rootTarget.URL)
		}
		if _, ok := validated[release.Path]; ok {
			continue
		}
		validated[release.Path] = struct{}{}
		reader, err := open(release.Path)
		if err != nil {
			return err
		}
		manifest, parseErr := parseReleaseManifest(reader)
		closeErr := reader.Close()
		if parseErr != nil {
			return parseErr
		}
		if closeErr != nil {
			return closeErr
		}
		for _, entry := range manifest.Entries {
			canonicalPath := releaseEntryPath(release.Path, entry.Path)
			metadata, ok := snapshot.Metadata[canonicalPath]
			if !ok || !metadata.Required {
				return fmt.Errorf("%s: required Release index is absent from manifest", canonicalPath)
			}
			if metadata.ChecksumType != "sha256" || !strings.EqualFold(metadata.Checksum, entry.SHA256) || metadata.Size != entry.Size {
				return fmt.Errorf("%s: Release size or checksum declaration differs from manifest", canonicalPath)
			}
			indexReader, err := open(canonicalPath)
			if err != nil {
				return err
			}
			verifyErr := verifyReleaseEntryReader(canonicalPath, entry, indexReader)
			closeErr := indexReader.Close()
			if verifyErr != nil {
				return verifyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if manifest.AcquireByHash {
				byHashPath := releaseByHashPath(canonicalPath, entry.SHA256)
				alias, ok := snapshot.Metadata[byHashPath]
				if !ok || alias.Path != metadata.Path || alias.Checksum != entry.SHA256 || alias.Size != entry.Size {
					return fmt.Errorf("%s: by-hash mapping is absent or inconsistent", byHashPath)
				}
			}
		}
	}
	return nil
}

func isDistributionReleasePath(cleanPath string) bool {
	base := path.Base(cleanPath)
	return (base == "Release" || base == "InRelease") &&
		(strings.HasPrefix(cleanPath, "dists/") || strings.Contains(cleanPath, "/dists/"))
}

func resolveManifestTarget(snapshot *filerepo.LiveSnapshot, target filerepo.MetadataTarget) (filerepo.MetadataObject, bool) {
	for _, candidate := range append([]string{target.URL}, target.Candidates...) {
		object, ok := snapshot.Metadata[candidate]
		if !ok || object.State != filerepo.MetadataPresent {
			continue
		}
		if object.Path != candidate {
			object, ok = snapshot.Metadata[object.Path]
		}
		return object, ok
	}
	return filerepo.MetadataObject{}, false
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession, paths *filerepo.PathIndexBuilder) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata: map[string]filerepo.MetadataObject{},
	}
	artifactCount := 0
	for _, target := range session.Targets() {
		var err error
		switch target.Kind {
		case "release":
			artifactCount, err = buildReleaseTarget(ctx, session, snapshot, target, paths, artifactCount)
		case "packages", "sources":
			artifactCount, err = buildFlatIndexTarget(ctx, session, snapshot, target, paths, artifactCount)
		default:
			return nil, fmt.Errorf("%s: unsupported seed metadata target kind %q", target.URL, target.Kind)
		}
		if err != nil {
			return nil, err
		}
	}
	snapshot.ArtifactCount = artifactCount
	return snapshot, nil
}

func buildReleaseTarget(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	target filerepo.MetadataTarget,
	paths *filerepo.PathIndexBuilder,
	artifactCount int,
) (int, error) {
	blob, absent, err := fetchFirstAvailable(ctx, session, target)
	if err != nil {
		return artifactCount, err
	}
	addExactMetadata(snapshot, blob.Path, true, 0, "")
	addNegativeMetadata(session, snapshot, absent)
	if err := addReleaseCompanions(ctx, session, snapshot, blob.Path); err != nil {
		session.Release(target)
		return artifactCount, err
	}
	manifest, err := parseReleaseBlob(blob)
	session.Release(target)
	if err != nil {
		return artifactCount, err
	}
	return fetchReleaseEntries(ctx, session, snapshot, blob.Path, manifest, paths, artifactCount)
}

func buildFlatIndexTarget(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	target filerepo.MetadataTarget,
	paths *filerepo.PathIndexBuilder,
	artifactCount int,
) (int, error) {
	rootPath := strings.Trim(strings.TrimSpace(path.Dir(target.URL)), "/")
	if rootPath == "." {
		rootPath = ""
	}
	releaseTarget := debFlatIndexTarget(rootPath, "release")
	releaseBlob, absentRelease, releaseErr := fetchFirstAvailable(ctx, session, releaseTarget)
	if releaseErr == nil {
		addExactMetadata(snapshot, releaseBlob.Path, false, 0, "")
		addNegativeMetadata(session, snapshot, absentRelease)
		if err := addReleaseCompanions(ctx, session, snapshot, releaseBlob.Path); err != nil {
			return artifactCount, err
		}
		manifest, err := parseReleaseBlob(releaseBlob)
		session.Release(releaseTarget)
		if err != nil {
			return artifactCount, err
		}
		return fetchReleaseEntries(ctx, session, snapshot, releaseBlob.Path, manifest, paths, artifactCount)
	}
	if !filerepo.IsMetadataAbsent(releaseErr) {
		return artifactCount, releaseErr
	}

	indexBlob, absent, err := fetchFirstAvailable(ctx, session, target)
	if err != nil {
		return artifactCount, err
	}
	defer session.Release(target)
	addExactMetadata(snapshot, indexBlob.Path, true, 0, "")
	addNegativeMetadata(session, snapshot, absent)
	return parseIndexBlob(indexBlob, target.Kind, paths, artifactCount)
}

func addReleaseCompanions(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	releasePath string,
) error {
	if path.Base(releasePath) != "Release" {
		return nil
	}
	for _, suffix := range []string{".gpg", ".sig", ".asc"} {
		companion, err := session.FetchOptionalAnchor(ctx, releasePath+suffix)
		if err != nil {
			return err
		}
		if companion.Path != "" {
			snapshot.Metadata[companion.Path] = companion
		}
	}
	return nil
}

func fetchFirstAvailable(ctx context.Context, session *filerepo.RefreshSession, target filerepo.MetadataTarget) (filerepo.MetadataBlob, []string, error) {
	var absent []string
	var lastErr error
	for _, candidate := range append([]string{target.URL}, target.Candidates...) {
		blob, err := session.FetchAnchor(ctx, filerepo.MetadataTarget{URL: candidate})
		if err == nil {
			return blob, absent, nil
		}
		if !filerepo.IsMetadataAbsent(err) {
			return filerepo.MetadataBlob{}, nil, err
		}
		lastErr = err
		absent = append(absent, candidate)
	}
	return filerepo.MetadataBlob{}, absent, lastErr
}

func addExactMetadata(snapshot *filerepo.LiveSnapshot, cleanPath string, required bool, size int64, sha256sum string) {
	sumType := ""
	if sha256sum != "" {
		sumType = "sha256"
	}
	snapshot.Metadata[cleanPath] = filerepo.MetadataObject{
		Path: cleanPath, Required: required, Size: size,
		ChecksumType: sumType, Checksum: sha256sum,
	}
}

func addNegativeMetadata(session *filerepo.RefreshSession, snapshot *filerepo.LiveSnapshot, cleanPaths []string) {
	for _, cleanPath := range cleanPaths {
		state := filerepo.MetadataNotFound
		if anchor, ok := session.Anchor(cleanPath); ok {
			state = anchor.State
		}
		snapshot.Metadata[cleanPath] = filerepo.MetadataObject{Path: cleanPath, State: state}
	}
}

func parseIndexBlob(
	blob filerepo.MetadataBlob,
	kind string,
	paths *filerepo.PathIndexBuilder,
	artifactCount int,
) (int, error) {
	blobReader, err := blob.Open()
	if err != nil {
		return artifactCount, err
	}
	reader, err := filerepo.OpenCompressed(blobReader, blob.Path)
	if err != nil {
		_ = blobReader.Close()
		return artifactCount, fmt.Errorf("%s: %w", blob.Path, err)
	}
	defer func() { _ = reader.Close() }()

	if kind == "packages" {
		count, err := parsePackages(reader, paths, artifactCount)
		if err != nil {
			return artifactCount, fmt.Errorf("%s: %w", blob.Path, err)
		}
		return count, nil
	}
	count, err := parseSources(reader, paths, artifactCount)
	if err != nil {
		return artifactCount, fmt.Errorf("%s: %w", blob.Path, err)
	}
	return count, nil
}

func fetchReleaseEntries(
	ctx context.Context,
	session *filerepo.RefreshSession,
	snapshot *filerepo.LiveSnapshot,
	releasePath string,
	manifest releaseManifest,
	paths *filerepo.PathIndexBuilder,
	artifactCount int,
) (int, error) {
	if len(manifest.Entries) == 0 {
		return artifactCount, fmt.Errorf("%s: Release SHA256 section contains no metadata", releasePath)
	}
	parseTargets, err := selectReleaseIndexes(manifest.Entries)
	if err != nil {
		return artifactCount, err
	}
	for _, entry := range manifest.Entries {
		canonicalPath := releaseEntryPath(releasePath, entry.Path)
		requestPath := canonicalPath
		byHashPath := ""
		if manifest.AcquireByHash {
			byHashPath = releaseByHashPath(canonicalPath, entry.SHA256)
			requestPath = byHashPath
		}
		_, err := session.FetchVerified(ctx, requestPath, canonicalPath, entry.Size, entry.SHA256)
		if err != nil && byHashPath != "" && filerepo.IsMetadataAbsent(err) {
			_, err = session.FetchVerified(ctx, canonicalPath, canonicalPath, entry.Size, entry.SHA256)
		}
		emptyDigest := sha256.Sum256(nil)
		if err != nil && filerepo.IsMetadataAbsent(err) && entry.Size == 0 &&
			strings.EqualFold(entry.SHA256, hex.EncodeToString(emptyDigest[:])) {
			_, err = session.MaterializeVerifiedEmpty(ctx, canonicalPath, entry.SHA256)
		}
		if err != nil {
			return artifactCount, err
		}
		addExactMetadata(snapshot, canonicalPath, true, entry.Size, entry.SHA256)
		if byHashPath != "" {
			snapshot.Metadata[byHashPath] = filerepo.MetadataObject{
				Path: canonicalPath, Size: entry.Size,
				ChecksumType: "sha256", Checksum: entry.SHA256,
			}
		}
		session.Release(filerepo.MetadataTarget{URL: canonicalPath})
	}
	for _, target := range parseTargets {
		canonicalPath := releaseEntryPath(releasePath, target.entry.Path)
		blob, err := session.FetchVerified(ctx, canonicalPath, canonicalPath, target.entry.Size, target.entry.SHA256)
		if err != nil {
			return artifactCount, err
		}
		artifactCount, err = parseIndexBlob(blob, target.kind, paths, artifactCount)
		session.Release(filerepo.MetadataTarget{URL: canonicalPath})
		if err != nil {
			return artifactCount, err
		}
	}
	return artifactCount, nil
}

type releaseParseTarget struct {
	entry    releaseEntry
	kind     string
	priority int
}

func selectReleaseIndexes(entries []releaseEntry) ([]releaseParseTarget, error) {
	selected := map[string]releaseParseTarget{}
	unsupported := map[string]string{}
	compressions := []struct {
		suffix   string
		priority int
	}{
		{suffix: ".xz", priority: 1},
		{suffix: ".zst", priority: 2},
		{suffix: ".zstd", priority: 2},
		{suffix: ".gz", priority: 3},
		{suffix: ".bz2"},
		{suffix: ".lz4"},
	}
	for _, entry := range entries {
		logicalPath := entry.Path
		priority := 4
		for _, compression := range compressions {
			if strings.HasSuffix(logicalPath, compression.suffix) {
				logicalPath = strings.TrimSuffix(logicalPath, compression.suffix)
				priority = compression.priority
				break
			}
		}
		var kind string
		switch path.Base(logicalPath) {
		case "Packages":
			kind = "packages"
		case "Sources":
			kind = "sources"
		default:
			continue
		}
		if priority == 0 {
			unsupported[logicalPath] = entry.Path
			continue
		}
		current, exists := selected[logicalPath]
		if !exists || priority < current.priority {
			selected[logicalPath] = releaseParseTarget{entry: entry, kind: kind, priority: priority}
		}
	}
	for logicalPath, sourcePath := range unsupported {
		if _, ok := selected[logicalPath]; !ok {
			return nil, fmt.Errorf("%s: no supported representation for cleanup index (%s)", logicalPath, sourcePath)
		}
	}
	result := make([]releaseParseTarget, 0, len(selected))
	for _, target := range selected {
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].entry.Path < result[j].entry.Path })
	return result, nil
}

func releaseEntryPath(releasePath, entryPath string) string {
	prefix := strings.TrimSuffix(releasePath, "/InRelease")
	prefix = strings.TrimSuffix(prefix, "/Release")
	return path.Join(prefix, entryPath)
}

func releaseByHashPath(canonicalPath, digest string) string {
	return path.Join(path.Dir(canonicalPath), "by-hash", "SHA256", strings.ToLower(digest))
}

func parsePackages(input io.Reader, paths *filerepo.PathIndexBuilder, count int) (int, error) {
	return parseDebStanzas(input, count, func(fields map[string]string) int {
		filename := strings.TrimSpace(fields["Filename"])
		if filename == "" {
			return 0
		}
		paths.Add(filename)
		for _, suffix := range debPackageSidecarSuffixes {
			paths.Add(filename + suffix)
		}
		return 1
	})
}

func parseSources(input io.Reader, paths *filerepo.PathIndexBuilder, count int) (int, error) {
	return parseDebStanzas(input, count, func(fields map[string]string) int {
		directory := strings.TrimSpace(fields["Directory"])
		if directory == "" {
			return 0
		}
		added := 0
		for _, line := range strings.Split(fields["Checksums-Sha256"], "\n") {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			artifactPath := path.Join(directory, parts[2])
			paths.Add(artifactPath)
			for _, suffix := range debPackageSidecarSuffixes {
				paths.Add(artifactPath + suffix)
			}
			added++
		}
		return added
	})
}

func parseReleaseBlob(blob filerepo.MetadataBlob) (releaseManifest, error) {
	reader, err := blob.Open()
	if err != nil {
		return releaseManifest{}, err
	}
	defer func() { _ = reader.Close() }()
	return parseReleaseManifest(reader)
}

func parseReleaseManifest(reader io.Reader) (releaseManifest, error) {
	var manifest releaseManifest
	entries := map[string]releaseEntry{}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, 10<<20)
	inSHA256 := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.EqualFold(strings.TrimSpace(line), "Acquire-By-Hash: yes") {
			manifest.AcquireByHash = true
		}
		if strings.HasPrefix(line, "SHA256:") {
			inSHA256 = true
			continue
		}
		if inSHA256 && !strings.HasPrefix(line, " ") {
			break
		}
		if !inSHA256 {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			continue
		}
		digest := strings.ToLower(parts[0])
		size, err := strconv.ParseInt(parts[1], 10, 64)
		cleanPath := strings.TrimPrefix(path.Clean("/"+parts[2]), "/")
		if err != nil || size < 0 || len(digest) != sha256.Size*2 || !httpcache.SafePath(cleanPath) {
			return releaseManifest{}, fmt.Errorf("invalid Release SHA256 entry %q", line)
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return releaseManifest{}, fmt.Errorf("invalid Release SHA256 digest for %s", cleanPath)
		}
		entry := releaseEntry{Path: cleanPath, Size: size, SHA256: digest}
		if existing, ok := entries[cleanPath]; ok && existing != entry {
			return releaseManifest{}, fmt.Errorf("conflicting Release SHA256 entries for %s", cleanPath)
		}
		entries[cleanPath] = entry
	}
	if err := scanner.Err(); err != nil {
		return releaseManifest{}, err
	}
	manifest.Entries = make([]releaseEntry, 0, len(entries))
	for _, entry := range entries {
		manifest.Entries = append(manifest.Entries, entry)
	}
	sort.Slice(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].Path < manifest.Entries[j].Path })
	return manifest, nil
}

func verifyReleaseEntryReader(cleanPath string, entry releaseEntry, reader io.Reader) error {
	sum := sha256.New()
	size, err := io.Copy(sum, reader)
	if err != nil {
		return err
	}
	actual := hex.EncodeToString(sum.Sum(nil))
	if size != entry.Size {
		return fmt.Errorf("%s: Release size mismatch: got %d, want %d", cleanPath, size, entry.Size)
	}
	if !strings.EqualFold(entry.SHA256, actual) {
		return fmt.Errorf("%s: Release SHA256 mismatch", cleanPath)
	}
	return nil
}

func parseDebStanzas(input io.Reader, count int, apply func(map[string]string) int) (int, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(nil, 10<<20)
	fields := map[string]string{}
	currentKey := ""
	flush := func() {
		if len(fields) == 0 {
			return
		}
		count += apply(fields)
		for key := range fields {
			delete(fields, key)
		}
		currentKey = ""
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			if currentKey != "" {
				fields[currentKey] += "\n" + strings.TrimSpace(line)
			}
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		currentKey = key
		fields[key] = strings.TrimSpace(value)
	}
	flush()
	return count, scanner.Err()
}
