package deb

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
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

func parseReleaseManifest(reader io.Reader) (releaseManifest, error) {
	var manifest releaseManifest
	entries := make(map[string]releaseEntry)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, maxReleaseSize)
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
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		digest := strings.ToLower(fields[0])
		size, sizeErr := strconv.ParseInt(fields[1], 10, 64)
		entryPath := strings.TrimPrefix(path.Clean("/"+fields[2]), "/")
		if sizeErr != nil || size < 0 || len(digest) != sha256.Size*2 || !httpcache.SafePath(entryPath) {
			return releaseManifest{}, fmt.Errorf("invalid Release SHA256 entry %q", line)
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return releaseManifest{}, fmt.Errorf("invalid Release SHA256 digest for %s", entryPath)
		}
		entry := releaseEntry{Path: entryPath, Size: size, SHA256: digest}
		if previous, exists := entries[entryPath]; exists && previous != entry {
			return releaseManifest{}, fmt.Errorf("conflicting Release SHA256 entries for %s", entryPath)
		}
		entries[entryPath] = entry
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

func releaseByHashPath(canonicalPath, digest string) string {
	return path.Join(path.Dir(canonicalPath), "by-hash", "SHA256", strings.ToLower(digest))
}

func verifyReleaseEntryReader(cleanPath string, entry releaseEntry, reader io.Reader) error {
	sum := sha256.New()
	size, err := io.Copy(sum, reader)
	if err != nil {
		return err
	}
	if size != entry.Size {
		return fmt.Errorf("%s: Release size mismatch: got %d, want %d", cleanPath, size, entry.Size)
	}
	if actual := hex.EncodeToString(sum.Sum(nil)); !strings.EqualFold(entry.SHA256, actual) {
		return fmt.Errorf("%s: Release SHA256 mismatch", cleanPath)
	}
	return nil
}
