package deb

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type releaseEntry struct {
	Path   string
	Size   int64
	SHA256 string
	SHA512 string
}

type releaseManifest struct {
	AcquireByHash bool
	Entries       []releaseEntry
	Fields        map[string]string
}

func parseReleaseManifest(ctx context.Context, reader io.Reader) (releaseManifest, error) {
	if err := ctx.Err(); err != nil {
		return releaseManifest{}, err
	}
	manifest := releaseManifest{Fields: make(map[string]string)}
	entries := make(map[string]releaseEntry)
	strongEntries := 0
	limited := &io.LimitedReader{R: reader, N: maxReleaseSize + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(nil, maxReleaseSize)
	section := ""
	currentField := ""
	clearsigned := false
	payload := true
	signatureStarted := false
	signatureEnded := false
	lines := 0
	for scanner.Scan() {
		lines++
		if lines&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return releaseManifest{}, err
			}
		}
		line := scanner.Text()
		if line == "-----BEGIN PGP SIGNED MESSAGE-----" {
			if lines != 1 {
				return releaseManifest{}, fmt.Errorf("invalid clearsigned Release preamble")
			}
			clearsigned = true
			payload = false
			continue
		}
		if signatureStarted {
			if line == "-----END PGP SIGNATURE-----" {
				signatureEnded = true
			}
			continue
		}
		if signatureEnded {
			continue
		}
		if clearsigned && !payload {
			if line == "" {
				payload = true
			}
			continue
		}
		if clearsigned && line == "-----BEGIN PGP SIGNATURE-----" {
			signatureStarted = true
			continue
		}
		if clearsigned && strings.HasPrefix(line, "- ") {
			line = strings.TrimPrefix(line, "- ")
		}
		if strings.EqualFold(strings.TrimSpace(line), "Acquire-By-Hash: yes") {
			manifest.AcquireByHash = true
		}
		trimmed := strings.TrimSpace(line)
		if line == trimmed {
			section = ""
			currentField = ""
			switch trimmed {
			case "SHA256:", "SHA512:":
				section = strings.TrimSuffix(trimmed, ":")
				continue
			}
			if key, value, ok := strings.Cut(line, ":"); ok {
				key = strings.ToLower(strings.TrimSpace(key))
				if key == "" || len(manifest.Fields) >= 512 {
					return releaseManifest{}, fmt.Errorf("invalid or excessive Release control fields")
				}
				if _, duplicate := manifest.Fields[key]; duplicate {
					return releaseManifest{}, fmt.Errorf("duplicate Release field %q", key)
				}
				manifest.Fields[key] = strings.TrimSpace(value)
				currentField = key
			}
			continue
		}
		if section == "" {
			if currentField != "" && (strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")) {
				manifest.Fields[currentField] += "\n" + strings.TrimSpace(line)
			}
			continue
		}
		if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			section = ""
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return releaseManifest{}, fmt.Errorf("invalid Release %s entry %q", section, line)
		}
		digest := strings.ToLower(fields[0])
		size, sizeErr := strconv.ParseInt(fields[1], 10, 64)
		entryPath := fields[2]
		_, pathErr := storeio.CleanRelative(entryPath)
		digestBytes := sha256.Size
		if section == "SHA512" {
			digestBytes = sha512.Size
		}
		if sizeErr != nil || size < 0 || len(digest) != digestBytes*2 || pathErr != nil {
			return releaseManifest{}, fmt.Errorf("invalid Release %s entry %q", section, line)
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return releaseManifest{}, fmt.Errorf("invalid Release %s digest for %s", section, entryPath)
		}
		entry := entries[entryPath]
		if entry.Path != "" && entry.Size != size {
			return releaseManifest{}, fmt.Errorf("conflicting Release sizes for %s", entryPath)
		}
		entry.Path, entry.Size = entryPath, size
		if section == "SHA256" {
			if entry.SHA256 != "" && entry.SHA256 != digest {
				return releaseManifest{}, fmt.Errorf("conflicting Release SHA256 entries for %s", entryPath)
			}
			entry.SHA256 = digest
		} else {
			if entry.SHA512 != "" && entry.SHA512 != digest {
				return releaseManifest{}, fmt.Errorf("conflicting Release SHA512 entries for %s", entryPath)
			}
			entry.SHA512 = digest
		}
		strongEntries++
		if strongEntries > 200000 {
			return releaseManifest{}, fmt.Errorf("release contains too many strong checksum entries")
		}
		entries[entryPath] = entry
	}
	if err := scanner.Err(); err != nil {
		return releaseManifest{}, err
	}
	if limited.N == 0 {
		return releaseManifest{}, fmt.Errorf("release exceeds %d bytes", maxReleaseSize)
	}
	if clearsigned && (!signatureStarted || !signatureEnded) {
		return releaseManifest{}, fmt.Errorf("incomplete clearsigned Release signature")
	}
	if strongEntries == 0 {
		return releaseManifest{}, fmt.Errorf("release contains no strong checksum entries")
	}
	manifest.Entries = make([]releaseEntry, 0, len(entries))
	for _, entry := range entries {
		manifest.Entries = append(manifest.Entries, entry)
	}
	sort.Slice(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].Path < manifest.Entries[j].Path })
	return manifest, nil
}

func releaseManifestsEqual(left, right releaseManifest) bool {
	return left.AcquireByHash == right.AcquireByHash && reflect.DeepEqual(left.Entries, right.Entries) && reflect.DeepEqual(left.Fields, right.Fields)
}

func releaseByHashPath(canonicalPath, algorithm, digest string) string {
	return path.Join(path.Dir(canonicalPath), "by-hash", algorithm, strings.ToLower(digest))
}
