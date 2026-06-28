package deb

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/repo/filerepo"
)

type rootSpec struct {
	Suite         string
	Components    []string
	Architectures []string
	Source        bool
}

func (s *rootSpec) Key() string { return s.Suite }

func (s *rootSpec) Targets() []filerepo.MetadataTarget {
	targets := []filerepo.MetadataTarget{{
		URL:        path.Join("dists", s.Suite, "InRelease"),
		Candidates: []string{path.Join("dists", s.Suite, "Release")},
		Kind:       "release",
	}}
	for _, component := range s.Components {
		for _, arch := range s.Architectures {
			basePath := path.Join("dists", s.Suite, component, "binary-"+arch, "Packages")
			targets = append(targets, filerepo.MetadataTarget{
				URL:        basePath + ".xz",
				Candidates: []string{basePath + ".gz", basePath},
				Kind:       "packages",
			})
		}
		if s.Source {
			basePath := path.Join("dists", s.Suite, component, "source", "Sources")
			targets = append(targets, filerepo.MetadataTarget{
				URL:        basePath + ".xz",
				Candidates: []string{basePath + ".gz", basePath},
				Kind:       "sources",
			})
		}
	}
	return targets
}

func (s *rootSpec) Merge(other filerepo.RootSpec) bool {
	candidate, ok := other.(*rootSpec)
	if !ok || s.Suite != candidate.Suite {
		return false
	}
	changed := false
	for _, component := range candidate.Components {
		if !slices.Contains(s.Components, component) {
			s.Components = append(s.Components, component)
			changed = true
		}
	}
	for _, arch := range candidate.Architectures {
		if !slices.Contains(s.Architectures, arch) {
			s.Architectures = append(s.Architectures, arch)
			changed = true
		}
	}
	if candidate.Source && !s.Source {
		s.Source = true
		changed = true
	}
	return changed
}

type discoverer struct{}

func (discoverer) Discover(cleanPath string) (filerepo.RootSpec, bool) {
	trimmed := strings.Trim(strings.TrimSpace(cleanPath), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 || parts[0] != "dists" {
		return nil, false
	}
	suite := parts[1]
	if suite == "" {
		return nil, false
	}
	if len(parts) == 3 && (parts[2] == "InRelease" || parts[2] == "Release") {
		return &rootSpec{Suite: suite}, true
	}
	if len(parts) < 5 {
		return nil, false
	}
	component := parts[2]
	segment := parts[3]
	fileName := parts[4]
	switch {
	case strings.HasPrefix(segment, "binary-") && strings.HasPrefix(fileName, "Packages"):
		arch := strings.TrimPrefix(segment, "binary-")
		if component == "" || arch == "" {
			return nil, false
		}
		return &rootSpec{Suite: suite, Components: []string{component}, Architectures: []string{arch}}, true
	case segment == "source" && strings.HasPrefix(fileName, "Sources"):
		if component == "" {
			return nil, false
		}
		return &rootSpec{Suite: suite, Components: []string{component}, Source: true}, true
	default:
		return nil, false
	}
}

func buildSnapshot(ctx context.Context, session *filerepo.RefreshSession) (*filerepo.LiveSnapshot, error) {
	snapshot := &filerepo.LiveSnapshot{
		Metadata:  map[string]filerepo.MetadataObject{},
		Artifacts: map[string]filerepo.RepoObject{},
		Auxiliary: map[string]filerepo.RepoObject{},
	}
	releaseSums := map[string]string{}
	for _, target := range session.Targets() {
		blob, err := session.Fetch(ctx, target)
		if err != nil {
			return nil, err
		}
		snapshot.Metadata[blob.Path] = filerepo.MetadataObject{Path: blob.Path, Required: true}
		if strings.HasSuffix(blob.Path, "/Release") {
			gpgPath := blob.Path + ".gpg"
			if _, err := session.Fetch(ctx, filerepo.MetadataTarget{URL: gpgPath}); err != nil {
				return nil, err
			}
			snapshot.Metadata[gpgPath] = filerepo.MetadataObject{Path: gpgPath, Required: true}
		}
		switch target.Kind {
		case "release":
			releaseSums = parseReleaseSHA256(blob.Body)
			continue
		case "packages", "sources":
			if err := verifyReleaseChecksum(releaseSums, blob.Path, blob.Body); err != nil {
				return nil, err
			}
			reader, err := filerepo.OpenCompressed(blob.Body, blob.Path)
			if err != nil {
				return nil, err
			}
			if target.Kind == "packages" {
				err = parsePackages(reader, snapshot)
			} else {
				err = parseSources(reader, snapshot)
			}
			_ = reader.Close()
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%s: unsupported metadata target kind %q", blob.Path, target.Kind)
		}
	}
	return snapshot, nil
}

func parsePackages(input io.Reader, snapshot *filerepo.LiveSnapshot) error {
	return parseDebStanzas(input, func(fields map[string]string) {
		filename := strings.TrimSpace(fields["Filename"])
		if filename == "" {
			return
		}
		checksum := strings.TrimSpace(fields["SHA256"])
		snapshot.Artifacts[filename] = filerepo.RepoObject{Path: filename, Identity: checksum}
		for _, suffix := range []string{".sha256", ".sha512", ".md5sum"} {
			auxPath := filename + suffix
			snapshot.Auxiliary[auxPath] = filerepo.RepoObject{Path: auxPath, Identity: checksum}
		}
	})
}

func parseSources(input io.Reader, snapshot *filerepo.LiveSnapshot) error {
	return parseDebStanzas(input, func(fields map[string]string) {
		directory := strings.TrimSpace(fields["Directory"])
		if directory == "" {
			return
		}
		for _, line := range strings.Split(fields["Checksums-Sha256"], "\n") {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			artifactPath := path.Join(directory, parts[2])
			snapshot.Artifacts[artifactPath] = filerepo.RepoObject{Path: artifactPath, Identity: parts[0]}
			for _, suffix := range []string{".sha256", ".sha512", ".md5sum"} {
				auxPath := artifactPath + suffix
				snapshot.Auxiliary[auxPath] = filerepo.RepoObject{Path: auxPath, Identity: parts[0]}
			}
		}
	})
}

func parseReleaseSHA256(body []byte) map[string]string {
	result := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	inSHA256 := false
	for scanner.Scan() {
		line := scanner.Text()
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
		if len(parts) >= 3 {
			result[parts[2]] = parts[0]
		}
	}
	return result
}

func verifyReleaseChecksum(sums map[string]string, cleanPath string, body []byte) error {
	if len(sums) == 0 {
		return fmt.Errorf("%s: Release SHA256 section is missing", cleanPath)
	}
	trimmed := strings.TrimPrefix(cleanPath, "dists/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 2 {
		trimmed = parts[1]
	}
	expected := sums[trimmed]
	if expected == "" {
		return fmt.Errorf("%s: missing Release SHA256", cleanPath)
	}
	sum := sha256.Sum256(body)
	actual := hex.EncodeToString(sum[:])
	if !strings.EqualFold(expected, actual) {
		return fmt.Errorf("%s: Release SHA256 mismatch", cleanPath)
	}
	return nil
}

func parseDebStanzas(input io.Reader, apply func(map[string]string)) error {
	scanner := bufio.NewScanner(input)
	fields := map[string]string{}
	currentKey := ""
	flush := func() {
		if len(fields) == 0 {
			return
		}
		apply(fields)
		fields = map[string]string{}
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
	return scanner.Err()
}
